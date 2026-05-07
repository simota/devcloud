package dynamodb

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

func splitDisjunctivePredicates(expression string) ([]string, error) {
	fields := strings.Fields(expression)
	if len(fields) == 0 {
		return nil, errors.New("empty expression")
	}
	parts := []string{}
	var current []string
	for _, field := range fields {
		if strings.ToUpper(field) == "OR" {
			if len(current) == 0 {
				return nil, errors.New("invalid OR expression")
			}
			parts = append(parts, strings.Join(current, " "))
			current = nil
			continue
		}
		current = append(current, field)
	}
	if len(current) == 0 {
		return nil, errors.New("invalid OR expression")
	}
	parts = append(parts, strings.Join(current, " "))
	return parts, nil
}

func splitConjunctivePredicates(expression string) ([]string, error) {
	fields := strings.Fields(expression)
	if len(fields) == 0 {
		return nil, errors.New("empty expression")
	}
	parts := []string{}
	var current []string
	betweenNeedsAnd := false
	for _, field := range fields {
		upper := strings.ToUpper(field)
		if upper == "BETWEEN" {
			betweenNeedsAnd = true
			current = append(current, field)
			continue
		}
		if upper == "AND" && !betweenNeedsAnd {
			if len(current) == 0 {
				return nil, errors.New("invalid AND expression")
			}
			parts = append(parts, strings.Join(current, " "))
			current = nil
			continue
		}
		if upper == "AND" && betweenNeedsAnd {
			betweenNeedsAnd = false
		}
		current = append(current, field)
	}
	if len(current) == 0 {
		return nil, errors.New("invalid AND expression")
	}
	parts = append(parts, strings.Join(current, " "))
	return parts, nil
}

func splitBetweenExpression(expression string) (attr string, lower string, upper string, ok bool) {
	fields := strings.Fields(expression)
	if len(fields) != 5 || strings.ToUpper(fields[1]) != "BETWEEN" || strings.ToUpper(fields[3]) != "AND" {
		return "", "", "", false
	}
	return fields[0], fields[2], fields[4], true
}

func splitInExpression(expression string) (attr string, values []string, ok bool) {
	left, right, found := strings.Cut(expression, " IN ")
	if !found {
		left, right, found = strings.Cut(expression, " in ")
	}
	if !found {
		return "", nil, false
	}
	right = strings.TrimSpace(right)
	if !strings.HasPrefix(right, "(") || !strings.HasSuffix(right, ")") {
		return "", nil, false
	}
	return strings.TrimSpace(left), splitCommaSeparated(strings.TrimSuffix(strings.TrimPrefix(right, "("), ")")), true
}

func splitComparisonExpression(expression string) (left string, operator string, right string, ok bool) {
	for _, op := range []string{"<=", ">=", "<>", "=", "<", ">"} {
		if left, right, ok := strings.Cut(expression, op); ok {
			return left, op, right, true
		}
	}
	return "", "", "", false
}

func splitArithmeticUpdateExpression(expression string) (left string, operator string, right string, ok bool) {
	depth := 0
	for index, char := range expression {
		switch char {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case '+', '-':
			if depth == 0 {
				return strings.TrimSpace(expression[:index]), string(char), strings.TrimSpace(expression[index+1:]), true
			}
		}
	}
	return "", "", "", false
}

func splitCommaSeparated(value string) []string {
	parts := []string{}
	depth := 0
	start := 0
	for index, char := range value {
		switch char {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(value[start:index]))
				start = index + 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(value[start:]))
	return parts
}

type updateClause struct {
	keyword string
	body    string
}

func splitUpdateClauses(expression string) ([]updateClause, error) {
	type clauseStart struct {
		keyword string
		index   int
	}
	upper := strings.ToUpper(expression)
	starts := []clauseStart{}
	for _, keyword := range []string{"SET", "REMOVE", "ADD", "DELETE"} {
		for offset := 0; offset < len(upper); {
			index := strings.Index(upper[offset:], keyword)
			if index < 0 {
				break
			}
			absolute := offset + index
			if isUpdateClauseBoundary(upper, absolute, len(keyword)) {
				starts = append(starts, clauseStart{keyword: keyword, index: absolute})
			}
			offset = absolute + len(keyword)
		}
	}
	if len(starts) == 0 {
		return nil, errors.New("update expression must include SET, REMOVE, ADD, or DELETE")
	}
	sort.Slice(starts, func(i, j int) bool {
		return starts[i].index < starts[j].index
	})
	clauses := make([]updateClause, 0, len(starts))
	for i, start := range starts {
		next := len(expression)
		if i+1 < len(starts) {
			next = starts[i+1].index
		}
		body := strings.TrimSpace(expression[start.index+len(start.keyword) : next])
		if body == "" {
			return nil, fmt.Errorf("%s update expression clause is empty", start.keyword)
		}
		clauses = append(clauses, updateClause{keyword: start.keyword, body: body})
	}
	return clauses, nil
}

func isUpdateClauseBoundary(expression string, index int, keywordLength int) bool {
	beforeOK := index == 0 || expression[index-1] == ' '
	after := index + keywordLength
	afterOK := after < len(expression) && expression[after] == ' '
	return beforeOK && afterOK
}

func resolveAttributeName(token string, names map[string]string) string {
	if strings.HasPrefix(token, "#") {
		if value, ok := names[token]; ok {
			return value
		}
	}
	return token
}
