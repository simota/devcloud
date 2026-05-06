package redshift

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type extendedQuerySession struct {
	statements map[string]extendedPreparedStatement
	portals    map[string]extendedPortal
	failed     bool
}

type extendedPreparedStatement struct {
	statement     string
	parameterOIDs []int32
}

type extendedPortal struct {
	statementName       string
	executableStatement string
	executed            bool
	result              queryResult
	nextRow             int
}

type extendedBindParameter struct {
	value string
	null  bool
}

func newExtendedQuerySession() *extendedQuerySession {
	return &extendedQuerySession{
		statements: map[string]extendedPreparedStatement{},
		portals:    map[string]extendedPortal{},
	}
}

func (e *extendedQuerySession) handleParse(w io.Writer, payload []byte) {
	reader := bytes.NewReader(payload)
	name, ok := readCStringFromReader(reader)
	if !ok {
		e.protocolError(w)
		return
	}
	statement, ok := readCStringFromReader(reader)
	if !ok {
		e.protocolError(w)
		return
	}
	parameterCount, ok := readInt16FromReader(reader)
	if !ok {
		e.protocolError(w)
		return
	}
	if parameterCount < 0 {
		e.protocolError(w)
		return
	}
	parameterOIDs := make([]int32, 0, parameterCount)
	for i := 0; i < int(parameterCount); i++ {
		oid, ok := readInt32FromReader(reader)
		if !ok {
			e.protocolError(w)
			return
		}
		parameterOIDs = append(parameterOIDs, oid)
	}
	e.statements[name] = extendedPreparedStatement{statement: statement, parameterOIDs: parameterOIDs}
	writeMessage(w, '1', nil)
}

func (e *extendedQuerySession) handleBind(w io.Writer, payload []byte) {
	reader := bytes.NewReader(payload)
	portalName, ok := readCStringFromReader(reader)
	if !ok {
		e.protocolError(w)
		return
	}
	statementName, ok := readCStringFromReader(reader)
	if !ok {
		e.protocolError(w)
		return
	}
	prepared, ok := e.statements[statementName]
	if !ok {
		e.failed = true
		writeErrorResponse(w, "26000", "prepared statement does not exist")
		return
	}
	formatCodeCount, ok := readInt16FromReader(reader)
	if !ok {
		e.protocolError(w)
		return
	}
	formatCodes, ok := readInt16Values(reader, int(formatCodeCount))
	if !ok {
		e.protocolError(w)
		return
	}
	parameterCount, ok := readInt16FromReader(reader)
	if !ok {
		e.protocolError(w)
		return
	}
	if parameterCount < 0 {
		e.protocolError(w)
		return
	}
	if len(prepared.parameterOIDs) > 0 && int(parameterCount) != len(prepared.parameterOIDs) {
		e.failed = true
		writeErrorResponse(w, "08P01", "bind parameter count does not match prepared statement")
		return
	}
	parameters := make([]extendedBindParameter, 0, parameterCount)
	for i := 0; i < int(parameterCount); i++ {
		formatCode := int16(0)
		switch len(formatCodes) {
		case 0:
		case 1:
			formatCode = formatCodes[0]
		default:
			if i >= len(formatCodes) {
				e.protocolError(w)
				return
			}
			formatCode = formatCodes[i]
		}
		if formatCode != 0 {
			e.failed = true
			writeErrorResponse(w, "0A000", "binary extended query bind parameters are not supported in the local Redshift compatibility layer")
			return
		}
		valueLength, ok := readInt32FromReader(reader)
		if !ok {
			e.protocolError(w)
			return
		}
		if valueLength < -1 {
			e.protocolError(w)
			return
		}
		if valueLength == -1 {
			parameters = append(parameters, extendedBindParameter{null: true})
			continue
		}
		if int64(valueLength) > int64(reader.Len()) {
			e.protocolError(w)
			return
		}
		value := make([]byte, int(valueLength))
		if _, err := io.ReadFull(reader, value); err != nil {
			e.protocolError(w)
			return
		}
		parameters = append(parameters, extendedBindParameter{value: string(value)})
	}
	executableStatement, err := applyExtendedBindParameters(prepared.statement, parameters)
	if err != nil {
		e.failed = true
		writeErrorResponse(w, "08P01", err.Error())
		return
	}
	resultFormatCount, ok := readInt16FromReader(reader)
	if !ok {
		e.protocolError(w)
		return
	}
	resultFormatCodes, ok := readInt16Values(reader, int(resultFormatCount))
	if !ok {
		e.protocolError(w)
		return
	}
	for _, formatCode := range resultFormatCodes {
		if formatCode != 0 {
			e.failed = true
			writeErrorResponse(w, "0A000", "binary extended query result formats are not supported in the local Redshift compatibility layer")
			return
		}
	}
	e.portals[portalName] = extendedPortal{statementName: statementName, executableStatement: executableStatement}
	writeMessage(w, '2', nil)
}

func (e *extendedQuerySession) handleDescribe(s *Server, w io.Writer, payload []byte) {
	targetType, name, ok := parseDescribeOrClosePayload(payload)
	if !ok {
		e.protocolError(w)
		return
	}
	statement, ok := e.statementForTarget(targetType, name)
	if !ok {
		e.failed = true
		writeErrorResponse(w, "26000", "prepared statement or portal does not exist")
		return
	}
	if targetType == 'S' {
		writeParameterDescription(w, e.statements[name].parameterOIDs)
	}
	result, ok := s.describeExtendedQuery(statement)
	if !ok || len(result.fields) == 0 {
		writeMessage(w, 'n', nil)
		return
	}
	writeRowDescription(w, result.fields)
}

func (e *extendedQuerySession) handleExecute(s *Server, w io.Writer, payload []byte) {
	reader := bytes.NewReader(payload)
	portalName, ok := readCStringFromReader(reader)
	if !ok {
		e.protocolError(w)
		return
	}
	maxRows, ok := readInt32FromReader(reader)
	if !ok {
		e.protocolError(w)
		return
	}
	if maxRows < 0 {
		e.protocolError(w)
		return
	}
	portal, ok := e.portals[portalName]
	if !ok {
		e.failed = true
		writeErrorResponse(w, "26000", "portal does not exist")
		return
	}
	prepared := e.statements[portal.statementName]
	if !portal.executed {
		result, err := s.executeSQL(portal.executableStatement)
		s.recordSQLHistory(prepared.statement, result, err)
		if err != nil {
			e.failed = true
			writeErrorResponse(w, "0A000", err.Error())
			return
		}
		portal.result = result
		portal.executed = true
	}
	rowsToWrite := portal.result.rows[portal.nextRow:]
	if maxRows > 0 && int(maxRows) < len(rowsToWrite) {
		rowsToWrite = rowsToWrite[:maxRows]
	}
	for _, row := range rowsToWrite {
		writeDataRow(w, row)
	}
	portal.nextRow += len(rowsToWrite)
	if portal.nextRow < len(portal.result.rows) {
		e.portals[portalName] = portal
		writeMessage(w, 's', nil)
		return
	}
	e.portals[portalName] = portal
	writeCommandComplete(w, portal.result.tag)
}

func (e *extendedQuerySession) handleClose(w io.Writer, payload []byte) {
	targetType, name, ok := parseDescribeOrClosePayload(payload)
	if !ok {
		e.protocolError(w)
		return
	}
	switch targetType {
	case 'S':
		delete(e.statements, name)
	case 'P':
		delete(e.portals, name)
	}
	writeMessage(w, '3', nil)
}

func (e *extendedQuerySession) handleSync(_ io.Writer) {
	e.failed = false
}

func (e *extendedQuerySession) statementForTarget(targetType byte, name string) (string, bool) {
	switch targetType {
	case 'S':
		statement, ok := e.statements[name]
		return statement.statement, ok
	case 'P':
		portal, ok := e.portals[name]
		if !ok {
			return "", false
		}
		if _, ok := e.statements[portal.statementName]; !ok {
			return "", false
		}
		return portal.executableStatement, true
	default:
		return "", false
	}
}

func (e *extendedQuerySession) protocolError(w io.Writer) {
	e.failed = true
	writeErrorResponse(w, "08P01", "invalid PostgreSQL extended query message")
}

func applyExtendedBindParameters(statement string, parameters []extendedBindParameter) (string, error) {
	if len(parameters) == 0 {
		return statement, nil
	}
	var builder strings.Builder
	inString := false
	for i := 0; i < len(statement); i++ {
		ch := statement[i]
		if ch == '\'' {
			builder.WriteByte(ch)
			if inString && i+1 < len(statement) && statement[i+1] == '\'' {
				i++
				builder.WriteByte(statement[i])
				continue
			}
			inString = !inString
			continue
		}
		if !inString && ch == '$' && i+1 < len(statement) && statement[i+1] >= '0' && statement[i+1] <= '9' {
			j := i + 1
			for j < len(statement) && statement[j] >= '0' && statement[j] <= '9' {
				j++
			}
			index, err := strconv.Atoi(statement[i+1 : j])
			if err != nil || index < 1 || index > len(parameters) {
				return "", fmt.Errorf("bind parameter %s has no value", statement[i:j])
			}
			builder.WriteString(sqlLiteralForExtendedBind(parameters[index-1]))
			i = j - 1
			continue
		}
		builder.WriteByte(ch)
	}
	return builder.String(), nil
}

func sqlLiteralForExtendedBind(parameter extendedBindParameter) string {
	if parameter.null {
		return "NULL"
	}
	return "'" + strings.ReplaceAll(parameter.value, "'", "''") + "'"
}

func (s *Server) describeExtendedQuery(statement string) (queryResult, bool) {
	normalized := strings.TrimSpace(strings.TrimRight(statement, ";"))
	if !strings.HasPrefix(strings.ToLower(normalized), "select ") {
		return queryResult{}, false
	}
	result, err := s.executeSQL(normalized)
	if err != nil {
		return queryResult{}, false
	}
	result.rows = nil
	return result, true
}
