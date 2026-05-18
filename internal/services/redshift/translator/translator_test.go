package translator

import (
	"context"
	"strings"
	"testing"
)

func TestRedshiftToPostgresRewritesCurrentTimestampFunctions(t *testing.T) {
	translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, "select getdate() as created_at, sysdate as snapshot_at")
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}

	if translated.BackendSQL != "select CURRENT_TIMESTAMP as created_at, CURRENT_TIMESTAMP as snapshot_at" {
		t.Fatalf("BackendSQL = %q", translated.BackendSQL)
	}
}

func TestRedshiftToPostgresRewritesNVLToCoalesce(t *testing.T) {
	translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, "select nvl(payload, 'unknown') as payload from events")
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}

	if translated.BackendSQL != "select COALESCE(payload, 'unknown') as payload from events" {
		t.Fatalf("BackendSQL = %q", translated.BackendSQL)
	}
}

func TestRedshiftToPostgresRewritesBeginTransactionModes(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "read only serializable",
			sql:  "BEGIN READ ONLY ISOLATION LEVEL SERIALIZABLE;",
			want: "BEGIN READ ONLY, ISOLATION LEVEL SERIALIZABLE;",
		},
		{
			name: "read write serializable",
			sql:  "begin read write isolation level serializable",
			want: "begin read write, isolation level serializable",
		},
		{
			name: "begin inside string literal is ignored",
			sql:  "select 'BEGIN READ ONLY ISOLATION LEVEL SERIALIZABLE' as statement",
			want: "select 'BEGIN READ ONLY ISOLATION LEVEL SERIALIZABLE' as statement",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesResetCommands(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "session context variable",
			sql:  "RESET app_context.user_id;",
			want: "SELECT set_config('app_context.user_id', NULL, false);",
		},
		{
			name: "query group",
			sql:  "reset query_group",
			want: "RESET application_name",
		},
		{
			name: "all is unchanged",
			sql:  "RESET ALL;",
			want: "RESET ALL;",
		},
		{
			name: "reset inside string literal is ignored",
			sql:  "select 'RESET app_context.user_id' as statement",
			want: "select 'RESET app_context.user_id' as statement",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesGrantAssumeRoleToNoop(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "grant assumerole to user",
			sql:  "GRANT ASSUMEROLE ON 'arn:aws:iam::123456789012:role/redshift-copy' TO user analytics_loader;",
			want: "select 1",
		},
		{
			name: "grant assumerole with operation scope",
			sql:  "grant assumerole on 'arn:aws:iam::123456789012:role/redshift-unload' to role analytics_role for unload",
			want: "select 1",
		},
		{
			name: "grant assumerole inside string literal is ignored",
			sql:  "select 'GRANT ASSUMEROLE ON ''arn:aws:iam::123456789012:role/redshift-copy'' TO user analytics_loader' as statement",
			want: "select 'GRANT ASSUMEROLE ON ''arn:aws:iam::123456789012:role/redshift-copy'' TO user analytics_loader' as statement",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesDatashareStatementsToNoop(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "create datashare",
			sql:  "CREATE DATASHARE sales_share;",
			want: "select 1",
		},
		{
			name: "alter datashare",
			sql:  "alter datashare sales_share add schema analytics",
			want: "select 1",
		},
		{
			name: "grant usage on datashare",
			sql:  "grant usage on datashare sales_share to namespace '12345678-1234-1234-1234-123456789012'",
			want: "select 1",
		},
		{
			name: "datashare inside string literal is ignored",
			sql:  "select 'CREATE DATASHARE sales_share' as statement",
			want: "select 'CREATE DATASHARE sales_share' as statement",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesMaskingPolicyStatementsToNoop(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "create masking policy",
			sql:  "CREATE MASKING POLICY mask_email WITH (email varchar(256)) USING ('***'::varchar(256));",
			want: "select 1",
		},
		{
			name: "attach masking policy",
			sql:  "attach masking policy mask_email on users (email) to role analyst priority 10",
			want: "select 1",
		},
		{
			name: "masking policy inside string literal is ignored",
			sql:  "select 'CREATE MASKING POLICY mask_email' as statement",
			want: "select 'CREATE MASKING POLICY mask_email' as statement",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesRandToRandom(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "uppercase rand",
			sql:  "select RAND() as sample_value from events",
			want: "select random() as sample_value from events",
		},
		{
			name: "rand inside string literal is ignored",
			sql:  "select 'RAND()' as label, RAND() as sample_value from events",
			want: "select 'RAND()' as label, random() as sample_value from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesNVL2ToCase(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "nvl2",
			sql:  "select nvl2(payload, 'present', 'missing') as payload_state from events",
			want: "select CASE WHEN payload IS NOT NULL THEN 'present' ELSE 'missing' END as payload_state from events",
		},
		{
			name: "nvl2 inside string literal is ignored",
			sql:  "select 'nvl2(payload, ''present'', ''missing'')' as label, NVL2(trim(payload), payload, 'missing') as payload_value from events",
			want: "select 'nvl2(payload, ''present'', ''missing'')' as label, CASE WHEN trim(payload) IS NOT NULL THEN payload ELSE 'missing' END as payload_value from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesLenToLength(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "len",
			sql:  "select len(payload) as payload_length from events",
			want: "select length(payload) as payload_length from events",
		},
		{
			name: "len inside string literal is ignored",
			sql:  "select 'len(payload)' as label, LEN(trim(payload)) as payload_length from events",
			want: "select 'len(payload)' as label, length(trim(payload)) as payload_length from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesCharIndexToPosition(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "charindex",
			sql:  "select charindex('needle', payload) as needle_index from events",
			want: "select position('needle' in payload) as needle_index from events",
		},
		{
			name: "charindex inside string literal is ignored",
			sql:  "select 'charindex(''needle'', payload)' as label, CHARINDEX(trim(needle), payload) as needle_index from events",
			want: "select 'charindex(''needle'', payload)' as label, position(trim(needle) in payload) as needle_index from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesSubstringNegativeStart(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "negative start",
			sql:  "select substring(payload, -2, 6) as payload_prefix from events",
			want: "select substring(payload from (case when -2 < 1 then 1 else -2 end) for (case when -2 <= 0 then case when -2 + 6 - 1 <= 0 then 0 else -2 + 6 - 1 end else 6 end)) as payload_prefix from events",
		},
		{
			name: "substring inside string literal is ignored",
			sql:  "select 'substring(payload, -2, 6)' as label, SUBSTRING(trim(payload), start_pos, payload_length) as payload_part from events",
			want: "select 'substring(payload, -2, 6)' as label, substring(trim(payload) from (case when start_pos < 1 then 1 else start_pos end) for (case when start_pos <= 0 then case when start_pos + payload_length - 1 <= 0 then 0 else start_pos + payload_length - 1 end else payload_length end)) as payload_part from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesStrtol(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "strtol",
			sql:  "select strtol(hex_payload, 16) as payload_id from events",
			want: "select (select (case when left(trim(hex_payload), 1) = '-' then -1 else 1 end) * coalesce(sum((strpos('0123456789abcdefghijklmnopqrstuvwxyz', digit) - 1)::numeric * power((16)::numeric, (length(regexp_replace(trim(hex_payload), '^[+-]', '')) - ordinality)::numeric)), 0)::bigint from regexp_split_to_table(lower(regexp_replace(trim(hex_payload), '^[+-]', '')), '') with ordinality as strtol_digits(digit, ordinality)) as payload_id from events",
		},
		{
			name: "strtol inside string literal is ignored",
			sql:  "select 'strtol(hex_payload, 16)' as label, STRTOL(trim(hex_payload), target_base) as payload_id from events",
			want: "select 'strtol(hex_payload, 16)' as label, (select (case when left(trim(trim(hex_payload)), 1) = '-' then -1 else 1 end) * coalesce(sum((strpos('0123456789abcdefghijklmnopqrstuvwxyz', digit) - 1)::numeric * power((target_base)::numeric, (length(regexp_replace(trim(trim(hex_payload)), '^[+-]', '')) - ordinality)::numeric)), 0)::bigint from regexp_split_to_table(lower(regexp_replace(trim(trim(hex_payload)), '^[+-]', '')), '') with ordinality as strtol_digits(digit, ordinality)) as payload_id from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesCRC32(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "crc32",
			sql:  "select crc32(payload) as payload_crc from events",
			want: "select (with recursive crc32_input(data) as (select convert_to((payload)::text, 'UTF8')), crc32_state(step, crc) as (select 0, 4294967295::bigint union all select step + 1, (case when ((case when step % 8 = 0 then crc # get_byte(data, step / 8) else crc end) & 1) = 1 then (((case when step % 8 = 0 then crc # get_byte(data, step / 8) else crc end) >> 1) # 3988292384) else ((case when step % 8 = 0 then crc # get_byte(data, step / 8) else crc end) >> 1) end) from crc32_state, crc32_input where step < length(data) * 8) select case when data is null then null else (crc # 4294967295)::bigint end from crc32_state, crc32_input order by step desc limit 1) as payload_crc from events",
		},
		{
			name: "crc32 inside string literal is ignored",
			sql:  "select 'crc32(payload)' as label, CRC32(trim(payload)) as payload_crc from events",
			want: "select 'crc32(payload)' as label, (with recursive crc32_input(data) as (select convert_to((trim(payload))::text, 'UTF8')), crc32_state(step, crc) as (select 0, 4294967295::bigint union all select step + 1, (case when ((case when step % 8 = 0 then crc # get_byte(data, step / 8) else crc end) & 1) = 1 then (((case when step % 8 = 0 then crc # get_byte(data, step / 8) else crc end) >> 1) # 3988292384) else ((case when step % 8 = 0 then crc # get_byte(data, step / 8) else crc end) >> 1) end) from crc32_state, crc32_input where step < length(data) * 8) select case when data is null then null else (crc # 4294967295)::bigint end from crc32_state, crc32_input order by step desc limit 1) as payload_crc from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesDigestFunctions(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "md5 digest",
			sql:  "select md5_digest(payload) as payload_md5 from events",
			want: "select md5((payload)::text) as payload_md5 from events",
		},
		{
			name: "func sha1",
			sql:  "select func_sha1(payload) as payload_sha1 from events",
			want: "select encode(digest((payload)::text, 'sha1'), 'hex') as payload_sha1 from events",
		},
		{
			name: "digest functions inside string literal are ignored",
			sql:  "select 'md5_digest(payload), func_sha1(payload)' as label, MD5_DIGEST(trim(payload)) as payload_md5 from events",
			want: "select 'md5_digest(payload), func_sha1(payload)' as label, md5((trim(payload))::text) as payload_md5 from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesRegexpSubstr(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "first match",
			sql:  "select regexp_substr(payload, '[0-9]+', 1, 1) as payload_match from events",
			want: "select regexp_match(payload, '[0-9]+') as payload_match from events",
		},
		{
			name: "later occurrence",
			sql:  "select REGEXP_SUBSTR(payload, '[0-9]+', 3, 2) as payload_match from events",
			want: "select (select regexp_substr_match from regexp_matches(substring(payload from 3), '[0-9]+', 'g') with ordinality as regexp_substr_matches(regexp_substr_match, regexp_substr_ordinality) where regexp_substr_ordinality = 2) as payload_match from events",
		},
		{
			name: "regexp_substr inside string literal is ignored",
			sql:  "select 'regexp_substr(payload, ''[0-9]+'', 1, 1)' as label, REGEXP_SUBSTR(trim(payload), pattern, start_pos, occurrence) as payload_match from events",
			want: "select 'regexp_substr(payload, ''[0-9]+'', 1, 1)' as label, (select regexp_substr_match from regexp_matches(substring(trim(payload) from start_pos), pattern, 'g') with ordinality as regexp_substr_matches(regexp_substr_match, regexp_substr_ordinality) where regexp_substr_ordinality = occurrence) as payload_match from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesRegexpCount(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "regexp count",
			sql:  "select regexp_count(payload, '[0-9]+') as number_count from events",
			want: "select (case when payload is null or '[0-9]+' is null then null else (select count(*)::int from regexp_matches(payload, '[0-9]+', 'g')) end) as number_count from events",
		},
		{
			name: "regexp_count inside string literal is ignored",
			sql:  "select 'regexp_count(payload, ''[0-9]+'')' as label, REGEXP_COUNT(trim(payload), pattern) as number_count from events",
			want: "select 'regexp_count(payload, ''[0-9]+'')' as label, (case when trim(payload) is null or pattern is null then null else (select count(*)::int from regexp_matches(trim(payload), pattern, 'g')) end) as number_count from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesRegexpInstr(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "regexp instr",
			sql:  "select REGEXP_INSTR(payload, '[0-9]+', 1, 2, 0, 'i') as number_position from events",
			want: "select regexp_instr(payload, '[0-9]+', 1, 2, 0, 'i') as number_position from events",
		},
		{
			name: "subexpression parameter",
			sql:  "select REGEXP_INSTR(payload, '([0-9]+)', 1, 1, 0, 'ie') as number_position from events",
			want: "select regexp_instr(payload, '([0-9]+)', 1, 1, 0, 'i', 1) as number_position from events",
		},
		{
			name: "regexp_instr inside string literal is ignored",
			sql:  "select 'regexp_instr(payload, ''[0-9]+'')' as label, REGEXP_INSTR(trim(payload), pattern) as number_position from events",
			want: "select 'regexp_instr(payload, ''[0-9]+'')' as label, regexp_instr(trim(payload), pattern) as number_position from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesSplitPartNegativePosition(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "negative position",
			sql:  "select split_part(payload, '/', -2) as parent_segment from events",
			want: "select reverse(split_part(reverse(payload), reverse('/'), 2)) as parent_segment from events",
		},
		{
			name: "split_part inside string literal is ignored",
			sql:  "select 'split_part(payload, ''/'', -1)' as label, SPLIT_PART(trim(payload), delimiter, -1) as last_segment from events",
			want: "select 'split_part(payload, ''/'', -1)' as label, reverse(split_part(reverse(trim(payload)), reverse(delimiter), 1)) as last_segment from events",
		},
		{
			name: "positive position is unchanged",
			sql:  "select split_part(payload, '/', 2) as child_segment from events",
			want: "select split_part(payload, '/', 2) as child_segment from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresDoesNotRewriteFunctionsInsideStringLiterals(t *testing.T) {
	translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, `select 'getdate() sysdate nvl(a,b)' as literal, "nvl" as quoted_name, getdate() as now`)
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}

	if !strings.Contains(translated.BackendSQL, "'getdate() sysdate nvl(a,b)'") {
		t.Fatalf("BackendSQL rewrote literal: %q", translated.BackendSQL)
	}
	if !strings.Contains(translated.BackendSQL, "CURRENT_TIMESTAMP as now") {
		t.Fatalf("BackendSQL did not rewrite GETDATE(): %q", translated.BackendSQL)
	}
	if !strings.Contains(translated.BackendSQL, `"nvl" as quoted_name`) {
		t.Fatalf("BackendSQL rewrote quoted identifier: %q", translated.BackendSQL)
	}
}

func TestRedshiftToPostgresRewritesSTVTablesToReadOnlyStubs(t *testing.T) {
	stvStub := postgresRedshiftSystemTableStub()
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "from stv table",
			sql:  "select * from stv_recents",
			want: "select * from " + stvStub + " as stv_recents",
		},
		{
			name: "join stv table with alias",
			sql:  "select r.query from events e join STV_RECENTS as r on r.query = e.query_id",
			want: "select r.query from events e join " + stvStub + " as r on r.query = e.query_id",
		},
		{
			name: "from stv table with where clause",
			sql:  "select query from stv_recents where userid = 100",
			want: "select query from " + stvStub + " as stv_recents where userid = 100",
		},
		{
			name: "stv table inside string literal is ignored",
			sql:  "select 'from stv_recents' as label from events",
			want: "select 'from stv_recents' as label from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesSTLTablesToReadOnlyStubs(t *testing.T) {
	stlStub := postgresRedshiftSystemTableStub()
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "from stl table",
			sql:  "select * from stl_query",
			want: "select * from " + stlStub + " as stl_query",
		},
		{
			name: "join stl table with alias",
			sql:  "select q.query from events e join STL_QUERY as q on q.query = e.query_id",
			want: "select q.query from events e join " + stlStub + " as q on q.query = e.query_id",
		},
		{
			name: "stl table inside string literal is ignored",
			sql:  "select 'from stl_query' as label from events",
			want: "select 'from stl_query' as label from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesMetadataViewsToReadOnlyStubs(t *testing.T) {
	systemStub := postgresRedshiftSystemTableStub()
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "from svv view",
			sql:  "select * from svv_table_info",
			want: "select * from " + systemStub + " as svv_table_info",
		},
		{
			name: "join svl view with alias",
			sql:  "select q.query from events e join SVL_QUERY_SUMMARY as q on q.query = e.query_id",
			want: "select q.query from events e join " + systemStub + " as q on q.query = e.query_id",
		},
		{
			name: "from sys view",
			sql:  "select query_id from sys_query_history where status = 'success'",
			want: "select query_id from " + systemStub + " as sys_query_history where status = 'success'",
		},
		{
			name: "metadata view inside string literal is ignored",
			sql:  "select 'from svv_table_info join svl_query_summary' as label from events",
			want: "select 'from svv_table_info join svl_query_summary' as label from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesInformationSchemaColumnsToRedshiftProjection(t *testing.T) {
	columnsProjection := postgresInformationSchemaColumns()
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "from information schema columns",
			sql:  "select column_name, remarks from information_schema.columns where table_schema = 'public'",
			want: "select column_name, remarks from " + columnsProjection + " as columns where table_schema = 'public'",
		},
		{
			name: "join information schema columns with alias",
			sql:  "select c.column_name, c.remarks from events e join information_schema.columns as c on c.table_name = e.table_name",
			want: "select c.column_name, c.remarks from events e join " + columnsProjection + " as c on c.table_name = e.table_name",
		},
		{
			name: "information schema columns inside string literal is ignored",
			sql:  "select 'from information_schema.columns' as label from events",
			want: "select 'from information_schema.columns' as label from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesDefaultFunctionInCreateTableBackendSQL(t *testing.T) {
	translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, "create table events(id int, created_at timestamp default getdate())")
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}

	if strings.Contains(strings.ToLower(translated.BackendSQL), "getdate") {
		t.Fatalf("BackendSQL still contains GETDATE(): %q", translated.BackendSQL)
	}
	if !strings.Contains(translated.BackendSQL, "default CURRENT_TIMESTAMP") {
		t.Fatalf("BackendSQL did not rewrite default GETDATE(): %q", translated.BackendSQL)
	}
}

func TestRedshiftToPostgresRewritesDecodeToCase(t *testing.T) {
	translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, "select decode(status, 'ok', 1, 'failed', 0, -1) as score from events")
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}

	want := "select CASE status WHEN 'ok' THEN 1 WHEN 'failed' THEN 0 ELSE -1 END as score from events"
	if translated.BackendSQL != want {
		t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, want)
	}
}

func TestRedshiftToPostgresRewritesGreatestLeastToIgnoreNulls(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "greatest",
			sql:  "select greatest(score_a, score_b, score_c) as best_score from events",
			want: "select (select max(greatest_value) from (values (score_a), (score_b), (score_c)) as redshift_greatest_values(greatest_value)) as best_score from events",
		},
		{
			name: "least",
			sql:  "select LEAST(score_a, score_b, 0) as worst_score from events",
			want: "select (select min(least_value) from (values (score_a), (score_b), (0)) as redshift_least_values(least_value)) as worst_score from events",
		},
		{
			name: "greatest and least inside string literal are ignored",
			sql:  "select 'greatest(score_a, score_b), least(score_a, score_b)' as label, GREATEST(score_a, score_b) as best_score from events",
			want: "select 'greatest(score_a, score_b), least(score_a, score_b)' as label, (select max(greatest_value) from (values (score_a), (score_b)) as redshift_greatest_values(greatest_value)) as best_score from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesRoundNegativeScale(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "negative scale",
			sql:  "select round(amount, -2) as rounded_amount from events",
			want: "select round((amount)::numeric, -2) as rounded_amount from events",
		},
		{
			name: "round inside string literal is ignored",
			sql:  "select 'round(amount, -2)' as label, ROUND(total_amount, -3) as rounded_amount from events",
			want: "select 'round(amount, -2)' as label, round((total_amount)::numeric, -3) as rounded_amount from events",
		},
		{
			name: "non-negative scale is unchanged",
			sql:  "select round(amount, 2) as rounded_amount from events",
			want: "select round(amount, 2) as rounded_amount from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesJSONExtractPathText(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "path text",
			sql:  "select json_extract_path_text(payload, 'user', 'name') as user_name from events",
			want: "select jsonb_extract_path_text((payload)::jsonb, 'user', 'name') as user_name from events",
		},
		{
			name: "trailing null_if_invalid boolean",
			sql:  "select JSON_EXTRACT_PATH_TEXT(payload, 'user', 'name', true) as user_name from events",
			want: "select jsonb_extract_path_text((payload)::jsonb, 'user', 'name') as user_name from events",
		},
		{
			name: "json_extract_path_text inside string literal is ignored",
			sql:  "select 'json_extract_path_text(payload, ''user'', true)' as label, JSON_EXTRACT_PATH_TEXT(trim(payload), 'user', 'name', false) as user_name from events",
			want: "select 'json_extract_path_text(payload, ''user'', true)' as label, jsonb_extract_path_text((trim(payload))::jsonb, 'user', 'name') as user_name from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesJSONExtractArrayElementText(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "array element text",
			sql:  "select json_extract_array_element_text(payload, 0) as first_event from events",
			want: "select ((payload)::jsonb -> 0)::text as first_event from events",
		},
		{
			name: "json_extract_array_element_text inside string literal is ignored",
			sql:  "select 'json_extract_array_element_text(payload, 0)' as label, JSON_EXTRACT_ARRAY_ELEMENT_TEXT(trim(payload), event_index) as event_value from events",
			want: "select 'json_extract_array_element_text(payload, 0)' as label, ((trim(payload))::jsonb -> event_index)::text as event_value from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesJSONArrayLength(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "array length",
			sql:  "select json_array_length(payload) as event_count from events",
			want: "select jsonb_array_length((payload)::jsonb) as event_count from events",
		},
		{
			name: "json_array_length inside string literal is ignored",
			sql:  "select 'json_array_length(payload)' as label, JSON_ARRAY_LENGTH(trim(payload)) as event_count from events",
			want: "select 'json_array_length(payload)' as label, jsonb_array_length((trim(payload))::jsonb) as event_count from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesJSONParse(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "json parse",
			sql:  "select json_parse(payload) as payload_json from events",
			want: "select (payload)::jsonb as payload_json from events",
		},
		{
			name: "json_parse inside string literal is ignored",
			sql:  "select 'json_parse(payload)' as label, JSON_PARSE(trim(payload)) as payload_json from events",
			want: "select 'json_parse(payload)' as label, (trim(payload))::jsonb as payload_json from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesIsValidJSON(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "valid json",
			sql:  "select is_valid_json(payload) as payload_is_json from events",
			want: "select coalesce(json_valid((payload)::text), false) as payload_is_json from events",
		},
		{
			name: "valid json array",
			sql:  "select IS_VALID_JSON_ARRAY(payload) as payload_is_json_array from events",
			want: "select (case when coalesce(json_valid((payload)::text), false) then jsonb_typeof((payload)::jsonb) = 'array' else false end) as payload_is_json_array from events",
		},
		{
			name: "is_valid_json inside string literal is ignored",
			sql:  "select 'is_valid_json(payload)' as label, IS_VALID_JSON(trim(payload)) as payload_is_json from events",
			want: "select 'is_valid_json(payload)' as label, coalesce(json_valid((trim(payload))::text), false) as payload_is_json from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesPartiQLNavigation(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "dot path",
			sql:  "select payload.user.name as user_name from events",
			want: "select (payload)::jsonb -> 'user' ->> 'name' as user_name from events",
		},
		{
			name: "subscript path",
			sql:  "select payload.events[0].type as first_event_type from events",
			want: "select (payload)::jsonb -> 'events' -> 0 ->> 'type' as first_event_type from events",
		},
		{
			name: "partiql navigation inside string literal is ignored",
			sql:  "select 'payload.user.name payload[0]' as label, payload.items[0] as first_item from events",
			want: "select 'payload.user.name payload[0]' as label, (payload)::jsonb -> 'items' ->> 0 as first_item from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesObjectTransform(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "keep and set paths",
			sql:  `select object_transform(payload KEEP '"user"."name"' SET '"user"."active"', true, '"event_count"', 3) as transformed_payload from events`,
			want: `select jsonb_set(jsonb_set(jsonb_set(jsonb_set('{}'::jsonb, ARRAY['user'], coalesce('{}'::jsonb #> ARRAY['user'], '{}'::jsonb), true), ARRAY['user', 'name'], ((payload)::jsonb #> ARRAY['user', 'name']), true), ARRAY['user', 'active'], to_jsonb(true), true), ARRAY['event_count'], to_jsonb(3), true) as transformed_payload from events`,
		},
		{
			name: "object_transform inside string literal is ignored",
			sql:  `select 'object_transform(payload KEEP ''"user"'')' as label, OBJECT_TRANSFORM(trim(payload) SET '"user"."name"', username) as transformed_payload from events`,
			want: `select 'object_transform(payload KEEP ''"user"'')' as label, jsonb_set(jsonb_set('{}'::jsonb, ARRAY['user'], coalesce('{}'::jsonb #> ARRAY['user'], '{}'::jsonb), true), ARRAY['user', 'name'], to_jsonb(username), true) as transformed_payload from events`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesDateAddAndDateDiff(t *testing.T) {
	translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, "select dateadd(day, 7, created_at) as expires_at, datediff(hour, started_at, ended_at) as hours from events")
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}

	if !strings.Contains(translated.BackendSQL, "created_at + (7 * interval '1 day') as expires_at") {
		t.Fatalf("BackendSQL did not rewrite DATEADD(): %q", translated.BackendSQL)
	}
	if !strings.Contains(translated.BackendSQL, "floor(extract(epoch from (ended_at - started_at)) / 3600)::int as hours") {
		t.Fatalf("BackendSQL did not rewrite DATEDIFF(): %q", translated.BackendSQL)
	}
}

func TestRedshiftToPostgresRewritesTimeOfDay(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "timeofday",
			sql:  "select timeofday() as current_time_text",
			want: "select clock_timestamp()::text as current_time_text",
		},
		{
			name: "timeofday inside string literal is ignored",
			sql:  "select 'timeofday()' as label, TIMEOFDAY() as current_time_text",
			want: "select 'timeofday()' as label, clock_timestamp()::text as current_time_text",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesConvertTimezone(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "utc to jst",
			sql:  "select convert_timezone('UTC', 'JST', created_at) as created_at_jst from events",
			want: "select created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Tokyo' as created_at_jst from events",
		},
		{
			name: "convert_timezone inside string literal is ignored",
			sql:  "select 'convert_timezone(''UTC'', ''JST'', created_at)' as label, CONVERT_TIMEZONE('utc', 'jst', created_at) as created_at_jst from events",
			want: "select 'convert_timezone(''UTC'', ''JST'', created_at)' as label, created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Tokyo' as created_at_jst from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesDatePartAndDateTruncPartAliases(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "date_part weekday alias",
			sql:  "select date_part(weekday, created_at) as weekday from events",
			want: "select date_part('dow', created_at) as weekday from events",
		},
		{
			name: "date_trunc quarter alias",
			sql:  "select date_trunc(qtr, created_at) as quarter_start from events",
			want: "select date_trunc('quarter', created_at) as quarter_start from events",
		},
		{
			name: "aliases inside string literal are ignored",
			sql:  "select 'date_part(weekday, created_at)' as label, DATE_PART(dw, created_at) as weekday from events",
			want: "select 'date_part(weekday, created_at)' as label, date_part('dow', created_at) as weekday from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesLastDay(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "last day",
			sql:  "select last_day(created_at) as month_end from events",
			want: "select (date_trunc('month', created_at) + interval '1 month - 1 day')::date as month_end from events",
		},
		{
			name: "last day inside string literal is ignored",
			sql:  "select 'last_day(created_at)' as label, LAST_DAY(created_at::date) as month_end from events",
			want: "select 'last_day(created_at)' as label, (date_trunc('month', created_at::date) + interval '1 month - 1 day')::date as month_end from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesMonthsBetween(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "months between",
			sql:  "select months_between(ended_at, started_at) as months_elapsed from events",
			want: "select (extract(year from age(ended_at, started_at)) * 12 + extract(month from age(ended_at, started_at))) as months_elapsed from events",
		},
		{
			name: "months between inside string literal is ignored",
			sql:  "select 'months_between(ended_at, started_at)' as label, MONTHS_BETWEEN(ended_at::date, started_at::date) as months_elapsed from events",
			want: "select 'months_between(ended_at, started_at)' as label, (extract(year from age(ended_at::date, started_at::date)) * 12 + extract(month from age(ended_at::date, started_at::date))) as months_elapsed from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesAddMonths(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "add months",
			sql:  "select add_months(created_at, 2) as renewal_at from events",
			want: "select created_at + (2 * interval '1 month') as renewal_at from events",
		},
		{
			name: "add months inside string literal is ignored",
			sql:  "select 'add_months(created_at, 2)' as label, ADD_MONTHS(created_at::date, months_to_add) as renewal_at from events",
			want: "select 'add_months(created_at, 2)' as label, created_at::date + (months_to_add * interval '1 month') as renewal_at from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesNextDay(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "next day",
			sql:  "select next_day(created_at, 'Tuesday') as next_tuesday from events",
			want: "select ((created_at)::date + ((2 - extract(dow from (created_at)::date)::int + 6) % 7 + 1))::date as next_tuesday from events",
		},
		{
			name: "next day inside string literal is ignored",
			sql:  "select 'next_day(created_at, ''Tue'')' as label, NEXT_DAY(created_at::date, 'Tue') as next_tuesday from events",
			want: "select 'next_day(created_at, ''Tue'')' as label, ((created_at::date)::date + ((2 - extract(dow from (created_at::date)::date)::int + 6) % 7 + 1))::date as next_tuesday from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesToDateAndToTimestampTimezoneFormats(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "to_timestamp trailing timezone abbreviation",
			sql:  "select to_timestamp(raw_value, 'YYYY-MM-DD HH24:MI:SS TZ') as observed_at from events",
			want: "select to_timestamp(regexp_replace(raw_value, '[[:space:]]*([[:alpha:]_/]+|[+-][0-9]{2}(:?[0-9]{2})?)$', ''), 'YYYY-MM-DD HH24:MI:SS') as observed_at from events",
		},
		{
			name: "to_date trailing timezone offset inside string literal is ignored",
			sql:  "select 'to_date(raw_value, ''YYYY-MM-DD OF'')' as label, TO_DATE(raw_value, 'YYYY-MM-DD OF') as observed_date from events",
			want: "select 'to_date(raw_value, ''YYYY-MM-DD OF'')' as label, to_date(regexp_replace(raw_value, '[[:space:]]*([[:alpha:]_/]+|[+-][0-9]{2}(:?[0-9]{2})?)$', ''), 'YYYY-MM-DD') as observed_date from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesToCharTimezoneFormats(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "timezone abbreviation",
			sql:  "select to_char(observed_at, 'YYYY-MM-DD HH24:MI:SS TZ') as observed_at_text from events",
			want: `select to_char(observed_at, 'YYYY-MM-DD HH24:MI:SS "UTC"') as observed_at_text from events`,
		},
		{
			name: "timezone offset inside string literal is ignored",
			sql:  "select 'to_char(observed_at, ''YYYY-MM-DD OF'')' as label, TO_CHAR(observed_at, 'YYYY-MM-DD OF') as observed_at_text from events",
			want: `select 'to_char(observed_at, ''YYYY-MM-DD OF'')' as label, to_char(observed_at, 'YYYY-MM-DD "+00"') as observed_at_text from events`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesListAggWithinGroup(t *testing.T) {
	translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, "select listagg(name, ',') within group (order by created_at desc) as names from events")
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}

	want := "select string_agg(name, ',' ORDER BY created_at desc) as names from events"
	if translated.BackendSQL != want {
		t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, want)
	}
}

func TestRedshiftToPostgresRewritesListAggWindowWithinGroup(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "partitioned window",
			sql:  "select listagg(name, ',') within group (order by created_at desc) over (partition by account_id) as names from events",
			want: "select array_to_string(array_agg(name) OVER (partition by account_id ORDER BY created_at desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), ',') as names from events",
		},
		{
			name: "empty over window",
			sql:  "select LISTAGG(name, ',') WITHIN GROUP (ORDER BY created_at) OVER () as names from events",
			want: "select array_to_string(array_agg(name) OVER (ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), ',') as names from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesMedianToPercentileCont(t *testing.T) {
	translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, "select median(duration_ms) as p50_duration from events")
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}

	want := "select percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms) as p50_duration from events"
	if translated.BackendSQL != want {
		t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, want)
	}
}

func TestRedshiftToPostgresRewritesBooleanBitAggregates(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "boolean bit aggregates",
			sql:  "select bit_and(is_ready) as all_ready, bit_or(is_ready) as any_ready from events",
			want: "select bool_and(is_ready) as all_ready, bool_or(is_ready) as any_ready from events",
		},
		{
			name: "boolean bit aggregates inside string literal are ignored",
			sql:  "select 'bit_and(is_ready), bit_or(is_ready)' as label, BIT_AND(is_ready) as all_ready from events",
			want: "select 'bit_and(is_ready), bit_or(is_ready)' as label, bool_and(is_ready) as all_ready from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesRatioToReportOver(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "partitioned window",
			sql:  "select ratio_to_report(revenue) over (partition by category) as revenue_share from sales",
			want: "select revenue / SUM(revenue) OVER (partition by category) as revenue_share from sales",
		},
		{
			name: "ratio_to_report inside string literal is ignored",
			sql:  "select 'ratio_to_report(revenue) over ()' as label, RATIO_TO_REPORT(revenue) OVER () as revenue_share from sales",
			want: "select 'ratio_to_report(revenue) over ()' as label, revenue / SUM(revenue) OVER () as revenue_share from sales",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesLateralColumnAliasReferences(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "select list alias reference",
			sql:  "select clicks / impressions as probability, round(100 * probability, 1) as percentage from raw_data",
			want: "select clicks / impressions as probability, round(100 * (clicks / impressions), 1) as percentage from raw_data",
		},
		{
			name: "alias reference chains through later select items",
			sql:  "select amount as subtotal, subtotal * tax_rate as tax, subtotal + tax as total from invoices",
			want: "select amount as subtotal, (amount) * tax_rate as tax, (amount) + ((amount) * tax_rate) as total from invoices",
		},
		{
			name: "implicit alias reference",
			sql:  "select clicks / impressions probability, probability * 100 percentage from raw_data",
			want: "select clicks / impressions as probability, (clicks / impressions) * 100 as percentage from raw_data",
		},
		{
			name: "string literal and qualified column are unchanged",
			sql:  "select clicks / impressions as probability, 'probability' as label, raw_data.probability as source_probability from raw_data",
			want: "select clicks / impressions as probability, 'probability' as label, raw_data.probability as source_probability from raw_data",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesPGTableMetadataViews(t *testing.T) {
	tableDef := postgresPGTableDef()
	tableInfo := postgresPGTableInfo()
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "from pg_table_def",
			sql:  "select schemaname, tablename, \"column\", encoding, distkey, sortkey, notnull from pg_table_def where tablename = 'events'",
			want: "select schemaname, tablename, \"column\", encoding, distkey, sortkey, notnull from " + tableDef + " as pg_table_def where tablename = 'events'",
		},
		{
			name: "join pg_table_info with alias",
			sql:  "select i.schema, i.\"table\", i.tbl_rows from events e join PG_TABLE_INFO as i on i.\"table\" = e.table_name",
			want: "select i.schema, i.\"table\", i.tbl_rows from events e join " + tableInfo + " as i on i.\"table\" = e.table_name",
		},
		{
			name: "pg table metadata inside string literal is ignored",
			sql:  "select 'from pg_table_def join pg_table_info' as label from events",
			want: "select 'from pg_table_def join pg_table_info' as label from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesApproximateCountDistinct(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "approximate count distinct",
			sql:  "select approximate count(distinct user_id) as active_users from events",
			want: "select count(distinct user_id) as active_users from events",
		},
		{
			name: "approximate count inside string literal is ignored",
			sql:  "select 'approximate count(distinct user_id)' as label, approximate count(distinct user_id) as active_users from events",
			want: "select 'approximate count(distinct user_id)' as label, count(distinct user_id) as active_users from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesLikeDefaultEscape(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "implicit backslash escape",
			sql:  `select id from events where payload like 'promo\_%'`,
			want: `select id from events where payload like 'promo\_%' ESCAPE '\'`,
		},
		{
			name: "explicit escape unchanged",
			sql:  `select id from events where payload like 'promo\_%' escape '\'`,
			want: `select id from events where payload like 'promo\_%' escape '\'`,
		},
		{
			name: "like inside string literal is ignored",
			sql:  `select 'payload like ''promo\_%''' as predicate from events`,
			want: `select 'payload like ''promo\_%''' as predicate from events`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesNullOrderingDefaults(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "ascending and descending defaults",
			sql:  "select id from events order by priority desc, created_at asc, id limit 10",
			want: "select id from events order by priority desc NULLS FIRST, created_at asc NULLS LAST, id NULLS LAST limit 10",
		},
		{
			name: "explicit null ordering unchanged",
			sql:  "select id from events order by priority desc nulls last, created_at nulls first",
			want: "select id from events order by priority desc nulls last, created_at nulls first",
		},
		{
			name: "order by inside string literal is ignored",
			sql:  "select 'order by created_at desc' as label from events order by id",
			want: "select 'order by created_at desc' as label from events order by id NULLS LAST",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesBooleanLiterals(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "typed boolean yes literal",
			sql:  "select boolean 'yes' as enabled",
			want: "select TRUE as enabled",
		},
		{
			name: "typed boolean numeric literal",
			sql:  "select boolean 0 as enabled",
			want: "select FALSE as enabled",
		},
		{
			name: "boolean column default literal",
			sql:  "create table events(id integer, active boolean default y, archived bool default 0)",
			want: "create table events(id integer, active boolean default TRUE, archived bool default FALSE)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresLeavesUnsupportedFunctionFormsUnchanged(t *testing.T) {
	input := "select dateadd(quarter, 1, created_at), datediff(quarter, started_at, ended_at), listagg(name) from events"
	translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, input)
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}

	if translated.BackendSQL != input {
		t.Fatalf("BackendSQL = %q, want unchanged %q", translated.BackendSQL, input)
	}
}

func TestRedshiftToPostgresExtractsMixedTableAttributes(t *testing.T) {
	translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, `create table if not exists analytics.events(
		id integer encode az64,
		created_at timestamp default getdate(),
		payload varchar(64)
	) diststyle even distkey (id) sortkey(created_at,id) backup yes`)
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}

	lowerBackend := strings.ToLower(translated.BackendSQL)
	for _, forbidden := range []string{"diststyle", "distkey", "sortkey", "encode", "backup", "getdate"} {
		if strings.Contains(lowerBackend, forbidden) {
			t.Fatalf("BackendSQL contains Redshift-only token %q: %s", forbidden, translated.BackendSQL)
		}
	}
	if !strings.Contains(translated.BackendSQL, "default CURRENT_TIMESTAMP") {
		t.Fatalf("BackendSQL did not rewrite default GETDATE(): %s", translated.BackendSQL)
	}
	if len(translated.MetadataEffects) != 1 {
		t.Fatalf("MetadataEffects = %#v", translated.MetadataEffects)
	}
	effect := translated.MetadataEffects[0]
	if effect.Schema != "analytics" || effect.Table != "events" || effect.Value != "even" || effect.Name != "id" || effect.Backup != "yes" {
		t.Fatalf("metadata effect = %#v", effect)
	}
	if len(effect.SortKeys) != 2 || effect.SortKeys[0] != "created_at" || effect.SortKeys[1] != "id" {
		t.Fatalf("sort keys = %#v", effect.SortKeys)
	}
	if len(effect.Columns) != 3 || effect.Columns[0].Encoding != "az64" || effect.Columns[1].DefaultValue != "getdate()" {
		t.Fatalf("columns = %#v", effect.Columns)
	}
}

func TestRedshiftToPostgresRewritesCreateTableLikeOptions(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "including defaults",
			sql:  "create table analytics.events_copy (like analytics.events including defaults) diststyle even",
			want: "create table analytics.events_copy (LIKE analytics.events INCLUDING DEFAULTS)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
			if len(translated.MetadataEffects) != 1 {
				t.Fatalf("MetadataEffects = %#v", translated.MetadataEffects)
			}
			effect := translated.MetadataEffects[0]
			if effect.Schema != "analytics" || effect.Table != "events_copy" || effect.Value != "even" {
				t.Fatalf("metadata effect = %#v", effect)
			}
			if len(effect.Columns) != 0 {
				t.Fatalf("columns = %#v, want none for LIKE table", effect.Columns)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesTemporaryTableScope(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "temp table",
			sql:  "create temp table session_events(id integer distkey, payload varchar(16)) diststyle key sortkey(id)",
			want: "create temp table session_events(id integer, payload text check (octet_length(payload) <= 16)) on commit preserve rows",
		},
		{
			name: "temporary table if not exists",
			sql:  "create temporary table if not exists session_events(id integer)",
			want: "create temporary table if not exists session_events(id integer) on commit preserve rows",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
			if len(translated.MetadataEffects) != 0 {
				t.Fatalf("MetadataEffects = %#v, want none for session-scoped temporary table", translated.MetadataEffects)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesCreateExternalTableLocation(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "spectrum location",
			sql:  "create external table spectrum.events(id integer, payload varchar(16)) stored as parquet location 's3://devcloud/events/'",
			want: "create table spectrum.events(id integer, payload text check (octet_length(payload) <= 16))",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
			if strings.Contains(strings.ToLower(translated.BackendSQL), "external") || strings.Contains(strings.ToLower(translated.BackendSQL), "location") {
				t.Fatalf("BackendSQL contains external table syntax: %q", translated.BackendSQL)
			}
			if len(translated.MetadataEffects) != 1 || translated.MetadataEffects[0].Schema != "spectrum" || translated.MetadataEffects[0].Table != "events" {
				t.Fatalf("MetadataEffects = %#v", translated.MetadataEffects)
			}
			if len(translated.MetadataEffects[0].Columns) != 2 || translated.MetadataEffects[0].Columns[1].DataType != "varchar(16)" {
				t.Fatalf("columns = %#v", translated.MetadataEffects[0].Columns)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesCreateExternalSchemaFromDataCatalog(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "data catalog external schema",
			sql:  "create external schema if not exists spectrum from data catalog database 'analytics' iam_role default",
			want: "create schema if not exists spectrum",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
			if strings.Contains(strings.ToLower(translated.BackendSQL), "external") || strings.Contains(strings.ToLower(translated.BackendSQL), "data catalog") {
				t.Fatalf("BackendSQL contains external schema syntax: %q", translated.BackendSQL)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesCreateMaterializedViewAutoRefreshYes(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "auto refresh yes",
			sql:  "create materialized view analytics.daily_events auto refresh yes as select event_date, count(*) as count from events group by event_date",
			want: "create materialized view analytics.daily_events as select event_date, count(*) as count from events group by event_date",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
			if strings.Contains(strings.ToLower(translated.BackendSQL), "auto refresh") {
				t.Fatalf("BackendSQL contains Redshift materialized view option: %q", translated.BackendSQL)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesMergeIntoUpdateInsert(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "matched update and not matched insert",
			sql:  "merge into analytics.events as target using staging.events as source on target.id = source.id when matched then update set payload = source.payload, updated_at = getdate() when not matched then insert (id, payload, updated_at) values (source.id, source.payload, getdate())",
			want: "with updated as (update analytics.events as target set payload = source.payload, updated_at = CURRENT_TIMESTAMP from staging.events as source where target.id = source.id returning 1) insert into analytics.events (id, payload, updated_at) select source.id, source.payload, CURRENT_TIMESTAMP from staging.events as source where not exists (select 1 from analytics.events as target where target.id = source.id)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesInsertValuesDefaultIdentity(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "default identity column value",
			sql:  "insert into analytics.events(event_id) values(default)",
			want: "insert into analytics.events(event_id) default values",
		},
		{
			name: "uppercase with semicolon",
			sql:  "INSERT INTO events VALUES ( DEFAULT );",
			want: "INSERT INTO events default values",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRemovesInsertSelectReturning(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "insert select returning all columns",
			sql:  "insert into analytics.events(id, created_at) select id, getdate() from staging.events returning *",
			want: "insert into analytics.events(id, created_at) select id, CURRENT_TIMESTAMP from staging.events",
		},
		{
			name: "returning inside string literal is ignored",
			sql:  "insert into audit.messages(message) select 'returning *' from staging.messages returning message;",
			want: "insert into audit.messages(message) select 'returning *' from staging.messages",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesCreateViewNoSchemaBinding(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "late binding view",
			sql:  "create view analytics.recent_events as select id, created_at from events with no schema binding",
			want: "create view analytics.recent_events as select id, created_at from events",
		},
		{
			name: "quoted literal is unchanged",
			sql:  "create view analytics.labels as select 'with no schema binding' as label from events with no schema binding",
			want: "create view analytics.labels as select 'with no schema binding' as label from events",
		},
		{
			name: "uppercase with semicolon",
			sql:  "create view analytics.recent_events as select id from events WITH NO SCHEMA BINDING;",
			want: "create view analytics.recent_events as select id from events;",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesAlterColumnEncode(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "alter column encode",
			sql:  "alter table analytics.events alter column payload encode zstd",
			want: "alter table analytics.events alter column payload set statistics -1",
		},
		{
			name: "if exists without column keyword",
			sql:  "ALTER TABLE IF EXISTS events ALTER payload ENCODE az64;",
			want: "alter table if exists events alter column payload set statistics -1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
			if strings.Contains(strings.ToLower(translated.BackendSQL), "encode") {
				t.Fatalf("BackendSQL contains Redshift ENCODE syntax: %q", translated.BackendSQL)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesAlterTableAddColumnDefaultIdentity(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "add column default identity",
			sql:  "alter table analytics.events add column event_id bigint default identity(1, 1)",
			want: "alter table analytics.events add column event_id bigint generated by default as identity (start with 1 increment by 1)",
		},
		{
			name: "if exists with spaced identity arguments",
			sql:  "ALTER TABLE IF EXISTS events ADD id integer DEFAULT IDENTITY ( 100 , 5 );",
			want: "alter table if exists events add column id integer generated by default as identity (start with 100 increment by 5)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
			if strings.Contains(strings.ToLower(translated.BackendSQL), "default identity") {
				t.Fatalf("BackendSQL contains Redshift DEFAULT IDENTITY syntax: %q", translated.BackendSQL)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesTruncateImmediateCommit(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "truncate table",
			sql:  "truncate table analytics.events",
			want: "commit; truncate table analytics.events",
		},
		{
			name: "uppercase with semicolon",
			sql:  "TRUNCATE events;",
			want: "commit; TRUNCATE events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesQualifyToSubquery(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "window alias predicate",
			sql:  "select user_id, row_number() over (partition by user_id order by created_at desc) as rn from events qualify rn = 1",
			want: "select * from (select user_id, row_number() over (partition by user_id order by created_at desc) as rn from events) as devcloud_qualify where rn = 1",
		},
		{
			name: "preserves outer order by",
			sql:  "select user_id, rank() over (order by score desc) as rank from scores qualify rank <= 10 order by rank",
			want: "select * from (select user_id, rank() over (order by score desc) as rank from scores) as devcloud_qualify where rank <= 10 order by rank NULLS LAST",
		},
		{
			name: "direct window predicate",
			sql:  "select user_id, event_id from events qualify row_number() over (partition by user_id order by created_at desc) = 1",
			want: "select user_id, event_id from (select user_id, event_id, row_number() over (partition by user_id order by created_at desc) as __devcloud_qualify_1 from events) as devcloud_qualify where __devcloud_qualify_1 = 1",
		},
		{
			name: "qualify inside string literal is ignored",
			sql:  "select 'qualify rn = 1' as label, row_number() over (order by id) as rn from events qualify rn = 1",
			want: "select * from (select 'qualify rn = 1' as label, row_number() over (order by id) as rn from events) as devcloud_qualify where rn = 1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesSelectTopToLimit(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "top rows",
			sql:  "select top 10 id, created_at from events order by created_at desc",
			want: "select id, created_at from events order by created_at desc NULLS FIRST limit 10",
		},
		{
			name: "parenthesized top rows with semicolon",
			sql:  "SELECT TOP (5) id, getdate() as observed_at FROM events;",
			want: "SELECT id, CURRENT_TIMESTAMP as observed_at FROM events limit 5",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesSuperColumnTypeToJSONB(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		want     string
		dataType string
	}{
		{
			name:     "create table super column",
			sql:      "create table events(id integer, payload SUPER)",
			want:     "create table events(id integer, payload jsonb)",
			dataType: "super",
		},
		{
			name:     "create table hllsketch column",
			sql:      "create table metrics(id integer, estimate HLLSKETCH)",
			want:     "create table metrics(id integer, estimate bytea)",
			dataType: "hllsketch",
		},
		{
			name:     "create table varbyte column",
			sql:      "create table events(id integer, digest VARBYTE)",
			want:     "create table events(id integer, digest bytea)",
			dataType: "varbyte",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
			if len(translated.MetadataEffects) != 1 || len(translated.MetadataEffects[0].Columns) != 2 {
				t.Fatalf("MetadataEffects = %#v", translated.MetadataEffects)
			}
			if translated.MetadataEffects[0].Columns[1].DataType != tc.dataType {
				t.Fatalf("column metadata = %#v, want data type %q", translated.MetadataEffects[0].Columns[1], tc.dataType)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesSpatialColumnTypesToText(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		want     string
		dataType string
	}{
		{
			name:     "create table geometry column",
			sql:      "create table places(id integer, shape GEOMETRY)",
			want:     "create table places(id integer, shape text)",
			dataType: "geometry",
		},
		{
			name:     "create table geography column",
			sql:      "create table places(id integer, footprint GEOGRAPHY)",
			want:     "create table places(id integer, footprint text)",
			dataType: "geography",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
			if len(translated.MetadataEffects) != 1 || len(translated.MetadataEffects[0].Columns) != 2 {
				t.Fatalf("MetadataEffects = %#v", translated.MetadataEffects)
			}
			if translated.MetadataEffects[0].Columns[1].DataType != tc.dataType {
				t.Fatalf("column metadata = %#v, want data type %q", translated.MetadataEffects[0].Columns[1], tc.dataType)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesTimestampColumnType(t *testing.T) {
	translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, "create table events(id integer, created_at TIMESTAMP)")
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}

	want := "create table events(id integer, created_at timestamp(6) without time zone)"
	if translated.BackendSQL != want {
		t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, want)
	}
	if len(translated.MetadataEffects) != 1 || len(translated.MetadataEffects[0].Columns) != 2 {
		t.Fatalf("MetadataEffects = %#v", translated.MetadataEffects)
	}
	if translated.MetadataEffects[0].Columns[1].DataType != "timestamp" {
		t.Fatalf("column metadata = %#v, want data type %q", translated.MetadataEffects[0].Columns[1], "timestamp")
	}
}

func TestRedshiftToPostgresRewritesTimestampTZColumnType(t *testing.T) {
	translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, "create table events(id integer, observed_at TIMESTAMPTZ)")
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}

	want := "create table events(id integer, observed_at timestamp(6) without time zone)"
	if translated.BackendSQL != want {
		t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, want)
	}
	if len(translated.MetadataEffects) != 1 || len(translated.MetadataEffects[0].Columns) != 2 {
		t.Fatalf("MetadataEffects = %#v", translated.MetadataEffects)
	}
	if translated.MetadataEffects[0].Columns[1].DataType != "timestamptz" {
		t.Fatalf("column metadata = %#v, want data type %q", translated.MetadataEffects[0].Columns[1], "timestamptz")
	}
}

func TestRedshiftToPostgresRewritesTimeColumnTypes(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		want     string
		dataType string
	}{
		{
			name:     "create table time column",
			sql:      "create table events(id integer, started_at TIME)",
			want:     "create table events(id integer, started_at time(6) without time zone)",
			dataType: "time",
		},
		{
			name:     "create table timetz column",
			sql:      "create table events(id integer, started_at TIMETZ)",
			want:     "create table events(id integer, started_at time(6) with time zone)",
			dataType: "timetz",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
			if len(translated.MetadataEffects) != 1 || len(translated.MetadataEffects[0].Columns) != 2 {
				t.Fatalf("MetadataEffects = %#v", translated.MetadataEffects)
			}
			if translated.MetadataEffects[0].Columns[1].DataType != tc.dataType {
				t.Fatalf("column metadata = %#v, want data type %q", translated.MetadataEffects[0].Columns[1], tc.dataType)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesByteLimitedStringColumnTypes(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		want     string
		dataType string
	}{
		{
			name:     "create table varchar column",
			sql:      "create table events(id integer, payload VARCHAR(8))",
			want:     "create table events(id integer, payload text check (octet_length(payload) <= 8))",
			dataType: "varchar(8)",
		},
		{
			name:     "create table char column",
			sql:      "create table events(id integer, code CHAR(4))",
			want:     "create table events(id integer, code text check (octet_length(code) <= 4))",
			dataType: "char(4)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
			if len(translated.MetadataEffects) != 1 || len(translated.MetadataEffects[0].Columns) != 2 {
				t.Fatalf("MetadataEffects = %#v", translated.MetadataEffects)
			}
			if translated.MetadataEffects[0].Columns[1].DataType != tc.dataType {
				t.Fatalf("column metadata = %#v, want data type %q", translated.MetadataEffects[0].Columns[1], tc.dataType)
			}
		})
	}
}

func TestRedshiftToPostgresRejectsMalformedCreateTable(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "unterminated column list", sql: "create table events(id integer"},
		{name: "missing column type", sql: "create table events(id)"},
		{name: "empty column name", sql: `create table events("" integer)`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql); err == nil {
				t.Fatal("Translate() error = nil")
			}
		})
	}
}
