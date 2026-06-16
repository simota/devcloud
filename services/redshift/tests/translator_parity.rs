//! 1:1 port of `internal/services/redshift/translator/translator_test.rs`.
//!
//! Every case keeps the exact SQL input and expected backend SQL from legacy —
//! the legacy implementation is the parity oracle, quirks included.

use devcloud_redshift::translator::{
    postgres_information_schema_columns, postgres_pg_table_def, postgres_pg_table_info,
    postgres_redshift_system_table_stub, RedshiftToPostgres, RedshiftTranslator, Session,
    TranslationResult,
};

fn translate(sql: &str) -> TranslationResult {
    RedshiftToPostgres
        .translate(&Session::default(), sql)
        .unwrap_or_else(|err| panic!("Translate() error = {err}"))
}

/// Table-driven helper: each case is (name, sql, want backend SQL).
fn assert_backend_sql(cases: &[(&str, &str, &str)]) {
    for (name, sql, want) in cases {
        let translated = translate(sql);
        assert_eq!(
            &translated.backend_sql, want,
            "{name}: BackendSQL = {:?}, want {want:?}",
            translated.backend_sql
        );
    }
}

#[test]
fn rewrites_current_timestamp_functions() {
    let translated = translate("select getdate() as created_at, sysdate as snapshot_at");
    assert_eq!(
        translated.backend_sql,
        "select CURRENT_TIMESTAMP as created_at, CURRENT_TIMESTAMP as snapshot_at"
    );
}

#[test]
fn rewrites_nvl_to_coalesce() {
    let translated = translate("select nvl(payload, 'unknown') as payload from events");
    assert_eq!(
        translated.backend_sql,
        "select COALESCE(payload, 'unknown') as payload from events"
    );
}

#[test]
fn rewrites_begin_transaction_modes() {
    assert_backend_sql(&[
        (
            "read only serializable",
            "BEGIN READ ONLY ISOLATION LEVEL SERIALIZABLE;",
            "BEGIN READ ONLY, ISOLATION LEVEL SERIALIZABLE;",
        ),
        (
            "read write serializable",
            "begin read write isolation level serializable",
            "begin read write, isolation level serializable",
        ),
        (
            "begin inside string literal is ignored",
            "select 'BEGIN READ ONLY ISOLATION LEVEL SERIALIZABLE' as statement",
            "select 'BEGIN READ ONLY ISOLATION LEVEL SERIALIZABLE' as statement",
        ),
    ]);
}

#[test]
fn rewrites_reset_commands() {
    assert_backend_sql(&[
        (
            "session context variable",
            "RESET app_context.user_id;",
            "SELECT set_config('app_context.user_id', NULL, false);",
        ),
        ("query group", "reset query_group", "RESET application_name"),
        ("all is unchanged", "RESET ALL;", "RESET ALL;"),
        (
            "reset inside string literal is ignored",
            "select 'RESET app_context.user_id' as statement",
            "select 'RESET app_context.user_id' as statement",
        ),
    ]);
}

#[test]
fn rewrites_grant_assume_role_to_noop() {
    assert_backend_sql(&[
        (
            "grant assumerole to user",
            "GRANT ASSUMEROLE ON 'arn:aws:iam::123456789012:role/redshift-copy' TO user analytics_loader;",
            "select 1",
        ),
        (
            "grant assumerole with operation scope",
            "grant assumerole on 'arn:aws:iam::123456789012:role/redshift-unload' to role analytics_role for unload",
            "select 1",
        ),
        (
            "grant assumerole inside string literal is ignored",
            "select 'GRANT ASSUMEROLE ON ''arn:aws:iam::123456789012:role/redshift-copy'' TO user analytics_loader' as statement",
            "select 'GRANT ASSUMEROLE ON ''arn:aws:iam::123456789012:role/redshift-copy'' TO user analytics_loader' as statement",
        ),
    ]);
}

#[test]
fn rewrites_create_user_password_disable() {
    assert_backend_sql(&[
        (
            "password disable",
            "CREATE USER analytics_loader PASSWORD DISABLE;",
            "CREATE USER analytics_loader PASSWORD NULL;",
        ),
        (
            "password disable with quoted user",
            r#"create user "reporting.user" password disable valid until '2026-12-31'"#,
            r#"create user "reporting.user" password NULL valid until '2026-12-31'"#,
        ),
        (
            "create user inside string literal is ignored",
            "select 'CREATE USER analytics_loader PASSWORD DISABLE' as statement",
            "select 'CREATE USER analytics_loader PASSWORD DISABLE' as statement",
        ),
    ]);
}

#[test]
fn rewrites_create_procedure_argument_modes() {
    assert_backend_sql(&[
        (
            "redshift argument mode after name",
            "CREATE OR REPLACE PROCEDURE test_sp2(f1 IN int, f2 INOUT varchar(256), out_var OUT varchar(256)) AS $$ BEGIN f2 := f2 || '+'; SELECT count(*) INTO out_var FROM my_etl; END; $$ LANGUAGE plpgsql;",
            "CREATE OR REPLACE PROCEDURE test_sp2(IN f1 int, INOUT f2 varchar(256), OUT out_var varchar(256) DEFAULT NULL) AS $$ BEGIN f2 := f2 || '+'; SELECT count(*) INTO out_var FROM my_etl; END; $$ LANGUAGE plpgsql;",
        ),
        (
            "postgres argument mode before name is unchanged",
            "create procedure update_counter(INOUT value integer) language plpgsql as $$ begin value := value + 1; end; $$",
            "create procedure update_counter(INOUT value integer) language plpgsql as $$ begin value := value + 1; end; $$",
        ),
        (
            "postgres out argument receives default",
            "create procedure read_counter(OUT value integer) language plpgsql as $$ begin value := 1; end; $$",
            "create procedure read_counter(OUT value integer DEFAULT NULL) language plpgsql as $$ begin value := 1; end; $$",
        ),
        (
            "create procedure inside string literal is ignored",
            "select 'CREATE OR REPLACE PROCEDURE test_sp2(f1 IN int, out_var OUT varchar(256)) LANGUAGE plpgsql' as statement",
            "select 'CREATE OR REPLACE PROCEDURE test_sp2(f1 IN int, out_var OUT varchar(256)) LANGUAGE plpgsql' as statement",
        ),
    ]);
}

#[test]
fn rewrites_create_function_plpython_language() {
    assert_backend_sql(&[
        (
            "python udf language",
            "CREATE FUNCTION f_py(x int) RETURNS int IMMUTABLE AS $$ return x + 1 $$ LANGUAGE plpythonu;",
            "CREATE FUNCTION f_py(x int) RETURNS int IMMUTABLE AS $$ return x + 1 $$ LANGUAGE plpython3u;",
        ),
        (
            "or replace python udf language with body mention",
            "create or replace function f_py(x int) returns int stable as $$ label = 'LANGUAGE plpythonu'; return x $$ language PLPYTHONU",
            "create or replace function f_py(x int) returns int stable as $$ label = 'LANGUAGE plpythonu'; return x $$ language plpython3u",
        ),
        (
            "create function inside string literal is ignored",
            "select 'CREATE FUNCTION f_py(x int) RETURNS int AS $$ return x $$ LANGUAGE plpythonu' as statement",
            "select 'CREATE FUNCTION f_py(x int) RETURNS int AS $$ return x $$ LANGUAGE plpythonu' as statement",
        ),
    ]);
}

#[test]
fn rewrites_create_function_sql_stable() {
    assert_backend_sql(&[
        (
            "sql udf stable before body",
            "CREATE FUNCTION f_sql_greater(float, float) RETURNS float STABLE AS $$ SELECT CASE WHEN $1 > $2 THEN $1 ELSE $2 END $$ LANGUAGE sql;",
            "CREATE FUNCTION f_sql_greater(float, float) RETURNS float AS $$ SELECT CASE WHEN $1 > $2 THEN $1 ELSE $2 END $$ LANGUAGE sql STABLE;",
        ),
        (
            "sql udf already postgres option order",
            "create function f_sql_identity(integer) returns integer as $$ select $1 $$ language sql stable",
            "create function f_sql_identity(integer) returns integer as $$ select $1 $$ language sql stable",
        ),
        (
            "create function inside string literal is ignored",
            "select 'CREATE FUNCTION f_sql_identity(integer) RETURNS integer STABLE AS $$ SELECT $1 $$ LANGUAGE sql' as statement",
            "select 'CREATE FUNCTION f_sql_identity(integer) RETURNS integer STABLE AS $$ SELECT $1 $$ LANGUAGE sql' as statement",
        ),
    ]);
}

#[test]
fn rewrites_create_model_to_noop() {
    assert_backend_sql(&[
        (
            "create model from table target function iam role",
            "CREATE MODEL churn_model FROM training.customers TARGET churned FUNCTION predict_churn IAM_ROLE 'arn:aws:iam::123456789012:role/redshift-ml';",
            "select 1",
        ),
        (
            "create model inside string literal is ignored",
            "select 'CREATE MODEL churn_model FROM training.customers TARGET churned FUNCTION predict_churn IAM_ROLE ''arn:aws:iam::123456789012:role/redshift-ml''' as statement",
            "select 'CREATE MODEL churn_model FROM training.customers TARGET churned FUNCTION predict_churn IAM_ROLE ''arn:aws:iam::123456789012:role/redshift-ml''' as statement",
        ),
    ]);
}

#[test]
fn rewrites_create_external_function_to_noop() {
    assert_backend_sql(&[
        (
            "create external function lambda",
            "CREATE EXTERNAL FUNCTION f_lambda(varchar) RETURNS varchar STABLE LAMBDA 'arn:aws:lambda:us-east-1:123456789012:function:redshift-fn' IAM_ROLE 'arn:aws:iam::123456789012:role/redshift-lambda';",
            "select 1",
        ),
        (
            "lowercase create external function lambda",
            "create external function f_lambda(integer) returns integer volatile lambda 'arn:aws:lambda:us-east-1:123456789012:function:redshift-fn'",
            "select 1",
        ),
        (
            "create external function inside string literal is ignored",
            "select 'CREATE EXTERNAL FUNCTION f_lambda(varchar) RETURNS varchar LAMBDA ''arn:aws:lambda:us-east-1:123456789012:function:redshift-fn''' as statement",
            "select 'CREATE EXTERNAL FUNCTION f_lambda(varchar) RETURNS varchar LAMBDA ''arn:aws:lambda:us-east-1:123456789012:function:redshift-fn''' as statement",
        ),
    ]);
}

#[test]
fn rewrites_explain_verbose() {
    assert_backend_sql(&[
        (
            "verbose annotation option",
            "EXPLAIN VERBOSE SELECT * FROM events;",
            "EXPLAIN (VERBOSE) SELECT * FROM events;",
        ),
        (
            "explain inside string literal is ignored",
            "select 'EXPLAIN VERBOSE SELECT * FROM events' as statement",
            "select 'EXPLAIN VERBOSE SELECT * FROM events' as statement",
        ),
    ]);
}

#[test]
fn rewrites_datashare_statements_to_noop() {
    assert_backend_sql(&[
        ("create datashare", "CREATE DATASHARE sales_share;", "select 1"),
        (
            "alter datashare",
            "alter datashare sales_share add schema analytics",
            "select 1",
        ),
        (
            "grant usage on datashare",
            "grant usage on datashare sales_share to namespace '12345678-1234-1234-1234-123456789012'",
            "select 1",
        ),
        (
            "datashare inside string literal is ignored",
            "select 'CREATE DATASHARE sales_share' as statement",
            "select 'CREATE DATASHARE sales_share' as statement",
        ),
    ]);
}

#[test]
fn rewrites_masking_policy_statements_to_noop() {
    assert_backend_sql(&[
        (
            "create masking policy",
            "CREATE MASKING POLICY mask_email WITH (email varchar(256)) USING ('***'::varchar(256));",
            "select 1",
        ),
        (
            "attach masking policy",
            "attach masking policy mask_email on users (email) to role analyst priority 10",
            "select 1",
        ),
        (
            "masking policy inside string literal is ignored",
            "select 'CREATE MASKING POLICY mask_email' as statement",
            "select 'CREATE MASKING POLICY mask_email' as statement",
        ),
    ]);
}

#[test]
fn rewrites_row_access_policy_statements_to_noop() {
    assert_backend_sql(&[
        (
            "create row access policy",
            "CREATE ROW ACCESS POLICY tenant_filter WITH (tenant_id int) USING (tenant_id = current_setting('app.tenant_id')::int);",
            "select 1",
        ),
        (
            "row access policy inside string literal is ignored",
            "select 'CREATE ROW ACCESS POLICY tenant_filter' as statement",
            "select 'CREATE ROW ACCESS POLICY tenant_filter' as statement",
        ),
    ]);
}

#[test]
fn rewrites_rand_to_random() {
    assert_backend_sql(&[
        (
            "uppercase rand",
            "select RAND() as sample_value from events",
            "select random() as sample_value from events",
        ),
        (
            "rand inside string literal is ignored",
            "select 'RAND()' as label, RAND() as sample_value from events",
            "select 'RAND()' as label, random() as sample_value from events",
        ),
    ]);
}

#[test]
fn rewrites_nvl2_to_case() {
    assert_backend_sql(&[
        (
            "nvl2",
            "select nvl2(payload, 'present', 'missing') as payload_state from events",
            "select CASE WHEN payload IS NOT NULL THEN 'present' ELSE 'missing' END as payload_state from events",
        ),
        (
            "nvl2 inside string literal is ignored",
            "select 'nvl2(payload, ''present'', ''missing'')' as label, NVL2(trim(payload), payload, 'missing') as payload_value from events",
            "select 'nvl2(payload, ''present'', ''missing'')' as label, CASE WHEN trim(payload) IS NOT NULL THEN payload ELSE 'missing' END as payload_value from events",
        ),
    ]);
}

#[test]
fn rewrites_len_to_length() {
    assert_backend_sql(&[
        (
            "len",
            "select len(payload) as payload_length from events",
            "select length(payload) as payload_length from events",
        ),
        (
            "len inside string literal is ignored",
            "select 'len(payload)' as label, LEN(trim(payload)) as payload_length from events",
            "select 'len(payload)' as label, length(trim(payload)) as payload_length from events",
        ),
    ]);
}

#[test]
fn rewrites_charindex_to_position() {
    assert_backend_sql(&[
        (
            "charindex",
            "select charindex('needle', payload) as needle_index from events",
            "select position('needle' in payload) as needle_index from events",
        ),
        (
            "charindex inside string literal is ignored",
            "select 'charindex(''needle'', payload)' as label, CHARINDEX(trim(needle), payload) as needle_index from events",
            "select 'charindex(''needle'', payload)' as label, position(trim(needle) in payload) as needle_index from events",
        ),
    ]);
}

#[test]
fn rewrites_substring_negative_start() {
    assert_backend_sql(&[
        (
            "negative start",
            "select substring(payload, -2, 6) as payload_prefix from events",
            "select substring(payload from (case when -2 < 1 then 1 else -2 end) for (case when -2 <= 0 then case when -2 + 6 - 1 <= 0 then 0 else -2 + 6 - 1 end else 6 end)) as payload_prefix from events",
        ),
        (
            "substring inside string literal is ignored",
            "select 'substring(payload, -2, 6)' as label, SUBSTRING(trim(payload), start_pos, payload_length) as payload_part from events",
            "select 'substring(payload, -2, 6)' as label, substring(trim(payload) from (case when start_pos < 1 then 1 else start_pos end) for (case when start_pos <= 0 then case when start_pos + payload_length - 1 <= 0 then 0 else start_pos + payload_length - 1 end else payload_length end)) as payload_part from events",
        ),
    ]);
}

#[test]
fn rewrites_strtol() {
    assert_backend_sql(&[
        (
            "strtol",
            "select strtol(hex_payload, 16) as payload_id from events",
            "select (select (case when left(trim(hex_payload), 1) = '-' then -1 else 1 end) * coalesce(sum((strpos('0123456789abcdefghijklmnopqrstuvwxyz', digit) - 1)::numeric * power((16)::numeric, (length(regexp_replace(trim(hex_payload), '^[+-]', '')) - ordinality)::numeric)), 0)::bigint from regexp_split_to_table(lower(regexp_replace(trim(hex_payload), '^[+-]', '')), '') with ordinality as strtol_digits(digit, ordinality)) as payload_id from events",
        ),
        (
            "strtol inside string literal is ignored",
            "select 'strtol(hex_payload, 16)' as label, STRTOL(trim(hex_payload), target_base) as payload_id from events",
            "select 'strtol(hex_payload, 16)' as label, (select (case when left(trim(trim(hex_payload)), 1) = '-' then -1 else 1 end) * coalesce(sum((strpos('0123456789abcdefghijklmnopqrstuvwxyz', digit) - 1)::numeric * power((target_base)::numeric, (length(regexp_replace(trim(trim(hex_payload)), '^[+-]', '')) - ordinality)::numeric)), 0)::bigint from regexp_split_to_table(lower(regexp_replace(trim(trim(hex_payload)), '^[+-]', '')), '') with ordinality as strtol_digits(digit, ordinality)) as payload_id from events",
        ),
    ]);
}

#[test]
fn rewrites_crc32() {
    assert_backend_sql(&[
        (
            "crc32",
            "select crc32(payload) as payload_crc from events",
            "select (with recursive crc32_input(data) as (select convert_to((payload)::text, 'UTF8')), crc32_state(step, crc) as (select 0, 4294967295::bigint union all select step + 1, (case when ((case when step % 8 = 0 then crc # get_byte(data, step / 8) else crc end) & 1) = 1 then (((case when step % 8 = 0 then crc # get_byte(data, step / 8) else crc end) >> 1) # 3988292384) else ((case when step % 8 = 0 then crc # get_byte(data, step / 8) else crc end) >> 1) end) from crc32_state, crc32_input where step < length(data) * 8) select case when data is null then null else (crc # 4294967295)::bigint end from crc32_state, crc32_input order by step desc limit 1) as payload_crc from events",
        ),
        (
            "crc32 inside string literal is ignored",
            "select 'crc32(payload)' as label, CRC32(trim(payload)) as payload_crc from events",
            "select 'crc32(payload)' as label, (with recursive crc32_input(data) as (select convert_to((trim(payload))::text, 'UTF8')), crc32_state(step, crc) as (select 0, 4294967295::bigint union all select step + 1, (case when ((case when step % 8 = 0 then crc # get_byte(data, step / 8) else crc end) & 1) = 1 then (((case when step % 8 = 0 then crc # get_byte(data, step / 8) else crc end) >> 1) # 3988292384) else ((case when step % 8 = 0 then crc # get_byte(data, step / 8) else crc end) >> 1) end) from crc32_state, crc32_input where step < length(data) * 8) select case when data is null then null else (crc # 4294967295)::bigint end from crc32_state, crc32_input order by step desc limit 1) as payload_crc from events",
        ),
    ]);
}

#[test]
fn rewrites_digest_functions() {
    assert_backend_sql(&[
        (
            "md5 digest",
            "select md5_digest(payload) as payload_md5 from events",
            "select md5((payload)::text) as payload_md5 from events",
        ),
        (
            "func sha1",
            "select func_sha1(payload) as payload_sha1 from events",
            "select encode(digest((payload)::text, 'sha1'), 'hex') as payload_sha1 from events",
        ),
        (
            "digest functions inside string literal are ignored",
            "select 'md5_digest(payload), func_sha1(payload)' as label, MD5_DIGEST(trim(payload)) as payload_md5 from events",
            "select 'md5_digest(payload), func_sha1(payload)' as label, md5((trim(payload))::text) as payload_md5 from events",
        ),
    ]);
}

#[test]
fn rewrites_regexp_substr() {
    assert_backend_sql(&[
        (
            "first match",
            "select regexp_substr(payload, '[0-9]+', 1, 1) as payload_match from events",
            "select regexp_match(payload, '[0-9]+') as payload_match from events",
        ),
        (
            "later occurrence",
            "select REGEXP_SUBSTR(payload, '[0-9]+', 3, 2) as payload_match from events",
            "select (select regexp_substr_match from regexp_matches(substring(payload from 3), '[0-9]+', 'g') with ordinality as regexp_substr_matches(regexp_substr_match, regexp_substr_ordinality) where regexp_substr_ordinality = 2) as payload_match from events",
        ),
        (
            "regexp_substr inside string literal is ignored",
            "select 'regexp_substr(payload, ''[0-9]+'', 1, 1)' as label, REGEXP_SUBSTR(trim(payload), pattern, start_pos, occurrence) as payload_match from events",
            "select 'regexp_substr(payload, ''[0-9]+'', 1, 1)' as label, (select regexp_substr_match from regexp_matches(substring(trim(payload) from start_pos), pattern, 'g') with ordinality as regexp_substr_matches(regexp_substr_match, regexp_substr_ordinality) where regexp_substr_ordinality = occurrence) as payload_match from events",
        ),
    ]);
}

#[test]
fn rewrites_regexp_count() {
    assert_backend_sql(&[
        (
            "regexp count",
            "select regexp_count(payload, '[0-9]+') as number_count from events",
            "select (case when payload is null or '[0-9]+' is null then null else (select count(*)::int from regexp_matches(payload, '[0-9]+', 'g')) end) as number_count from events",
        ),
        (
            "regexp_count inside string literal is ignored",
            "select 'regexp_count(payload, ''[0-9]+'')' as label, REGEXP_COUNT(trim(payload), pattern) as number_count from events",
            "select 'regexp_count(payload, ''[0-9]+'')' as label, (case when trim(payload) is null or pattern is null then null else (select count(*)::int from regexp_matches(trim(payload), pattern, 'g')) end) as number_count from events",
        ),
    ]);
}

#[test]
fn rewrites_regexp_instr() {
    assert_backend_sql(&[
        (
            "regexp instr",
            "select REGEXP_INSTR(payload, '[0-9]+', 1, 2, 0, 'i') as number_position from events",
            "select regexp_instr(payload, '[0-9]+', 1, 2, 0, 'i') as number_position from events",
        ),
        (
            "subexpression parameter",
            "select REGEXP_INSTR(payload, '([0-9]+)', 1, 1, 0, 'ie') as number_position from events",
            "select regexp_instr(payload, '([0-9]+)', 1, 1, 0, 'i', 1) as number_position from events",
        ),
        (
            "regexp_instr inside string literal is ignored",
            "select 'regexp_instr(payload, ''[0-9]+'')' as label, REGEXP_INSTR(trim(payload), pattern) as number_position from events",
            "select 'regexp_instr(payload, ''[0-9]+'')' as label, regexp_instr(trim(payload), pattern) as number_position from events",
        ),
    ]);
}

#[test]
fn rewrites_split_part_negative_position() {
    assert_backend_sql(&[
        (
            "negative position",
            "select split_part(payload, '/', -2) as parent_segment from events",
            "select reverse(split_part(reverse(payload), reverse('/'), 2)) as parent_segment from events",
        ),
        (
            "split_part inside string literal is ignored",
            "select 'split_part(payload, ''/'', -1)' as label, SPLIT_PART(trim(payload), delimiter, -1) as last_segment from events",
            "select 'split_part(payload, ''/'', -1)' as label, reverse(split_part(reverse(trim(payload)), reverse(delimiter), 1)) as last_segment from events",
        ),
        (
            "positive position is unchanged",
            "select split_part(payload, '/', 2) as child_segment from events",
            "select split_part(payload, '/', 2) as child_segment from events",
        ),
    ]);
}

#[test]
fn does_not_rewrite_functions_inside_string_literals() {
    let translated = translate(
        r#"select 'getdate() sysdate nvl(a,b)' as literal, "nvl" as quoted_name, getdate() as now"#,
    );
    assert!(
        translated
            .backend_sql
            .contains("'getdate() sysdate nvl(a,b)'"),
        "BackendSQL rewrote literal: {:?}",
        translated.backend_sql
    );
    assert!(
        translated.backend_sql.contains("CURRENT_TIMESTAMP as now"),
        "BackendSQL did not rewrite GETDATE(): {:?}",
        translated.backend_sql
    );
    assert!(
        translated.backend_sql.contains(r#""nvl" as quoted_name"#),
        "BackendSQL rewrote quoted identifier: {:?}",
        translated.backend_sql
    );
}

#[test]
fn rewrites_stv_tables_to_read_only_stubs() {
    let stv_stub = postgres_redshift_system_table_stub();
    assert_backend_sql(&[
        (
            "from stv table",
            "select * from stv_recents",
            &format!("select * from {stv_stub} as stv_recents"),
        ),
        (
            "join stv table with alias",
            "select r.query from events e join STV_RECENTS as r on r.query = e.query_id",
            &format!("select r.query from events e join {stv_stub} as r on r.query = e.query_id"),
        ),
        (
            "from stv table with where clause",
            "select query from stv_recents where userid = 100",
            &format!("select query from {stv_stub} as stv_recents where userid = 100"),
        ),
        (
            "stv table inside string literal is ignored",
            "select 'from stv_recents' as label from events",
            "select 'from stv_recents' as label from events",
        ),
    ]);
}

#[test]
fn rewrites_stl_tables_to_read_only_stubs() {
    let stl_stub = postgres_redshift_system_table_stub();
    assert_backend_sql(&[
        (
            "from stl table",
            "select * from stl_query",
            &format!("select * from {stl_stub} as stl_query"),
        ),
        (
            "join stl table with alias",
            "select q.query from events e join STL_QUERY as q on q.query = e.query_id",
            &format!("select q.query from events e join {stl_stub} as q on q.query = e.query_id"),
        ),
        (
            "stl table inside string literal is ignored",
            "select 'from stl_query' as label from events",
            "select 'from stl_query' as label from events",
        ),
    ]);
}

#[test]
fn rewrites_metadata_views_to_read_only_stubs() {
    let system_stub = postgres_redshift_system_table_stub();
    assert_backend_sql(&[
        (
            "from svv view",
            "select * from svv_table_info",
            &format!("select * from {system_stub} as svv_table_info"),
        ),
        (
            "join svl view with alias",
            "select q.query from events e join SVL_QUERY_SUMMARY as q on q.query = e.query_id",
            &format!(
                "select q.query from events e join {system_stub} as q on q.query = e.query_id"
            ),
        ),
        (
            "from sys view",
            "select query_id from sys_query_history where status = 'success'",
            &format!(
                "select query_id from {system_stub} as sys_query_history where status = 'success'"
            ),
        ),
        (
            "metadata view inside string literal is ignored",
            "select 'from svv_table_info join svl_query_summary' as label from events",
            "select 'from svv_table_info join svl_query_summary' as label from events",
        ),
    ]);
}

#[test]
fn rewrites_information_schema_columns_to_redshift_projection() {
    let columns_projection = postgres_information_schema_columns();
    assert_backend_sql(&[
        (
            "from information schema columns",
            "select column_name, remarks from information_schema.columns where table_schema = 'public'",
            &format!(
                "select column_name, remarks from {columns_projection} as columns where table_schema = 'public'"
            ),
        ),
        (
            "join information schema columns with alias",
            "select c.column_name, c.remarks from events e join information_schema.columns as c on c.table_name = e.table_name",
            &format!(
                "select c.column_name, c.remarks from events e join {columns_projection} as c on c.table_name = e.table_name"
            ),
        ),
        (
            "information schema columns inside string literal is ignored",
            "select 'from information_schema.columns' as label from events",
            "select 'from information_schema.columns' as label from events",
        ),
    ]);
}

#[test]
fn rewrites_default_function_in_create_table_backend_sql() {
    let translated =
        translate("create table events(id int, created_at timestamp default getdate())");
    assert!(
        !translated.backend_sql.to_lowercase().contains("getdate"),
        "BackendSQL still contains GETDATE(): {:?}",
        translated.backend_sql
    );
    assert!(
        translated.backend_sql.contains("default CURRENT_TIMESTAMP"),
        "BackendSQL did not rewrite default GETDATE(): {:?}",
        translated.backend_sql
    );
}

#[test]
fn rewrites_decode_to_case() {
    let translated =
        translate("select decode(status, 'ok', 1, 'failed', 0, -1) as score from events");
    assert_eq!(
        translated.backend_sql,
        "select CASE status WHEN 'ok' THEN 1 WHEN 'failed' THEN 0 ELSE -1 END as score from events"
    );
}

#[test]
fn rewrites_greatest_least_to_ignore_nulls() {
    assert_backend_sql(&[
        (
            "greatest",
            "select greatest(score_a, score_b, score_c) as best_score from events",
            "select (select max(greatest_value) from (values (score_a), (score_b), (score_c)) as redshift_greatest_values(greatest_value)) as best_score from events",
        ),
        (
            "least",
            "select LEAST(score_a, score_b, 0) as worst_score from events",
            "select (select min(least_value) from (values (score_a), (score_b), (0)) as redshift_least_values(least_value)) as worst_score from events",
        ),
        (
            "greatest and least inside string literal are ignored",
            "select 'greatest(score_a, score_b), least(score_a, score_b)' as label, GREATEST(score_a, score_b) as best_score from events",
            "select 'greatest(score_a, score_b), least(score_a, score_b)' as label, (select max(greatest_value) from (values (score_a), (score_b)) as redshift_greatest_values(greatest_value)) as best_score from events",
        ),
    ]);
}

#[test]
fn rewrites_round_negative_scale() {
    assert_backend_sql(&[
        (
            "negative scale",
            "select round(amount, -2) as rounded_amount from events",
            "select round((amount)::numeric, -2) as rounded_amount from events",
        ),
        (
            "round inside string literal is ignored",
            "select 'round(amount, -2)' as label, ROUND(total_amount, -3) as rounded_amount from events",
            "select 'round(amount, -2)' as label, round((total_amount)::numeric, -3) as rounded_amount from events",
        ),
        (
            "non-negative scale is unchanged",
            "select round(amount, 2) as rounded_amount from events",
            "select round(amount, 2) as rounded_amount from events",
        ),
    ]);
}

#[test]
fn rewrites_json_extract_path_text() {
    assert_backend_sql(&[
        (
            "path text",
            "select json_extract_path_text(payload, 'user', 'name') as user_name from events",
            "select jsonb_extract_path_text((payload)::jsonb, 'user', 'name') as user_name from events",
        ),
        (
            "trailing null_if_invalid boolean",
            "select JSON_EXTRACT_PATH_TEXT(payload, 'user', 'name', true) as user_name from events",
            "select jsonb_extract_path_text((payload)::jsonb, 'user', 'name') as user_name from events",
        ),
        (
            "json_extract_path_text inside string literal is ignored",
            "select 'json_extract_path_text(payload, ''user'', true)' as label, JSON_EXTRACT_PATH_TEXT(trim(payload), 'user', 'name', false) as user_name from events",
            "select 'json_extract_path_text(payload, ''user'', true)' as label, jsonb_extract_path_text((trim(payload))::jsonb, 'user', 'name') as user_name from events",
        ),
    ]);
}

#[test]
fn rewrites_json_extract_array_element_text() {
    assert_backend_sql(&[
        (
            "array element text",
            "select json_extract_array_element_text(payload, 0) as first_event from events",
            "select ((payload)::jsonb -> 0)::text as first_event from events",
        ),
        (
            "json_extract_array_element_text inside string literal is ignored",
            "select 'json_extract_array_element_text(payload, 0)' as label, JSON_EXTRACT_ARRAY_ELEMENT_TEXT(trim(payload), event_index) as event_value from events",
            "select 'json_extract_array_element_text(payload, 0)' as label, ((trim(payload))::jsonb -> event_index)::text as event_value from events",
        ),
    ]);
}

#[test]
fn rewrites_json_array_length() {
    assert_backend_sql(&[
        (
            "array length",
            "select json_array_length(payload) as event_count from events",
            "select jsonb_array_length((payload)::jsonb) as event_count from events",
        ),
        (
            "json_array_length inside string literal is ignored",
            "select 'json_array_length(payload)' as label, JSON_ARRAY_LENGTH(trim(payload)) as event_count from events",
            "select 'json_array_length(payload)' as label, jsonb_array_length((trim(payload))::jsonb) as event_count from events",
        ),
    ]);
}

#[test]
fn rewrites_json_parse() {
    assert_backend_sql(&[
        (
            "json parse",
            "select json_parse(payload) as payload_json from events",
            "select (payload)::jsonb as payload_json from events",
        ),
        (
            "json_parse inside string literal is ignored",
            "select 'json_parse(payload)' as label, JSON_PARSE(trim(payload)) as payload_json from events",
            "select 'json_parse(payload)' as label, (trim(payload))::jsonb as payload_json from events",
        ),
    ]);
}

#[test]
fn rewrites_is_valid_json() {
    assert_backend_sql(&[
        (
            "valid json",
            "select is_valid_json(payload) as payload_is_json from events",
            "select coalesce(json_valid((payload)::text), false) as payload_is_json from events",
        ),
        (
            "valid json array",
            "select IS_VALID_JSON_ARRAY(payload) as payload_is_json_array from events",
            "select (case when coalesce(json_valid((payload)::text), false) then jsonb_typeof((payload)::jsonb) = 'array' else false end) as payload_is_json_array from events",
        ),
        (
            "is_valid_json inside string literal is ignored",
            "select 'is_valid_json(payload)' as label, IS_VALID_JSON(trim(payload)) as payload_is_json from events",
            "select 'is_valid_json(payload)' as label, coalesce(json_valid((trim(payload))::text), false) as payload_is_json from events",
        ),
    ]);
}

#[test]
fn rewrites_partiql_navigation() {
    assert_backend_sql(&[
        (
            "dot path",
            "select payload.user.name as user_name from events",
            "select (payload)::jsonb -> 'user' ->> 'name' as user_name from events",
        ),
        (
            "subscript path",
            "select payload.events[0].type as first_event_type from events",
            "select (payload)::jsonb -> 'events' -> 0 ->> 'type' as first_event_type from events",
        ),
        (
            "partiql navigation inside string literal is ignored",
            "select 'payload.user.name payload[0]' as label, payload.items[0] as first_item from events",
            "select 'payload.user.name payload[0]' as label, (payload)::jsonb -> 'items' ->> 0 as first_item from events",
        ),
    ]);
}

#[test]
fn rewrites_object_transform() {
    assert_backend_sql(&[
        (
            "keep and set paths",
            r#"select object_transform(payload KEEP '"user"."name"' SET '"user"."active"', true, '"event_count"', 3) as transformed_payload from events"#,
            r#"select jsonb_set(jsonb_set(jsonb_set(jsonb_set('{}'::jsonb, ARRAY['user'], coalesce('{}'::jsonb #> ARRAY['user'], '{}'::jsonb), true), ARRAY['user', 'name'], ((payload)::jsonb #> ARRAY['user', 'name']), true), ARRAY['user', 'active'], to_jsonb(true), true), ARRAY['event_count'], to_jsonb(3), true) as transformed_payload from events"#,
        ),
        (
            "object_transform inside string literal is ignored",
            r#"select 'object_transform(payload KEEP ''"user"'')' as label, OBJECT_TRANSFORM(trim(payload) SET '"user"."name"', username) as transformed_payload from events"#,
            r#"select 'object_transform(payload KEEP ''"user"'')' as label, jsonb_set(jsonb_set('{}'::jsonb, ARRAY['user'], coalesce('{}'::jsonb #> ARRAY['user'], '{}'::jsonb), true), ARRAY['user', 'name'], to_jsonb(username), true) as transformed_payload from events"#,
        ),
    ]);
}

#[test]
fn rewrites_dateadd_and_datediff() {
    let translated = translate(
        "select dateadd(day, 7, created_at) as expires_at, datediff(hour, started_at, ended_at) as hours from events",
    );
    assert!(
        translated
            .backend_sql
            .contains("created_at + (7 * interval '1 day') as expires_at"),
        "BackendSQL did not rewrite DATEADD(): {:?}",
        translated.backend_sql
    );
    assert!(
        translated
            .backend_sql
            .contains("floor(extract(epoch from (ended_at - started_at)) / 3600)::int as hours"),
        "BackendSQL did not rewrite DATEDIFF(): {:?}",
        translated.backend_sql
    );
}

#[test]
fn rewrites_timeofday() {
    assert_backend_sql(&[
        (
            "timeofday",
            "select timeofday() as current_time_text",
            "select clock_timestamp()::text as current_time_text",
        ),
        (
            "timeofday inside string literal is ignored",
            "select 'timeofday()' as label, TIMEOFDAY() as current_time_text",
            "select 'timeofday()' as label, clock_timestamp()::text as current_time_text",
        ),
    ]);
}

#[test]
fn rewrites_convert_timezone() {
    assert_backend_sql(&[
        (
            "utc to jst",
            "select convert_timezone('UTC', 'JST', created_at) as created_at_jst from events",
            "select created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Tokyo' as created_at_jst from events",
        ),
        (
            "convert_timezone inside string literal is ignored",
            "select 'convert_timezone(''UTC'', ''JST'', created_at)' as label, CONVERT_TIMEZONE('utc', 'jst', created_at) as created_at_jst from events",
            "select 'convert_timezone(''UTC'', ''JST'', created_at)' as label, created_at AT TIME ZONE 'utc' AT TIME ZONE 'Asia/Tokyo' as created_at_jst from events",
        ),
        (
            "arbitrary iana zones pass through",
            "select convert_timezone('UTC', 'America/New_York', ts) as local_ts from events",
            "select ts AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' as local_ts from events",
        ),
        (
            "expression argument is preserved",
            "select convert_timezone('UTC', 'Europe/London', current_timestamp) as ts",
            "select current_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/London' as ts",
        ),
    ]);
}

#[test]
fn rewrites_date_part_and_date_trunc_part_aliases() {
    assert_backend_sql(&[
        (
            "date_part weekday alias",
            "select date_part(weekday, created_at) as weekday from events",
            "select date_part('dow', created_at) as weekday from events",
        ),
        (
            "date_trunc quarter alias",
            "select date_trunc(qtr, created_at) as quarter_start from events",
            "select date_trunc('quarter', created_at) as quarter_start from events",
        ),
        (
            "aliases inside string literal are ignored",
            "select 'date_part(weekday, created_at)' as label, DATE_PART(dw, created_at) as weekday from events",
            "select 'date_part(weekday, created_at)' as label, date_part('dow', created_at) as weekday from events",
        ),
    ]);
}

#[test]
fn rewrites_last_day() {
    assert_backend_sql(&[
        (
            "last day",
            "select last_day(created_at) as month_end from events",
            "select (date_trunc('month', created_at) + interval '1 month - 1 day')::date as month_end from events",
        ),
        (
            "last day inside string literal is ignored",
            "select 'last_day(created_at)' as label, LAST_DAY(created_at::date) as month_end from events",
            "select 'last_day(created_at)' as label, (date_trunc('month', created_at::date) + interval '1 month - 1 day')::date as month_end from events",
        ),
    ]);
}

#[test]
fn rewrites_months_between() {
    assert_backend_sql(&[
        (
            "months between",
            "select months_between(ended_at, started_at) as months_elapsed from events",
            "select (extract(year from age(ended_at, started_at)) * 12 + extract(month from age(ended_at, started_at))) as months_elapsed from events",
        ),
        (
            "months between inside string literal is ignored",
            "select 'months_between(ended_at, started_at)' as label, MONTHS_BETWEEN(ended_at::date, started_at::date) as months_elapsed from events",
            "select 'months_between(ended_at, started_at)' as label, (extract(year from age(ended_at::date, started_at::date)) * 12 + extract(month from age(ended_at::date, started_at::date))) as months_elapsed from events",
        ),
    ]);
}

#[test]
fn rewrites_add_months() {
    assert_backend_sql(&[
        (
            "add months",
            "select add_months(created_at, 2) as renewal_at from events",
            "select created_at + (2 * interval '1 month') as renewal_at from events",
        ),
        (
            "add months inside string literal is ignored",
            "select 'add_months(created_at, 2)' as label, ADD_MONTHS(created_at::date, months_to_add) as renewal_at from events",
            "select 'add_months(created_at, 2)' as label, created_at::date + (months_to_add * interval '1 month') as renewal_at from events",
        ),
    ]);
}

#[test]
fn rewrites_next_day() {
    assert_backend_sql(&[
        (
            "next day",
            "select next_day(created_at, 'Tuesday') as next_tuesday from events",
            "select ((created_at)::date + ((2 - extract(dow from (created_at)::date)::int + 6) % 7 + 1))::date as next_tuesday from events",
        ),
        (
            "next day inside string literal is ignored",
            "select 'next_day(created_at, ''Tue'')' as label, NEXT_DAY(created_at::date, 'Tue') as next_tuesday from events",
            "select 'next_day(created_at, ''Tue'')' as label, ((created_at::date)::date + ((2 - extract(dow from (created_at::date)::date)::int + 6) % 7 + 1))::date as next_tuesday from events",
        ),
    ]);
}

#[test]
fn rewrites_to_date_and_to_timestamp_timezone_formats() {
    assert_backend_sql(&[
        (
            "to_timestamp trailing timezone abbreviation",
            "select to_timestamp(raw_value, 'YYYY-MM-DD HH24:MI:SS TZ') as observed_at from events",
            "select to_timestamp(regexp_replace(raw_value, '[[:space:]]*([[:alpha:]_/]+|[+-][0-9]{2}(:?[0-9]{2})?)$', ''), 'YYYY-MM-DD HH24:MI:SS') as observed_at from events",
        ),
        (
            "to_date trailing timezone offset inside string literal is ignored",
            "select 'to_date(raw_value, ''YYYY-MM-DD OF'')' as label, TO_DATE(raw_value, 'YYYY-MM-DD OF') as observed_date from events",
            "select 'to_date(raw_value, ''YYYY-MM-DD OF'')' as label, to_date(regexp_replace(raw_value, '[[:space:]]*([[:alpha:]_/]+|[+-][0-9]{2}(:?[0-9]{2})?)$', ''), 'YYYY-MM-DD') as observed_date from events",
        ),
    ]);
}

#[test]
fn rewrites_to_char_timezone_formats() {
    assert_backend_sql(&[
        (
            "timezone abbreviation",
            "select to_char(observed_at, 'YYYY-MM-DD HH24:MI:SS TZ') as observed_at_text from events",
            r#"select to_char(observed_at, 'YYYY-MM-DD HH24:MI:SS "UTC"') as observed_at_text from events"#,
        ),
        (
            "timezone offset inside string literal is ignored",
            "select 'to_char(observed_at, ''YYYY-MM-DD OF'')' as label, TO_CHAR(observed_at, 'YYYY-MM-DD OF') as observed_at_text from events",
            r#"select 'to_char(observed_at, ''YYYY-MM-DD OF'')' as label, to_char(observed_at, 'YYYY-MM-DD "+00"') as observed_at_text from events"#,
        ),
    ]);
}

#[test]
fn rewrites_listagg_within_group() {
    let translated = translate(
        "select listagg(name, ',') within group (order by created_at desc) as names from events",
    );
    assert_eq!(
        translated.backend_sql,
        "select string_agg(name, ',' ORDER BY created_at desc) as names from events"
    );
}

#[test]
fn rewrites_listagg_window_within_group() {
    assert_backend_sql(&[
        (
            "partitioned window",
            "select listagg(name, ',') within group (order by created_at desc) over (partition by account_id) as names from events",
            "select array_to_string(array_agg(name) OVER (partition by account_id ORDER BY created_at desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), ',') as names from events",
        ),
        (
            "empty over window",
            "select LISTAGG(name, ',') WITHIN GROUP (ORDER BY created_at) OVER () as names from events",
            "select array_to_string(array_agg(name) OVER (ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), ',') as names from events",
        ),
    ]);
}

#[test]
fn rewrites_median_to_percentile_cont() {
    let translated = translate("select median(duration_ms) as p50_duration from events");
    assert_eq!(
        translated.backend_sql,
        "select percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms) as p50_duration from events"
    );
}

#[test]
fn rewrites_boolean_bit_aggregates() {
    assert_backend_sql(&[
        (
            "boolean bit aggregates",
            "select bit_and(is_ready) as all_ready, bit_or(is_ready) as any_ready from events",
            "select bool_and(is_ready) as all_ready, bool_or(is_ready) as any_ready from events",
        ),
        (
            "boolean bit aggregates inside string literal are ignored",
            "select 'bit_and(is_ready), bit_or(is_ready)' as label, BIT_AND(is_ready) as all_ready from events",
            "select 'bit_and(is_ready), bit_or(is_ready)' as label, bool_and(is_ready) as all_ready from events",
        ),
    ]);
}

#[test]
fn rewrites_ratio_to_report_over() {
    assert_backend_sql(&[
        (
            "partitioned window",
            "select ratio_to_report(revenue) over (partition by category) as revenue_share from sales",
            "select revenue / SUM(revenue) OVER (partition by category) as revenue_share from sales",
        ),
        (
            "ratio_to_report inside string literal is ignored",
            "select 'ratio_to_report(revenue) over ()' as label, RATIO_TO_REPORT(revenue) OVER () as revenue_share from sales",
            "select 'ratio_to_report(revenue) over ()' as label, revenue / SUM(revenue) OVER () as revenue_share from sales",
        ),
    ]);
}

#[test]
fn rewrites_lateral_column_alias_references() {
    assert_backend_sql(&[
        (
            "select list alias reference",
            "select clicks / impressions as probability, round(100 * probability, 1) as percentage from raw_data",
            "select clicks / impressions as probability, round(100 * (clicks / impressions), 1) as percentage from raw_data",
        ),
        (
            "alias reference chains through later select items",
            "select amount as subtotal, subtotal * tax_rate as tax, subtotal + tax as total from invoices",
            "select amount as subtotal, (amount) * tax_rate as tax, (amount) + ((amount) * tax_rate) as total from invoices",
        ),
        (
            "implicit alias reference",
            "select clicks / impressions probability, probability * 100 percentage from raw_data",
            "select clicks / impressions as probability, (clicks / impressions) * 100 as percentage from raw_data",
        ),
        (
            "string literal and qualified column are unchanged",
            "select clicks / impressions as probability, 'probability' as label, raw_data.probability as source_probability from raw_data",
            "select clicks / impressions as probability, 'probability' as label, raw_data.probability as source_probability from raw_data",
        ),
    ]);
}

#[test]
fn rewrites_pg_table_metadata_views() {
    let table_def = postgres_pg_table_def();
    let table_info = postgres_pg_table_info();
    assert_backend_sql(&[
        (
            "from pg_table_def",
            "select schemaname, tablename, \"column\", encoding, distkey, sortkey, notnull from pg_table_def where tablename = 'events'",
            &format!(
                "select schemaname, tablename, \"column\", encoding, distkey, sortkey, notnull from {table_def} as pg_table_def where tablename = 'events'"
            ),
        ),
        (
            "join pg_table_info with alias",
            "select i.schema, i.\"table\", i.tbl_rows from events e join PG_TABLE_INFO as i on i.\"table\" = e.table_name",
            &format!(
                "select i.schema, i.\"table\", i.tbl_rows from events e join {table_info} as i on i.\"table\" = e.table_name"
            ),
        ),
        (
            "pg table metadata inside string literal is ignored",
            "select 'from pg_table_def join pg_table_info' as label from events",
            "select 'from pg_table_def join pg_table_info' as label from events",
        ),
    ]);
}

#[test]
fn rewrites_approximate_count_distinct() {
    assert_backend_sql(&[
        (
            "approximate count distinct",
            "select approximate count(distinct user_id) as active_users from events",
            "select count(distinct user_id) as active_users from events",
        ),
        (
            "approximate count inside string literal is ignored",
            "select 'approximate count(distinct user_id)' as label, approximate count(distinct user_id) as active_users from events",
            "select 'approximate count(distinct user_id)' as label, count(distinct user_id) as active_users from events",
        ),
    ]);
}

#[test]
fn rewrites_like_default_escape() {
    assert_backend_sql(&[
        (
            "implicit backslash escape",
            r"select id from events where payload like 'promo\_%'",
            r"select id from events where payload like 'promo\_%' ESCAPE '\'",
        ),
        (
            "explicit escape unchanged",
            r"select id from events where payload like 'promo\_%' escape '\'",
            r"select id from events where payload like 'promo\_%' escape '\'",
        ),
        (
            "like inside string literal is ignored",
            r"select 'payload like ''promo\_%''' as predicate from events",
            r"select 'payload like ''promo\_%''' as predicate from events",
        ),
    ]);
}

#[test]
fn rewrites_null_ordering_defaults() {
    assert_backend_sql(&[
        (
            "ascending and descending defaults",
            "select id from events order by priority desc, created_at asc, id limit 10",
            "select id from events order by priority desc NULLS FIRST, created_at asc NULLS LAST, id NULLS LAST limit 10",
        ),
        (
            "explicit null ordering unchanged",
            "select id from events order by priority desc nulls last, created_at nulls first",
            "select id from events order by priority desc nulls last, created_at nulls first",
        ),
        (
            "order by inside string literal is ignored",
            "select 'order by created_at desc' as label from events order by id",
            "select 'order by created_at desc' as label from events order by id NULLS LAST",
        ),
    ]);
}

#[test]
fn rewrites_boolean_literals() {
    assert_backend_sql(&[
        (
            "typed boolean yes literal",
            "select boolean 'yes' as enabled",
            "select TRUE as enabled",
        ),
        (
            "typed boolean numeric literal",
            "select boolean 0 as enabled",
            "select FALSE as enabled",
        ),
        (
            "boolean column default literal",
            "create table events(id integer, active boolean default y, archived bool default 0)",
            "create table events(id integer, active boolean default TRUE, archived bool default FALSE)",
        ),
    ]);
}

#[test]
fn leaves_unsupported_function_forms_unchanged() {
    let input = "select dateadd(quarter, 1, created_at), datediff(quarter, started_at, ended_at), listagg(name) from events";
    let translated = translate(input);
    assert_eq!(
        translated.backend_sql, input,
        "BackendSQL = {:?}, want unchanged {input:?}",
        translated.backend_sql
    );
}

#[test]
fn extracts_mixed_table_attributes() {
    let translated = translate(
        "create table if not exists analytics.events(\n\t\tid integer encode az64,\n\t\tcreated_at timestamp default getdate(),\n\t\tpayload varchar(64)\n\t) diststyle even distkey (id) sortkey(created_at,id) backup yes",
    );

    let lower_backend = translated.backend_sql.to_lowercase();
    for forbidden in [
        "diststyle",
        "distkey",
        "sortkey",
        "encode",
        "backup",
        "getdate",
    ] {
        assert!(
            !lower_backend.contains(forbidden),
            "BackendSQL contains Redshift-only token {forbidden:?}: {}",
            translated.backend_sql
        );
    }
    assert!(
        translated.backend_sql.contains("default CURRENT_TIMESTAMP"),
        "BackendSQL did not rewrite default GETDATE(): {}",
        translated.backend_sql
    );
    assert_eq!(
        translated.metadata_effects.len(),
        1,
        "MetadataEffects = {:?}",
        translated.metadata_effects
    );
    let effect = &translated.metadata_effects[0];
    assert!(
        effect.schema == "analytics"
            && effect.table == "events"
            && effect.value == "even"
            && effect.name == "id"
            && effect.backup == "yes",
        "metadata effect = {effect:?}"
    );
    assert_eq!(
        effect.sort_keys,
        vec!["created_at".to_string(), "id".to_string()],
        "sort keys = {:?}",
        effect.sort_keys
    );
    assert!(
        effect.columns.len() == 3
            && effect.columns[0].encoding == "az64"
            && effect.columns[1].default_value == "getdate()",
        "columns = {:?}",
        effect.columns
    );
}

#[test]
fn rewrites_create_table_like_options() {
    let translated = translate(
        "create table analytics.events_copy (like analytics.events including defaults) diststyle even",
    );
    assert_eq!(
        translated.backend_sql,
        "create table analytics.events_copy (LIKE analytics.events INCLUDING DEFAULTS)"
    );
    assert_eq!(
        translated.metadata_effects.len(),
        1,
        "MetadataEffects = {:?}",
        translated.metadata_effects
    );
    let effect = &translated.metadata_effects[0];
    assert!(
        effect.schema == "analytics" && effect.table == "events_copy" && effect.value == "even",
        "metadata effect = {effect:?}"
    );
    assert!(
        effect.columns.is_empty(),
        "columns = {:?}, want none for LIKE table",
        effect.columns
    );
}

#[test]
fn rewrites_temporary_table_scope() {
    let cases = [
        (
            "temp table",
            "create temp table session_events(id integer distkey, payload varchar(16)) diststyle key sortkey(id)",
            "create temp table session_events(id integer, payload text check (octet_length(payload) <= 16)) on commit preserve rows",
        ),
        (
            "temporary table if not exists",
            "create temporary table if not exists session_events(id integer)",
            "create temporary table if not exists session_events(id integer) on commit preserve rows",
        ),
    ];
    for (name, sql, want) in cases {
        let translated = translate(sql);
        assert_eq!(translated.backend_sql, want, "{name}");
        assert!(
            translated.metadata_effects.is_empty(),
            "{name}: MetadataEffects = {:?}, want none for session-scoped temporary table",
            translated.metadata_effects
        );
    }
}

#[test]
fn rewrites_create_external_table_location() {
    let translated = translate(
        "create external table spectrum.events(id integer, payload varchar(16)) stored as parquet location 's3://devcloud/events/'",
    );
    assert_eq!(
        translated.backend_sql,
        "create table spectrum.events(id integer, payload text check (octet_length(payload) <= 16))"
    );
    let lower = translated.backend_sql.to_lowercase();
    assert!(
        !lower.contains("external") && !lower.contains("location"),
        "BackendSQL contains external table syntax: {:?}",
        translated.backend_sql
    );
    assert!(
        translated.metadata_effects.len() == 1
            && translated.metadata_effects[0].schema == "spectrum"
            && translated.metadata_effects[0].table == "events",
        "MetadataEffects = {:?}",
        translated.metadata_effects
    );
    assert!(
        translated.metadata_effects[0].columns.len() == 2
            && translated.metadata_effects[0].columns[1].data_type == "varchar(16)",
        "columns = {:?}",
        translated.metadata_effects[0].columns
    );
}

#[test]
fn rewrites_create_external_schema_from_data_catalog() {
    let translated = translate(
        "create external schema if not exists spectrum from data catalog database 'analytics' iam_role default",
    );
    assert_eq!(
        translated.backend_sql,
        "create schema if not exists spectrum"
    );
    let lower = translated.backend_sql.to_lowercase();
    assert!(
        !lower.contains("external") && !lower.contains("data catalog"),
        "BackendSQL contains external schema syntax: {:?}",
        translated.backend_sql
    );
}

#[test]
fn rewrites_create_materialized_view_auto_refresh_yes() {
    let translated = translate(
        "create materialized view analytics.daily_events auto refresh yes as select event_date, count(*) as count from events group by event_date",
    );
    assert_eq!(
        translated.backend_sql,
        "create materialized view analytics.daily_events as select event_date, count(*) as count from events group by event_date"
    );
    assert!(
        !translated
            .backend_sql
            .to_lowercase()
            .contains("auto refresh"),
        "BackendSQL contains Redshift materialized view option: {:?}",
        translated.backend_sql
    );
}

#[test]
fn rewrites_merge_into_update_insert() {
    assert_backend_sql(&[
        (
            "matched update and not matched insert",
            "merge into analytics.events as target using staging.events as source on target.id = source.id when matched then update set payload = source.payload, updated_at = getdate() when not matched then insert (id, payload, updated_at) values (source.id, source.payload, getdate())",
            "insert into analytics.events (id, payload, updated_at) select source.id, source.payload, CURRENT_TIMESTAMP from staging.events as source where not exists (select 1 from analytics.events as target where target.id = source.id); update analytics.events as target set payload = source.payload, updated_at = CURRENT_TIMESTAMP from staging.events as source where target.id = source.id",
        ),
        (
            // ON condition references the column that the UPDATE rewrites:
            // emitting INSERT first keeps the membership check on pre-update
            // state (see the legacy test comment).
            "on clause references updated column",
            "merge into target using source on target.k = source.k when matched then update set k = source.k_new when not matched then insert (k, v) values (source.k, source.v)",
            "insert into target (k, v) select source.k, source.v from source where not exists (select 1 from target where target.k = source.k); update target set k = source.k_new from source where target.k = source.k",
        ),
        (
            // Uppercase / mixed-case MERGE keywords exercise the
            // case-insensitive keyword matcher.
            "uppercase merge keywords",
            "MERGE INTO analytics.events AS target USING staging.events AS source ON target.id = source.id WHEN MATCHED THEN UPDATE SET payload = source.payload WHEN NOT MATCHED THEN INSERT (id, payload) VALUES (source.id, source.payload)",
            "insert into analytics.events (id, payload) select source.id, source.payload from staging.events AS source where not exists (select 1 from analytics.events AS target where target.id = source.id); update analytics.events AS target set payload = source.payload from staging.events AS source where target.id = source.id",
        ),
    ]);
}

#[test]
fn rewrites_insert_values_default_identity() {
    assert_backend_sql(&[
        (
            "default identity column value",
            "insert into analytics.events(event_id) values(default)",
            "insert into analytics.events(event_id) default values",
        ),
        (
            "uppercase with semicolon",
            "INSERT INTO events VALUES ( DEFAULT );",
            "INSERT INTO events default values",
        ),
    ]);
}

#[test]
fn removes_insert_select_returning() {
    assert_backend_sql(&[
        (
            "insert select returning all columns",
            "insert into analytics.events(id, created_at) select id, getdate() from staging.events returning *",
            "insert into analytics.events(id, created_at) select id, CURRENT_TIMESTAMP from staging.events",
        ),
        (
            "returning inside string literal is ignored",
            "insert into audit.messages(message) select 'returning *' from staging.messages returning message;",
            "insert into audit.messages(message) select 'returning *' from staging.messages",
        ),
    ]);
}

#[test]
fn rewrites_create_view_no_schema_binding() {
    assert_backend_sql(&[
        (
            "late binding view",
            "create view analytics.recent_events as select id, created_at from events with no schema binding",
            "create view analytics.recent_events as select id, created_at from events",
        ),
        (
            "quoted literal is unchanged",
            "create view analytics.labels as select 'with no schema binding' as label from events with no schema binding",
            "create view analytics.labels as select 'with no schema binding' as label from events",
        ),
        (
            "uppercase with semicolon",
            "create view analytics.recent_events as select id from events WITH NO SCHEMA BINDING;",
            "create view analytics.recent_events as select id from events;",
        ),
    ]);
}

#[test]
fn rewrites_alter_column_encode() {
    let cases = [
        (
            "alter column encode",
            "alter table analytics.events alter column payload encode zstd",
            "alter table analytics.events alter column payload set statistics -1",
        ),
        (
            "if exists without column keyword",
            "ALTER TABLE IF EXISTS events ALTER payload ENCODE az64;",
            "alter table if exists events alter column payload set statistics -1",
        ),
    ];
    for (name, sql, want) in cases {
        let translated = translate(sql);
        assert_eq!(translated.backend_sql, want, "{name}");
        assert!(
            !translated.backend_sql.to_lowercase().contains("encode"),
            "{name}: BackendSQL contains Redshift ENCODE syntax: {:?}",
            translated.backend_sql
        );
    }
}

#[test]
fn rewrites_alter_table_add_column_default_identity() {
    let cases = [
        (
            "add column default identity",
            "alter table analytics.events add column event_id bigint default identity(1, 1)",
            "alter table analytics.events add column event_id bigint generated by default as identity (start with 1 increment by 1)",
        ),
        (
            "if exists with spaced identity arguments",
            "ALTER TABLE IF EXISTS events ADD id integer DEFAULT IDENTITY ( 100 , 5 );",
            "alter table if exists events add column id integer generated by default as identity (start with 100 increment by 5)",
        ),
    ];
    for (name, sql, want) in cases {
        let translated = translate(sql);
        assert_eq!(translated.backend_sql, want, "{name}");
        assert!(
            !translated
                .backend_sql
                .to_lowercase()
                .contains("default identity"),
            "{name}: BackendSQL contains Redshift DEFAULT IDENTITY syntax: {:?}",
            translated.backend_sql
        );
    }
}

#[test]
fn rewrites_truncate_immediate_commit() {
    assert_backend_sql(&[
        (
            "truncate table",
            "truncate table analytics.events",
            "commit; truncate table analytics.events",
        ),
        (
            "uppercase with semicolon",
            "TRUNCATE events;",
            "commit; TRUNCATE events",
        ),
    ]);
}

#[test]
fn rewrites_qualify_to_subquery() {
    assert_backend_sql(&[
        (
            "window alias predicate",
            "select user_id, row_number() over (partition by user_id order by created_at desc) as rn from events qualify rn = 1",
            "select * from (select user_id, row_number() over (partition by user_id order by created_at desc) as rn from events) as devcloud_qualify where rn = 1",
        ),
        (
            "preserves outer order by",
            "select user_id, rank() over (order by score desc) as rank from scores qualify rank <= 10 order by rank",
            "select * from (select user_id, rank() over (order by score desc) as rank from scores) as devcloud_qualify where rank <= 10 order by rank NULLS LAST",
        ),
        (
            "direct window predicate",
            "select user_id, event_id from events qualify row_number() over (partition by user_id order by created_at desc) = 1",
            "select user_id, event_id from (select user_id, event_id, row_number() over (partition by user_id order by created_at desc) as __devcloud_qualify_1 from events) as devcloud_qualify where __devcloud_qualify_1 = 1",
        ),
        (
            "qualify inside string literal is ignored",
            "select 'qualify rn = 1' as label, row_number() over (order by id) as rn from events qualify rn = 1",
            "select * from (select 'qualify rn = 1' as label, row_number() over (order by id) as rn from events) as devcloud_qualify where rn = 1",
        ),
    ]);
}

#[test]
fn rewrites_select_top_to_limit() {
    assert_backend_sql(&[
        (
            "top rows",
            "select top 10 id, created_at from events order by created_at desc",
            "select id, created_at from events order by created_at desc NULLS FIRST limit 10",
        ),
        (
            "parenthesized top rows with semicolon",
            "SELECT TOP (5) id, getdate() as observed_at FROM events;",
            "SELECT id, CURRENT_TIMESTAMP as observed_at FROM events limit 5",
        ),
    ]);
}

/// Shared assertion for the column-type rewrite tests: backend SQL plus the
/// single CREATE TABLE metadata effect's second column data type.
fn assert_column_type_rewrite(cases: &[(&str, &str, &str, &str)]) {
    for (name, sql, want, data_type) in cases {
        let translated = translate(sql);
        assert_eq!(&translated.backend_sql, want, "{name}");
        assert!(
            translated.metadata_effects.len() == 1
                && translated.metadata_effects[0].columns.len() == 2,
            "{name}: MetadataEffects = {:?}",
            translated.metadata_effects
        );
        assert_eq!(
            &translated.metadata_effects[0].columns[1].data_type, data_type,
            "{name}: column metadata = {:?}, want data type {data_type:?}",
            translated.metadata_effects[0].columns[1]
        );
    }
}

#[test]
fn rewrites_super_column_type_to_jsonb() {
    assert_column_type_rewrite(&[
        (
            "create table super column",
            "create table events(id integer, payload SUPER)",
            "create table events(id integer, payload jsonb)",
            "super",
        ),
        (
            "create table hllsketch column",
            "create table metrics(id integer, estimate HLLSKETCH)",
            "create table metrics(id integer, estimate bytea)",
            "hllsketch",
        ),
        (
            "create table varbyte column",
            "create table events(id integer, digest VARBYTE)",
            "create table events(id integer, digest bytea)",
            "varbyte",
        ),
    ]);
}

#[test]
fn rewrites_spatial_column_types_to_text() {
    assert_column_type_rewrite(&[
        (
            "create table geometry column",
            "create table places(id integer, shape GEOMETRY)",
            "create table places(id integer, shape text)",
            "geometry",
        ),
        (
            "create table geography column",
            "create table places(id integer, footprint GEOGRAPHY)",
            "create table places(id integer, footprint text)",
            "geography",
        ),
    ]);
}

#[test]
fn rewrites_timestamp_column_type() {
    assert_column_type_rewrite(&[(
        "create table timestamp column",
        "create table events(id integer, created_at TIMESTAMP)",
        "create table events(id integer, created_at timestamp(6) without time zone)",
        "timestamp",
    )]);
}

#[test]
fn rewrites_timestamptz_column_type() {
    assert_column_type_rewrite(&[(
        "create table timestamptz column",
        "create table events(id integer, observed_at TIMESTAMPTZ)",
        "create table events(id integer, observed_at timestamp(6) without time zone)",
        "timestamptz",
    )]);
}

#[test]
fn rewrites_time_column_types() {
    assert_column_type_rewrite(&[
        (
            "create table time column",
            "create table events(id integer, started_at TIME)",
            "create table events(id integer, started_at time(6) without time zone)",
            "time",
        ),
        (
            "create table timetz column",
            "create table events(id integer, started_at TIMETZ)",
            "create table events(id integer, started_at time(6) with time zone)",
            "timetz",
        ),
    ]);
}

#[test]
fn rewrites_byte_limited_string_column_types() {
    assert_column_type_rewrite(&[
        (
            "create table varchar column",
            "create table events(id integer, payload VARCHAR(8))",
            "create table events(id integer, payload text check (octet_length(payload) <= 8))",
            "varchar(8)",
        ),
        (
            "create table char column",
            "create table events(id integer, code CHAR(4))",
            "create table events(id integer, code text check (octet_length(code) <= 4))",
            "char(4)",
        ),
    ]);
}

#[test]
fn rejects_malformed_create_table() {
    let cases = [
        ("unterminated column list", "create table events(id integer"),
        ("missing column type", "create table events(id)"),
        ("empty column name", r#"create table events("" integer)"#),
    ];
    for (name, sql) in cases {
        assert!(
            RedshiftToPostgres
                .translate(&Session::default(), sql)
                .is_err(),
            "{name}: Translate() error = nil"
        );
    }
}
