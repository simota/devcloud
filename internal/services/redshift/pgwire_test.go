package redshift

import (
	"bytes"
	"encoding/binary"
	"net"
	"reflect"
	"strings"
	"testing"
)

func TestPgWireSelectOneWithPasswordAuth(t *testing.T) {
	server := NewServer(Config{
		AuthMode: "strict",
		Password: "dev",
	})
	client, serverConn := net.Pipe()
	defer client.Close()
	go server.handleSQLConn(serverConn)

	if err := writeTestStartup(client, map[string]string{
		"user":            "dev",
		"database":        "dev",
		"client_encoding": "UTF8",
	}); err != nil {
		t.Fatalf("write startup: %v", err)
	}

	messageType, payload := readTestMessage(t, client)
	if messageType != 'R' || binary.BigEndian.Uint32(payload) != uint32(pgAuthCleartext) {
		t.Fatalf("auth request = %q %#v", messageType, payload)
	}
	if err := writeTestTypedMessage(client, 'p', []byte("dev\x00")); err != nil {
		t.Fatalf("write password: %v", err)
	}
	waitForReady(t, client)

	if err := writeTestTypedMessage(client, 'Q', []byte("select 1;\x00")); err != nil {
		t.Fatalf("write query: %v", err)
	}

	var sawRow bool
	for {
		messageType, payload = readTestMessage(t, client)
		switch messageType {
		case 'D':
			if !bytes.Contains(payload, []byte("1")) {
				t.Fatalf("data row payload = %#v", payload)
			}
			sawRow = true
		case 'Z':
			if !sawRow {
				t.Fatal("ReadyForQuery arrived before DataRow")
			}
			writeTestTypedMessage(client, 'X', nil)
			return
		}
	}
}

func TestPgWireMinimalExtendedQuerySelectOne(t *testing.T) {
	server := NewServer(Config{})
	session := newExtendedQuerySession()

	var parse bytes.Buffer
	writeCString(&parse, "stmt1")
	writeCString(&parse, "select 1")
	binary.Write(&parse, binary.BigEndian, int16(0))
	var wire bytes.Buffer
	session.handleParse(&wire, parse.Bytes())
	messageTypes := readTestBufferMessageTypes(t, &wire)
	if !reflect.DeepEqual(messageTypes, []byte{'1'}) {
		t.Fatalf("parse responses = %q", messageTypes)
	}

	var bind bytes.Buffer
	writeCString(&bind, "portal1")
	writeCString(&bind, "stmt1")
	binary.Write(&bind, binary.BigEndian, int16(0))
	binary.Write(&bind, binary.BigEndian, int16(0))
	binary.Write(&bind, binary.BigEndian, int16(0))
	wire.Reset()
	session.handleBind(&wire, bind.Bytes())
	messageTypes = readTestBufferMessageTypes(t, &wire)
	if !reflect.DeepEqual(messageTypes, []byte{'2'}) {
		t.Fatalf("bind responses = %q", messageTypes)
	}

	var describe bytes.Buffer
	describe.WriteByte('P')
	writeCString(&describe, "portal1")
	wire.Reset()
	session.handleDescribe(server, &wire, describe.Bytes())
	messageTypes = readTestBufferMessageTypes(t, &wire)
	if !reflect.DeepEqual(messageTypes, []byte{'T'}) {
		t.Fatalf("describe responses = %q", messageTypes)
	}

	var execute bytes.Buffer
	writeCString(&execute, "portal1")
	binary.Write(&execute, binary.BigEndian, int32(0))
	wire.Reset()
	session.handleExecute(server, &wire, execute.Bytes())
	if !bytes.Contains(wire.Bytes(), []byte("SELECT 1")) {
		t.Fatalf("execute response missing command tag: %#v", wire.Bytes())
	}
	messageTypes = readTestBufferMessageTypes(t, &wire)
	if !reflect.DeepEqual(messageTypes, []byte{'D', 'C'}) {
		t.Fatalf("execute responses = %q", messageTypes)
	}
}

func TestPgWireExtendedProtocolDescribePreparedStatementAndSyncRecovery(t *testing.T) {
	server := NewServer(Config{})
	session := newExtendedQuerySession()

	var parse bytes.Buffer
	writeCString(&parse, "stmt1")
	writeCString(&parse, "select 1")
	binary.Write(&parse, binary.BigEndian, int16(0))
	var wire bytes.Buffer
	session.handleParse(&wire, parse.Bytes())
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'1'}) {
		t.Fatalf("parse responses = %q", messageTypes)
	}

	var describe bytes.Buffer
	describe.WriteByte('S')
	writeCString(&describe, "stmt1")
	wire.Reset()
	session.handleDescribe(server, &wire, describe.Bytes())
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'t', 'T'}) {
		t.Fatalf("describe prepared statement responses = %q", messageTypes)
	}

	wire.Reset()
	session.handleBind(&wire, []byte("broken"))
	if !session.failed {
		t.Fatal("protocol error did not mark extended session failed")
	}
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'E'}) {
		t.Fatalf("protocol error responses = %q", messageTypes)
	}

	session.handleSync(&wire)
	wire.Reset()
	session.handleBind(&wire, bindPayload("portal1", "stmt1"))
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'2'}) {
		t.Fatalf("bind after sync recovery responses = %q", messageTypes)
	}
}

func TestPgWireExtendedProtocolBindTextParametersWithoutLoggingValues(t *testing.T) {
	server := NewServer(Config{})
	for _, statement := range []string{
		"create table public.events(id int, payload varchar(64))",
		"insert into public.events(id, payload) values (777, 'alpha')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute setup %q: %v", statement, err)
		}
	}
	session := newExtendedQuerySession()

	var parse bytes.Buffer
	writeCString(&parse, "stmt1")
	writeCString(&parse, "select payload from public.events where id = $1")
	binary.Write(&parse, binary.BigEndian, int16(1))
	binary.Write(&parse, binary.BigEndian, pgTypeInt4OID)
	var wire bytes.Buffer
	session.handleParse(&wire, parse.Bytes())
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'1'}) {
		t.Fatalf("parse responses = %q", messageTypes)
	}

	wire.Reset()
	session.handleBind(&wire, bindPayloadWithTextParams("portal1", "stmt1", "777"))
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'2'}) {
		t.Fatalf("bind responses = %q", messageTypes)
	}

	var execute bytes.Buffer
	writeCString(&execute, "portal1")
	binary.Write(&execute, binary.BigEndian, int32(0))
	wire.Reset()
	session.handleExecute(server, &wire, execute.Bytes())
	if !bytes.Contains(wire.Bytes(), []byte("alpha")) {
		t.Fatalf("execute response missing selected row: %#v", wire.Bytes())
	}
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'D', 'C'}) {
		t.Fatalf("execute responses = %q", messageTypes)
	}

	statements := server.StatementSnapshots()
	if len(statements) != 1 {
		t.Fatalf("statement history count = %d", len(statements))
	}
	if statements[0].QueryPreview != "select payload from public.events where id = $1" {
		t.Fatalf("statement history logged executable SQL with bind values: %#v", statements[0])
	}
}

func TestPgWireExtendedProtocolDescribePortalWithTextParameters(t *testing.T) {
	server := NewServer(Config{})
	for _, statement := range []string{
		"create table public.events(id int, payload varchar(64))",
		"insert into public.events(id, payload) values (42, 'portal-describe')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute setup %q: %v", statement, err)
		}
	}
	session := newExtendedQuerySession()

	var parse bytes.Buffer
	writeCString(&parse, "stmt1")
	writeCString(&parse, "select payload from public.events where id = $1")
	binary.Write(&parse, binary.BigEndian, int16(1))
	binary.Write(&parse, binary.BigEndian, pgTypeInt4OID)
	var wire bytes.Buffer
	session.handleParse(&wire, parse.Bytes())
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'1'}) {
		t.Fatalf("parse responses = %q", messageTypes)
	}

	wire.Reset()
	session.handleBind(&wire, bindPayloadWithTextParams("portal1", "stmt1", "42"))
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'2'}) {
		t.Fatalf("bind responses = %q", messageTypes)
	}

	var describe bytes.Buffer
	describe.WriteByte('P')
	writeCString(&describe, "portal1")
	wire.Reset()
	session.handleDescribe(server, &wire, describe.Bytes())
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'T'}) {
		t.Fatalf("describe portal responses = %q", messageTypes)
	}
}

func TestPgWireExtendedProtocolRejectsBinaryResultFormats(t *testing.T) {
	session := newExtendedQuerySession()

	var parse bytes.Buffer
	writeCString(&parse, "stmt1")
	writeCString(&parse, "select 1")
	binary.Write(&parse, binary.BigEndian, int16(0))
	var wire bytes.Buffer
	session.handleParse(&wire, parse.Bytes())
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'1'}) {
		t.Fatalf("parse responses = %q", messageTypes)
	}

	wire.Reset()
	session.handleBind(&wire, bindPayloadWithResultFormats("portal1", "stmt1", 1))
	if !session.failed {
		t.Fatal("binary result format did not mark extended session failed")
	}
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'E'}) {
		t.Fatalf("bind responses = %q", messageTypes)
	}
	if strings.Contains(wire.String(), "select 1") {
		t.Fatalf("binary result format error leaked SQL text: %#v", wire.String())
	}
}

func TestPgWireExtendedProtocolExecuteHonorsMaxRowsAndResumesPortal(t *testing.T) {
	server := NewServer(Config{})
	for _, statement := range []string{
		"create table public.events(id int, payload varchar(64))",
		"insert into public.events(id, payload) values (1, 'one')",
		"insert into public.events(id, payload) values (2, 'two')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute setup %q: %v", statement, err)
		}
	}
	session := newExtendedQuerySession()

	var parse bytes.Buffer
	writeCString(&parse, "stmt1")
	writeCString(&parse, "select id, payload from public.events")
	binary.Write(&parse, binary.BigEndian, int16(0))
	var wire bytes.Buffer
	session.handleParse(&wire, parse.Bytes())
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'1'}) {
		t.Fatalf("parse responses = %q", messageTypes)
	}

	wire.Reset()
	session.handleBind(&wire, bindPayload("portal1", "stmt1"))
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'2'}) {
		t.Fatalf("bind responses = %q", messageTypes)
	}

	wire.Reset()
	session.handleExecute(server, &wire, executePayload("portal1", 1))
	if bytes.Contains(wire.Bytes(), []byte("two")) {
		t.Fatalf("first execute returned more than maxRows: %#v", wire.Bytes())
	}
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'D', 's'}) {
		t.Fatalf("first execute responses = %q", messageTypes)
	}

	wire.Reset()
	session.handleExecute(server, &wire, executePayload("portal1", 0))
	if !bytes.Contains(wire.Bytes(), []byte("two")) {
		t.Fatalf("second execute did not resume portal: %#v", wire.Bytes())
	}
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'D', 'C'}) {
		t.Fatalf("second execute responses = %q", messageTypes)
	}

	statements := server.StatementSnapshots()
	if len(statements) != 1 {
		t.Fatalf("statement history count = %d", len(statements))
	}
}

func TestPgWireExtendedProtocolCloseStatementAndPortal(t *testing.T) {
	server := NewServer(Config{})
	session := newExtendedQuerySession()

	var parse bytes.Buffer
	writeCString(&parse, "stmt1")
	writeCString(&parse, "select 1")
	binary.Write(&parse, binary.BigEndian, int16(0))
	var wire bytes.Buffer
	session.handleParse(&wire, parse.Bytes())
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'1'}) {
		t.Fatalf("parse responses = %q", messageTypes)
	}

	wire.Reset()
	session.handleBind(&wire, bindPayload("portal1", "stmt1"))
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'2'}) {
		t.Fatalf("bind responses = %q", messageTypes)
	}

	var closePortal bytes.Buffer
	closePortal.WriteByte('P')
	writeCString(&closePortal, "portal1")
	wire.Reset()
	session.handleClose(&wire, closePortal.Bytes())
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'3'}) {
		t.Fatalf("close portal responses = %q", messageTypes)
	}

	var describePortal bytes.Buffer
	describePortal.WriteByte('P')
	writeCString(&describePortal, "portal1")
	wire.Reset()
	session.handleDescribe(server, &wire, describePortal.Bytes())
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'E'}) {
		t.Fatalf("describe closed portal responses = %q", messageTypes)
	}

	session.handleSync(&wire)
	var closeStatement bytes.Buffer
	closeStatement.WriteByte('S')
	writeCString(&closeStatement, "stmt1")
	wire.Reset()
	session.handleClose(&wire, closeStatement.Bytes())
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'3'}) {
		t.Fatalf("close statement responses = %q", messageTypes)
	}
}

func TestPgWireRunsMultipleSQLCoreStatements(t *testing.T) {
	server := NewServer(Config{
		AuthMode: "strict",
		Password: "dev",
	})
	client, serverConn := net.Pipe()
	defer client.Close()
	go server.handleSQLConn(serverConn)

	if err := writeTestStartup(client, map[string]string{"user": "dev", "database": "dev"}); err != nil {
		t.Fatalf("write startup: %v", err)
	}
	readTestMessage(t, client)
	if err := writeTestTypedMessage(client, 'p', []byte("dev\x00")); err != nil {
		t.Fatalf("write password: %v", err)
	}
	waitForReady(t, client)

	sql := strings.Join([]string{
		"create schema if not exists loop",
		"create table loop.events(id integer encode raw, payload varchar(64)) distkey(id)",
		"insert into loop.events values (1, 'created')",
		"select id, payload from loop.events where id = 1",
	}, ";\n") + ";\x00"
	if err := writeTestTypedMessage(client, 'Q', []byte(sql)); err != nil {
		t.Fatalf("write query: %v", err)
	}

	var sawCreatedPayload bool
	for {
		messageType, payload := readTestMessage(t, client)
		switch messageType {
		case 'D':
			if bytes.Contains(payload, []byte("created")) {
				sawCreatedPayload = true
			}
		case 'Z':
			if !sawCreatedPayload {
				t.Fatal("ReadyForQuery arrived before selected row")
			}
			writeTestTypedMessage(client, 'X', nil)
			return
		}
	}
}

func TestPgWireRejectsBadPasswordWithoutLeakingValue(t *testing.T) {
	server := NewServer(Config{
		AuthMode: "strict",
		Password: "dev",
	})
	client, serverConn := net.Pipe()
	defer client.Close()
	go server.handleSQLConn(serverConn)

	if err := writeTestStartup(client, map[string]string{"user": "dev"}); err != nil {
		t.Fatalf("write startup: %v", err)
	}
	readTestMessage(t, client)
	if err := writeTestTypedMessage(client, 'p', []byte("wrong-secret\x00")); err != nil {
		t.Fatalf("write password: %v", err)
	}

	messageType, payload := readTestMessage(t, client)
	if messageType != 'E' {
		t.Fatalf("message type = %q, want ErrorResponse", messageType)
	}
	if strings.Contains(string(payload), "wrong-secret") {
		t.Fatalf("error leaked password: %q", string(payload))
	}
}

func bindPayload(portalName string, statementName string) []byte {
	var bind bytes.Buffer
	writeCString(&bind, portalName)
	writeCString(&bind, statementName)
	binary.Write(&bind, binary.BigEndian, int16(0))
	binary.Write(&bind, binary.BigEndian, int16(0))
	binary.Write(&bind, binary.BigEndian, int16(0))
	return bind.Bytes()
}

func executePayload(portalName string, maxRows int32) []byte {
	var execute bytes.Buffer
	writeCString(&execute, portalName)
	binary.Write(&execute, binary.BigEndian, maxRows)
	return execute.Bytes()
}

func bindPayloadWithResultFormats(portalName string, statementName string, formats ...int16) []byte {
	var bind bytes.Buffer
	writeCString(&bind, portalName)
	writeCString(&bind, statementName)
	binary.Write(&bind, binary.BigEndian, int16(0))
	binary.Write(&bind, binary.BigEndian, int16(0))
	binary.Write(&bind, binary.BigEndian, int16(len(formats)))
	for _, format := range formats {
		binary.Write(&bind, binary.BigEndian, format)
	}
	return bind.Bytes()
}

func bindPayloadWithTextParams(portalName string, statementName string, values ...string) []byte {
	var bind bytes.Buffer
	writeCString(&bind, portalName)
	writeCString(&bind, statementName)
	binary.Write(&bind, binary.BigEndian, int16(0))
	binary.Write(&bind, binary.BigEndian, int16(len(values)))
	for _, value := range values {
		binary.Write(&bind, binary.BigEndian, int32(len(value)))
		bind.WriteString(value)
	}
	binary.Write(&bind, binary.BigEndian, int16(0))
	return bind.Bytes()
}
