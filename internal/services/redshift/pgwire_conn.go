package redshift

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"time"
)

func (s *Server) handleSQLConn(conn net.Conn) {
	defer conn.Close()

	params, err := s.readStartup(conn)
	if err != nil {
		return
	}
	if err := writeAuthCleartextPassword(conn); err != nil {
		return
	}
	password, err := readPasswordMessage(conn)
	if err != nil {
		return
	}
	if !s.passwordAllowed(password) {
		writeErrorResponse(conn, "28P01", "password authentication failed")
		return
	}
	if err := writeAuthenticationOK(conn); err != nil {
		return
	}
	if err := writeParameterStatuses(conn, params); err != nil {
		return
	}
	if err := writeBackendKeyData(conn); err != nil {
		return
	}
	if err := writeReadyForQuery(conn); err != nil {
		return
	}

	extended := newExtendedQuerySession()
	for {
		messageType := []byte{0}
		if _, err := io.ReadFull(conn, messageType); err != nil {
			return
		}
		payload, err := readMessagePayload(conn)
		if err != nil {
			return
		}
		switch messageType[0] {
		case 'Q':
			s.handleSimpleQuery(conn, readCString(payload))
		case 'P':
			if extended.failed {
				continue
			}
			extended.handleParse(conn, payload)
		case 'B':
			if extended.failed {
				continue
			}
			extended.handleBind(conn, payload)
		case 'D':
			if extended.failed {
				continue
			}
			extended.handleDescribe(s, conn, payload)
		case 'E':
			if extended.failed {
				continue
			}
			extended.handleExecute(s, conn, payload)
		case 'C':
			if extended.failed {
				continue
			}
			extended.handleClose(conn, payload)
		case 'S':
			extended.handleSync(conn)
			writeReadyForQuery(conn)
		case 'X':
			return
		default:
			writeErrorResponse(conn, "0A000", "unsupported PostgreSQL wire message")
			writeReadyForQuery(conn)
		}
	}
}

func (s *Server) readStartup(rw io.ReadWriter) (map[string]string, error) {
	for {
		payload, err := readMessagePayload(rw)
		if err != nil {
			return nil, err
		}
		if len(payload) < 4 {
			return nil, errors.New("short startup message")
		}
		code := int32(binary.BigEndian.Uint32(payload[:4]))
		switch code {
		case pgSSLRequestCode:
			if _, err := rw.Write([]byte{'N'}); err != nil {
				return nil, err
			}
			continue
		case pgProtocolVersion:
			return parseStartupParameters(payload[4:]), nil
		default:
			writeErrorResponse(rw, "08P01", "unsupported PostgreSQL startup protocol")
			return nil, errors.New("unsupported startup protocol")
		}
	}
}

func (s *Server) handleSimpleQuery(w io.Writer, query string) {
	statements := splitSQLStatements(query)
	if len(statements) == 0 {
		writeMessage(w, 'I', nil)
		writeReadyForQuery(w)
		return
	}
	for _, statement := range statements {
		if err := s.validateStatementSize(statement); err != nil {
			s.recordSQLHistory("[statement exceeds maxStatementBytes]", queryResult{}, err)
			writeErrorResponse(w, "54000", err.Error())
			break
		}
		result, err := s.executeSQL(statement)
		s.recordSQLHistory(statement, result, err)
		if err != nil {
			writeErrorResponse(w, "0A000", err.Error())
			break
		}
		if len(result.fields) > 0 {
			writeRowDescription(w, result.fields)
			for _, row := range result.rows {
				writeDataRow(w, row)
			}
		}
		writeCommandComplete(w, result.tag)
	}
	writeReadyForQuery(w)
}

func (s *Server) recordSQLHistory(statementText string, result queryResult, executionErr error) StatementSnapshot {
	now := time.Now().UTC()
	stmt := &statement{
		ID:                s.nextStatementIDValue(),
		ClusterIdentifier: defaultString(s.config.ClusterIdentifier, "devcloud"),
		Database:          defaultString(s.config.Database, "dev"),
		DbUser:            defaultString(s.config.User, "dev"),
		QueryString:       statementText,
		CreatedAt:         now,
		UpdatedAt:         now,
		Status:            "FINISHED",
		HasResultSet:      len(result.fields) > 0,
		Result:            result,
	}
	if executionErr != nil {
		stmt.Status = "FAILED"
		stmt.Error = executionErr.Error()
		stmt.HasResultSet = false
		stmt.Result = queryResult{}
	}

	s.mu.Lock()
	s.statements[stmt.ID] = stmt
	_ = s.persistLocked()
	s.mu.Unlock()
	return statementSnapshotFromStatement(stmt)
}

func (s *Server) passwordAllowed(password string) bool {
	if strings.EqualFold(s.config.AuthMode, "strict") {
		return password == s.config.Password
	}
	expected := defaultString(s.config.Password, "dev")
	return password == "" || password == expected
}
