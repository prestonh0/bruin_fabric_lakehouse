package fabricspark

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeLivyServer emulates the subset of the Fabric Lakehouse Livy API the
// connector uses: session lifecycle and statement submit/poll.
type fakeLivyServer struct {
	t *testing.T

	mu             sync.Mutex
	sessions       map[string]string // id -> state
	nextSessionID  int
	statements     map[string]fakeStatement // "session/statement" -> statement
	nextStatement  int
	submittedCodes []string
	submittedKinds []string

	// respondWith lets tests shape the output per submitted code.
	respondWith func(code, kind string) map[string]any

	// killSessions marks sessions that respond 404 to statement submission.
	killSessions map[string]bool

	// throttleFirstSubmit makes the first statement submission return 429.
	throttleFirstSubmit bool
	throttled           bool
}

type fakeStatement struct {
	code string
	kind string
}

func newFakeLivyServer(t *testing.T) *fakeLivyServer {
	return &fakeLivyServer{
		t:            t,
		sessions:     map[string]string{},
		statements:   map[string]fakeStatement{},
		killSessions: map[string]bool{},
	}
}

func defaultSQLOutput(code, kind string) map[string]any {
	if kind == StatementKindPySpark {
		return map[string]any{
			"status": "ok",
			"data":   map[string]any{"text/plain": "pyspark output"},
		}
	}
	return map[string]any{
		"status": "ok",
		"data": map[string]any{
			"application/json": map[string]any{
				"schema": map[string]any{
					"fields": []map[string]any{
						{"name": "id", "type": "long"},
						{"name": "name", "type": "string"},
					},
				},
				"data": [][]any{{1, "jane"}, {2, "joe"}},
			},
		},
	}
}

func (f *fakeLivyServer) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()

		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		path := strings.TrimPrefix(r.URL.Path, "/")
		parts := strings.Split(path, "/")

		switch {
		case r.Method == http.MethodPost && path == "sessions":
			f.nextSessionID++
			id := fmt.Sprint(f.nextSessionID)
			f.sessions[id] = "idle"
			_ = json.NewEncoder(w).Encode(map[string]any{"id": f.nextSessionID, "state": "starting"})

		case r.Method == http.MethodGet && len(parts) == 2 && parts[0] == "sessions":
			state, ok := f.sessions[parts[1]]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"id": parts[1], "state": state})

		case r.Method == http.MethodDelete && len(parts) == 2 && parts[0] == "sessions":
			delete(f.sessions, parts[1])
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodPost && len(parts) == 3 && parts[2] == "statements":
			sessionID := parts[1]
			if f.killSessions[sessionID] {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if f.throttleFirstSubmit && !f.throttled {
				f.throttled = true
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}

			var payload struct {
				Code string `json:"code"`
				Kind string `json:"kind"`
			}
			require.NoError(f.t, json.NewDecoder(r.Body).Decode(&payload))

			f.nextStatement++
			key := fmt.Sprintf("%s/%d", sessionID, f.nextStatement)
			f.statements[key] = fakeStatement{code: payload.Code, kind: payload.Kind}
			f.submittedCodes = append(f.submittedCodes, payload.Code)
			f.submittedKinds = append(f.submittedKinds, payload.Kind)

			_ = json.NewEncoder(w).Encode(map[string]any{"id": f.nextStatement, "state": "waiting"})

		case r.Method == http.MethodGet && len(parts) == 4 && parts[2] == "statements":
			key := fmt.Sprintf("%s/%s", parts[1], parts[3])
			statement, ok := f.statements[key]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			respond := f.respondWith
			if respond == nil {
				respond = defaultSQLOutput
			}
			output := respond(statement.code, statement.kind)
			state := "available"
			if status, _ := output["status"].(string); status == "error" {
				// Livy reports error output with state=available and
				// output.status=error; both paths must be handled.
				state = "available"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"id": parts[3], "state": state, "output": output})

		default:
			f.t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	})
}

func newTestDB(t *testing.T, server *httptest.Server) *DB {
	t.Helper()

	c := validConfig()
	c.AccessToken = "test-token"
	c.PollIntervalMillis = 1
	c.HTTPTimeoutSeconds = 10
	c.SessionStartTimeoutSeconds = 10

	db, err := NewDB(c)
	require.NoError(t, err)
	// Point the client at the fake server instead of api.fabric.microsoft.com.
	db.client.baseURL = server.URL
	return db
}

func TestDBSelect(t *testing.T) {
	t.Parallel()

	fake := newFakeLivyServer(t)
	server := httptest.NewServer(fake.handler())
	defer server.Close()

	db := newTestDB(t, server)
	rows, err := db.Select(context.Background(), &query.Query{Query: "/* strip me */ SELECT * FROM users;"})
	require.NoError(t, err)

	require.Len(t, rows, 2)
	assert.Equal(t, "jane", rows[0][1])

	// The submitted code must be sanitized: block comment stripped, trailing
	// semicolon removed.
	require.Len(t, fake.submittedCodes, 1)
	assert.Equal(t, "SELECT * FROM users", fake.submittedCodes[0])
	assert.Equal(t, StatementKindSQL, fake.submittedKinds[0])
}

func TestDBSelectWithSchema(t *testing.T) {
	t.Parallel()

	fake := newFakeLivyServer(t)
	server := httptest.NewServer(fake.handler())
	defer server.Close()

	db := newTestDB(t, server)
	result, err := db.SelectWithSchema(context.Background(), &query.Query{Query: "SELECT * FROM users"})
	require.NoError(t, err)

	assert.Equal(t, []string{"id", "name"}, result.Columns)
	assert.Equal(t, []string{"long", "string"}, result.ColumnTypes)
	require.Len(t, result.Rows, 2)
}

func TestDBRunQueryWithoutResultMultiStatement(t *testing.T) {
	t.Parallel()

	fake := newFakeLivyServer(t)
	server := httptest.NewServer(fake.handler())
	defer server.Close()

	db := newTestDB(t, server)
	err := db.RunQueryWithoutResult(context.Background(), &query.Query{
		Query: "DELETE FROM t WHERE x = ';not a boundary;';\nINSERT INTO t SELECT 1",
	})
	require.NoError(t, err)

	require.Len(t, fake.submittedCodes, 2)
	assert.Equal(t, "DELETE FROM t WHERE x = ';not a boundary;'", fake.submittedCodes[0])
	assert.Equal(t, "INSERT INTO t SELECT 1", fake.submittedCodes[1])
}

func TestDBPing(t *testing.T) {
	t.Parallel()

	fake := newFakeLivyServer(t)
	server := httptest.NewServer(fake.handler())
	defer server.Close()

	db := newTestDB(t, server)
	require.NoError(t, db.Ping(context.Background()))
	require.Len(t, fake.submittedCodes, 1)
	assert.Equal(t, "SELECT 1", fake.submittedCodes[0])
}

func TestDBRunPySpark(t *testing.T) {
	t.Parallel()

	fake := newFakeLivyServer(t)
	server := httptest.NewServer(fake.handler())
	defer server.Close()

	db := newTestDB(t, server)
	output, err := db.RunPySpark(context.Background(), "df = spark.range(3)\nprint(df.count())")
	require.NoError(t, err)
	assert.Equal(t, "pyspark output", output)

	require.Len(t, fake.submittedKinds, 1)
	assert.Equal(t, StatementKindPySpark, fake.submittedKinds[0])
	assert.Contains(t, fake.submittedCodes[0], "spark.range(3)")
}

func TestDBStatementErrorIsSurfaced(t *testing.T) {
	t.Parallel()

	fake := newFakeLivyServer(t)
	fake.respondWith = func(code, kind string) map[string]any {
		return map[string]any{
			"status":    "error",
			"ename":     "AnalysisException",
			"evalue":    "Table or view not found: missing_table",
			"traceback": []string{"line 1", "line 2"},
		}
	}
	server := httptest.NewServer(fake.handler())
	defer server.Close()

	db := newTestDB(t, server)
	_, err := db.Select(context.Background(), &query.Query{Query: "SELECT * FROM missing_table"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Table or view not found")
}

func TestDBRecreatesExpiredSession(t *testing.T) {
	t.Parallel()

	fake := newFakeLivyServer(t)
	server := httptest.NewServer(fake.handler())
	defer server.Close()

	db := newTestDB(t, server)

	// First query establishes session 1.
	require.NoError(t, db.Ping(context.Background()))

	// Fabric expires the session: subsequent submissions to it 404.
	fake.mu.Lock()
	fake.killSessions["1"] = true
	fake.mu.Unlock()

	// The client must transparently create session 2 and retry.
	require.NoError(t, db.Ping(context.Background()))

	fake.mu.Lock()
	defer fake.mu.Unlock()
	assert.Equal(t, 2, fake.nextSessionID)
	assert.Len(t, fake.submittedCodes, 2)
}

func TestDBRetriesThrottledSubmit(t *testing.T) {
	t.Parallel()

	fake := newFakeLivyServer(t)
	fake.throttleFirstSubmit = true
	server := httptest.NewServer(fake.handler())
	defer server.Close()

	db := newTestDB(t, server)
	require.NoError(t, db.Ping(context.Background()))
	require.Len(t, fake.submittedCodes, 1)
}

func TestDBClose(t *testing.T) {
	t.Parallel()

	fake := newFakeLivyServer(t)
	server := httptest.NewServer(fake.handler())
	defer server.Close()

	db := newTestDB(t, server)
	require.NoError(t, db.Ping(context.Background()))
	require.NoError(t, db.Close(context.Background()))

	fake.mu.Lock()
	defer fake.mu.Unlock()
	assert.Empty(t, fake.sessions)
}

func TestQuoteIdentifier(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "`schema`.`table`", QuoteIdentifier("schema.table"))
	assert.Equal(t, "`weird``name`", QuoteIdentifier("weird`name"))
}

func TestSplitStatements(t *testing.T) {
	t.Parallel()

	statements := splitStatements("SELECT 1; -- comment with ; inside\nSELECT ';';SELECT 3")
	require.Len(t, statements, 3)
	assert.Equal(t, "SELECT 1", strings.TrimSpace(statements[0]))
	assert.Contains(t, statements[1], "comment with ; inside")
	assert.Contains(t, statements[1], "SELECT ';'")
	assert.Equal(t, "SELECT 3", strings.TrimSpace(statements[2]))
}
