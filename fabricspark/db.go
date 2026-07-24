package fabricspark

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
)

// DB is the Fabric Lakehouse Spark client. It lazily creates a single Livy
// session per connection and serializes statements through it, mirroring how
// a Spark SQL warehouse connection behaves.
type DB struct {
	config        *Config
	client        *LivyClient
	schemaCreator *ansisql.SchemaCreator

	mu      sync.Mutex
	session *LivySession

	// High-concurrency mode state: idle REPLs ready for checkout, plus a
	// count of live acquires so the pool stays within MaxConcurrentREPLs.
	hcSessionTag string
	hcPool       chan *HCSession
	hcLive       int
}

// NewDB builds a Fabric Spark client from the config. No network calls are
// made until the first statement runs.
func NewDB(c *Config) (*DB, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	tokens, err := NewTokenProvider(c)
	if err != nil {
		return nil, err
	}

	db := &DB{
		config:        c,
		client:        NewLivyClient(c, tokens),
		schemaCreator: ansisql.NewSchemaCreator(),
	}

	if c.HighConcurrency {
		db.hcSessionTag = c.SessionTag
		if db.hcSessionTag == "" {
			db.hcSessionTag = randomSessionTag()
		}
		db.hcPool = make(chan *HCSession, db.maxREPLs())
	}

	return db, nil
}

func (db *DB) maxREPLs() int {
	if db.config.MaxConcurrentREPLs > 0 {
		return db.config.MaxConcurrentREPLs
	}
	return 4
}

func randomSessionTag() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		// Fall back to a fixed tag; sharing one Spark app is safe, just
		// coarser packing than intended.
		return "bruin-fabric-spark"
	}
	return "bruin-" + hex.EncodeToString(buf)
}

// QuoteIdentifier quotes a Spark identifier with backticks, part by part.
func QuoteIdentifier(identifier string) string {
	parts := strings.Split(identifier, ".")
	quoted := make([]string, len(parts))
	for i, part := range parts {
		quoted[i] = "`" + strings.ReplaceAll(part, "`", "``") + "`"
	}
	return strings.Join(quoted, ".")
}

func (db *DB) sessionPayload() map[string]any {
	name := db.config.SessionName
	if name == "" {
		name = DefaultSessionName
	}

	payload := map[string]any{"name": name}
	if len(db.config.SparkConfig) > 0 {
		conf := make(map[string]any, len(db.config.SparkConfig))
		for k, v := range db.config.SparkConfig {
			conf[k] = v
		}
		payload["conf"] = conf
	}
	if db.config.EnvironmentID != "" {
		payload["environmentId"] = db.config.EnvironmentID
	}
	return payload
}

// ensureSession creates the Livy session on first use, or recreates it after
// Fabric expires it. Callers must hold db.mu.
func (db *DB) ensureSession(ctx context.Context) (*LivySession, error) {
	if db.session != nil {
		return db.session, nil
	}

	session, err := db.client.CreateSession(ctx, db.sessionPayload())
	if err != nil {
		return nil, err
	}
	db.session = session
	return session, nil
}

// runStatement executes code in the connection's Spark session. In singleton
// mode statements serialize on one session; in high-concurrency mode each
// call checks out a REPL from the pool so parallel assets execute
// concurrently inside the shared Spark application. In both modes a session
// reported gone by Fabric (expired idle timeout) is transparently recreated
// once.
func (db *DB) runStatement(ctx context.Context, code, kind string) (*StatementOutput, error) {
	if db.config.HighConcurrency {
		return db.runStatementHC(ctx, code, kind)
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	session, err := db.ensureSession(ctx)
	if err != nil {
		return nil, err
	}

	statement, err := db.client.SubmitStatement(ctx, session.IDString(), code, kind)
	var submitErr *StatementSubmitError
	if errors.As(err, &submitErr) && submitErr.SessionGone() {
		db.session = nil
		session, err = db.ensureSession(ctx)
		if err != nil {
			return nil, err
		}
		statement, err = db.client.SubmitStatement(ctx, session.IDString(), code, kind)
	}
	if err != nil {
		return nil, err
	}

	return db.client.WaitForStatement(ctx, session.IDString(), statement.ID.String())
}

// hcAcquirePayload mirrors sessionPayload with the packing tag added.
func (db *DB) hcAcquirePayload() map[string]any {
	payload := db.sessionPayload()
	payload["sessionTag"] = db.hcSessionTag
	return payload
}

// checkoutREPL returns an idle REPL, acquiring a new one when the pool is
// empty and below its cap, or waiting for a return otherwise.
func (db *DB) checkoutREPL(ctx context.Context) (*HCSession, error) {
	for {
		select {
		case repl := <-db.hcPool:
			return repl, nil
		default:
		}

		db.mu.Lock()
		if db.hcLive < db.maxREPLs() {
			db.hcLive++
			db.mu.Unlock()

			repl, err := db.client.AcquireHCSession(ctx, db.hcAcquirePayload())
			if err != nil {
				db.discardREPL(nil)
				return nil, err
			}
			return repl, nil
		}
		db.mu.Unlock()

		// Pool exhausted and at cap: wait for a REPL to come back. The wait
		// is bounded so that when another goroutine's acquire fails (freeing
		// a live slot without ever putting a REPL in the pool), waiters loop
		// back and take the slot instead of blocking forever.
		select {
		case repl := <-db.hcPool:
			return repl, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func (db *DB) returnREPL(repl *HCSession) {
	db.hcPool <- repl
}

// discardREPL drops a dead (or failed-to-acquire) REPL from the live count so
// a replacement can be acquired.
func (db *DB) discardREPL(repl *HCSession) {
	db.mu.Lock()
	db.hcLive--
	db.mu.Unlock()

	if repl != nil {
		// Best-effort release; the REPL is likely already gone server-side.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = db.client.ReleaseHCSession(ctx, repl.HCID)
	}
}

func (db *DB) runStatementHC(ctx context.Context, code, kind string) (*StatementOutput, error) {
	repl, err := db.checkoutREPL(ctx)
	if err != nil {
		return nil, err
	}

	statement, err := db.client.SubmitHCStatement(ctx, repl, code, kind)
	var submitErr *StatementSubmitError
	if errors.As(err, &submitErr) && submitErr.SessionGone() {
		// This REPL (or its Spark app) is gone; replace it and retry once.
		db.discardREPL(repl)
		repl, err = db.checkoutREPL(ctx)
		if err != nil {
			return nil, err
		}
		statement, err = db.client.SubmitHCStatement(ctx, repl, code, kind)
	}
	if err != nil {
		db.discardREPL(repl)
		return nil, err
	}

	output, err := db.client.WaitForHCStatement(ctx, repl, statement.ID.String())
	if err != nil {
		// A Spark-level failure leaves the REPL healthy — keep it warm.
		// Transport-level failures (timeouts, lost statements) discard it.
		var failed *StatementFailedError
		if errors.As(err, &failed) {
			db.returnREPL(repl)
		} else {
			db.discardREPL(repl)
		}
		return nil, err
	}

	db.returnREPL(repl)
	return output, nil
}

// runSQL executes a single SQL statement and parses its tabular output.
func (db *DB) runSQL(ctx context.Context, sql string) (*SQLResult, error) {
	code := strings.TrimSpace(sanitizeSQL(sql))
	code = strings.TrimSuffix(code, ";")
	if code == "" {
		return &SQLResult{}, nil
	}

	output, err := db.runStatement(ctx, code, StatementKindSQL)
	if err != nil {
		return nil, err
	}
	return ParseSQLOutput(output)
}

// RunQueryWithoutResult executes the query, discarding any result rows.
// Multi-statement inputs (";"-separated) are executed in order, matching how
// bruin's materializers emit multiple statements per asset.
func (db *DB) RunQueryWithoutResult(ctx context.Context, queryObj *query.Query) error {
	for _, statement := range splitStatements(queryObj.String()) {
		if _, err := db.runSQL(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

// Select runs the query and returns the rows.
func (db *DB) Select(ctx context.Context, queryObj *query.Query) ([][]interface{}, error) {
	result, err := db.runSQL(ctx, queryObj.String())
	if err != nil {
		return nil, err
	}
	return result.Data, nil
}

// SelectWithSchema runs the query and returns rows together with column
// names and types.
func (db *DB) SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error) {
	result, err := db.runSQL(ctx, queryObj.String())
	if err != nil {
		return nil, err
	}

	out := &query.QueryResult{
		Columns:     make([]string, 0, len(result.Schema.Fields)),
		ColumnTypes: make([]string, 0, len(result.Schema.Fields)),
		Rows:        result.Data,
	}
	if out.Rows == nil {
		out.Rows = [][]interface{}{}
	}
	for i := range result.Schema.Fields {
		field := &result.Schema.Fields[i]
		out.Columns = append(out.Columns, field.Name)
		out.ColumnTypes = append(out.ColumnTypes, field.TypeName())
	}
	return out, nil
}

// Ping verifies the connection by running a trivial query. Note that on a
// cold Fabric capacity this starts a Spark session, which can take minutes.
func (db *DB) Ping(ctx context.Context) error {
	err := db.RunQueryWithoutResult(ctx, &query.Query{Query: "SELECT 1"})
	if err != nil {
		return errors.Wrap(err, "failed to run test query on Fabric Lakehouse Spark connection")
	}
	return nil
}

// RunPySpark executes PySpark code in the shared session and returns its
// plain-text output.
func (db *DB) RunPySpark(ctx context.Context, code string) (string, error) {
	output, err := db.runStatement(ctx, code, StatementKindPySpark)
	if err != nil {
		return "", err
	}
	return ParseTextOutput(output), nil
}

// Close tears down the Livy session (or releases all pooled REPLs in
// high-concurrency mode), releasing Spark capacity. Safe to call when no
// session was ever created.
func (db *DB) Close(ctx context.Context) error {
	var firstErr error

	if db.hcPool != nil {
		for {
			select {
			case repl := <-db.hcPool:
				if err := db.client.ReleaseHCSession(ctx, repl.HCID); err != nil && firstErr == nil {
					firstErr = err
				}
				db.mu.Lock()
				db.hcLive--
				db.mu.Unlock()
			default:
				return firstErr
			}
		}
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.session == nil {
		return nil
	}
	err := db.client.CloseSession(ctx, db.session.IDString())
	db.session = nil
	return err
}

// GetIngestrURI exposes the connection as an ingestr OneLake destination; see
// Config.GetIngestrURI for the mapping and its requirements.
func (db *DB) GetIngestrURI() (string, error) {
	return db.config.GetIngestrURI(), nil
}

type annotatedSchemaQueryRunner struct {
	db           *DB
	assetName    string
	pipelineName string
}

func (r *annotatedSchemaQueryRunner) RunQueryWithoutResult(ctx context.Context, q *query.Query) error {
	annotatedQuery, err := ansisql.AddAnnotationComment(ctx, q, r.assetName, "schema", r.pipelineName)
	if err != nil {
		return errors.Wrap(err, "failed to add schema annotation comment")
	}
	return r.db.RunQueryWithoutResult(ctx, annotatedQuery)
}

// CreateSchemaIfNotExist creates the asset's schema when the asset name is
// schema-qualified. Only meaningful for schema-enabled lakehouses; for
// non-schema lakehouses assets use bare table names and this is a no-op.
func (db *DB) CreateSchemaIfNotExist(ctx context.Context, asset *pipeline.Asset, pipelineName string) error {
	runner := &annotatedSchemaQueryRunner{
		db:           db,
		assetName:    asset.Name,
		pipelineName: pipelineName,
	}
	return db.schemaCreator.CreateSchemaIfNotExist(ctx, runner, asset)
}

// GetDatabases lists the schemas visible to the session.
func (db *DB) GetDatabases(ctx context.Context) ([]string, error) {
	result, err := db.Select(ctx, &query.Query{Query: "SHOW DATABASES"})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list Fabric Lakehouse databases")
	}

	var databases []string
	for _, row := range result {
		if len(row) > 0 {
			if name, ok := row[0].(string); ok {
				databases = append(databases, name)
			}
		}
	}
	sort.Strings(databases)
	return databases, nil
}

// GetTables lists the tables in a database.
func (db *DB) GetTables(ctx context.Context, databaseName string) ([]string, error) {
	if databaseName == "" {
		return nil, errors.New("database name cannot be empty")
	}

	result, err := db.Select(ctx, &query.Query{Query: "SHOW TABLES IN " + QuoteIdentifier(databaseName)})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list tables in database %q", databaseName)
	}

	var tables []string
	for _, row := range result {
		// SHOW TABLES returns (namespace, tableName, isTemporary).
		if len(row) > 1 {
			if name, ok := row[1].(string); ok {
				tables = append(tables, name)
			}
		}
	}
	return tables, nil
}

// GetColumns describes the columns of a table.
func (db *DB) GetColumns(ctx context.Context, databaseName, tableName string) ([]*ansisql.DBColumn, error) {
	if databaseName == "" {
		return nil, errors.New("database name cannot be empty")
	}
	if tableName == "" {
		return nil, errors.New("table name cannot be empty")
	}

	q := fmt.Sprintf("DESCRIBE TABLE %s.%s", QuoteIdentifier(databaseName), QuoteIdentifier(tableName))
	result, err := db.Select(ctx, &query.Query{Query: q})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to describe table %s.%s", databaseName, tableName)
	}

	columns := make([]*ansisql.DBColumn, 0, len(result))
	for _, row := range result {
		if len(row) < 2 {
			continue
		}
		name, _ := row[0].(string)
		colType, _ := row[1].(string)
		// DESCRIBE emits a partition-info section after a blank/comment row.
		if name == "" || strings.HasPrefix(name, "#") {
			break
		}
		columns = append(columns, &ansisql.DBColumn{
			Name: name,
			Type: colType,
		})
	}
	return columns, nil
}

// splitStatements splits a SQL string on semicolons at statement boundaries.
// It respects quotes and line comments so semicolons inside literals survive.
func splitStatements(sql string) []string {
	var statements []string
	var current strings.Builder

	inSingle, inDouble, inBacktick, inLineComment := false, false, false, false
	runes := []rune(sql)
	for i := 0; i < len(runes); i++ {
		ch := runes[i]

		switch {
		case inLineComment:
			if ch == '\n' {
				inLineComment = false
			}
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
		case inBacktick:
			if ch == '`' {
				inBacktick = false
			}
		default:
			switch ch {
			case '\'':
				inSingle = true
			case '"':
				inDouble = true
			case '`':
				inBacktick = true
			case '-':
				if i+1 < len(runes) && runes[i+1] == '-' {
					inLineComment = true
				}
			case ';':
				statements = append(statements, current.String())
				current.Reset()
				continue
			}
		}
		current.WriteRune(ch)
	}
	if strings.TrimSpace(current.String()) != "" {
		statements = append(statements, current.String())
	}

	filtered := make([]string, 0, len(statements))
	for _, s := range statements {
		if strings.TrimSpace(s) != "" {
			filtered = append(filtered, s)
		}
	}
	return filtered
}
