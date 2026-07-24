package fabricspark

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

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

	return &DB{
		config:        c,
		client:        NewLivyClient(c, tokens),
		schemaCreator: ansisql.NewSchemaCreator(),
	}, nil
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

// runStatement executes code in the shared session, transparently recreating
// the session once if Fabric reports it gone (expired idle timeout).
func (db *DB) runStatement(ctx context.Context, code, kind string) (*StatementOutput, error) {
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

// Close tears down the Livy session, releasing Spark capacity. Safe to call
// when no session was ever created.
func (db *DB) Close(ctx context.Context) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.session == nil {
		return nil
	}
	err := db.client.CloseSession(ctx, db.session.IDString())
	db.session = nil
	return err
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
