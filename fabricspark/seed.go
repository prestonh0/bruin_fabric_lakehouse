package fabricspark

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

// seedBatchSize bounds how many rows go into a single INSERT statement so
// individual Livy statements stay well under Fabric's payload limits.
const seedBatchSize = 500

// SeedOperator loads a CSV file into a lakehouse Delta table for
// `fabric.spark.seed` assets. Bruin's shared seed operator delegates to
// ingestr, which has no Fabric Lakehouse Spark destination — so this
// implementation loads the file natively through the Spark session:
// CREATE OR REPLACE TABLE followed by batched INSERTs.
//
// Column types come from the asset's `columns` definitions when present
// (values are CAST accordingly); undeclared columns default to STRING.
type SeedOperator struct {
	connection config.ConnectionGetter
}

// NewSeedOperator builds the seed operator.
func NewSeedOperator(conn config.ConnectionGetter) *SeedOperator {
	return &SeedOperator{connection: conn}
}

// Run implements scheduler execution for a task instance.
func (o SeedOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return o.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

// RunTask loads the asset's CSV into its target table.
func (o SeedOperator) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) error {
	seedPath, ok := t.Parameters.GetString("path")
	if !ok || seedPath == "" {
		return errors.New("seed assets require a `path` parameter pointing to the CSV file")
	}
	if !filepath.IsAbs(seedPath) {
		seedPath = filepath.Join(filepath.Dir(t.ExecutableFile.Path), seedPath)
	}

	headers, rows, err := readSeedCSV(seedPath)
	if err != nil {
		return err
	}

	statements, err := buildSeedStatements(t, headers, rows)
	if err != nil {
		return err
	}

	connName, err := p.GetConnectionNameForAsset(t)
	if err != nil {
		return err
	}
	rawConn := o.connection.GetConnection(connName)
	if rawConn == nil {
		return config.NewConnectionNotFoundError(ctx, "", connName)
	}
	conn, ok := rawConn.(Client)
	if !ok {
		return errors.Errorf("connection '%s' is not a fabric spark connection", connName)
	}

	if err := conn.CreateSchemaIfNotExist(ctx, t, p.Name); err != nil {
		return err
	}

	for _, statement := range statements {
		q := &query.Query{Query: statement}
		annotatedQuery, err := ansisql.AddAnnotationComment(ctx, q, t.Name, "main", p.Name)
		if err != nil {
			return err
		}
		if err := conn.RunQueryWithoutResult(ctx, annotatedQuery); err != nil {
			return err
		}
	}

	return nil
}

func readSeedCSV(path string) ([]string, [][]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to open seed file %s", path)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to parse seed file %s", path)
	}
	if len(records) == 0 {
		return nil, nil, errors.Errorf("seed file %s is empty, it must contain at least a header row", path)
	}

	headers := records[0]
	for i, h := range headers {
		headers[i] = strings.TrimSpace(h)
	}
	return headers, records[1:], nil
}

// buildSeedStatements produces a CREATE OR REPLACE TABLE plus batched INSERT
// statements for the parsed CSV.
func buildSeedStatements(t *pipeline.Asset, headers []string, rows [][]string) ([]string, error) {
	if len(headers) == 0 {
		return nil, errors.New("seed file has no columns")
	}

	types := make([]string, len(headers))
	declared := make(map[string]string, len(t.Columns))
	for _, col := range t.Columns {
		declared[strings.ToLower(col.Name)] = col.Type
	}
	for i, h := range headers {
		if declaredType, ok := declared[strings.ToLower(h)]; ok && declaredType != "" {
			types[i] = declaredType
		} else {
			types[i] = "STRING"
		}
	}

	columnDefs := make([]string, len(headers))
	for i, h := range headers {
		columnDefs[i] = fmt.Sprintf("%s %s", QuoteIdentifier(h), types[i])
	}

	statements := []string{
		fmt.Sprintf("CREATE OR REPLACE TABLE %s (\n%s\n)", t.Name, strings.Join(columnDefs, ",\n")),
	}

	for start := 0; start < len(rows); start += seedBatchSize {
		end := min(start+seedBatchSize, len(rows))

		values := make([]string, 0, end-start)
		for _, row := range rows[start:end] {
			cells := make([]string, len(headers))
			for i := range headers {
				var cell string
				if i < len(row) {
					cell = row[i]
				}
				cells[i] = seedValueLiteral(cell, types[i])
			}
			values = append(values, "("+strings.Join(cells, ", ")+")")
		}

		statements = append(statements, fmt.Sprintf("INSERT INTO %s VALUES\n%s", t.Name, strings.Join(values, ",\n")))
	}

	return statements, nil
}

// seedValueLiteral renders one CSV cell as a Spark SQL literal, casting to
// the column type when it isn't a string.
func seedValueLiteral(value, sqlType string) string {
	isString := isStringType(sqlType)
	if value == "" && !isString {
		return "NULL"
	}

	escaped := strings.ReplaceAll(value, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, "'", `\'`)
	literal := "'" + escaped + "'"

	if isString {
		return literal
	}
	return fmt.Sprintf("CAST(%s AS %s)", literal, strings.ToUpper(sqlType))
}

func isStringType(sqlType string) bool {
	lc := strings.ToLower(sqlType)
	return lc == "" || lc == "string" || strings.Contains(lc, "char") || strings.Contains(lc, "text")
}
