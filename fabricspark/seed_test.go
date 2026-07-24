package fabricspark

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildSeedStatements(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name: "raw.countries",
		Columns: []pipeline.Column{
			{Name: "code", Type: "string"},
			{Name: "population", Type: "bigint"},
		},
	}

	headers := []string{"code", "population", "note"}
	rows := [][]string{
		{"NL", "17000000", "it's flat"},
		{"DE", "", "umlauts \\ backslash"},
	}

	statements, err := buildSeedStatements(asset, headers, rows)
	require.NoError(t, err)
	require.Len(t, statements, 2)

	// Declared columns keep their types; undeclared default to STRING.
	assert.Contains(t, statements[0], "CREATE OR REPLACE TABLE raw.countries")
	assert.Contains(t, statements[0], "`code` string")
	assert.Contains(t, statements[0], "`population` bigint")
	assert.Contains(t, statements[0], "`note` STRING")

	insert := statements[1]
	// Typed columns are CAST; empty non-string cells become NULL.
	assert.Contains(t, insert, "CAST('17000000' AS BIGINT)")
	assert.Contains(t, insert, "NULL")
	// Quotes and backslashes are escaped.
	assert.Contains(t, insert, `'it\'s flat'`)
	assert.Contains(t, insert, `'umlauts \\ backslash'`)
}

func TestBuildSeedStatementsBatches(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{Name: "raw.numbers"}
	rows := make([][]string, seedBatchSize+1)
	for i := range rows {
		rows[i] = []string{"x"}
	}

	statements, err := buildSeedStatements(asset, []string{"value"}, rows)
	require.NoError(t, err)
	// 1 CREATE + 2 INSERT batches.
	require.Len(t, statements, 3)
	assert.Equal(t, seedBatchSize, strings.Count(statements[1], "('x')"))
	assert.Equal(t, 1, strings.Count(statements[2], "('x')"))
}

func TestSeedOperatorRunTask(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	csvPath := filepath.Join(dir, "countries.csv")
	require.NoError(t, os.WriteFile(csvPath, []byte("code,name\nNL,Netherlands\nDE,Germany\n"), 0o644))

	client := &fakeClient{}
	conn := &fakeConnectionGetter{connections: map[string]any{"fabric-spark-default": client}}
	operator := NewSeedOperator(conn)

	asset := &pipeline.Asset{
		Name:       "raw.countries",
		Type:       AssetTypeFabricSparkSeed,
		Connection: "fabric-spark-default",
		ExecutableFile: pipeline.ExecutableFile{
			Path: filepath.Join(dir, "countries.asset.yml"),
		},
		Parameters: pipeline.ParameterMap{"path": "countries.csv"},
	}
	p := &pipeline.Pipeline{Name: "my-pipeline", Assets: []*pipeline.Asset{asset}}

	require.NoError(t, operator.RunTask(context.Background(), p, asset))

	// Schema creation + CREATE OR REPLACE + one INSERT batch.
	assert.Equal(t, []string{"raw.countries"}, client.schemas)
	require.Len(t, client.queries, 2)
	assert.Contains(t, client.queries[0], "CREATE OR REPLACE TABLE raw.countries")
	assert.Contains(t, client.queries[1], "INSERT INTO raw.countries VALUES")
	assert.Contains(t, client.queries[1], "('NL', 'Netherlands')")
}

func TestSeedOperatorMissingPath(t *testing.T) {
	t.Parallel()

	operator := NewSeedOperator(&fakeConnectionGetter{connections: map[string]any{}})
	asset := &pipeline.Asset{Name: "raw.x", Type: AssetTypeFabricSparkSeed}
	p := &pipeline.Pipeline{Name: "p", Assets: []*pipeline.Asset{asset}}

	err := operator.RunTask(context.Background(), p, asset)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "path")
}
