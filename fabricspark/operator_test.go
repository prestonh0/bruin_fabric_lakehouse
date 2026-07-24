package fabricspark

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeClient records the statements the operator sends to the connection.
type fakeClient struct {
	queries     []string
	pysparkRuns []string
	schemas     []string
}

func (c *fakeClient) RunQueryWithoutResult(_ context.Context, q *query.Query) error {
	c.queries = append(c.queries, q.Query)
	return nil
}

func (c *fakeClient) Select(_ context.Context, _ *query.Query) ([][]interface{}, error) {
	return nil, nil
}

func (c *fakeClient) SelectWithSchema(_ context.Context, _ *query.Query) (*query.QueryResult, error) {
	return &query.QueryResult{}, nil
}

func (c *fakeClient) Ping(_ context.Context) error { return nil }

func (c *fakeClient) CreateSchemaIfNotExist(_ context.Context, asset *pipeline.Asset, _ string) error {
	c.schemas = append(c.schemas, asset.Name)
	return nil
}

func (c *fakeClient) RunPySpark(_ context.Context, code string) (string, error) {
	c.pysparkRuns = append(c.pysparkRuns, code)
	return "3", nil
}

// fakeConnectionGetter implements config.ConnectionGetter.
type fakeConnectionGetter struct {
	connections map[string]any
}

func (f *fakeConnectionGetter) GetConnection(name string) any {
	return f.connections[name]
}

// passthroughExtractor implements query.QueryExtractor without jinja.
type passthroughExtractor struct {
	reextractCalls int
}

func (e *passthroughExtractor) ExtractQueriesFromString(content string) ([]*query.Query, error) {
	return []*query.Query{{Query: content}}, nil
}

func (e *passthroughExtractor) CloneForAsset(_ context.Context, _ *pipeline.Pipeline, _ *pipeline.Asset) (query.QueryExtractor, error) {
	return e, nil
}

func (e *passthroughExtractor) ReextractQueriesFromSlice(content []string) ([]string, error) {
	e.reextractCalls++
	return content, nil
}

func TestBasicOperatorRunTask(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	conn := &fakeConnectionGetter{connections: map[string]any{"fabric-spark-default": client}}

	operator := NewBasicOperator(conn, &passthroughExtractor{}, NewMaterializer(false))

	asset := &pipeline.Asset{
		Name:       "reporting.events",
		Type:       AssetTypeFabricSparkQuery,
		Connection: "fabric-spark-default",
		ExecutableFile: pipeline.ExecutableFile{
			Content: "SELECT * FROM raw.events",
		},
		Materialization: pipeline.Materialization{
			Type:     pipeline.MaterializationTypeTable,
			Strategy: pipeline.MaterializationStrategyCreateReplace,
		},
	}
	p := &pipeline.Pipeline{Name: "my-pipeline", Assets: []*pipeline.Asset{asset}}

	err := operator.RunTask(context.Background(), p, asset)
	require.NoError(t, err)

	// Schema creation must have been attempted for the materialized asset.
	assert.Equal(t, []string{"reporting.events"}, client.schemas)

	require.Len(t, client.queries, 1)
	assert.Contains(t, client.queries[0], "CREATE OR REPLACE TABLE reporting.events")
}

func TestBasicOperatorMissingConnection(t *testing.T) {
	t.Parallel()

	operator := NewBasicOperator(&fakeConnectionGetter{connections: map[string]any{}}, &passthroughExtractor{}, NewMaterializer(false))

	asset := &pipeline.Asset{
		Name:           "reporting.events",
		Type:           AssetTypeFabricSparkQuery,
		Connection:     "missing",
		ExecutableFile: pipeline.ExecutableFile{Content: "SELECT 1"},
	}
	p := &pipeline.Pipeline{Name: "my-pipeline", Assets: []*pipeline.Asset{asset}}

	err := operator.RunTask(context.Background(), p, asset)
	require.Error(t, err)
}

func TestBasicOperatorRejectsWrongConnectionType(t *testing.T) {
	t.Parallel()

	conn := &fakeConnectionGetter{connections: map[string]any{"other": "not a client"}}
	operator := NewBasicOperator(conn, &passthroughExtractor{}, NewMaterializer(false))

	asset := &pipeline.Asset{
		Name:           "reporting.events",
		Type:           AssetTypeFabricSparkQuery,
		Connection:     "other",
		ExecutableFile: pipeline.ExecutableFile{Content: "SELECT 1"},
	}
	p := &pipeline.Pipeline{Name: "my-pipeline", Assets: []*pipeline.Asset{asset}}

	err := operator.RunTask(context.Background(), p, asset)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a fabric spark connection")
}

func TestBasicOperatorAntiJoinRendersRunWindow(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	conn := &fakeConnectionGetter{connections: map[string]any{"fabric-spark-default": client}}
	extractor := &passthroughExtractor{}
	operator := NewBasicOperator(conn, extractor, NewMaterializer(false))

	asset := &pipeline.Asset{
		Name:       "reporting.orders",
		Type:       AssetTypeFabricSparkQuery,
		Connection: "fabric-spark-default",
		ExecutableFile: pipeline.ExecutableFile{
			Content: "SELECT * FROM staging.orders",
		},
		Materialization: pipeline.Materialization{
			Type:            pipeline.MaterializationTypeTable,
			Strategy:        MaterializationStrategyAntiJoin,
			IncrementalKey:  "order_ts",
			TimeGranularity: pipeline.MaterializationTimeGranularityTimestamp,
		},
		Columns: []pipeline.Column{
			{Name: "source_system", PrimaryKey: true},
			{Name: "order_number", PrimaryKey: true},
		},
	}
	p := &pipeline.Pipeline{Name: "my-pipeline", Assets: []*pipeline.Asset{asset}}

	require.NoError(t, operator.RunTask(context.Background(), p, asset))

	// A window-bounded anti join must go through placeholder re-rendering.
	assert.Equal(t, 1, extractor.reextractCalls)
	require.Len(t, client.queries, 4)
	assert.Contains(t, client.queries[2], "LEFT ANTI JOIN")
	assert.Contains(t, client.queries[2], "src.source_system <=> tgt.source_system AND src.order_number <=> tgt.order_number")
}

func TestPySparkOperatorRunTask(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	conn := &fakeConnectionGetter{connections: map[string]any{"fabric-spark-default": client}}
	operator := NewPySparkOperator(conn)

	asset := &pipeline.Asset{
		Name:       "transform.enrich",
		Type:       AssetTypeFabricSparkPySpark,
		Connection: "fabric-spark-default",
		ExecutableFile: pipeline.ExecutableFile{
			Content: "df = spark.range(3)\nprint(df.count())",
		},
	}
	p := &pipeline.Pipeline{Name: "my-pipeline", Assets: []*pipeline.Asset{asset}}

	err := operator.RunTask(context.Background(), p, asset)
	require.NoError(t, err)

	require.Len(t, client.pysparkRuns, 1)
	assert.Contains(t, client.pysparkRuns[0], "spark.range(3)")
}
