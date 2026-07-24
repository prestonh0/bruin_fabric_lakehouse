package fabricspark

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildTableExistsShowQuery(t *testing.T) {
	t.Parallel()

	q, err := BuildTableExistsShowQuery("events")
	require.NoError(t, err)
	assert.Equal(t, "SHOW TABLES LIKE 'events'", q)

	q, err = BuildTableExistsShowQuery("reporting.events")
	require.NoError(t, err)
	assert.Equal(t, "SHOW TABLES IN `reporting` LIKE 'events'", q)

	_, err = BuildTableExistsShowQuery("a.b.c")
	require.Error(t, err)

	_, err = BuildTableExistsShowQuery("reporting.")
	require.Error(t, err)
}

// selectFakeClient extends fakeClient with canned Select results.
type selectFakeClient struct {
	fakeClient
	selectResults [][]interface{}
	selectQueries []string
}

func (c *selectFakeClient) Select(_ context.Context, q *query.Query) ([][]interface{}, error) {
	c.selectQueries = append(c.selectQueries, q.Query)
	return c.selectResults, nil
}

func tableSensorAsset() (*pipeline.Pipeline, *pipeline.Asset) {
	asset := &pipeline.Asset{
		Name:       "wait_for_events",
		Type:       AssetTypeFabricSparkTableSensor,
		Connection: "fabric-spark-default",
		Parameters: pipeline.ParameterMap{"table": "reporting.events"},
	}
	p := &pipeline.Pipeline{Name: "p", Assets: []*pipeline.Asset{asset}}
	return p, asset
}

func TestTableSensorTableExists(t *testing.T) {
	t.Parallel()

	client := &selectFakeClient{selectResults: [][]interface{}{{"reporting", "events", false}}}
	conn := &fakeConnectionGetter{connections: map[string]any{"fabric-spark-default": client}}
	sensor := NewTableSensor(conn, "once")

	p, asset := tableSensorAsset()
	require.NoError(t, sensor.RunTask(context.Background(), p, asset))

	require.Len(t, client.selectQueries, 1)
	assert.Equal(t, "SHOW TABLES IN `reporting` LIKE 'events'", client.selectQueries[0])
}

func TestTableSensorTableMissingOnceMode(t *testing.T) {
	t.Parallel()

	client := &selectFakeClient{selectResults: [][]interface{}{}}
	conn := &fakeConnectionGetter{connections: map[string]any{"fabric-spark-default": client}}
	sensor := NewTableSensor(conn, "once")

	p, asset := tableSensorAsset()
	err := sensor.RunTask(context.Background(), p, asset)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected result")
}

func TestTableSensorSkipMode(t *testing.T) {
	t.Parallel()

	sensor := NewTableSensor(&fakeConnectionGetter{connections: map[string]any{}}, "skip")
	p, asset := tableSensorAsset()
	require.NoError(t, sensor.RunTask(context.Background(), p, asset))
}

func TestTableSensorMissingParameter(t *testing.T) {
	t.Parallel()

	sensor := NewTableSensor(&fakeConnectionGetter{connections: map[string]any{}}, "once")
	asset := &pipeline.Asset{Name: "x", Type: AssetTypeFabricSparkTableSensor}
	p := &pipeline.Pipeline{Name: "p", Assets: []*pipeline.Asset{asset}}

	err := sensor.RunTask(context.Background(), p, asset)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "table")
}
