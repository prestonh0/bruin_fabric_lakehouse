package fabricspark

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func scd2Asset(strategy pipeline.MaterializationStrategy, incrementalKey string) *pipeline.Asset {
	return &pipeline.Asset{
		Name: "dims.customers",
		Materialization: pipeline.Materialization{
			Type:           pipeline.MaterializationTypeTable,
			Strategy:       strategy,
			IncrementalKey: incrementalKey,
		},
		Columns: []pipeline.Column{
			{Name: "customer_id", Type: "bigint", PrimaryKey: true},
			{Name: "segment", Type: "string"},
			{Name: "updated_at", Type: "timestamp"},
		},
	}
}

func TestSCD2ByColumnIncremental(t *testing.T) {
	t.Parallel()

	got, err := NewMaterializer(false).Render(scd2Asset(pipeline.MaterializationStrategySCD2ByColumn, ""), "SELECT * FROM src")
	require.NoError(t, err)
	require.Len(t, got, 1)

	sql := got[0]
	assert.Contains(t, sql, "MERGE INTO dims.customers AS target")
	// Change detection compares every non-key column.
	assert.Contains(t, sql, "target.segment != source.segment")
	assert.Contains(t, sql, "target.updated_at != source.updated_at")
	// Keys never appear in the change comparison.
	assert.NotContains(t, sql, "target.customer_id != source.customer_id")
	// Bookkeeping columns are written on insert.
	assert.Contains(t, sql, "_valid_from, _valid_until, _is_current")
	assert.Contains(t, sql, "TIMESTAMP '9999-12-31 00:00:00'")
	// Vanished keys are closed out.
	assert.Contains(t, sql, "WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE")
	// Without an incremental key, validity uses the wall clock.
	assert.Contains(t, sql, "CURRENT_TIMESTAMP()")
}

func TestSCD2ByColumnUsesIncrementalKeyWhenSet(t *testing.T) {
	t.Parallel()

	got, err := NewMaterializer(false).Render(scd2Asset(pipeline.MaterializationStrategySCD2ByColumn, "updated_at"), "SELECT * FROM src")
	require.NoError(t, err)
	assert.Contains(t, got[0], "source.updated_at, TIMESTAMP '9999-12-31 00:00:00', TRUE")
}

func TestSCD2ByTimeIncremental(t *testing.T) {
	t.Parallel()

	got, err := NewMaterializer(false).Render(scd2Asset(pipeline.MaterializationStrategySCD2ByTime, "updated_at"), "SELECT * FROM src")
	require.NoError(t, err)
	require.Len(t, got, 1)

	sql := got[0]
	assert.Contains(t, sql, "MERGE INTO dims.customers AS target")
	// Row versioning is time-driven, not column-comparison-driven.
	assert.Contains(t, sql, "target._valid_from < source.updated_at")
	assert.Contains(t, sql, "t1._valid_from < s1.updated_at")
	assert.Contains(t, sql, "WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE")
	assert.NotContains(t, sql, "!=")
}

func TestSCD2ByTimeValidation(t *testing.T) {
	t.Parallel()

	// Missing incremental key.
	_, err := NewMaterializer(false).Render(scd2Asset(pipeline.MaterializationStrategySCD2ByTime, ""), "SELECT 1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "incremental_key")

	// Incremental key must be a time type.
	asset := scd2Asset(pipeline.MaterializationStrategySCD2ByTime, "segment")
	_, err = NewMaterializer(false).Render(asset, "SELECT 1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TIMESTAMP or DATE")
}

func TestSCD2ReservedColumnsRejected(t *testing.T) {
	t.Parallel()

	asset := scd2Asset(pipeline.MaterializationStrategySCD2ByColumn, "")
	asset.Columns = append(asset.Columns, pipeline.Column{Name: "_is_current", Type: "boolean"})

	_, err := NewMaterializer(false).Render(asset, "SELECT 1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reserved")
}

func TestSCD2RequiresPrimaryKey(t *testing.T) {
	t.Parallel()

	asset := scd2Asset(pipeline.MaterializationStrategySCD2ByColumn, "")
	for i := range asset.Columns {
		asset.Columns[i].PrimaryKey = false
	}

	_, err := NewMaterializer(false).Render(asset, "SELECT 1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "primary_key")
}

func TestSCD2FullRefreshKeepsBookkeepingColumns(t *testing.T) {
	t.Parallel()

	// --full-refresh flips to the create+replace path, which must still
	// rebuild the SCD2 bookkeeping columns rather than a plain table.
	for _, strategy := range []pipeline.MaterializationStrategy{
		pipeline.MaterializationStrategySCD2ByColumn,
		pipeline.MaterializationStrategySCD2ByTime,
	} {
		got, err := NewMaterializer(true).Render(scd2Asset(strategy, "updated_at"), "SELECT * FROM src")
		require.NoError(t, err, strategy)
		require.Len(t, got, 1, strategy)
		assert.Contains(t, got[0], "CREATE OR REPLACE TABLE dims.customers", strategy)
		assert.Contains(t, got[0], "AS _valid_from", strategy)
		assert.Contains(t, got[0], "TRUE AS _is_current", strategy)
	}
}
