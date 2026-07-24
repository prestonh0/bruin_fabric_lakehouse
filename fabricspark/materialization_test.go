package fabricspark

import (
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaterializerRender(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		asset       *pipeline.Asset
		query       string
		fullRefresh bool
		want        []string
		wantErr     string
	}{
		{
			name:  "no materialization passes through",
			asset: &pipeline.Asset{Name: "events"},
			query: "SELECT 1",
			want:  []string{"SELECT 1"},
		},
		{
			name: "view",
			asset: &pipeline.Asset{
				Name: "reporting.events_view",
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeView,
				},
			},
			query: "SELECT * FROM events",
			want: []string{
				"DROP TABLE IF EXISTS reporting.events_view",
				"CREATE OR REPLACE VIEW reporting.events_view AS SELECT * FROM events",
			},
		},
		{
			name: "table create+replace",
			asset: &pipeline.Asset{
				Name: "reporting.events",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyCreateReplace,
				},
			},
			query: "SELECT * FROM raw.events;",
			want: []string{
				"CREATE OR REPLACE TABLE reporting.events\nAS SELECT * FROM raw.events",
			},
		},
		{
			name: "table create+replace with partitioning",
			asset: &pipeline.Asset{
				Name: "reporting.events",
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					Strategy:    pipeline.MaterializationStrategyCreateReplace,
					PartitionBy: "event_date",
				},
			},
			query: "SELECT * FROM raw.events",
			want: []string{
				"CREATE OR REPLACE TABLE reporting.events\nPARTITIONED BY (event_date)\nAS SELECT * FROM raw.events",
			},
		},
		{
			name: "partition_by and cluster_by together fail",
			asset: &pipeline.Asset{
				Name: "reporting.events",
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					Strategy:    pipeline.MaterializationStrategyCreateReplace,
					PartitionBy: "event_date",
					ClusterBy:   []string{"user_id"},
				},
			},
			query:   "SELECT 1",
			wantErr: "cannot be used together",
		},
		{
			name: "append",
			asset: &pipeline.Asset{
				Name: "reporting.events",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyAppend,
				},
			},
			query: "SELECT * FROM raw.events",
			want:  []string{"INSERT INTO reporting.events SELECT * FROM raw.events"},
		},
		{
			name: "delete+insert requires incremental key",
			asset: &pipeline.Asset{
				Name: "reporting.events",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDeleteInsert,
				},
			},
			query:   "SELECT 1",
			wantErr: "incremental_key",
		},
		{
			name: "truncate+insert uses delete from",
			asset: &pipeline.Asset{
				Name: "reporting.events",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyTruncateInsert,
				},
			},
			query: "SELECT * FROM raw.events",
			want: []string{
				"DELETE FROM reporting.events",
				"INSERT INTO reporting.events SELECT * FROM raw.events",
			},
		},
		{
			name: "merge",
			asset: &pipeline.Asset{
				Name: "reporting.users",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true},
					{Name: "name", UpdateOnMerge: true},
				},
			},
			query: "SELECT id, name FROM staging.users",
			want: []string{
				"MERGE INTO reporting.users target\n" +
					"USING (SELECT id, name FROM staging.users) source ON target.id = source.id\n" +
					"WHEN MATCHED THEN UPDATE SET name = source.name\n" +
					"WHEN NOT MATCHED THEN INSERT(id, name) VALUES(id, name)",
			},
		},
		{
			name: "merge requires primary key",
			asset: &pipeline.Asset{
				Name: "reporting.users",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{{Name: "id"}},
			},
			query:   "SELECT 1",
			wantErr: "primary_key",
		},
		{
			name: "time interval date granularity",
			asset: &pipeline.Asset{
				Name: "reporting.events",
				Materialization: pipeline.Materialization{
					Type:            pipeline.MaterializationTypeTable,
					Strategy:        pipeline.MaterializationStrategyTimeInterval,
					IncrementalKey:  "event_date",
					TimeGranularity: pipeline.MaterializationTimeGranularityDate,
				},
			},
			query: "SELECT * FROM raw.events",
			want: []string{
				"DELETE FROM reporting.events WHERE event_date BETWEEN '{{start_date}}' AND '{{end_date}}'",
				"INSERT INTO reporting.events SELECT * FROM raw.events",
			},
		},
		{
			name: "ddl",
			asset: &pipeline.Asset{
				Name: "reporting.events",
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					Strategy:    pipeline.MaterializationStrategyDDL,
					PartitionBy: "event_date",
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "bigint", PrimaryKey: true},
					{Name: "event_date", Type: "date", Description: "partition column"},
				},
			},
			want: []string{
				"CREATE TABLE IF NOT EXISTS reporting.events (\n" +
					"id bigint,\n" +
					"event_date date COMMENT 'partition column'\n" +
					")\nPARTITIONED BY (event_date)",
			},
		},
		{
			name: "incremental scd2 not supported",
			asset: &pipeline.Asset{
				Name: "reporting.users",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
			},
			query:   "SELECT 1",
			wantErr: "--full-refresh",
		},
		{
			name: "full refresh flips delete+insert to create+replace",
			asset: &pipeline.Asset{
				Name: "reporting.events",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "dt",
				},
			},
			query:       "SELECT * FROM raw.events",
			fullRefresh: true,
			want: []string{
				"CREATE OR REPLACE TABLE reporting.events\nAS SELECT * FROM raw.events",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := NewMaterializer(tt.fullRefresh)
			got, err := m.Render(tt.asset, tt.query)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMaterializerRenderDeleteInsertShape(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name: "reporting.events",
		Materialization: pipeline.Materialization{
			Type:           pipeline.MaterializationTypeTable,
			Strategy:       pipeline.MaterializationStrategyDeleteInsert,
			IncrementalKey: "dt",
		},
	}

	got, err := NewMaterializer(false).Render(asset, "SELECT * FROM raw.events")
	require.NoError(t, err)
	require.Len(t, got, 4)
	assert.True(t, strings.HasPrefix(got[0], "CREATE OR REPLACE TEMPORARY VIEW __bruin_tmp_"))
	assert.Contains(t, got[1], "DELETE FROM reporting.events WHERE dt IN (SELECT DISTINCT dt FROM __bruin_tmp_")
	assert.Contains(t, got[2], "INSERT INTO reporting.events SELECT * FROM __bruin_tmp_")
	assert.True(t, strings.HasPrefix(got[3], "DROP VIEW IF EXISTS __bruin_tmp_"))
}

func TestRendererJoinsStatements(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name: "reporting.v",
		Materialization: pipeline.Materialization{
			Type: pipeline.MaterializationTypeView,
		},
	}

	rendered, err := NewRenderer(false).Render(asset, "SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, "DROP TABLE IF EXISTS reporting.v;\nCREATE OR REPLACE VIEW reporting.v AS SELECT 1", rendered)
}
