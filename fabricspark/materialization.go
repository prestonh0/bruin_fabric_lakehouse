package fabricspark

import (
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
)

type (
	// MaterializerFunc converts an asset + rendered query into the list of
	// Spark SQL statements that materialize it.
	MaterializerFunc = func(task *pipeline.Asset, query string) ([]string, error)
	// AssetMaterializationMap maps materialization type/strategy pairs to
	// their builder.
	AssetMaterializationMap = map[pipeline.MaterializationType]map[pipeline.MaterializationStrategy]MaterializerFunc
)

// MaterializationStrategyAntiJoin is a connector-specific strategy: it
// appends only the rows from the query that do not already exist in the
// target, matched via LEFT ANTI JOIN on the (possibly compound) business key
// formed by the asset's `primary_key` columns. Unlike `merge` it never
// updates existing rows, so it stays a pure, idempotent insert.
//
// When `incremental_key` and `time_granularity` are also set, the target side
// of the anti join is restricted to the pipeline's run window (the same
// {{start_*}}/{{end_*}} placeholders as `time_interval`), which keeps the
// join cheap on large tables — at the cost of only deduplicating against
// rows whose incremental_key falls inside that window.
const MaterializationStrategyAntiJoin = pipeline.MaterializationStrategy("anti_join")

var matMap = AssetMaterializationMap{
	pipeline.MaterializationTypeView: {
		pipeline.MaterializationStrategyNone:          viewMaterializer,
		pipeline.MaterializationStrategyAppend:        errorMaterializer,
		pipeline.MaterializationStrategyCreateReplace: errorMaterializer,
		pipeline.MaterializationStrategyDeleteInsert:  errorMaterializer,
	},
	pipeline.MaterializationTypeTable: {
		pipeline.MaterializationStrategyNone:           buildCreateReplaceQuery,
		pipeline.MaterializationStrategyAppend:         buildAppendQuery,
		pipeline.MaterializationStrategyCreateReplace:  buildCreateReplaceQuery,
		pipeline.MaterializationStrategyDeleteInsert:   buildIncrementalQuery,
		pipeline.MaterializationStrategyTruncateInsert: buildTruncateInsertQuery,
		pipeline.MaterializationStrategyMerge:          buildMergeQuery,
		pipeline.MaterializationStrategyTimeInterval:   buildTimeIntervalQuery,
		pipeline.MaterializationStrategyDDL:            buildDDLQuery,
		pipeline.MaterializationStrategySCD2ByColumn:   scd2NotSupportedMaterializer,
		pipeline.MaterializationStrategySCD2ByTime:     scd2NotSupportedMaterializer,
		MaterializationStrategyAntiJoin:                buildAntiJoinQuery,
	},
}

func errorMaterializer(asset *pipeline.Asset, query string) ([]string, error) {
	return nil, fmt.Errorf("materialization strategy %s is not supported for materialization type %s and asset type %s", asset.Materialization.Strategy, asset.Materialization.Type, asset.Type)
}

func scd2NotSupportedMaterializer(asset *pipeline.Asset, query string) ([]string, error) {
	return nil, fmt.Errorf("incremental SCD2 strategies are not supported by the Fabric Spark connector yet; run with --full-refresh to rebuild the table, or use the `merge` strategy")
}

func viewMaterializer(asset *pipeline.Asset, query string) ([]string, error) {
	return []string{
		fmt.Sprintf("DROP TABLE IF EXISTS %s", asset.Name),
		fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s", asset.Name, query),
	}, nil
}

func buildAppendQuery(asset *pipeline.Asset, query string) ([]string, error) {
	return []string{fmt.Sprintf("INSERT INTO %s %s", asset.Name, query)}, nil
}

func buildIncrementalQuery(task *pipeline.Asset, query string) ([]string, error) {
	mat := task.Materialization

	if mat.IncrementalKey == "" {
		return nil, fmt.Errorf("materialization strategy %s requires the `incremental_key` field to be set", pipeline.MaterializationStrategyDeleteInsert)
	}

	tempViewName := "__bruin_tmp_" + helpers.PrefixGenerator()

	return []string{
		fmt.Sprintf("CREATE OR REPLACE TEMPORARY VIEW %s AS %s", tempViewName, query),
		fmt.Sprintf("DELETE FROM %s WHERE %s IN (SELECT DISTINCT %s FROM %s)", task.Name, mat.IncrementalKey, mat.IncrementalKey, tempViewName),
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", task.Name, tempViewName),
		"DROP VIEW IF EXISTS " + tempViewName,
	}, nil
}

func buildTruncateInsertQuery(task *pipeline.Asset, query string) ([]string, error) {
	// DELETE FROM instead of TRUNCATE: it is supported on every Delta table
	// in Fabric Spark regardless of runtime version, with the same effect.
	return []string{
		"DELETE FROM " + task.Name,
		fmt.Sprintf("INSERT INTO %s %s", task.Name, strings.TrimSuffix(query, ";")),
	}, nil
}

func buildMergeQuery(asset *pipeline.Asset, query string) ([]string, error) {
	if len(asset.Columns) == 0 {
		return nil, fmt.Errorf("materialization strategy %s requires the `columns` field to be set", asset.Materialization.Strategy)
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return nil, fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column", asset.Materialization.Strategy)
	}

	nonPrimaryKeys := asset.ColumnNamesWithUpdateOnMerge()
	columnNames := asset.ColumnNames()

	on := make([]string, 0, len(primaryKeys))
	for _, key := range primaryKeys {
		on = append(on, fmt.Sprintf("target.%s = source.%s", key, key))
	}
	onQuery := strings.Join(on, " AND ")

	allColumnValues := strings.Join(columnNames, ", ")

	mergeLines := []string{
		fmt.Sprintf("MERGE INTO %s target", asset.Name),
		fmt.Sprintf("USING (%s) source ON %s", strings.TrimSuffix(query, ";"), onQuery),
	}

	if len(nonPrimaryKeys) > 0 {
		matchedUpdateStatements := make([]string, 0, len(nonPrimaryKeys))
		for _, col := range nonPrimaryKeys {
			matchedUpdateStatements = append(matchedUpdateStatements, fmt.Sprintf("%s = source.%s", col, col))
		}
		mergeLines = append(mergeLines, "WHEN MATCHED THEN UPDATE SET "+strings.Join(matchedUpdateStatements, ", "))
	}

	mergeLines = append(mergeLines, fmt.Sprintf("WHEN NOT MATCHED THEN INSERT(%s) VALUES(%s)", allColumnValues, allColumnValues))

	return []string{strings.Join(mergeLines, "\n")}, nil
}

func buildCreateReplaceQuery(task *pipeline.Asset, query string) ([]string, error) {
	mat := task.Materialization
	query = strings.TrimSuffix(strings.TrimSpace(query), ";")

	partitionBy := ""
	if mat.PartitionBy != "" {
		partitionBy = fmt.Sprintf("\nPARTITIONED BY (%s)", mat.PartitionBy)
	}

	clusterBy := ""
	if len(mat.ClusterBy) > 0 {
		// Liquid clustering; requires Fabric runtime 1.3+ (Delta 3.1+).
		clusterBy = "\nCLUSTER BY (" + strings.Join(mat.ClusterBy, ", ") + ")"
	}
	if partitionBy != "" && clusterBy != "" {
		return nil, fmt.Errorf("`partition_by` and `cluster_by` cannot be used together on Fabric Spark assets")
	}

	// CREATE OR REPLACE on a Delta table is atomic and keeps table history,
	// so no temp-table dance is needed.
	return []string{
		fmt.Sprintf("CREATE OR REPLACE TABLE %s%s%s\nAS %s", task.Name, partitionBy, clusterBy, query),
	}, nil
}

func buildAntiJoinQuery(task *pipeline.Asset, query string) ([]string, error) {
	primaryKeys := task.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return nil, fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column — the primary-key columns form the (compound) business key for the anti join", MaterializationStrategyAntiJoin)
	}

	mat := task.Materialization
	if mat.IncrementalKey != "" && mat.TimeGranularity == "" {
		return nil, fmt.Errorf("materialization strategy %s requires `time_granularity` ('date' or 'timestamp') when `incremental_key` is set, so the target scan can be bounded to the run window", MaterializationStrategyAntiJoin)
	}
	if mat.TimeGranularity != "" && mat.TimeGranularity != pipeline.MaterializationTimeGranularityTimestamp && mat.TimeGranularity != pipeline.MaterializationTimeGranularityDate {
		return nil, fmt.Errorf("time_granularity must be either 'date', or 'timestamp'")
	}

	// Null-safe equality so business-key columns containing NULLs still
	// dedupe instead of always re-inserting.
	on := make([]string, 0, len(primaryKeys))
	for _, key := range primaryKeys {
		on = append(on, fmt.Sprintf("src.%s <=> tgt.%s", key, key))
	}

	keyColumns := strings.Join(primaryKeys, ", ")
	targetSide := fmt.Sprintf("(SELECT %s FROM %s)", keyColumns, task.Name)
	if mat.IncrementalKey != "" {
		startVar, endVar := "{{start_timestamp}}", "{{end_timestamp}}"
		if mat.TimeGranularity == pipeline.MaterializationTimeGranularityDate {
			startVar, endVar = "{{start_date}}", "{{end_date}}"
		}
		targetSide = fmt.Sprintf("(SELECT %s FROM %s WHERE %s BETWEEN '%s' AND '%s')", keyColumns, task.Name, mat.IncrementalKey, startVar, endVar)
	}

	tempViewName := "__bruin_tmp_" + helpers.PrefixGenerator()

	return []string{
		fmt.Sprintf("CREATE OR REPLACE TEMPORARY VIEW %s AS %s", tempViewName, query),
		fmt.Sprintf("INSERT INTO %s\nSELECT src.* FROM %s src\nLEFT ANTI JOIN %s tgt ON %s", task.Name, tempViewName, targetSide, strings.Join(on, " AND ")),
		"DROP VIEW IF EXISTS " + tempViewName,
	}, nil
}

func buildTimeIntervalQuery(asset *pipeline.Asset, query string) ([]string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return nil, fmt.Errorf("incremental_key is required for time_interval strategy")
	}

	if asset.Materialization.TimeGranularity == "" {
		return nil, fmt.Errorf("time_granularity is required for time_interval strategy")
	}

	if asset.Materialization.TimeGranularity != pipeline.MaterializationTimeGranularityTimestamp && asset.Materialization.TimeGranularity != pipeline.MaterializationTimeGranularityDate {
		return nil, fmt.Errorf("time_granularity must be either 'date', or 'timestamp'")
	}

	startVar := "{{start_timestamp}}"
	endVar := "{{end_timestamp}}"
	if asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityDate {
		startVar = "{{start_date}}"
		endVar = "{{end_date}}"
	}

	return []string{
		fmt.Sprintf("DELETE FROM %s WHERE %s BETWEEN '%s' AND '%s'", asset.Name, asset.Materialization.IncrementalKey, startVar, endVar),
		fmt.Sprintf("INSERT INTO %s %s", asset.Name, query),
	}, nil
}

func buildDDLQuery(asset *pipeline.Asset, query string) ([]string, error) {
	columnDefs := make([]string, 0, len(asset.Columns))
	for _, col := range asset.Columns {
		def := fmt.Sprintf("%s %s", col.Name, col.SQLType())
		// Spark SQL has no PRIMARY KEY constraint; primary keys stay
		// metadata-only and drive the merge strategy + checks instead.
		if col.Description != "" {
			def += fmt.Sprintf(" COMMENT '%s'", strings.ReplaceAll(col.Description, "'", "\\'"))
		}
		columnDefs = append(columnDefs, def)
	}

	partitionBy := ""
	if asset.Materialization.PartitionBy != "" {
		partitionBy = fmt.Sprintf("\nPARTITIONED BY (%s)", asset.Materialization.PartitionBy)
	}

	clusterBy := ""
	if len(asset.Materialization.ClusterBy) > 0 {
		clusterBy = "\nCLUSTER BY (" + strings.Join(asset.Materialization.ClusterBy, ", ") + ")"
	}

	ddl := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (\n%s\n)%s%s",
		asset.Name,
		strings.Join(columnDefs, ",\n"),
		partitionBy,
		clusterBy,
	)

	return []string{ddl}, nil
}
