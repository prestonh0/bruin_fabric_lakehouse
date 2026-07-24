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
		pipeline.MaterializationStrategySCD2ByColumn:   buildSCD2ByColumnQuery,
		pipeline.MaterializationStrategySCD2ByTime:     buildSCD2ByTimeQuery,
		MaterializationStrategyAntiJoin:                buildAntiJoinQuery,
	},
}

func errorMaterializer(asset *pipeline.Asset, query string) ([]string, error) {
	return nil, fmt.Errorf("materialization strategy %s is not supported for materialization type %s and asset type %s", asset.Materialization.Strategy, asset.Materialization.Type, asset.Type)
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

	// An SCD2 asset under --full-refresh must still be rebuilt with its
	// bookkeeping columns, not as a plain table.
	if mat.Strategy == pipeline.MaterializationStrategySCD2ByTime {
		return buildSCD2ByTimeFullRefresh(task, query)
	}
	if mat.Strategy == pipeline.MaterializationStrategySCD2ByColumn {
		return buildSCD2ByColumnFullRefresh(task, query)
	}

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

// scd2ReservedColumns are managed by the SCD2 strategies and cannot appear in
// the asset's column list.
func scd2CheckReservedColumn(name string) error {
	switch name {
	case "_valid_from", "_valid_until", "_is_current":
		return fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", name)
	}
	return nil
}

// buildSCD2ByColumnQuery emits an incremental SCD2 merge that closes the
// current row and inserts a new one whenever any non-key column changed.
//
// Requires Fabric runtime 1.3+ (Delta 3.x): the merge uses
// WHEN NOT MATCHED BY SOURCE to close rows for keys that disappeared from
// the source.
func buildSCD2ByColumnQuery(asset *pipeline.Asset, query string) ([]string, error) {
	query = strings.TrimRight(query, ";")

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return nil, fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column", asset.Materialization.Strategy)
	}

	incrementalKey := asset.Materialization.IncrementalKey

	var (
		compareConds     = make([]string, 0, len(asset.Columns))
		compareCondsS1T1 = make([]string, 0, len(asset.Columns))
		insertCols       = make([]string, 0, len(asset.Columns)+3)
		insertValues     = make([]string, 0, len(asset.Columns)+3)
	)

	for _, col := range asset.Columns {
		if err := scd2CheckReservedColumn(col.Name); err != nil {
			return nil, err
		}
		insertCols = append(insertCols, col.Name)
		insertValues = append(insertValues, "source."+col.Name)
		if !col.PrimaryKey {
			compareConds = append(compareConds, fmt.Sprintf("target.%s != source.%s", col.Name, col.Name))
			compareCondsS1T1 = append(compareCondsS1T1, fmt.Sprintf("t1.%s != s1.%s", col.Name, col.Name))
		}
	}

	insertCols = append(insertCols, "_valid_from", "_valid_until", "_is_current")

	validFromExpr := "CURRENT_TIMESTAMP()"
	validUntilUpdateExpr := "CURRENT_TIMESTAMP()"
	if incrementalKey != "" {
		validFromExpr = "source." + incrementalKey
		validUntilUpdateExpr = "source." + incrementalKey
	}
	insertValues = append(insertValues, validFromExpr, "TIMESTAMP '9999-12-31 00:00:00'", "TRUE")

	pkListUsing := strings.Join(primaryKeys, ", ")

	onConditions := make([]string, 0, len(primaryKeys)+1)
	for _, pk := range primaryKeys {
		onConditions = append(onConditions, fmt.Sprintf("target.%s = source.%s", pk, pk))
	}
	onConditions = append(onConditions, "target._is_current AND source._is_current")
	onCondition := strings.Join(onConditions, " AND ")

	var whereCondition, matchedCondition string
	if len(compareCondsS1T1) > 0 {
		whereCondition = "(" + strings.Join(compareCondsS1T1, " OR ") + ") AND t1._is_current"
		matchedCondition = strings.Join(compareConds, " OR ")
	} else {
		whereCondition = "FALSE AND t1._is_current"
		matchedCondition = "FALSE"
	}

	queryStr := fmt.Sprintf(
		`
MERGE INTO %s AS target
USING (
  WITH s1 AS (
    %s
  )
  SELECT *, TRUE AS _is_current
  FROM   s1
  UNION ALL
  SELECT s1.*, FALSE AS _is_current
  FROM   s1
  JOIN   %s AS t1 USING (%s)
  WHERE  %s
) AS source
ON  %s

WHEN MATCHED AND (
    %s
) THEN
  UPDATE SET
    _valid_until = %s,
    _is_current  = FALSE

WHEN NOT MATCHED THEN
  INSERT (%s)
  VALUES (%s)

WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN
  UPDATE SET
    _valid_until = CURRENT_TIMESTAMP(),
    _is_current  = FALSE`,
		asset.Name,
		strings.TrimSpace(query),
		asset.Name,
		pkListUsing,
		whereCondition,
		onCondition,
		matchedCondition,
		validUntilUpdateExpr,
		strings.Join(insertCols, ", "),
		strings.Join(insertValues, ", "),
	)

	return []string{strings.TrimSpace(queryStr)}, nil
}

// buildSCD2ByTimeQuery emits an incremental SCD2 merge driven by the
// incremental_key timestamp: a newer source row closes the current target row
// and becomes the new current version.
//
// Requires Fabric runtime 1.3+ (Delta 3.x) for WHEN NOT MATCHED BY SOURCE.
func buildSCD2ByTimeQuery(asset *pipeline.Asset, query string) ([]string, error) {
	query = strings.TrimRight(query, ";")

	if asset.Materialization.IncrementalKey == "" {
		return nil, fmt.Errorf("incremental_key is required for scd2_by_time strategy")
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return nil, fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column", asset.Materialization.Strategy)
	}

	var (
		insertCols   = make([]string, 0, len(asset.Columns)+3)
		insertValues = make([]string, 0, len(asset.Columns)+3)
	)
	for _, col := range asset.Columns {
		if err := scd2CheckReservedColumn(col.Name); err != nil {
			return nil, err
		}
		if col.Name == asset.Materialization.IncrementalKey {
			lcType := strings.ToLower(col.Type)
			if lcType != "timestamp" && lcType != "date" {
				return nil, fmt.Errorf("incremental_key must be TIMESTAMP or DATE in scd2_by_time strategy")
			}
		}
		insertCols = append(insertCols, col.Name)
		insertValues = append(insertValues, "source."+col.Name)
	}

	pkListUsing := strings.Join(primaryKeys, ", ")
	incrementalKey := asset.Materialization.IncrementalKey

	insertCols = append(insertCols, "_valid_from", "_valid_until", "_is_current")
	insertValues = append(insertValues, "source."+incrementalKey, "TIMESTAMP '9999-12-31 00:00:00'", "TRUE")

	joinConds := make([]string, 0, len(primaryKeys)+1)
	for _, pk := range primaryKeys {
		joinConds = append(joinConds, fmt.Sprintf("target.%s = source.%s", pk, pk))
	}
	joinConds = append(joinConds, "target._is_current AND source._is_current")
	onCondition := strings.Join(joinConds, " AND ")

	queryStr := fmt.Sprintf(
		`
MERGE INTO %s AS target
USING (
  WITH s1 AS (
    %s
  )
  SELECT s1.*, TRUE AS _is_current
  FROM   s1
  UNION ALL
  SELECT s1.*, FALSE AS _is_current
  FROM s1
  JOIN   %s AS t1 USING (%s)
  WHERE  t1._valid_from < s1.%s AND t1._is_current
) AS source
ON  %s

WHEN MATCHED AND (
  target._valid_from < source.%s
) THEN
  UPDATE SET
    _valid_until = source.%s,
    _is_current  = FALSE

WHEN NOT MATCHED THEN
  INSERT (%s)
  VALUES (%s)

WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN
  UPDATE SET
    _valid_until = CURRENT_TIMESTAMP(),
    _is_current  = FALSE`,
		asset.Name,
		strings.TrimSpace(query),
		asset.Name,
		pkListUsing,
		incrementalKey,
		onCondition,
		incrementalKey,
		incrementalKey,
		strings.Join(insertCols, ", "),
		strings.Join(insertValues, ", "),
	)

	return []string{strings.TrimSpace(queryStr)}, nil
}

func buildSCD2ByColumnFullRefresh(asset *pipeline.Asset, query string) ([]string, error) {
	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return nil, fmt.Errorf("materialization strategy 'scd2_by_column' requires the `primary_key` field to be set on at least one column")
	}

	validFromExpr := "CURRENT_TIMESTAMP()"
	if asset.Materialization.IncrementalKey != "" {
		validFromExpr = asset.Materialization.IncrementalKey
	}

	stmt := fmt.Sprintf(
		`CREATE OR REPLACE TABLE %s AS
SELECT
  %s AS _valid_from,
  src.*,
  TIMESTAMP '9999-12-31 00:00:00' AS _valid_until,
  TRUE AS _is_current
FROM (
%s
) AS src`,
		asset.Name,
		validFromExpr,
		strings.TrimSpace(query),
	)

	return []string{stmt}, nil
}

func buildSCD2ByTimeFullRefresh(asset *pipeline.Asset, query string) ([]string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return nil, fmt.Errorf("incremental_key is required for scd2_by_time strategy")
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return nil, fmt.Errorf("materialization strategy 'scd2_by_time' requires the `primary_key` field to be set on at least one column")
	}

	stmt := fmt.Sprintf(
		`CREATE OR REPLACE TABLE %s AS
SELECT
  %s AS _valid_from,
  src.*,
  TIMESTAMP '9999-12-31 00:00:00' AS _valid_until,
  TRUE AS _is_current
FROM (
%s
) AS src`,
		asset.Name,
		asset.Materialization.IncrementalKey,
		strings.TrimSpace(query),
	)

	return []string{stmt}, nil
}
