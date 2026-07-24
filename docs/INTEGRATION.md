# Integrating the connector into bruin

Bruin platform connectors are compiled into the `bruin` CLI — there is no
plugin system. This repo keeps the connector as a standalone Go module (so it
builds and tests on its own against `github.com/bruin-data/bruin` as a
library), structured so that it can be dropped into the bruin codebase as
`pkg/fabricspark` for an upstream contribution.

The registration points below mirror how the existing `databricks` connector
is wired; searching bruin for `databricks` (case-insensitive) shows every
touchpoint and this connector follows the same shape at each one.

## 1. Copy the package

Copy `fabricspark/` to `pkg/fabricspark/` in the bruin repo and change the
imports from `github.com/prestonh0/bruin_fabric_lakehouse/fabricspark` to
`github.com/bruin-data/bruin/pkg/fabricspark`. Move the two asset-type
constants out of `operator.go` into `pkg/pipeline` (step 2) to match bruin
convention.

## 2. Asset types — `pkg/pipeline/pipeline.go`

```go
AssetTypeFabricSparkQuery       = AssetType("fabric.spark.sql")
AssetTypeFabricSparkPySpark     = AssetType("fabric.spark.pyspark")
AssetTypeFabricSparkSeed        = AssetType("fabric.spark.seed")
AssetTypeFabricSparkQuerySensor = AssetType("fabric.spark.sensor.query")
AssetTypeFabricSparkTableSensor = AssetType("fabric.spark.sensor.table")
```

Add both to `AssetTypeConnectionMapping` with connection type
`"fabric_spark"`, and add `"fabric_spark": {AssetTypeFabricSparkQuery}` to
the pipeline-level connection mapping so `default_connections.fabric_spark`
resolves.

## 3. Connection config — `pkg/config/connections.go`

```go
type FabricSparkConnection struct {
	ConnectionMetadata `yaml:",inline" mapstructure:",squash"`

	WorkspaceID   string `yaml:"workspace_id" json:"workspace_id" mapstructure:"workspace_id"`
	WorkspaceName string `yaml:"workspace_name,omitempty" json:"workspace_name,omitempty" mapstructure:"workspace_name"`
	LakehouseID   string `yaml:"lakehouse_id" json:"lakehouse_id" mapstructure:"lakehouse_id"`
	LakehouseName string `yaml:"lakehouse_name" json:"lakehouse_name" mapstructure:"lakehouse_name"`
	Schema        string `yaml:"schema,omitempty" json:"schema,omitempty" mapstructure:"schema"`
	Endpoint      string `yaml:"endpoint,omitempty" json:"endpoint,omitempty" mapstructure:"endpoint"`

	TenantID     string `yaml:"tenant_id,omitempty" json:"tenant_id,omitempty" mapstructure:"tenant_id"`
	ClientID     string `yaml:"client_id,omitempty" json:"client_id,omitempty" mapstructure:"client_id" jsonschema:"oneof_required=service_principal"`
	ClientSecret string `yaml:"client_secret,omitempty" json:"client_secret,omitempty" mapstructure:"client_secret" jsonschema:"oneof_required=service_principal" sensitive:"true"`
	AccessToken  string `yaml:"access_token,omitempty" json:"access_token,omitempty" mapstructure:"access_token" jsonschema:"oneof_required=token" sensitive:"true"`

	SessionName   string            `yaml:"session_name,omitempty" json:"session_name,omitempty" mapstructure:"session_name"`
	EnvironmentID string            `yaml:"environment_id,omitempty" json:"environment_id,omitempty" mapstructure:"environment_id"`
	SparkConfig   map[string]string `yaml:"spark_config,omitempty" json:"spark_config,omitempty" mapstructure:"spark_config"`

	HighConcurrency    bool   `yaml:"high_concurrency,omitempty" json:"high_concurrency,omitempty" mapstructure:"high_concurrency"`
	SessionTag         string `yaml:"session_tag,omitempty" json:"session_tag,omitempty" mapstructure:"session_tag"`
	MaxConcurrentREPLs int    `yaml:"max_concurrent_repls,omitempty" json:"max_concurrent_repls,omitempty" mapstructure:"max_concurrent_repls"`

	HTTPTimeoutSeconds         int `yaml:"http_timeout_seconds,omitempty" json:"http_timeout_seconds,omitempty" mapstructure:"http_timeout_seconds"`
	SessionStartTimeoutSeconds int `yaml:"session_start_timeout_seconds,omitempty" json:"session_start_timeout_seconds,omitempty" mapstructure:"session_start_timeout_seconds"`
	StatementTimeoutSeconds    int `yaml:"statement_timeout_seconds,omitempty" json:"statement_timeout_seconds,omitempty" mapstructure:"statement_timeout_seconds"`
}

func (c FabricSparkConnection) GetName() string { return c.Name }
```

Add `FabricSpark []FabricSparkConnection
`yaml:"fabric_spark,omitempty" ...`` to the `Connections` struct, plus the
usual entries in `MergeFrom`/`ConnectionsSummary` (follow the Databricks
lines).

## 4. Connection manager — `pkg/connection/connection.go`

```go
FabricSpark map[string]*fabricspark.DB
```

```go
func (m *Manager) AddFabricSparkConnectionFromConfig(connection *config.FabricSparkConnection) error {
	m.mutex.Lock()
	if m.FabricSpark == nil {
		m.FabricSpark = make(map[string]*fabricspark.DB)
	}
	m.mutex.Unlock()

	client, err := fabricspark.NewDB(&fabricspark.Config{
		WorkspaceID:                connection.WorkspaceID,
		LakehouseID:                connection.LakehouseID,
		LakehouseName:              connection.LakehouseName,
		SchemaName:                 connection.Schema,
		Endpoint:                   connection.Endpoint,
		TenantID:                   connection.TenantID,
		ClientID:                   connection.ClientID,
		ClientSecret:               connection.ClientSecret,
		AccessToken:                connection.AccessToken,
		SessionName:                connection.SessionName,
		EnvironmentID:              connection.EnvironmentID,
		SparkConfig:                connection.SparkConfig,
		HTTPTimeoutSeconds:         connection.HTTPTimeoutSeconds,
		SessionStartTimeoutSeconds: connection.SessionStartTimeoutSeconds,
		StatementTimeoutSeconds:    connection.StatementTimeoutSeconds,
	})
	if err != nil {
		return err
	}

	m.mutex.Lock()
	m.FabricSpark[connection.Name] = client
	m.mutex.Unlock()
	return nil
}
```

Register it in the parallel-processing block at the bottom of the file:

```go
processConnections(cm.SelectedEnvironment.Connections.FabricSpark, connectionManager.AddFabricSparkConnectionFromConfig, &wg, &errList, &mu)
```

and add the `FabricSpark` map to `GetConnection`'s lookup chain.

## 5. Executor defaults — `pkg/executor/defaults.go`

```go
pipeline.AssetTypeFabricSparkQuery: {
	scheduler.TaskInstanceTypeMain:         NoOpOperator{},
	scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
	scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
},
pipeline.AssetTypeFabricSparkPySpark: {
	scheduler.TaskInstanceTypeMain:         NoOpOperator{},
	scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
	scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
},
```

…and identical blocks for `AssetTypeFabricSparkSeed`,
`AssetTypeFabricSparkQuerySensor` and `AssetTypeFabricSparkTableSensor`.

## 6. Operator wiring — `cmd/run.go`

Follow the Databricks block in `setupExecutors`:

```go
if s.WillRunTaskOfType(pipeline.AssetTypeFabricSparkQuery) || estimateCustomCheckType == pipeline.AssetTypeFabricSparkQuery ||
	s.WillRunTaskOfType(pipeline.AssetTypeFabricSparkPySpark) {
	fabricSparkOperator := fabricspark.NewBasicOperator(conn, wholeFileExtractor, pipeline.HookWrapperMaterializerList{
		Mat:     fabricspark.NewMaterializer(fullRefresh),
		Hoister: hoister,
	})
	fabricSparkCheckRunner := fabricspark.NewColumnCheckOperator(conn)
	fabricSparkPySparkOperator := fabricspark.NewPySparkOperator(conn)
	fabricSparkSeedOperator := fabricspark.NewSeedOperator(conn)
	fabricSparkQuerySensor := fabricspark.NewQuerySensor(conn, wholeFileExtractor, sensorMode)
	fabricSparkTableSensor := fabricspark.NewTableSensor(conn, sensorMode)

	mainExecutors[pipeline.AssetTypeFabricSparkQuery][scheduler.TaskInstanceTypeMain] = fabricSparkOperator
	mainExecutors[pipeline.AssetTypeFabricSparkQuery][scheduler.TaskInstanceTypeColumnCheck] = fabricSparkCheckRunner
	mainExecutors[pipeline.AssetTypeFabricSparkQuery][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
	mainExecutors[pipeline.AssetTypeFabricSparkPySpark][scheduler.TaskInstanceTypeMain] = fabricSparkPySparkOperator
	mainExecutors[pipeline.AssetTypeFabricSparkPySpark][scheduler.TaskInstanceTypeColumnCheck] = fabricSparkCheckRunner
	mainExecutors[pipeline.AssetTypeFabricSparkPySpark][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
	mainExecutors[pipeline.AssetTypeFabricSparkSeed][scheduler.TaskInstanceTypeMain] = fabricSparkSeedOperator
	mainExecutors[pipeline.AssetTypeFabricSparkSeed][scheduler.TaskInstanceTypeColumnCheck] = fabricSparkCheckRunner
	mainExecutors[pipeline.AssetTypeFabricSparkSeed][scheduler.TaskInstanceTypeCustomCheck] = customCheckRunner
	mainExecutors[pipeline.AssetTypeFabricSparkQuerySensor][scheduler.TaskInstanceTypeMain] = fabricSparkQuerySensor
	mainExecutors[pipeline.AssetTypeFabricSparkTableSensor][scheduler.TaskInstanceTypeMain] = fabricSparkTableSensor
}
```

Note the table sensor is `fabricspark.NewTableSensor`, not the ansisql one:
Fabric Spark has no information_schema, so the connector probes with
`SHOW TABLES` and counts rows client-side.

Seed assets (`fabric.spark.seed`) do NOT use bruin's shared ingestr-based
seed operator — `fabricspark.NewSeedOperator` loads the CSV natively through
the Spark session. The ingestr URI the connection exposes (see
`Config.GetIngestrURI`) is for regular `ingestr` assets targeting the
lakehouse as an ingestr OneLake destination; it requires `workspace_name` and
service-principal auth.

Note: `pipeline.HookWrapperMaterializerList` expects the list-returning
`Render(asset, query) ([]string, error)` interface, which
`fabricspark.Materializer` implements. If wiring without hooks, pass
`fabricspark.NewMaterializer(fullRefresh)` directly — `BasicOperator` accepts
anything satisfying the same interface.

## 7. The `anti_join` strategy and lint

The connector defines a custom materialization strategy,
`fabricspark.MaterializationStrategyAntiJoin` (`"anti_join"`). Strategy names
are only validated by the lint rule that checks against
`pipeline.AllAvailableMaterializationStrategies` — execution does not gate on
the list — so at runtime the strategy works as-is. For `bruin validate` to
accept it upstream, add the constant to `pkg/pipeline/pipeline.go` next to
the other `MaterializationStrategy*` constants and append it to
`AllAvailableMaterializationStrategies` (or to a per-platform allowlist if
one exists by then, since only Fabric Spark implements it).

## 8. Optional niceties

- `pkg/lint`: add both asset types to the valid-type and connection-exists
  rules (most of this falls out of the `AssetTypeConnectionMapping` entry).
- `bruin connections ping`: works out of the box via `DB.Ping`. Be aware a
  ping cold-starts a Spark session and can take minutes on an idle capacity.
- `bruin query`/data diff: `DB` implements `Select`, `SelectWithSchema`,
  `GetDatabases`, `GetTables`, `GetColumns`.
- Session cleanup: call `DB.Close(ctx)` at the end of a run (e.g. from the
  connection manager's cleanup path) to release the Fabric Spark session
  instead of waiting for its idle timeout.
