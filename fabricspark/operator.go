package fabricspark

import (
	"context"
	"io"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

// Asset types provided by this connector. These are the values used in the
// `type` field of bruin asset definitions.
const (
	AssetTypeFabricSparkQuery   = pipeline.AssetType("fabric.spark.sql")
	AssetTypeFabricSparkPySpark = pipeline.AssetType("fabric.spark.pyspark")
)

type materializer interface {
	Render(task *pipeline.Asset, query string) ([]string, error)
	LogIfFullRefreshAndDDL(writer interface{}, asset *pipeline.Asset) error
}

// Client is the connection interface the operators need; *DB implements it.
type Client interface {
	RunQueryWithoutResult(ctx context.Context, query *query.Query) error
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
	SelectWithSchema(ctx context.Context, query *query.Query) (*query.QueryResult, error)
	Ping(ctx context.Context) error
	CreateSchemaIfNotExist(ctx context.Context, asset *pipeline.Asset, pipelineName string) error
	RunPySpark(ctx context.Context, code string) (string, error)
}

// BasicOperator runs `fabric.spark.sql` assets: it extracts the query,
// applies materialization and executes the resulting statements in the
// lakehouse Spark session.
type BasicOperator struct {
	connection   config.ConnectionGetter
	extractor    query.QueryExtractor
	materializer materializer
}

// NewBasicOperator builds the SQL operator.
func NewBasicOperator(conn config.ConnectionGetter, extractor query.QueryExtractor, materializer materializer) *BasicOperator {
	return &BasicOperator{
		connection:   conn,
		extractor:    extractor,
		materializer: materializer,
	}
}

// Run implements scheduler execution for a task instance.
func (o BasicOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return o.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

// RunTask executes a single SQL asset.
func (o BasicOperator) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) error {
	ctx = query.WithQueryType(ctx, query.QueryTypeMain)
	extractor, err := o.extractor.CloneForAsset(ctx, p, t)
	if err != nil {
		return errors.Wrapf(err, "failed to clone extractor for asset %s", t.Name)
	}
	queries, err := extractor.ExtractQueriesFromString(t.ExecutableFile.Content)
	if err != nil {
		return errors.Wrap(err, "cannot extract queries from the task file")
	}

	if len(queries) == 0 {
		return nil
	}

	if len(queries) > 1 && t.Materialization.Type != pipeline.MaterializationTypeNone {
		return errors.New("cannot enable materialization for tasks with multiple queries")
	}

	writer := ctx.Value(executor.KeyPrinter)
	if err := o.materializer.LogIfFullRefreshAndDDL(writer, t); err != nil {
		return err
	}

	materializedQueries, err := o.materializer.Render(t, queries[0].String())
	if err != nil {
		return err
	}

	// Both time_interval and window-bounded anti_join emit {{start_*}} /
	// {{end_*}} placeholders that must be rendered against the run window.
	needsWindowRendering := t.Materialization.Strategy == pipeline.MaterializationStrategyTimeInterval ||
		(t.Materialization.Strategy == MaterializationStrategyAntiJoin && t.Materialization.IncrementalKey != "")
	if needsWindowRendering {
		materializedQueries, err = extractor.ReextractQueriesFromSlice(materializedQueries)
		if err != nil {
			return err
		}
	}

	conn, err := o.getClient(ctx, p, t)
	if err != nil {
		return err
	}

	if t.Materialization.Type != pipeline.MaterializationTypeNone {
		if err := conn.CreateSchemaIfNotExist(ctx, t, p.Name); err != nil {
			return err
		}
	}

	for _, queryString := range materializedQueries {
		q := &query.Query{Query: queryString}
		annotatedQuery, err := ansisql.AddAnnotationComment(ctx, q, t.Name, "main", p.Name)
		if err != nil {
			return err
		}

		ansisql.LogQueryIfVerbose(ctx, writer, annotatedQuery.Query)

		if err := conn.RunQueryWithoutResult(ctx, annotatedQuery); err != nil {
			return err
		}
	}

	return nil
}

func (o BasicOperator) getClient(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) (Client, error) {
	connName, err := p.GetConnectionNameForAsset(t)
	if err != nil {
		return nil, err
	}

	rawConn := o.connection.GetConnection(connName)
	if rawConn == nil {
		return nil, config.NewConnectionNotFoundError(ctx, "", connName)
	}

	conn, ok := rawConn.(Client)
	if !ok {
		return nil, errors.Errorf("connection '%s' is not a fabric spark connection", connName)
	}
	return conn, nil
}

// PySparkOperator runs `fabric.spark.pyspark` assets: the asset file's Python
// code executes as a PySpark statement inside the shared lakehouse Spark
// session, with `spark` (SparkSession) and `sc` (SparkContext) predefined.
type PySparkOperator struct {
	connection config.ConnectionGetter
}

// NewPySparkOperator builds the PySpark operator.
func NewPySparkOperator(conn config.ConnectionGetter) *PySparkOperator {
	return &PySparkOperator{connection: conn}
}

// Run implements scheduler execution for a task instance.
func (o PySparkOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return o.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

// RunTask executes a single PySpark asset.
func (o PySparkOperator) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) error {
	code := t.ExecutableFile.Content
	if code == "" {
		return nil
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

	output, err := conn.RunPySpark(ctx, code)
	if err != nil {
		return errors.Wrapf(err, "pyspark asset %s failed", t.Name)
	}

	if output != "" {
		if writer, ok := ctx.Value(executor.KeyPrinter).(io.Writer); ok && writer != nil {
			_, _ = writer.Write([]byte(output + "\n"))
		}
	}

	return nil
}

// NewColumnCheckOperator wires the standard ANSI SQL column checks plus the
// Spark-specific accepted_values and pattern checks.
func NewColumnCheckOperator(manager config.ConnectionGetter) *ansisql.ColumnCheckOperator {
	return ansisql.NewColumnCheckOperator(map[string]ansisql.CheckRunner{
		"not_null":        ansisql.NewNotNullCheck(manager),
		"unique":          ansisql.NewUniqueCheck(manager),
		"positive":        ansisql.NewPositiveCheck(manager),
		"non_negative":    ansisql.NewNonNegativeCheck(manager),
		"negative":        ansisql.NewNegativeCheck(manager),
		"min":             ansisql.NewMinCheck(manager),
		"max":             ansisql.NewMaxCheck(manager),
		"accepted_values": &AcceptedValuesCheck{conn: manager},
		"pattern":         &PatternCheck{conn: manager},
	})
}
