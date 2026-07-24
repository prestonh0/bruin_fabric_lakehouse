package fabricspark

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

// NewQuerySensor builds the sensor for `fabric.spark.sensor.query` assets.
// The generic ANSI SQL query sensor works as-is because it only needs the
// connection's Select method, which *DB provides.
func NewQuerySensor(conn config.ConnectionGetter, extractor query.QueryExtractor, sensorMode string) *ansisql.QuerySensor {
	return ansisql.NewQuerySensor(conn, extractor, sensorMode)
}

// TableSensor waits until a table exists in the lakehouse, for
// `fabric.spark.sensor.table` assets.
//
// This is a Spark-specific implementation rather than ansisql.TableSensor:
// Fabric Spark lakehouses expose no information_schema to build a portable
// COUNT(*) existence query against, and probing the table directly errors
// (rather than returning zero) while the table doesn't exist yet. Instead the
// sensor pokes `SHOW TABLES IN <schema> LIKE '<table>'` and counts the
// returned rows client-side.
type TableSensor struct {
	connection config.ConnectionGetter
	sensorMode string
}

// NewTableSensor builds the table sensor. sensorMode mirrors bruin's sensor
// modes: "skip" (no-op), "once" or "" (single poke), anything else waits and
// re-pokes until the sensor timeout.
func NewTableSensor(conn config.ConnectionGetter, sensorMode string) *TableSensor {
	return &TableSensor{connection: conn, sensorMode: sensorMode}
}

// Run implements scheduler execution for a task instance.
func (ts *TableSensor) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return ts.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

// RunTask pokes for the table until it exists or the sensor times out.
func (ts *TableSensor) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) error {
	if ts.sensorMode == "skip" {
		return nil
	}

	tableName, ok := t.Parameters.GetString("table")
	if !ok || tableName == "" {
		return errors.New("table sensor requires a parameter named 'table'")
	}

	showQuery, err := BuildTableExistsShowQuery(tableName)
	if err != nil {
		return err
	}

	connName, err := p.GetConnectionNameForAsset(t)
	if err != nil {
		return err
	}
	rawConn := ts.connection.GetConnection(connName)
	if rawConn == nil {
		return config.NewConnectionNotFoundError(ctx, "", connName)
	}
	conn, ok := rawConn.(Client)
	if !ok {
		return errors.Errorf("connection '%s' is not a fabric spark connection", connName)
	}

	printer, printerExists := ctx.Value(executor.KeyPrinter).(io.Writer)
	if printerExists {
		fmt.Fprintln(printer, "Poking:", tableName)
	}

	sensorTimeout := helpers.GetSensorTimeout(t)
	timeout := time.After(sensorTimeout)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			if printerExists {
				fmt.Fprintln(printer, "Sensor timed out after", sensorTimeout)
			}
			return errors.Errorf("Sensor timed out after %s", sensorTimeout)
		default:
			res, err := conn.Select(ctx, &query.Query{Query: showQuery})
			if err != nil {
				if printerExists {
					fmt.Fprintln(printer, "Error: Sensor query failed:", err)
				}
				return err
			}

			if len(res) > 0 {
				return nil
			}

			if ts.sensorMode == "once" || ts.sensorMode == "" {
				return errors.New("Sensor didn't return the expected result")
			}

			pokeInterval := helpers.GetPokeInterval(ctx, t)
			if printerExists {
				fmt.Fprintln(printer, "Info: table not found yet, waiting for", pokeInterval, "seconds")
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(pokeInterval) * time.Second):
			}
		}
	}
}

// BuildTableExistsShowQuery renders the SHOW TABLES query used to probe for a
// table. Accepts `table` or `schema.table` names.
func BuildTableExistsShowQuery(tableName string) (string, error) {
	parts := strings.Split(tableName, ".")
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			return "", errors.Errorf("invalid table name %q", tableName)
		}
	}

	switch len(parts) {
	case 1:
		return fmt.Sprintf("SHOW TABLES LIKE '%s'", escapeSingleQuotes(parts[0])), nil
	case 2:
		return fmt.Sprintf("SHOW TABLES IN %s LIKE '%s'", QuoteIdentifier(parts[0]), escapeSingleQuotes(parts[1])), nil
	default:
		return "", errors.Errorf("table sensor supports `table` or `schema.table` names, got %q", tableName)
	}
}

func escapeSingleQuotes(s string) string {
	return strings.ReplaceAll(s, "'", `\'`)
}
