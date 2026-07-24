package fabricspark

import (
	"fmt"
	"io"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
)

// Materializer renders an asset's query into the list of Spark SQL statements
// that materialize it. Like the databricks connector, it returns a slice
// because several strategies need multiple statements sent as separate Livy
// statements.
type Materializer struct {
	MaterializationMap AssetMaterializationMap
	fullRefresh        bool
}

// NewMaterializer builds a materializer honoring the --full-refresh flag.
func NewMaterializer(fullRefresh bool) *Materializer {
	return &Materializer{
		MaterializationMap: matMap,
		fullRefresh:        fullRefresh,
	}
}

// Render produces the statements for the asset's materialization settings.
func (m *Materializer) Render(asset *pipeline.Asset, query string) ([]string, error) {
	mat := asset.Materialization
	if mat.Type == pipeline.MaterializationTypeNone {
		return []string{query}, nil
	}

	strategy := mat.Strategy
	if m.fullRefresh && mat.Type == pipeline.MaterializationTypeTable {
		if mat.Strategy != pipeline.MaterializationStrategyDDL {
			strategy = pipeline.MaterializationStrategyCreateReplace
		}
	}

	query = strings.TrimSuffix(strings.TrimSpace(query), ";")
	if matFunc, ok := m.MaterializationMap[mat.Type][strategy]; ok {
		return matFunc(asset, query)
	}

	return nil, fmt.Errorf("unsupported materialization type - strategy combination: (`%s` - `%s`)", mat.Type, mat.Strategy)
}

// LogIfFullRefreshAndDDL warns when --full-refresh is combined with the DDL
// strategy, which intentionally never drops the table.
func (m *Materializer) LogIfFullRefreshAndDDL(writer interface{}, asset *pipeline.Asset) error {
	if !m.fullRefresh {
		return nil
	}
	if asset.Materialization.Strategy != pipeline.MaterializationStrategyDDL {
		return nil
	}
	if writer == nil {
		return errors.New("no writer found in context, please create an issue for this: https://github.com/bruin-data/bruin/issues")
	}

	message := "Full refresh detected, but DDL strategy is in use — table will NOT be dropped or recreated.\n"
	writerObj, ok := writer.(io.Writer)
	if !ok {
		return errors.New("writer is not an io.Writer")
	}
	_, err := writerObj.Write([]byte(message))
	return err
}

// Renderer adapts the Materializer to interfaces that expect a single query
// string; statements are joined with semicolons.
type Renderer struct {
	mat *Materializer
}

// NewRenderer builds a single-string renderer honoring --full-refresh.
func NewRenderer(fullRefresh bool) *Renderer {
	return &Renderer{mat: NewMaterializer(fullRefresh)}
}

// Render renders the materialized statements as a single semicolon-joined string.
func (r *Renderer) Render(asset *pipeline.Asset, query string) (string, error) {
	queries, err := r.mat.Render(asset, query)
	if err != nil {
		return "", err
	}
	return strings.Join(queries, ";\n"), nil
}
