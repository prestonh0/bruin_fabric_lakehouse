// Package fabricspark implements a Bruin connector for Microsoft Fabric
// Lakehouses, using Spark SQL as the primary compute engine with the ability
// to run PySpark when needed.
//
// The connector talks to the Fabric Lakehouse Livy API — the same interface
// used by Microsoft's dbt-fabricspark adapter — so SQL and PySpark statements
// execute inside a Fabric Spark session against the lakehouse's Delta tables.
package fabricspark

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

const (
	// DefaultEndpoint is the public Microsoft Fabric REST API base URL.
	DefaultEndpoint = "https://api.fabric.microsoft.com/v1"

	// DefaultScope is the Azure AD scope used to acquire tokens for the
	// Fabric Livy API. This mirrors dbt-fabricspark's AZURE_CREDENTIAL_SCOPE.
	DefaultScope = "https://analysis.windows.net/powerbi/api/.default"

	// LivyAPIVersion is the Fabric Lakehouse Livy API version segment.
	LivyAPIVersion = "2023-12-01"

	// DefaultSessionName is the Spark session name shown in the Fabric
	// monitoring UI when the user doesn't configure one.
	DefaultSessionName = "bruin-fabric-spark"
)

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Config holds everything needed to reach a Fabric Lakehouse Spark endpoint.
//
// Authentication supports two modes:
//   - Service principal: TenantID + ClientID + ClientSecret (client-credentials flow)
//   - Static token: AccessToken (e.g. produced by `az account get-access-token
//     --scope "https://analysis.windows.net/powerbi/api/.default"`)
type Config struct {
	// WorkspaceID is the Fabric workspace GUID that contains the lakehouse.
	WorkspaceID string
	// LakehouseID is the lakehouse item GUID.
	LakehouseID string
	// LakehouseName is the display name of the lakehouse; it doubles as the
	// database name for non-schema lakehouses.
	LakehouseName string
	// SchemaName is the default schema for schema-enabled lakehouses
	// (optional; e.g. "dbo").
	SchemaName string
	// Endpoint is the Fabric REST API base URL; defaults to DefaultEndpoint.
	Endpoint string

	// TenantID, ClientID and ClientSecret configure service-principal auth.
	TenantID     string
	ClientID     string
	ClientSecret string
	// AccessToken is a pre-acquired bearer token; takes precedence over the
	// service-principal fields when set.
	AccessToken string
	// Scope overrides the Azure AD scope; defaults to DefaultScope.
	Scope string

	// SessionName names the Livy session in the Fabric monitoring UI.
	SessionName string
	// EnvironmentID optionally pins the session to a Fabric Environment
	// (custom pool / libraries).
	EnvironmentID string
	// SparkConfig holds extra spark conf key/values applied to the session,
	// e.g. {"spark.sql.caseSensitive": "true"}.
	SparkConfig map[string]string

	// HTTPTimeoutSeconds bounds each HTTP call to the Fabric API. Default 120.
	HTTPTimeoutSeconds int
	// SessionStartTimeoutSeconds bounds how long to wait for the Spark
	// session to become idle. Default 600 (Fabric cold starts can be slow).
	SessionStartTimeoutSeconds int
	// StatementTimeoutSeconds bounds how long a single statement may run.
	// Default 43200 (12h); 0 disables the timeout.
	StatementTimeoutSeconds int
	// PollIntervalMillis is the base interval between statement polls. Default 500.
	PollIntervalMillis int
}

// Validate checks that the config identifies a lakehouse and carries a usable
// credential.
func (c *Config) Validate() error {
	if c.WorkspaceID == "" {
		return errors.New("fabric spark connection requires `workspace_id`")
	}
	if !uuidPattern.MatchString(c.WorkspaceID) {
		return fmt.Errorf("`workspace_id` must be a GUID, got %q", c.WorkspaceID)
	}
	if c.LakehouseID == "" {
		return errors.New("fabric spark connection requires `lakehouse_id`")
	}
	if !uuidPattern.MatchString(c.LakehouseID) {
		return fmt.Errorf("`lakehouse_id` must be a GUID, got %q", c.LakehouseID)
	}
	if c.LakehouseName == "" {
		return errors.New("fabric spark connection requires `lakehouse_name`")
	}

	if c.AccessToken == "" {
		if c.TenantID == "" || c.ClientID == "" || c.ClientSecret == "" {
			return errors.New("fabric spark connection requires either `access_token` or the service principal fields `tenant_id`, `client_id` and `client_secret`")
		}
	}

	if c.Endpoint != "" {
		parsed, err := url.Parse(c.Endpoint)
		if err != nil {
			return errors.Wrapf(err, "invalid `endpoint` %q", c.Endpoint)
		}
		if parsed.Scheme != "https" {
			return fmt.Errorf("`endpoint` must use https, got %q", c.Endpoint)
		}
	}

	return nil
}

// LivyEndpoint returns the base URL of the lakehouse's Livy API, e.g.
//
//	https://api.fabric.microsoft.com/v1/workspaces/<ws>/lakehouses/<lh>/livyapi/versions/2023-12-01
func (c *Config) LivyEndpoint() string {
	endpoint := c.Endpoint
	if endpoint == "" {
		endpoint = DefaultEndpoint
	}
	endpoint = strings.TrimSuffix(endpoint, "/")

	return fmt.Sprintf("%s/workspaces/%s/lakehouses/%s/livyapi/versions/%s", endpoint, c.WorkspaceID, c.LakehouseID, LivyAPIVersion)
}

// GetIngestrURI implements the interface bruin uses to detect ingestr
// support. The Fabric Livy API has no ingestr counterpart, so this returns an
// empty string.
func (c *Config) GetIngestrURI() string {
	return ""
}
