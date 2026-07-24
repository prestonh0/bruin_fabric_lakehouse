package fabricspark

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validConfig() *Config {
	return &Config{
		WorkspaceID:   "11111111-2222-3333-4444-555555555555",
		LakehouseID:   "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
		LakehouseName: "my_lakehouse",
		TenantID:      "99999999-8888-7777-6666-555555555555",
		ClientID:      "client-id",
		ClientSecret:  "client-secret",
	}
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(c *Config)
		wantErr string
	}{
		{
			name:   "valid service principal config",
			mutate: func(c *Config) {},
		},
		{
			name: "valid static token config",
			mutate: func(c *Config) {
				c.TenantID, c.ClientID, c.ClientSecret = "", "", ""
				c.AccessToken = "token"
			},
		},
		{
			name:    "missing workspace id",
			mutate:  func(c *Config) { c.WorkspaceID = "" },
			wantErr: "workspace_id",
		},
		{
			name:    "non-guid workspace id",
			mutate:  func(c *Config) { c.WorkspaceID = "not-a-guid" },
			wantErr: "must be a GUID",
		},
		{
			name:    "missing lakehouse id",
			mutate:  func(c *Config) { c.LakehouseID = "" },
			wantErr: "lakehouse_id",
		},
		{
			name:    "missing lakehouse name",
			mutate:  func(c *Config) { c.LakehouseName = "" },
			wantErr: "lakehouse_name",
		},
		{
			name: "missing credentials entirely",
			mutate: func(c *Config) {
				c.TenantID, c.ClientID, c.ClientSecret = "", "", ""
			},
			wantErr: "access_token",
		},
		{
			name:    "http endpoint rejected",
			mutate:  func(c *Config) { c.Endpoint = "http://api.fabric.microsoft.com/v1" },
			wantErr: "https",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := validConfig()
			tt.mutate(c)

			err := c.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestConfigLivyEndpoint(t *testing.T) {
	t.Parallel()

	c := validConfig()
	assert.Equal(t,
		"https://api.fabric.microsoft.com/v1/workspaces/11111111-2222-3333-4444-555555555555/lakehouses/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/livyapi/versions/2023-12-01",
		c.LivyEndpoint(),
	)

	c.Endpoint = "https://custom.fabric.microsoft.com/v1/"
	assert.Equal(t,
		"https://custom.fabric.microsoft.com/v1/workspaces/11111111-2222-3333-4444-555555555555/lakehouses/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/livyapi/versions/2023-12-01",
		c.LivyEndpoint(),
	)
}

func TestConfigGetIngestrURI(t *testing.T) {
	t.Parallel()

	// Without a workspace name there is no ingestr mapping.
	assert.Equal(t, "", validConfig().GetIngestrURI())

	// Static-token auth cannot be forwarded to ingestr.
	c := validConfig()
	c.WorkspaceName = "Analytics"
	c.TenantID, c.ClientID, c.ClientSecret = "", "", ""
	c.AccessToken = "token"
	assert.Equal(t, "", c.GetIngestrURI())

	// Service principal + workspace name yields the OneLake destination URI.
	c = validConfig()
	c.WorkspaceName = "Analytics"
	uri := c.GetIngestrURI()
	assert.Contains(t, uri, "onelake://Analytics/my_lakehouse?")
	assert.Contains(t, uri, "client_id=client-id")
	assert.Contains(t, uri, "client_secret=client-secret")
	assert.Contains(t, uri, "tenant_id=99999999-8888-7777-6666-555555555555")
}
