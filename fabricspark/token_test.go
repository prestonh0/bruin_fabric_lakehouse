package fabricspark

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStaticTokenProvider(t *testing.T) {
	t.Parallel()

	provider, err := NewTokenProvider(&Config{AccessToken: "static-token"})
	require.NoError(t, err)

	token, err := provider.Token(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "static-token", token)
}

func TestServicePrincipalTokenProviderRequiresFields(t *testing.T) {
	t.Parallel()

	_, err := NewTokenProvider(&Config{TenantID: "t", ClientID: "c"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "client_secret")
}

func TestServicePrincipalTokenProviderFetchesAndCaches(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)

		require.NoError(t, r.ParseForm())
		assert.Equal(t, "/my-tenant/oauth2/v2.0/token", r.URL.Path)
		assert.Equal(t, "client_credentials", r.Form.Get("grant_type"))
		assert.Equal(t, "my-client", r.Form.Get("client_id"))
		assert.Equal(t, "my-secret", r.Form.Get("client_secret"))
		assert.Equal(t, DefaultScope, r.Form.Get("scope"))

		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "issued-token",
			"expires_in":   3600,
		})
	}))
	defer server.Close()

	provider := &servicePrincipalTokenProvider{
		tenantID:      "my-tenant",
		clientID:      "my-client",
		clientSecret:  "my-secret",
		scope:         DefaultScope,
		authorityBase: server.URL,
		httpClient:    server.Client(),
	}

	token, err := provider.Token(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "issued-token", token)

	// The second call must be served from cache.
	token, err = provider.Token(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "issued-token", token)
	assert.Equal(t, int64(1), calls.Load())

	// Force expiry and confirm a refresh happens.
	provider.expiresAt = time.Now().Add(-time.Minute)
	_, err = provider.Token(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(2), calls.Load())
}

func TestServicePrincipalTokenProviderSurfacesErrors(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid_client"}`))
	}))
	defer server.Close()

	provider := &servicePrincipalTokenProvider{
		tenantID:      "my-tenant",
		clientID:      "my-client",
		clientSecret:  "bad-secret",
		scope:         DefaultScope,
		authorityBase: server.URL,
		httpClient:    server.Client(),
	}

	_, err := provider.Token(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid_client")
}
