package fabricspark

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
)

// TokenProvider yields bearer tokens for the Fabric REST API.
type TokenProvider interface {
	Token(ctx context.Context) (string, error)
}

// staticTokenProvider returns a user-supplied token as-is.
type staticTokenProvider struct {
	token string
}

func (p *staticTokenProvider) Token(_ context.Context) (string, error) {
	return p.token, nil
}

// servicePrincipalTokenProvider implements the Azure AD client-credentials
// flow and caches the token until shortly before expiry.
type servicePrincipalTokenProvider struct {
	tenantID     string
	clientID     string
	clientSecret string
	scope        string

	// authorityBase is the AAD authority, overridable in tests.
	authorityBase string
	httpClient    *http.Client

	mu        sync.Mutex
	token     string
	expiresAt time.Time
}

const tokenExpiryMargin = 5 * time.Minute

// NewTokenProvider builds the right provider for the given config: a static
// provider when an access token is configured, otherwise a service-principal
// provider.
func NewTokenProvider(c *Config) (TokenProvider, error) {
	if c.AccessToken != "" {
		return &staticTokenProvider{token: c.AccessToken}, nil
	}

	if c.TenantID == "" || c.ClientID == "" || c.ClientSecret == "" {
		return nil, errors.New("service principal authentication requires `tenant_id`, `client_id` and `client_secret`")
	}

	scope := c.Scope
	if scope == "" {
		scope = DefaultScope
	}

	return &servicePrincipalTokenProvider{
		tenantID:      c.TenantID,
		clientID:      c.ClientID,
		clientSecret:  c.ClientSecret,
		scope:         scope,
		authorityBase: "https://login.microsoftonline.com",
		httpClient:    &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func (p *servicePrincipalTokenProvider) Token(ctx context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.token != "" && time.Now().Before(p.expiresAt.Add(-tokenExpiryMargin)) {
		return p.token, nil
	}

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", p.clientID)
	form.Set("client_secret", p.clientSecret)
	form.Set("scope", p.scope)

	tokenURL := fmt.Sprintf("%s/%s/oauth2/v2.0/token", strings.TrimSuffix(p.authorityBase, "/"), p.tenantID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", errors.Wrap(err, "failed to build Azure AD token request")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return "", errors.Wrap(err, "failed to reach Azure AD token endpoint")
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", errors.Wrap(err, "failed to read Azure AD token response")
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("azure AD token request failed (HTTP %d): %s", resp.StatusCode, summarizeBody(body))
	}

	var parsed struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", errors.Wrap(err, "failed to decode Azure AD token response")
	}
	if parsed.AccessToken == "" {
		return "", errors.New("azure AD token response did not contain an access token")
	}

	p.token = parsed.AccessToken
	p.expiresAt = time.Now().Add(time.Duration(parsed.ExpiresIn) * time.Second)

	return p.token, nil
}

// summarizeBody trims an HTTP error body so failures stay readable in logs.
func summarizeBody(body []byte) string {
	s := strings.TrimSpace(string(body))
	if len(s) > 512 {
		return s[:512] + "..."
	}
	return s
}
