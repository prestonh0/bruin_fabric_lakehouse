package fabricspark

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

// Statement kinds understood by the Fabric Livy API.
const (
	StatementKindSQL     = "sql"
	StatementKindPySpark = "pyspark"
)

// Livy session states.
const (
	sessionStateIdle         = "idle"
	sessionStateBusy         = "busy"
	sessionStateDead         = "dead"
	sessionStateError        = "error"
	sessionStateKilled       = "killed"
	sessionStateShuttingDown = "shutting_down"
	sessionStateSuccess      = "success"
)

// Livy statement states.
const (
	statementStateAvailable  = "available"
	statementStateError      = "error"
	statementStateCancelled  = "cancelled"
	statementStateCancelling = "cancelling"
)

// LivySession is the subset of the Livy session resource the connector needs.
type LivySession struct {
	ID    json.Number `json:"id"`
	State string      `json:"state"`
	AppID string      `json:"appId"`
}

// IDString normalizes the session ID, which Fabric returns as a number or a string.
func (s *LivySession) IDString() string {
	return s.ID.String()
}

// LivyStatement is a single code statement executed inside a session.
type LivyStatement struct {
	ID     json.Number      `json:"id"`
	State  string           `json:"state"`
	Output *StatementOutput `json:"output"`
}

// StatementOutput carries the result (or error) of a finished statement.
type StatementOutput struct {
	Status    string                     `json:"status"`
	EName     string                     `json:"ename"`
	EValue    string                     `json:"evalue"`
	Traceback []string                   `json:"traceback"`
	Data      map[string]json.RawMessage `json:"data"`
}

// SQLResult is the parsed "application/json" payload of a SQL statement.
type SQLResult struct {
	Schema SQLResultSchema `json:"schema"`
	Data   [][]interface{} `json:"data"`
}

// SQLResultSchema describes the columns of a SQL result.
type SQLResultSchema struct {
	Fields []SQLResultField `json:"fields"`
}

// SQLResultField is a single column descriptor. Type is a RawMessage because
// Spark encodes primitive types as strings and complex types as objects.
type SQLResultField struct {
	Name string          `json:"name"`
	Type json.RawMessage `json:"type"`
}

// TypeName renders the field type as a string, e.g. "string", "long", or the
// JSON representation for complex types.
func (f *SQLResultField) TypeName() string {
	var s string
	if err := json.Unmarshal(f.Type, &s); err == nil {
		return s
	}
	return string(f.Type)
}

// LivyClient is a minimal client for the Fabric Lakehouse Livy API.
type LivyClient struct {
	baseURL    string
	tokens     TokenProvider
	httpClient *http.Client

	sessionStartTimeout time.Duration
	statementTimeout    time.Duration
	pollInterval        time.Duration
}

// NewLivyClient builds a client for the lakehouse identified by the config.
func NewLivyClient(c *Config, tokens TokenProvider) *LivyClient {
	httpTimeout := time.Duration(c.HTTPTimeoutSeconds) * time.Second
	if httpTimeout <= 0 {
		httpTimeout = 120 * time.Second
	}
	sessionStartTimeout := time.Duration(c.SessionStartTimeoutSeconds) * time.Second
	if sessionStartTimeout <= 0 {
		sessionStartTimeout = 600 * time.Second
	}
	statementTimeout := time.Duration(c.StatementTimeoutSeconds) * time.Second
	if c.StatementTimeoutSeconds == 0 {
		statementTimeout = 0 // no timeout
	}
	pollInterval := time.Duration(c.PollIntervalMillis) * time.Millisecond
	if pollInterval <= 0 {
		pollInterval = 500 * time.Millisecond
	}

	return &LivyClient{
		baseURL:             c.LivyEndpoint(),
		tokens:              tokens,
		httpClient:          &http.Client{Timeout: httpTimeout},
		sessionStartTimeout: sessionStartTimeout,
		statementTimeout:    statementTimeout,
		pollInterval:        pollInterval,
	}
}

const maxRequestAttempts = 5

// doRequest performs an authenticated request with retries on transient
// failures: network errors, HTTP 429 (honoring Retry-After) and 5xx.
func (c *LivyClient) doRequest(ctx context.Context, method, path string, payload any) ([]byte, int, error) {
	var body []byte
	if payload != nil {
		var err error
		body, err = json.Marshal(payload)
		if err != nil {
			return nil, 0, errors.Wrap(err, "failed to encode Livy request payload")
		}
	}

	var lastErr error
	for attempt := 0; attempt < maxRequestAttempts; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<(attempt-1)) * time.Second
			select {
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			case <-time.After(backoff):
			}
		}

		token, err := c.tokens.Token(ctx)
		if err != nil {
			return nil, 0, errors.Wrap(err, "failed to acquire Fabric access token")
		}

		var reader io.Reader
		if body != nil {
			reader = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
		if err != nil {
			return nil, 0, errors.Wrap(err, "failed to build Livy request")
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = errors.Wrapf(err, "livy request %s %s failed", method, path)
			continue
		}

		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
		resp.Body.Close()
		if readErr != nil {
			lastErr = errors.Wrap(readErr, "failed to read Livy response body")
			continue
		}

		switch {
		case resp.StatusCode == http.StatusTooManyRequests:
			lastErr = fmt.Errorf("livy request %s %s throttled (HTTP 429): %s", method, path, summarizeBody(respBody))
			if wait := parseRetryAfter(resp.Header.Get("Retry-After")); wait > 0 {
				select {
				case <-ctx.Done():
					return nil, resp.StatusCode, ctx.Err()
				case <-time.After(wait):
				}
			}
			continue
		case resp.StatusCode >= 500:
			lastErr = fmt.Errorf("livy request %s %s failed (HTTP %d): %s", method, path, resp.StatusCode, summarizeBody(respBody))
			continue
		default:
			return respBody, resp.StatusCode, nil
		}
	}

	return nil, 0, errors.Wrapf(lastErr, "livy request failed after %d attempts", maxRequestAttempts)
}

func parseRetryAfter(header string) time.Duration {
	if header == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(header); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return 0
}

// CreateSession starts a new Spark session and waits until it is idle.
func (c *LivyClient) CreateSession(ctx context.Context, payload map[string]any) (*LivySession, error) {
	body, status, err := c.doRequest(ctx, http.MethodPost, "/sessions", payload)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Fabric Spark session")
	}
	if status >= 400 {
		return nil, fmt.Errorf("failed to create Fabric Spark session (HTTP %d): %s", status, summarizeBody(body))
	}

	var session LivySession
	if err := json.Unmarshal(body, &session); err != nil {
		return nil, errors.Wrap(err, "failed to decode Fabric Spark session response")
	}
	if session.IDString() == "" {
		return nil, fmt.Errorf("fabric Spark session response did not contain an id: %s", summarizeBody(body))
	}

	if err := c.waitForSessionIdle(ctx, session.IDString()); err != nil {
		return nil, err
	}

	return &session, nil
}

// GetSession fetches the current state of a session.
func (c *LivyClient) GetSession(ctx context.Context, sessionID string) (*LivySession, error) {
	body, status, err := c.doRequest(ctx, http.MethodGet, "/sessions/"+sessionID, nil)
	if err != nil {
		return nil, err
	}
	if status >= 400 {
		return nil, fmt.Errorf("failed to get Fabric Spark session %s (HTTP %d): %s", sessionID, status, summarizeBody(body))
	}

	var session LivySession
	if err := json.Unmarshal(body, &session); err != nil {
		return nil, errors.Wrap(err, "failed to decode Fabric Spark session response")
	}
	return &session, nil
}

// CloseSession deletes the session, releasing its Spark capacity.
func (c *LivyClient) CloseSession(ctx context.Context, sessionID string) error {
	body, status, err := c.doRequest(ctx, http.MethodDelete, "/sessions/"+sessionID, nil)
	if err != nil {
		return err
	}
	if status >= 400 && status != http.StatusNotFound {
		return fmt.Errorf("failed to close Fabric Spark session %s (HTTP %d): %s", sessionID, status, summarizeBody(body))
	}
	return nil
}

func (c *LivyClient) waitForSessionIdle(ctx context.Context, sessionID string) error {
	deadline := time.Now().Add(c.sessionStartTimeout)
	pollWait := 2 * time.Second

	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s waiting for Fabric Spark session %s to start; increase `session_start_timeout_seconds` or check capacity availability", c.sessionStartTimeout, sessionID)
		}

		session, err := c.GetSession(ctx, sessionID)
		if err != nil {
			return err
		}

		switch session.State {
		case sessionStateIdle:
			return nil
		case sessionStateDead, sessionStateError, sessionStateKilled, sessionStateShuttingDown:
			return fmt.Errorf("fabric Spark session %s failed to start (state: %s)", sessionID, session.State)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollWait):
		}
		if pollWait < 10*time.Second {
			pollWait += time.Second
		}
	}
}

var blockCommentPattern = regexp.MustCompile(`(?s)/\*.*?\*/`)

// sanitizeSQL strips block comments before submission — the Fabric Livy SQL
// path interpolates the statement into a server-side code block where they
// can break parsing (dbt-fabricspark does the same).
func sanitizeSQL(sql string) string {
	return blockCommentPattern.ReplaceAllString(sql, "\n")
}

// SubmitStatement submits code to a session and returns the statement handle.
func (c *LivyClient) SubmitStatement(ctx context.Context, sessionID, code, kind string) (*LivyStatement, error) {
	payload := map[string]any{"code": code, "kind": kind}
	body, status, err := c.doRequest(ctx, http.MethodPost, "/sessions/"+sessionID+"/statements", payload)
	if err != nil {
		return nil, errors.Wrap(err, "failed to submit statement to Fabric Spark session")
	}
	if status >= 400 {
		return nil, &StatementSubmitError{StatusCode: status, Body: summarizeBody(body)}
	}

	var statement LivyStatement
	if err := json.Unmarshal(body, &statement); err != nil {
		return nil, errors.Wrap(err, "failed to decode Livy statement response")
	}
	if statement.ID.String() == "" {
		return nil, fmt.Errorf("livy statement response did not contain an id: %s", summarizeBody(body))
	}
	return &statement, nil
}

// StatementSubmitError distinguishes HTTP-level submit failures so callers
// can recreate expired sessions on 404.
type StatementSubmitError struct {
	StatusCode int
	Body       string
}

func (e *StatementSubmitError) Error() string {
	return fmt.Sprintf("livy statement submit failed (HTTP %d): %s", e.StatusCode, e.Body)
}

// SessionGone reports whether the error indicates the session no longer exists.
func (e *StatementSubmitError) SessionGone() bool {
	return e.StatusCode == http.StatusNotFound
}

// WaitForStatement polls a statement until it completes, returning its output.
// A statement that finishes with an error state produces a descriptive error
// including the Spark evalue and traceback.
func (c *LivyClient) WaitForStatement(ctx context.Context, sessionID, statementID string) (*StatementOutput, error) {
	var deadline time.Time
	if c.statementTimeout > 0 {
		deadline = time.Now().Add(c.statementTimeout)
	}

	path := "/sessions/" + sessionID + "/statements/" + statementID
	pollWait := c.pollInterval
	notFoundRetries := 0

	for {
		if !deadline.IsZero() && time.Now().After(deadline) {
			return nil, fmt.Errorf("timed out after %s waiting for statement %s; increase `statement_timeout_seconds`", c.statementTimeout, statementID)
		}

		body, status, err := c.doRequest(ctx, http.MethodGet, path, nil)
		if err != nil {
			return nil, err
		}
		// A 404 can appear transiently right after submit, before the
		// statement is registered on the server side.
		if status == http.StatusNotFound {
			notFoundRetries++
			if notFoundRetries > 20 {
				return nil, fmt.Errorf("statement %s disappeared from Fabric Spark session %s", statementID, sessionID)
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(pollWait):
			}
			continue
		}
		if status >= 400 {
			return nil, fmt.Errorf("failed to poll statement %s (HTTP %d): %s", statementID, status, summarizeBody(body))
		}

		var statement LivyStatement
		if err := json.Unmarshal(body, &statement); err != nil {
			return nil, errors.Wrap(err, "failed to decode Livy statement poll response")
		}

		switch statement.State {
		case statementStateAvailable:
			return statementResult(&statement)
		case statementStateError, statementStateCancelled, statementStateCancelling:
			return nil, statementError(&statement)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(pollWait):
		}
		if pollWait < 3*time.Second {
			pollWait += 250 * time.Millisecond
		}
	}
}

func statementResult(statement *LivyStatement) (*StatementOutput, error) {
	if statement.Output == nil {
		return &StatementOutput{Status: "ok"}, nil
	}
	if statement.Output.Status != "ok" {
		return nil, statementError(statement)
	}
	return statement.Output, nil
}

func statementError(statement *LivyStatement) error {
	if statement.Output == nil {
		return fmt.Errorf("statement %s finished in state %q without output", statement.ID.String(), statement.State)
	}

	message := statement.Output.EValue
	if message == "" {
		message = fmt.Sprintf("statement finished in state %q", statement.State)
	}
	if len(statement.Output.Traceback) > 0 {
		traceback := ""
		for _, line := range statement.Output.Traceback {
			traceback += line
		}
		if len(traceback) > 2048 {
			traceback = traceback[:2048] + "..."
		}
		return fmt.Errorf("fabric Spark statement failed: %s\n%s", message, traceback)
	}
	return fmt.Errorf("fabric Spark statement failed: %s", message)
}

// ParseSQLOutput extracts the tabular result of a SQL statement.
func ParseSQLOutput(output *StatementOutput) (*SQLResult, error) {
	if output == nil || output.Data == nil {
		return &SQLResult{}, nil
	}
	raw, ok := output.Data["application/json"]
	if !ok {
		return &SQLResult{}, nil
	}

	var result SQLResult
	if err := json.Unmarshal(raw, &result); err != nil {
		// Some statements return a bare JSON array of rows.
		var rows [][]interface{}
		if arrErr := json.Unmarshal(raw, &rows); arrErr == nil {
			return &SQLResult{Data: rows}, nil
		}
		return nil, errors.Wrap(err, "failed to decode Fabric Spark SQL result")
	}
	return &result, nil
}

// ParseTextOutput extracts the plain-text output of a statement (the shape
// PySpark statements produce).
func ParseTextOutput(output *StatementOutput) string {
	if output == nil || output.Data == nil {
		return ""
	}
	raw, ok := output.Data["text/plain"]
	if !ok {
		return ""
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return string(raw)
	}
	return text
}
