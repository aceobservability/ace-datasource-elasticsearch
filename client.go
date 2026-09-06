package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aceobservability/ace/backend/pkg/datasource"
)

// Type is the RegisterDatasource key Ace uses for this module.
const Type = "elasticsearch"

const (
	signalLogs    = "logs"
	signalMetrics = "metrics"

	defaultIndex = "*"
	defaultLimit = 500
	maxLimit     = 5000
)

var defaultTimestampFields = []string{"@timestamp", "timestamp", "time", "ts", "event_time", "event.time"}
var defaultMessageFields = []string{"message", "msg", "log", "line", "event.original"}
var defaultLevelFields = []string{"level", "log.level", "severity", "severity_text"}

// Client implements the Ace Elasticsearch query datasource.
type Client struct {
	url        string
	httpClient *http.Client
	cfg        clientConfig
}

type clientConfig struct {
	Index        string
	TimestampKey string
	MessageKey   string
	LevelKey     string
}

// New constructs an Elasticsearch datasource client.
// httpClient is required so Ace can inject DatasourceClient (dial/redirect policy + auth).
// authConfig carries index / field settings from the stored datasource row.
func New(elasticsearchURL string, authConfig json.RawMessage, httpClient *http.Client) (*Client, error) {
	if httpClient == nil {
		return nil, fmt.Errorf("http client is required")
	}
	if strings.TrimSpace(elasticsearchURL) == "" {
		return nil, fmt.Errorf("datasource url is required")
	}

	if _, err := url.Parse(elasticsearchURL); err != nil {
		return nil, fmt.Errorf("invalid datasource url: %w", err)
	}

	cfg := parseConfig(authConfig)
	if cfg.Index == "" {
		cfg.Index = defaultIndex
	}
	if cfg.TimestampKey == "" {
		cfg.TimestampKey = "@timestamp"
	}
	if cfg.MessageKey == "" {
		cfg.MessageKey = "message"
	}
	if cfg.LevelKey == "" {
		cfg.LevelKey = "level"
	}

	return &Client{
		url:        elasticsearchURL,
		httpClient: httpClient,
		cfg:        cfg,
	}, nil
}

// HTTPClient returns the injected HTTP client. Ace SSRF tests inspect policy wiring.
func (c *Client) HTTPClient() *http.Client {
	return c.httpClient
}

func (c *Client) Query(ctx context.Context, query string, start, end time.Time, step time.Duration, limit int) (*datasource.QueryResult, error) {
	return c.QueryWithSignal(ctx, query, signalMetrics, start, end, step, limit)
}

func (c *Client) QueryWithSignal(ctx context.Context, query, signal string, start, end time.Time, step time.Duration, limit int) (*datasource.QueryResult, error) {
	normalizedSignal := normalizeSignal(signal)
	if normalizedSignal == "" {
		return nil, fmt.Errorf("invalid elasticsearch signal %q, must be one of: logs, metrics", signal)
	}

	switch normalizedSignal {
	case signalLogs:
		return c.queryLogs(ctx, query, start, end, limit)
	case signalMetrics:
		return c.queryMetrics(ctx, query, start, end, step)
	default:
		return nil, fmt.Errorf("invalid elasticsearch signal %q, must be one of: logs, metrics", signal)
	}
}

func (c *Client) queryLogs(ctx context.Context, query string, start, end time.Time, limit int) (*datasource.QueryResult, error) {
	index, body, err := parseSearchRequest(interpolateTemplate(query, start, end, 0))
	if err != nil {
		return nil, err
	}

	if strings.TrimSpace(index) == "" {
		index = c.cfg.Index
	}
	if strings.TrimSpace(index) == "" {
		index = defaultIndex
	}

	if _, hasQuery := body["query"]; !hasQuery {
		body["query"] = map[string]interface{}{"match_all": map[string]interface{}{}}
	}
	ensureTimeFilter(body, c.cfg.TimestampKey, start, end)

	if _, hasSize := body["size"]; !hasSize {
		body["size"] = clampLimit(limit)
	}
	if _, hasSort := body["sort"]; !hasSort {
		body["sort"] = []interface{}{
			map[string]interface{}{
				c.cfg.TimestampKey: map[string]interface{}{
					"order":         "desc",
					"unmapped_type": "date",
				},
			},
		}
	}

	response, err := c.search(ctx, index, body)
	if err != nil {
		return nil, err
	}

	entries := parseLogs(response, c.cfg)
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}

	return &datasource.QueryResult{
		Status:     "success",
		ResultType: signalLogs,
		Data: &datasource.QueryData{
			ResultType: "streams",
			Logs:       entries,
		},
	}, nil
}

func (c *Client) queryMetrics(ctx context.Context, query string, start, end time.Time, step time.Duration) (*datasource.QueryResult, error) {
	index, body, err := parseSearchRequest(interpolateTemplate(query, start, end, step))
	if err != nil {
		return nil, err
	}

	if strings.TrimSpace(index) == "" {
		index = c.cfg.Index
	}
	if strings.TrimSpace(index) == "" {
		index = defaultIndex
	}

	if _, hasQuery := body["query"]; !hasQuery {
		body["query"] = map[string]interface{}{"match_all": map[string]interface{}{}}
	}
	ensureTimeFilter(body, c.cfg.TimestampKey, start, end)

	if !hasAggregations(body) {
		body["aggs"] = map[string]interface{}{
			"timeseries": map[string]interface{}{
				"date_histogram": map[string]interface{}{
					"field":           c.cfg.TimestampKey,
					"fixed_interval":  fixedInterval(step),
					"min_doc_count":   0,
					"extended_bounds": map[string]interface{}{"min": start.UnixMilli(), "max": end.UnixMilli()},
				},
			},
		}
	}

	if _, hasSize := body["size"]; !hasSize {
		body["size"] = 0
	}

	response, err := c.search(ctx, index, body)
	if err != nil {
		return nil, err
	}

	metrics := parseMetrics(response, start, end)
	if len(metrics) == 0 {
		metrics = normaliseToMetrics(extractSourceRows(response))
	}

	return &datasource.QueryResult{
		Status:     "success",
		ResultType: signalMetrics,
		Data: &datasource.QueryData{
			ResultType: "matrix",
			Result:     metrics,
		},
	}, nil
}

func (c *Client) search(ctx context.Context, index string, requestBody map[string]interface{}) (map[string]interface{}, error) {
	targetURL, err := c.searchURL(index)
	if err != nil {
		return nil, err
	}

	payload, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to encode elasticsearch query: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("failed to create elasticsearch request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to query elasticsearch: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read elasticsearch response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return nil, fmt.Errorf("authentication failed with status %d", resp.StatusCode)
		}

		message := extractErrorMessage(body)
		if message == "" {
			message = strings.TrimSpace(string(body))
		}
		if message == "" {
			message = http.StatusText(resp.StatusCode)
		}

		return nil, fmt.Errorf("elasticsearch query failed with status %d: %s", resp.StatusCode, message)
	}

	decoded := map[string]interface{}{}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("failed to parse elasticsearch response: %w", err)
	}

	return decoded, nil
}

func (c *Client) searchURL(index string) (string, error) {
	parsed, err := url.Parse(c.url)
	if err != nil {
		return "", fmt.Errorf("invalid datasource url: %w", err)
	}

	trimmedIndex := strings.Trim(strings.TrimSpace(index), "/")
	if trimmedIndex == "" {
		trimmedIndex = defaultIndex
	}

	basePath := strings.TrimSuffix(parsed.Path, "/")
	if basePath == "" {
		parsed.Path = "/" + trimmedIndex + "/_search"
	} else {
		parsed.Path = basePath + "/" + trimmedIndex + "/_search"
	}

	return parsed.String(), nil
}

func parseConfig(authConfig json.RawMessage) clientConfig {
	if len(authConfig) == 0 {
		return clientConfig{}
	}

	raw := map[string]interface{}{}
	if err := json.Unmarshal(authConfig, &raw); err != nil {
		return clientConfig{}
	}

	return clientConfig{
		Index:        getMapString(raw, "index", "index_pattern", "indexPattern", "indices"),
		TimestampKey: getMapString(raw, "time_field", "timeField", "timestamp_field", "timestampField"),
		MessageKey:   getMapString(raw, "message_field", "messageField"),
		LevelKey:     getMapString(raw, "level_field", "levelField"),
	}
}

func parseSearchRequest(query string) (string, map[string]interface{}, error) {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return "", nil, fmt.Errorf("query is required")
	}

	if strings.HasPrefix(trimmed, "{") {
		parsed := map[string]interface{}{}
		decoder := json.NewDecoder(strings.NewReader(trimmed))
		decoder.UseNumber()
		if err := decoder.Decode(&parsed); err != nil {
			return "", nil, fmt.Errorf("invalid elasticsearch query JSON: %w", err)
		}

		index := getMapString(parsed, "index", "_index", "indices")
		if rawBody, hasBody := parsed["body"]; hasBody {
			bodyMap, ok := rawBody.(map[string]interface{})
			if !ok {
				return "", nil, fmt.Errorf("elasticsearch query field \"body\" must be an object")
			}

			return index, bodyMap, nil
		}

		body := map[string]interface{}{}
		for key, value := range parsed {
			switch key {
			case "index", "_index", "indices":
				continue
			default:
				body[key] = value
			}
		}

		return index, body, nil
	}

	return "", map[string]interface{}{
		"query": map[string]interface{}{
			"query_string": map[string]interface{}{
				"query": trimmed,
			},
		},
	}, nil
}

var (
	_ datasource.Client            = (*Client)(nil)
	_ datasource.SignalQueryClient = (*Client)(nil)
)
