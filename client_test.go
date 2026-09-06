package elasticsearch

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNew_requiresHTTPClient(t *testing.T) {
	t.Parallel()

	client, err := New("http://localhost:9200", nil, nil)
	if err == nil {
		t.Fatal("expected error for nil http client")
	}
	if client != nil {
		t.Fatal("expected nil client when http client is missing")
	}
	if !strings.Contains(err.Error(), "http client is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryWithSignal_Logs(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0)
	end := start.Add(15 * time.Minute)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST method, got %s", r.Method)
		}
		if r.URL.Path != "/logs-*/_search" {
			t.Fatalf("expected /logs-*/_search path, got %s", r.URL.Path)
		}

		payload, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("failed to read request body: %v", err)
		}

		var body map[string]interface{}
		if err := json.Unmarshal(payload, &body); err != nil {
			t.Fatalf("failed to decode request body: %v", err)
		}

		query, ok := body["query"].(map[string]interface{})
		if !ok {
			t.Fatalf("expected query object in request body")
		}
		if _, hasBool := query["bool"]; !hasBool {
			t.Fatalf("expected bool query with time filter, got %+v", query)
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"hits": {
				"hits": [
					{
						"_index": "logs-2026.02.22",
						"_id": "log-1",
						"_source": {
							"@timestamp": "2026-02-22T15:00:00Z",
							"message": "request failed",
							"level": "ERROR",
							"service.name": "api"
						}
					}
				]
			}
		}`))
	}))
	t.Cleanup(server.Close)

	client, err := New(server.URL, []byte(`{"index":"logs-*"}`), server.Client())
	if err != nil {
		t.Fatalf("failed to create elasticsearch client: %v", err)
	}

	result, err := client.QueryWithSignal(context.Background(), `error`, "logs", start, end, time.Minute, 200)
	if err != nil {
		t.Fatalf("unexpected query error: %v", err)
	}

	if result.ResultType != "logs" {
		t.Fatalf("expected logs result type, got %q", result.ResultType)
	}
	if result.Data == nil || len(result.Data.Logs) != 1 {
		t.Fatalf("expected exactly one log entry, got %+v", result.Data)
	}

	entry := result.Data.Logs[0]
	if entry.Line != "request failed" {
		t.Fatalf("expected line request failed, got %q", entry.Line)
	}
	if entry.Level != "error" {
		t.Fatalf("expected detected level error, got %q", entry.Level)
	}
	if entry.Labels["service.name"] != "api" {
		t.Fatalf("expected service.name label api, got %q", entry.Labels["service.name"])
	}
	if entry.Labels["index"] != "logs-2026.02.22" {
		t.Fatalf("expected index label from hit metadata, got %q", entry.Labels["index"])
	}
}

func TestQueryWithSignal_Metrics(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0)
	end := start.Add(10 * time.Minute)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics-*/_search" {
			t.Fatalf("expected /metrics-*/_search path, got %s", r.URL.Path)
		}

		payload, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("failed to read request body: %v", err)
		}

		var body map[string]interface{}
		if err := json.Unmarshal(payload, &body); err != nil {
			t.Fatalf("failed to decode request body: %v", err)
		}

		if _, ok := body["aggs"].(map[string]interface{}); !ok {
			t.Fatalf("expected generated aggs in metrics query body, got %+v", body)
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"aggregations": {
				"timeseries": {
					"buckets": [
						{"key": 1700000000000, "key_as_string": "2023-11-14T22:13:20.000Z", "doc_count": 10},
						{"key": 1700000060000, "key_as_string": "2023-11-14T22:14:20.000Z", "doc_count": 14}
					]
				}
			}
		}`))
	}))
	t.Cleanup(server.Close)

	client, err := New(server.URL, []byte(`{"index":"metrics-*"}`), server.Client())
	if err != nil {
		t.Fatalf("failed to create elasticsearch client: %v", err)
	}

	result, err := client.QueryWithSignal(context.Background(), "service.name:api", "metrics", start, end, 60*time.Second, 0)
	if err != nil {
		t.Fatalf("unexpected query error: %v", err)
	}

	if result.ResultType != "metrics" {
		t.Fatalf("expected metrics result type, got %q", result.ResultType)
	}
	if result.Data == nil || len(result.Data.Result) != 1 {
		t.Fatalf("expected one metric series, got %+v", result.Data)
	}

	series := result.Data.Result[0]
	if series.Metric["__name__"] != "timeseries.count" {
		t.Fatalf("expected metric name timeseries.count, got %q", series.Metric["__name__"])
	}
	if len(series.Values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(series.Values))
	}
}

func TestQueryWithSignal_InvalidSignal(t *testing.T) {
	t.Parallel()

	client, err := New("http://localhost:9200", nil, http.DefaultClient)
	if err != nil {
		t.Fatalf("failed to create elasticsearch client: %v", err)
	}

	_, err = client.QueryWithSignal(context.Background(), "*", "traces", time.Now().Add(-time.Hour), time.Now(), time.Minute, 0)
	if err == nil {
		t.Fatal("expected error for unsupported traces signal")
	}
	if !strings.Contains(err.Error(), "must be one of: logs, metrics") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestQueryAndTestConnection_againstFixtureHTTP(t *testing.T) {
	t.Parallel()

	var sawSearch, sawHealth bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/_search"):
			sawSearch = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"aggregations": {
					"timeseries": {
						"buckets": [
							{"key": 1600000000000, "key_as_string": "2020-09-13T12:26:40.000Z", "doc_count": 1}
						]
					}
				}
			}`))
		case r.URL.Path == "/_cluster/health":
			sawHealth = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"green"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	client, err := New(srv.URL, nil, srv.Client())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Unix(1600000000, 0).Add(-time.Hour)
	end := time.Unix(1600000000, 0)
	result, err := client.Query(ctx, "*", start, end, time.Minute, 0)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("Query status=%q error=%q", result.Status, result.Error)
	}
	if result.ResultType != "metrics" {
		t.Fatalf("ResultType=%q, want metrics", result.ResultType)
	}
	if !sawSearch {
		t.Fatal("expected fixture to receive /_search")
	}

	if err := client.TestConnection(ctx); err != nil {
		t.Fatalf("TestConnection: %v", err)
	}
	if !sawHealth {
		t.Fatal("expected TestConnection to hit /_cluster/health")
	}
}
