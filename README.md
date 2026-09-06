# ace-datasource-elasticsearch

Compile-time Elasticsearch datasource module for [Ace](https://github.com/aceobservability/ace).

Ace keeps the datasource contract and registry in
`github.com/aceobservability/ace/backend/pkg/datasource`. This module implements
that `Client` (plus `SignalQueryClient` and a module-side connection test). Ace
registers the factory at `init` and injects its SSRF-safe HTTP client — this
module does not import Ace `internal/` packages and does not construct an
unpolicy'd client.

## Contract

| Surface | Package |
| --- | --- |
| Query / result types | `github.com/aceobservability/ace/backend/pkg/datasource` |
| Registry type key | `elasticsearch` (`Type`) |
| Factory | `New(url string, authConfig json.RawMessage, httpClient *http.Client)` |

`httpClient` is required. Ace passes `ssrf.DatasourceClient` wrapped with stored
datasource credentials. `authConfig` carries Elasticsearch index / field
settings (`index`, `time_field`, `message_field`, `level_field`).

## Tests

```
go test ./...
```

Query and connection tests speak to an `httptest` fixture. No live Elasticsearch
is required.
