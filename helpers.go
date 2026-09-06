package elasticsearch

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aceobservability/ace/backend/pkg/datasource"
)

var sourceTimestampColumns = []string{"timestamp", "time", "ts", "datetime", "date", "_time", "event_time"}
var sourceMetricValueColumns = []string{"value", "val", "metric_value", "sum", "count", "avg", "min", "max"}

type metricSeries struct {
	Metric map[string]string
	Values [][]interface{}
}

type sourceField struct {
	Key   string
	Value interface{}
}

func getMapString(raw map[string]any, keys ...string) string {
	for _, key := range keys {
		value, ok := raw[key]
		if !ok {
			continue
		}
		switch typed := value.(type) {
		case string:
			trimmed := strings.TrimSpace(typed)
			if trimmed != "" {
				return trimmed
			}
		}
	}
	return ""
}

func anyToString(value interface{}) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case json.Number:
		return typed.String()
	default:
		return fmt.Sprint(typed)
	}
}

func anyToFloat64(value interface{}) (float64, bool) {
	switch typed := value.(type) {
	case nil:
		return 0, false
	case float64:
		return typed, true
	case int64:
		return float64(typed), true
	case int:
		return float64(typed), true
	case json.Number:
		floatVal, err := typed.Float64()
		if err != nil {
			return 0, false
		}
		return floatVal, true
	case string:
		if typed == "" {
			return 0, false
		}
		floatVal, err := strconv.ParseFloat(typed, 64)
		if err != nil {
			return 0, false
		}
		return floatVal, true
	}

	return 0, false
}

var structuredLevelPattern = regexp.MustCompile(`(?i)(?:^|[\s>\[(,])(?:level|lvl|severity|severity_text)=(?:"|')?(trace|debug|info|warn|warning|error|fatal|panic|critical)(?:\d+)?(?:"|')?(?:$|[\s,\])])`)

func detectLogLevel(labels map[string]string, line string) string {
	for _, key := range []string{"level", "lvl", "severity", "severity_text"} {
		if level, ok := labels[key]; ok {
			if normalized := normalizeLogLevel(level); normalized != "" {
				return normalized
			}
		}
	}

	if extracted := extractStructuredLogLevel(line); extracted != "" {
		return extracted
	}

	lower := strings.ToLower(line)
	switch {
	case strings.Contains(lower, "error") || strings.Contains(lower, "err="):
		return "error"
	case strings.Contains(lower, "warn"):
		return "warning"
	case strings.Contains(lower, "info"):
		return "info"
	case strings.Contains(lower, "debug"):
		return "debug"
	default:
		return ""
	}
}

func extractStructuredLogLevel(line string) string {
	match := structuredLevelPattern.FindStringSubmatch(line)
	if len(match) < 2 {
		return ""
	}

	return normalizeLogLevel(match[1])
}

func normalizeLogLevel(level string) string {
	normalized := strings.ToLower(strings.TrimSpace(strings.Trim(level, `"'`)))
	if normalized == "" {
		return ""
	}

	switch {
	case strings.HasPrefix(normalized, "trace"):
		return "debug"
	case strings.HasPrefix(normalized, "debug") || normalized == "dbg":
		return "debug"
	case strings.HasPrefix(normalized, "info") || normalized == "information" || normalized == "inf":
		return "info"
	case strings.HasPrefix(normalized, "warn") || normalized == "wrn":
		return "warning"
	case strings.HasPrefix(normalized, "error") || normalized == "err":
		return "error"
	case strings.HasPrefix(normalized, "fatal") || strings.HasPrefix(normalized, "panic") || strings.HasPrefix(normalized, "critical") || normalized == "crit":
		return "error"
	case normalized == "unspecified" || normalized == "unknown" || normalized == "default":
		return ""
	default:
		return ""
	}
}

func metricSignature(metric map[string]string) string {
	if len(metric) == 0 {
		return ""
	}

	keys := make([]string, 0, len(metric))
	for key := range metric {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+metric[key])
	}

	return strings.Join(parts, "|")
}

func valueTimestamp(value []interface{}) float64 {
	if len(value) == 0 {
		return 0
	}

	timestamp, ok := parseFloat(value[0])
	if !ok {
		return 0
	}

	return timestamp
}

func secondsToRFC3339(seconds float64) string {
	nanos := int64(math.Round(seconds * float64(time.Second)))
	return time.Unix(0, nanos).UTC().Format(time.RFC3339Nano)
}

func parseTimestampSeconds(value interface{}) (float64, bool) {
	if value == nil {
		return 0, false
	}

	if typed, ok := value.(time.Time); ok {
		return float64(typed.UnixNano()) / float64(time.Second), true
	}

	if typed, ok := value.(string); ok {
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return 0, false
		}

		if parsed, ok := parseTimeString(trimmed); ok {
			return float64(parsed.UnixNano()) / float64(time.Second), true
		}

		numeric, err := strconv.ParseFloat(trimmed, 64)
		if err != nil {
			return 0, false
		}

		return normalizeEpochSeconds(numeric), true
	}

	numeric, ok := anyToFloat64(value)
	if !ok {
		return 0, false
	}

	return normalizeEpochSeconds(numeric), true
}

func parseTimeString(value string) (time.Time, bool) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	for _, layout := range layouts {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return parsed, true
		}

		parsed, err = time.ParseInLocation(layout, value, time.UTC)
		if err == nil {
			return parsed, true
		}
	}

	return time.Time{}, false
}

func normalizeEpochSeconds(value float64) float64 {
	absValue := math.Abs(value)
	switch {
	case absValue >= 1e18:
		return value / 1e9
	case absValue >= 1e15:
		return value / 1e6
	case absValue >= 1e12:
		return value / 1e3
	default:
		return value
	}
}

func parseFloat(value interface{}) (float64, bool) {
	switch typed := value.(type) {
	case nil:
		return 0, false
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int8:
		return float64(typed), true
	case int16:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint8:
		return float64(typed), true
	case uint16:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case json.Number:
		parsed, err := typed.Float64()
		if err != nil {
			return 0, false
		}
		return parsed, true
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return 0, false
		}
		parsed, err := strconv.ParseFloat(trimmed, 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

func normaliseToMetrics(rows []map[string]interface{}) []datasource.MetricResult {
	seriesBySignature := map[string]*metricSeries{}

	for _, row := range rows {
		timestampField, hasTimestamp := pickSourceField(row, sourceTimestampColumns)
		valueField, hasValue := pickSourceField(row, sourceMetricValueColumns)
		if !hasTimestamp || !hasValue {
			continue
		}

		timestampSeconds, ok := parseTimestampSeconds(timestampField.Value)
		if !ok {
			continue
		}

		value, ok := parseFloat(valueField.Value)
		if !ok {
			continue
		}

		excludedColumns := append(append([]string{}, sourceTimestampColumns...), sourceMetricValueColumns...)
		metric := collectRowLabels(row, excludedColumns)
		if metricName := pickMetricName(row); metricName != "" {
			metric["__name__"] = metricName
		}
		if _, ok := metric["__name__"]; !ok {
			metric["__name__"] = "value"
		}

		signature := metricSignature(metric)
		series, exists := seriesBySignature[signature]
		if !exists {
			series = &metricSeries{
				Metric: metric,
				Values: make([][]interface{}, 0, 32),
			}
			seriesBySignature[signature] = series
		}

		series.Values = append(series.Values, []interface{}{
			timestampSeconds,
			strconv.FormatFloat(value, 'f', -1, 64),
		})
	}

	if len(seriesBySignature) == 0 {
		return []datasource.MetricResult{}
	}

	signatures := make([]string, 0, len(seriesBySignature))
	for signature := range seriesBySignature {
		signatures = append(signatures, signature)
	}
	sort.Strings(signatures)

	results := make([]datasource.MetricResult, 0, len(signatures))
	for _, signature := range signatures {
		series := seriesBySignature[signature]
		sort.Slice(series.Values, func(i, j int) bool {
			return valueTimestamp(series.Values[i]) < valueTimestamp(series.Values[j])
		})

		results = append(results, datasource.MetricResult{
			Metric: series.Metric,
			Values: series.Values,
		})
	}

	return results
}

func pickMetricName(row map[string]interface{}) string {
	if field, ok := pickSourceField(row, []string{"__name__", "metric_name", "metric", "name", "series"}); ok {
		return strings.TrimSpace(anyToString(field.Value))
	}

	return ""
}

func pickSourceField(row map[string]interface{}, candidates []string) (sourceField, bool) {
	if len(row) == 0 {
		return sourceField{}, false
	}

	fieldsByNormalizedName := make(map[string]sourceField, len(row))
	for key, value := range row {
		normalized := normalizeColumnName(key)
		if normalized == "" {
			continue
		}

		if _, exists := fieldsByNormalizedName[normalized]; !exists {
			fieldsByNormalizedName[normalized] = sourceField{Key: key, Value: value}
		}
	}

	for _, candidate := range candidates {
		if field, ok := fieldsByNormalizedName[normalizeColumnName(candidate)]; ok {
			return field, true
		}
	}

	return sourceField{}, false
}

func collectRowLabels(row map[string]interface{}, excludedColumns []string) map[string]string {
	excluded := make(map[string]struct{}, len(excludedColumns))
	for _, column := range excludedColumns {
		excluded[normalizeColumnName(column)] = struct{}{}
	}

	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	labels := map[string]string{}
	for _, key := range keys {
		if _, skip := excluded[normalizeColumnName(key)]; skip {
			continue
		}

		value := strings.TrimSpace(anyToString(row[key]))
		if value == "" {
			continue
		}

		labels[key] = value
	}

	return labels
}

func normalizeColumnName(name string) string {
	trimmed := strings.ToLower(strings.TrimSpace(name))
	if trimmed == "" {
		return ""
	}

	b := strings.Builder{}
	b.Grow(len(trimmed))
	for _, char := range trimmed {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') {
			b.WriteRune(char)
		}
	}

	return b.String()
}
