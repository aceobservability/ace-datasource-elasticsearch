package elasticsearch

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aceobservability/ace/backend/pkg/datasource"
)

func parseLogs(response map[string]interface{}, cfg clientConfig) []datasource.LogEntry {
	hits := extractHits(response)
	if len(hits) == 0 {
		return []datasource.LogEntry{}
	}

	entries := make([]datasource.LogEntry, 0, len(hits))
	timestampCandidates := append([]string{cfg.TimestampKey}, defaultTimestampFields...)
	messageCandidates := append([]string{cfg.MessageKey}, defaultMessageFields...)
	levelCandidates := append([]string{cfg.LevelKey}, defaultLevelFields...)

	for _, hit := range hits {
		document := flattenHit(hit)
		if len(document) == 0 {
			continue
		}

		timestampValue, hasTimestamp := pickField(document, timestampCandidates)
		messageValue, hasMessage := pickField(document, messageCandidates)

		timestamp := ""
		if hasTimestamp {
			timestamp = formatTimestamp(timestampValue)
		}
		if timestamp == "" {
			timestamp = parseHitTimestamp(hit)
		}

		line := ""
		if hasMessage {
			line = strings.TrimSpace(anyToString(messageValue))
		}
		if line == "" {
			if payload, err := json.Marshal(document); err == nil {
				line = string(payload)
			}
		}

		excludedColumns := append([]string{}, timestampCandidates...)
		excludedColumns = append(excludedColumns, messageCandidates...)
		excludedColumns = append(excludedColumns, levelCandidates...)

		labels := collectLabels(document, excludedColumns)
		if indexName := strings.TrimSpace(anyToString(hit["_index"])); indexName != "" {
			labels["index"] = indexName
		}
		if id := strings.TrimSpace(anyToString(hit["_id"])); id != "" {
			labels["_id"] = id
		}

		if levelValue, ok := pickField(document, levelCandidates); ok {
			if level := strings.TrimSpace(anyToString(levelValue)); level != "" {
				labels["level"] = level
			}
		}

		entries = append(entries, datasource.LogEntry{
			Timestamp: timestamp,
			Line:      line,
			Labels:    labels,
			Level:     detectLogLevel(labels, line),
		})
	}

	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].Timestamp > entries[j].Timestamp
	})

	return entries
}

func parseMetrics(response map[string]interface{}, start, end time.Time) []datasource.MetricResult {
	aggregations, ok := response["aggregations"].(map[string]interface{})
	if !ok || len(aggregations) == 0 {
		return []datasource.MetricResult{}
	}

	seriesBySignature := map[string]*metricSeries{}
	defaultTimestamp := float64(end.Unix())
	if defaultTimestamp <= 0 {
		defaultTimestamp = float64(start.Unix())
	}

	for name, rawAgg := range aggregations {
		collectAggregationMetrics(name, rawAgg, map[string]string{}, defaultTimestamp, seriesBySignature)
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

func collectAggregationMetrics(
	aggName string,
	raw interface{},
	labels map[string]string,
	defaultTimestamp float64,
	out map[string]*metricSeries,
) {
	node, ok := raw.(map[string]interface{})
	if !ok || len(node) == 0 {
		return
	}

	if value, hasValue := node["value"]; hasValue {
		if numericValue, ok := anyToFloat64(value); ok && !math.IsNaN(numericValue) {
			appendMetricPoint(out, metricLabelsWithName(labels, aggName), defaultTimestamp, numericValue)
		}
	}

	if percentiles, ok := node["values"].(map[string]interface{}); ok {
		keys := make([]string, 0, len(percentiles))
		for key := range percentiles {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		for _, key := range keys {
			if value, ok := anyToFloat64(percentiles[key]); ok && !math.IsNaN(value) {
				percentileLabels := cloneMetricLabels(labels)
				percentileLabels["percentile"] = key
				appendMetricPoint(out, metricLabelsWithName(percentileLabels, aggName), defaultTimestamp, value)
			}
		}
	}

	if rawBuckets, hasBuckets := node["buckets"]; hasBuckets {
		buckets, ok := rawBuckets.([]interface{})
		if !ok {
			return
		}

		for _, rawBucket := range buckets {
			bucket, ok := rawBucket.(map[string]interface{})
			if !ok {
				continue
			}

			bucketTimestamp, bucketIsTime := parseBucketTimestamp(bucket)
			if bucketTimestamp <= 0 {
				bucketTimestamp = defaultTimestamp
			}

			bucketLabels := cloneMetricLabels(labels)
			if !bucketIsTime {
				bucketKey := strings.TrimSpace(anyToString(bucket["key_as_string"]))
				if bucketKey == "" {
					bucketKey = strings.TrimSpace(anyToString(bucket["key"]))
				}
				if bucketKey != "" {
					bucketLabels[aggName] = bucketKey
				}
			}

			if docCount, ok := anyToFloat64(bucket["doc_count"]); ok && !math.IsNaN(docCount) {
				appendMetricPoint(out, metricLabelsWithName(bucketLabels, buildMetricName(aggName, "count")), bucketTimestamp, docCount)
			}

			for key, value := range bucket {
				switch key {
				case "key", "key_as_string", "doc_count", "doc_count_error_upper_bound", "sum_other_doc_count":
					continue
				}

				if childMetric, ok := value.(map[string]interface{}); ok {
					if childValue, ok := anyToFloat64(childMetric["value"]); ok && !math.IsNaN(childValue) {
						appendMetricPoint(out, metricLabelsWithName(bucketLabels, buildMetricName(aggName, key)), bucketTimestamp, childValue)
					}

					if childPercentiles, ok := childMetric["values"].(map[string]interface{}); ok {
						for percentile, percentileValue := range childPercentiles {
							if numericPercentileValue, ok := anyToFloat64(percentileValue); ok && !math.IsNaN(numericPercentileValue) {
								childLabels := cloneMetricLabels(bucketLabels)
								childLabels["percentile"] = percentile
								appendMetricPoint(
									out,
									metricLabelsWithName(childLabels, buildMetricName(aggName, key)),
									bucketTimestamp,
									numericPercentileValue,
								)
							}
						}
					}
				}

				collectAggregationMetrics(buildMetricName(aggName, key), value, bucketLabels, bucketTimestamp, out)
			}
		}

		return
	}

	for key, value := range node {
		switch key {
		case "value", "values", "doc_count", "key", "key_as_string":
			continue
		default:
			collectAggregationMetrics(buildMetricName(aggName, key), value, labels, defaultTimestamp, out)
		}
	}
}

func appendMetricPoint(seriesBySignature map[string]*metricSeries, metric map[string]string, timestamp, value float64) {
	if len(metric) == 0 {
		return
	}

	signature := metricSignature(metric)
	series, ok := seriesBySignature[signature]
	if !ok {
		series = &metricSeries{
			Metric: metric,
			Values: make([][]interface{}, 0, 32),
		}
		seriesBySignature[signature] = series
	}

	series.Values = append(series.Values, []interface{}{
		timestamp,
		strconv.FormatFloat(value, 'f', -1, 64),
 mar	})
}

func extractSourceRows(response map[string]interface{}) []map[string]interface{} {
	hits := extractHits(response)
	if len(hits) == 0 {
		return []map[string]interface{}{}
	}

	rows := make([]map[string]interface{}, 0, len(hits))
	for _, hit := range hits {
		row := flattenHit(hit)
		if len(row) == 0 {
			continue
		}
		rows = append(rows, row)
	}

	return rows
}

func extractHits(response map[string]interface{}) []map[string]interface{} {
	hitsWrapper, ok := response["hits"].(map[string]interface{})
	if !ok {
		return nil
	}

	rawHits, ok := hitsWrapper["hits"].([]interface{})
	if !ok {
		return nil
	}

	hits := make([]map[string]interface{}, 0, len(rawHits))
	for _, raw := range rawHits {
		if hit, ok := raw.(map[string]interface{}); ok {
			hits = append(hits, hit)
		}
	}

	return hits
}

func flattenHit(hit map[string]interface{}) map[string]interface{} {
	document := map[string]interface{}{}
	if source, ok := hit["_source"].(map[string]interface{}); ok {
		for key, value := range source {
			document[key] = value
		}
	}

	if fields, ok := hit["fields"].(map[string]interface{}); ok {
		for key, value := range fields {
			if _, exists := document[key]; exists {
				continue
			}
			document[key] = firstFieldValue(value)
		}
	}

	return document
}

func firstFieldValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case []interface{}:
		if len(typed) == 0 {
			return nil
		}
		return typed[0]
	default:
		return typed
	}
}

func pickField(document map[string]interface{}, candidates []string) (interface{}, bool) {
	if len(document) == 0 {
		return nil, false
	}

	normalizedFields := make(map[string]interface{}, len(document))
	for key, value := range document {
		normalized := normalizeFieldName(key)
		if normalized == "" {
			continue
		}
		if _, exists := normalizedFields[normalized]; !exists {
			normalizedFields[normalized] = value
		}
	}

	for _, candidate := range candidates {
		if value, ok := document[candidate]; ok {
			return value, true
		}

		normalizedCandidate := normalizeFieldName(candidate)
		if normalizedCandidate == "" {
			continue
		}
		if value, ok := normalizedFields[normalizedCandidate]; ok {
			return value, true
		}
	}

	return nil, false
}

func collectLabels(document map[string]interface{}, excluded []string) map[string]string {
	excludedFields := make(map[string]struct{}, len(excluded))
	for _, field := range excluded {
		normalized := normalizeFieldName(field)
		if normalized == "" {
			continue
		}
		excludedFields[normalized] = struct{}{}
	}

	keys := make([]string, 0, len(document))
	for key := range document {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	labels := map[string]string{}
	for _, key := range keys {
		normalized := normalizeFieldName(key)
		if _, skip := excludedFields[normalized]; skip {
			continue
		}

		value := strings.TrimSpace(anyToString(document[key]))
		if value == "" {
			continue
		}
		labels[key] = value
	}

	return labels
}

func parseHitTimestamp(hit map[string]interface{}) string {
	if rawSort, ok := hit["sort"].([]interface{}); ok && len(rawSort) > 0 {
		if timestamp, ok := parseTimestampSeconds(rawSort[0]); ok {
			return secondsToRFC3339(timestamp)
		}
	}
	return ""
}

func formatTimestamp(value interface{}) string {
	if value == nil {
		return ""
	}

	switch typed := value.(type) {
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return ""
		}
		if parsed, ok := parseTimeString(trimmed); ok {
			return parsed.UTC().Format(time.RFC3339Nano)
		}
		if seconds, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return secondsToRFC3339(normalizeEpochSeconds(seconds))
		}
		return trimmed
	case time.Time:
		return typed.UTC().Format(time.RFC3339Nano)
	}

	if seconds, ok := parseTimestampSeconds(value); ok {
		return secondsToRFC3339(seconds)
	}

	return ""
}

func normalizeSignal(signal string) string {
	normalized := strings.ToLower(strings.TrimSpace(signal))
	if normalized == "" {
		return signalMetrics
	}

	switch normalized {
	case signalLogs, signalMetrics:
		return normalized
	default:
		return ""
	}
}

func clampLimit(limit int) int {
	if limit <= 0 {
		return defaultLimit
	}
	if limit > maxLimit {
		return maxLimit
	}
	return limit
}

func hasAggregations(body map[string]interface{}) bool {
	if _, ok := body["aggs"]; ok {
		return true
	}
	if _, ok := body["aggregations"]; ok {
		return true
	}
	return false
}

func ensureTimeFilter(body map[string]interface{}, timestampField string, start, end time.Time) {
	trimmedField := strings.TrimSpace(timestampField)
	if trimmedField == "" {
		trimmedField = "@timestamp"
	}

	rangeFilter := map[string]interface{}{
		"range": map[string]interface{}{
			trimmedField: map[string]interface{}{
				"gte":    start.UnixMilli(),
				"lte":    end.UnixMilli(),
				"format": "epoch_millis",
			},
		},
	}

	rawQuery, hasQuery := body["query"]
	if !hasQuery {
		body["query"] = map[string]interface{}{
			"bool": map[string]interface{}{
				"filter": []interface{}{rangeFilter},
			},
		}
		return
	}

	queryMap, ok := rawQuery.(map[string]interface{})
	if !ok {
		body["query"] = map[string]interface{}{
			"bool": map[string]interface{}{
				"must":   []interface{}{rawQuery},
				"filter": []interface{}{rangeFilter},
			},
		}
		return
	}

	if boolQueryRaw, hasBool := queryMap["bool"]; hasBool {
		boolQuery, ok := boolQueryRaw.(map[string]interface{})
		if !ok {
			queryMap["bool"] = map[string]interface{}{
				"must":   []interface{}{boolQueryRaw},
				"filter": []interface{}{rangeFilter},
			}
			body["query"] = queryMap
			return
		}

		filters := toInterfaceSlice(boolQuery["filter"])
		if !containsRangeFilter(filters, trimmedField) {
			filters = append(filters, rangeFilter)
		}
		boolQuery["filter"] = filters
		queryMap["bool"] = boolQuery
		body["query"] = queryMap
		return
	}

	body["query"] = map[string]interface{}{
		"bool": map[string]interface{}{
			"must":   []interface{}{queryMap},
			"filter": []interface{}{rangeFilter},
		},
	}
}

func containsRangeFilter(filters []interface{}, field string) bool {
	normalizedField := normalizeFieldName(field)
	for _, rawFilter := range filters {
		filterMap, ok := rawFilter.(map[string]interface{})
		if !ok {
			continue
		}
		rangeRaw, ok := filterMap["range"].(map[string]interface{})
		if !ok {
			continue
		}
		for key := range rangeRaw {
			if normalizeFieldName(key) == normalizedField {
				return true
			}
		}
	}
	return false
}

func toInterfaceSlice(value interface{}) []interface{} {
	switch typed := value.(type) {
	case nil:
		return []interface{}{}
	case []interface{}:
		return append([]interface{}{}, typed...)
	default:
		return []interface{}{typed}
	}
}

func fixedInterval(step time.Duration) string {
	if step <= 0 {
		return "30s"
	}

	seconds := int64(step / time.Second)
	if seconds <= 0 {
		seconds = 1
	}

	switch {
	case seconds%3600 == 0:
		return fmt.Sprintf("%dh", seconds/3600)
	case seconds%60 == 0:
		return fmt.Sprintf("%dm", seconds/60)
	default:
		return fmt.Sprintf("%ds", seconds)
	}
}

func parseBucketTimestamp(bucket map[string]interface{}) (float64, bool) {
	if rawKeyAsString, ok := bucket["key_as_string"]; ok {
		trimmed := strings.TrimSpace(anyToString(rawKeyAsString))
		if trimmed != "" {
			if parsed, ok := parseTimeString(trimmed); ok {
				return float64(parsed.UnixNano()) / float64(time.Second), true
			}
		}
	}

	keyValue, hasKey := bucket["key"]
	if !hasKey {
		return 0, false
	}

	numeric, ok := anyToFloat64(keyValue)
	if !ok {
		return 0, false
	}

	seconds := normalizeEpochSeconds(numeric)
	if math.Abs(numeric) >= 1e11 || math.Abs(seconds) >= 1e8 {
		return seconds, true
	}

	return seconds, false
}

func metricLabelsWithName(labels map[string]string, name string) map[string]string {
	metric := cloneMetricLabels(labels)
	trimmedName := strings.TrimSpace(name)
	if trimmedName == "" {
		trimmedName = "value"
	}
	metric["__name__"] = trimmedName
	return metric
}

func cloneMetricLabels(labels map[string]string) map[string]string {
	cloned := make(map[string]string, len(labels)+1)
	for key, value := range labels {
		cloned[key] = value
	}
	return cloned
}

func buildMetricName(parts ...string) string {
	cleaned := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		cleaned = append(cleaned, strings.ReplaceAll(trimmed, " ", "_"))
	}

	if len(cleaned) == 0 {
		return "value"
	}

	return strings.Join(cleaned, ".")
}

func normalizeFieldName(field string) string {
	trimmed := strings.ToLower(strings.TrimSpace(field))
	if trimmed == "" {
		return ""
	}

	builder := strings.Builder{}
	builder.Grow(len(trimmed))
	for _, char := range trimmed {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') {
			builder.WriteRune(char)
		}
	}

	return builder.String()
}

func extractErrorMessage(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}

	decoded := map[string]interface{}{}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return ""
	}

	errorValue, ok := decoded["error"]
	if !ok {
		return ""
	}

	switch typed := errorValue.(type) {
	case string:
		return strings.TrimSpace(typed)
	case map[string]interface{}:
		if reason := strings.TrimSpace(anyToString(typed["reason"])); reason != "" {
			return reason
		}
		if rootCause, ok := typed["root_cause"].([]interface{}); ok && len(rootCause) > 0 {
			if firstCause, ok := rootCause[0].(map[string]interface{}); ok {
				if reason := strings.TrimSpace(anyToString(firstCause["reason"])); reason != "" {
					return reason
				}
			}
		}

		if payload, err := json.Marshal(typed); err == nil {
			return string(payload)
		}
	}

	return ""
}

func interpolateTemplate(query string, start, end time.Time, step time.Duration) string {
	if strings.TrimSpace(query) == "" {
		return query
	}

	stepSeconds := int64(step / time.Second)
	if stepSeconds <= 0 {
		stepSeconds = 1
	}

	replacer := strings.NewReplacer(
		"{start}", strconv.FormatInt(start.Unix(), 10),
		"{end}", strconv.FormatInt(end.Unix(), 10),
		"{step}", strconv.FormatInt(stepSeconds, 10),
		"{start_ms}", strconv.FormatInt(start.UnixMilli(), 10),
		"{end_ms}", strconv.FormatInt(end.UnixMilli(), 10),
		"{start_ns}", strconv.FormatInt(start.UnixNano(), 10),
		"{end_ns}", strconv.FormatInt(end.UnixNano(), 10),
		"{start_rfc3339}", start.UTC().Format(time.RFC3339Nano),
		"{end_rfc3339}", end.UTC().Format(time.RFC3339Nano),
	)

	return replacer.Replace(query)
}
