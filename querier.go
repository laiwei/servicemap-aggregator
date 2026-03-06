package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// PrometheusQuerier 从 Prometheus HTTP API 查询指标
type PrometheusQuerier struct {
	baseURL string
	timeout time.Duration
	client  *http.Client
}

func NewPrometheusQuerier(baseURL string, timeout time.Duration) *PrometheusQuerier {
	return &PrometheusQuerier{
		baseURL: baseURL,
		timeout: timeout,
		client:  &http.Client{Timeout: timeout},
	}
}

// promResult Prometheus 即时查询响应
type promResult struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string       `json:"resultType"`
		Result     []promSample `json:"result"`
	} `json:"data"`
	Error string `json:"error"`
}

type promSample struct {
	Metric map[string]string `json:"metric"`
	Value  [2]interface{}    `json:"value"` // [timestamp, value_string]
}

// QueryInstant 执行即时查询，返回所有样本
func (q *PrometheusQuerier) QueryInstant(ctx context.Context, expr string) ([]promSample, error) {
	params := url.Values{}
	params.Set("query", expr)
	params.Set("time", strconv.FormatInt(time.Now().Unix(), 10))

	reqURL := fmt.Sprintf("%s/api/v1/query?%s", q.baseURL, params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	resp, err := q.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query prometheus: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("prometheus returned %d: %s", resp.StatusCode, body)
	}

	var result promResult
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	if result.Status != "success" {
		return nil, fmt.Errorf("prometheus error: %s", result.Error)
	}

	return result.Data.Result, nil
}

// sampleValue 从 promSample 中提取浮点值
func sampleValue(s promSample) float64 {
	if len(s.Value) < 2 {
		return 0
	}
	str, ok := s.Value[1].(string)
	if !ok {
		return 0
	}
	v, _ := strconv.ParseFloat(str, 64)
	return v
}
