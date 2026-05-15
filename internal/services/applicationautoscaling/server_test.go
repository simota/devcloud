package applicationautoscaling

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	s := NewServer(Config{Region: "us-east-1", AccountID: "000000000000", StoragePath: t.TempDir()})
	return httptest.NewServer(s)
}

func doRequest(t *testing.T, ts *httptest.Server, action string, body string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AnyScaleFrontendService."+action)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp, string(data)
}

func TestApplicationAutoScalingHappyPath(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	// RegisterScalableTarget
	resp, body := doRequest(t, ts, "RegisterScalableTarget", `{
		"ServiceNamespace":"dynamodb",
		"ResourceId":"table/Orders",
		"ScalableDimension":"dynamodb:table:WriteCapacityUnits",
		"MinCapacity":1,
		"MaxCapacity":10,
		"RoleARN":"arn:aws:iam::000000000000:role/dev"
	}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("RegisterScalableTarget status=%d body=%s", resp.StatusCode, body)
	}
	if strings.TrimSpace(body) != "{}" {
		t.Fatalf("RegisterScalableTarget body=%s", body)
	}

	// DescribeScalableTargets - 1 result
	resp, body = doRequest(t, ts, "DescribeScalableTargets", `{"ServiceNamespace":"dynamodb"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeScalableTargets status=%d body=%s", resp.StatusCode, body)
	}
	var describeResp describeScalableTargetsResponse
	if err := json.Unmarshal([]byte(body), &describeResp); err != nil {
		t.Fatalf("unmarshal describe response: %v body=%s", err, body)
	}
	if len(describeResp.ScalableTargets) != 1 {
		t.Fatalf("expected 1 target, got %d", len(describeResp.ScalableTargets))
	}
	if describeResp.ScalableTargets[0].MinCapacity != 1 || describeResp.ScalableTargets[0].MaxCapacity != 10 {
		t.Fatalf("unexpected capacity: %+v", describeResp.ScalableTargets[0])
	}

	// PutScalingPolicy (TargetTrackingScaling)
	resp, body = doRequest(t, ts, "PutScalingPolicy", `{
		"PolicyName":"WriteScaling",
		"ServiceNamespace":"dynamodb",
		"ResourceId":"table/Orders",
		"ScalableDimension":"dynamodb:table:WriteCapacityUnits",
		"PolicyType":"TargetTrackingScaling",
		"TargetTrackingScalingPolicyConfiguration":{"TargetValue":70.0}
	}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PutScalingPolicy status=%d body=%s", resp.StatusCode, body)
	}
	var putResp putScalingPolicyResponse
	if err := json.Unmarshal([]byte(body), &putResp); err != nil {
		t.Fatalf("unmarshal put policy: %v body=%s", err, body)
	}
	if !strings.HasPrefix(putResp.PolicyARN, "arn:aws:autoscaling:us-east-1:000000000000:scalingPolicy:") {
		t.Fatalf("unexpected PolicyARN: %s", putResp.PolicyARN)
	}
	if !strings.Contains(putResp.PolicyARN, ":resource/dynamodb/table/Orders:policyName/WriteScaling") {
		t.Fatalf("PolicyARN missing resource segment: %s", putResp.PolicyARN)
	}
	if putResp.Alarms == nil {
		t.Fatalf("Alarms must be non-nil array")
	}

	// DescribeScalingPolicies - 1 result
	resp, body = doRequest(t, ts, "DescribeScalingPolicies", `{"ServiceNamespace":"dynamodb"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeScalingPolicies status=%d body=%s", resp.StatusCode, body)
	}
	var listResp describeScalingPoliciesResponse
	if err := json.Unmarshal([]byte(body), &listResp); err != nil {
		t.Fatalf("unmarshal policies: %v body=%s", err, body)
	}
	if len(listResp.ScalingPolicies) != 1 || listResp.ScalingPolicies[0].PolicyName != "WriteScaling" {
		t.Fatalf("unexpected policies: %+v", listResp.ScalingPolicies)
	}

	// DescribeScalingActivities - always empty
	resp, body = doRequest(t, ts, "DescribeScalingActivities", `{"ServiceNamespace":"dynamodb","ResourceId":"table/Orders","ScalableDimension":"dynamodb:table:WriteCapacityUnits"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeScalingActivities status=%d body=%s", resp.StatusCode, body)
	}
	if !strings.Contains(body, `"ScalingActivities":[]`) {
		t.Fatalf("expected empty activities, got %s", body)
	}

	// DeregisterScalableTarget
	resp, body = doRequest(t, ts, "DeregisterScalableTarget", `{
		"ServiceNamespace":"dynamodb",
		"ResourceId":"table/Orders",
		"ScalableDimension":"dynamodb:table:WriteCapacityUnits"
	}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DeregisterScalableTarget status=%d body=%s", resp.StatusCode, body)
	}

	// DescribeScalableTargets - now empty
	resp, body = doRequest(t, ts, "DescribeScalableTargets", `{"ServiceNamespace":"dynamodb"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeScalableTargets (after deregister) status=%d body=%s", resp.StatusCode, body)
	}
	if !strings.Contains(body, `"ScalableTargets":[]`) {
		t.Fatalf("expected no targets, got %s", body)
	}

	// Cascading delete: policies also gone
	resp, body = doRequest(t, ts, "DescribeScalingPolicies", `{"ServiceNamespace":"dynamodb"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeScalingPolicies (after deregister) status=%d body=%s", resp.StatusCode, body)
	}
	if !strings.Contains(body, `"ScalingPolicies":[]`) {
		t.Fatalf("expected no policies after deregister, got %s", body)
	}
}

func TestApplicationAutoScalingRejectsNonDynamoDBNamespace(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	resp, body := doRequest(t, ts, "RegisterScalableTarget", `{
		"ServiceNamespace":"ec2",
		"ResourceId":"asg/foo",
		"ScalableDimension":"ec2:asg:DesiredCapacity",
		"MinCapacity":1,
		"MaxCapacity":5
	}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "ValidationException") {
		t.Fatalf("missing ValidationException: %s", body)
	}
	if !strings.Contains(body, "only dynamodb namespace is supported") {
		t.Fatalf("missing namespace message: %s", body)
	}
	if got := resp.Header.Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("X-Amzn-Errortype=%q", got)
	}
}

func TestApplicationAutoScalingRejectsUnknownTarget(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	resp, body := doRequest(t, ts, "WhoKnows", `{}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "UnknownOperationException") {
		t.Fatalf("missing UnknownOperationException: %s", body)
	}
}

func TestApplicationAutoScalingRejectsBadContentType(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Amz-Target", "AnyScaleFrontendService.DescribeScalableTargets")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", resp.StatusCode, string(body))
	}
	if !strings.Contains(string(body), "ValidationException") {
		t.Fatalf("missing ValidationException: %s", string(body))
	}
}

func TestApplicationAutoScalingDeregisterMissingReturnsObjectNotFound(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	resp, body := doRequest(t, ts, "DeregisterScalableTarget", `{
		"ServiceNamespace":"dynamodb",
		"ResourceId":"table/Missing",
		"ScalableDimension":"dynamodb:table:WriteCapacityUnits"
	}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "ObjectNotFoundException") {
		t.Fatalf("missing ObjectNotFoundException: %s", body)
	}
}

func TestApplicationAutoScalingMethodNotAllowed(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if got := resp.Header.Get("Allow"); got != "POST" {
		t.Fatalf("Allow=%q", got)
	}
}
