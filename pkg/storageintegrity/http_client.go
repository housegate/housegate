package storageintegrity

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"housegate/housegate/pkg/replay"
)

type HTTPDAClient struct {
	baseURL string
	client  *http.Client
}

func NewHTTPDAClient(baseURL string) *HTTPDAClient {
	return &HTTPDAClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *HTTPDAClient) PutPayload(ctx context.Context, req PutPayloadRequest) (PayloadCommitment, error) {
	var out PayloadCommitment
	err := c.post(ctx, "/v1/da/payloads", struct {
		TableID     string `json:"table_id"`
		StatementID string `json:"statement_id"`
		Payload     string `json:"payload_base64"`
	}{
		TableID:     req.TableID,
		StatementID: req.StatementID,
		Payload:     base64.StdEncoding.EncodeToString(req.Payload),
	}, &out)
	return out, err
}

func (c *HTTPDAClient) GetPayload(ctx context.Context, ref string) ([]byte, error) {
	var out struct {
		Payload string `json:"payload_base64"`
	}
	err := c.post(ctx, "/v1/da/get-payload", struct {
		Ref string `json:"ref"`
	}{Ref: ref}, &out)
	if err != nil {
		return nil, err
	}
	payload, err := base64.StdEncoding.DecodeString(out.Payload)
	if err != nil {
		return nil, fmt.Errorf("decode DA payload: %w", err)
	}
	return payload, nil
}

func (c *HTTPDAClient) post(ctx context.Context, path string, in, out any) error {
	if c == nil || c.baseURL == "" {
		return fmt.Errorf("storage integrity DA endpoint is required")
	}
	return postJSON(ctx, c.client, c.baseURL+path, in, out)
}

type HTTPArbiterClient struct {
	baseURL        string
	primaryPrefix  string
	fallbackPrefix string
	client         *http.Client
	// workerID identifies this HouseGate instance's worker to the arbiter on
	// every claim, so the control plane can enforce per-worker quarantine.
	workerID string
}

// WithWorkerID sets the worker identity carried on claim requests and returns
// the client for chaining. Empty worker ids are treated as anonymous by the
// arbiter (never quarantined) for backward compatibility.
func (c *HTTPArbiterClient) WithWorkerID(workerID string) *HTTPArbiterClient {
	if c != nil {
		c.workerID = workerID
	}
	return c
}

// claimBody is the request body for every Claim* RPC: it carries the worker id
// so the arbiter can refuse tasks to quarantined workers.
func (c *HTTPArbiterClient) claimBody() claimRequest {
	return claimRequest{WorkerID: c.workerID}
}

type claimRequest struct {
	WorkerID string `json:"worker_id,omitempty"`
}

// HTTPSequencerClient is the legacy control-plane client name kept for
// compatibility with older configs/tests that still say "sequencer".
type HTTPSequencerClient = HTTPArbiterClient

func NewHTTPArbiterClient(baseURL string) *HTTPArbiterClient {
	return &HTTPArbiterClient{
		baseURL:        strings.TrimRight(baseURL, "/"),
		primaryPrefix:  "/v1/arbiter",
		fallbackPrefix: "/v1/sequencer",
		client:         &http.Client{Timeout: 10 * time.Second},
	}
}

func NewHTTPSequencerClient(baseURL string) *HTTPSequencerClient {
	return &HTTPSequencerClient{
		baseURL:       strings.TrimRight(baseURL, "/"),
		primaryPrefix: "/v1/sequencer",
		client:        &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *HTTPArbiterClient) SubmitInsert(ctx context.Context, rec InsertRecord) error {
	var ack Ack
	if c == nil || c.baseURL == "" {
		return fmt.Errorf("storage integrity arbiter endpoint is required")
	}
	if err := c.post(ctx, "/inserts", rec, &ack); err != nil {
		return err
	}
	if !ack.OK {
		return fmt.Errorf("arbiter insert was not acknowledged")
	}
	return nil
}

func (c *HTTPArbiterClient) SubmitMutation(ctx context.Context, rec MutationRecord) error {
	var ack Ack
	if c == nil || c.baseURL == "" {
		return fmt.Errorf("storage integrity arbiter endpoint is required")
	}
	if err := c.post(ctx, "/mutations", rec, &ack); err != nil {
		return err
	}
	if !ack.OK {
		return fmt.Errorf("arbiter mutation was not acknowledged")
	}
	return nil
}

func (c *HTTPArbiterClient) GetActiveUnsafeBuffer(ctx context.Context, req ActiveUnsafeBufferRequest) (UnsafeBufferInfo, error) {
	var out struct {
		OK     bool             `json:"ok"`
		Buffer UnsafeBufferInfo `json:"buffer,omitempty"`
	}
	if err := c.post(ctx, "/unsafe-buffers/active", req, &out); err != nil {
		return UnsafeBufferInfo{}, err
	}
	if !out.OK {
		return UnsafeBufferInfo{}, fmt.Errorf("arbiter active unsafe buffer was not acknowledged")
	}
	return out.Buffer, nil
}

func (c *HTTPArbiterClient) CheckUnsafeBufferEpoch(ctx context.Context, req UnsafeBufferEpochCheckRequest) (UnsafeBufferEpochDecision, error) {
	var out struct {
		OK       bool                      `json:"ok"`
		Decision UnsafeBufferEpochDecision `json:"decision,omitempty"`
	}
	if err := c.post(ctx, "/unsafe-buffers/check-epoch", req, &out); err != nil {
		return UnsafeBufferEpochDecision{}, err
	}
	if !out.OK {
		return UnsafeBufferEpochDecision{}, fmt.Errorf("arbiter unsafe buffer epoch check was not acknowledged")
	}
	return out.Decision, nil
}

func (c *HTTPArbiterClient) ClaimReplayJob(ctx context.Context) (replay.ReplayJob, bool, error) {
	var out struct {
		OK  bool             `json:"ok"`
		Job replay.ReplayJob `json:"job,omitempty"`
	}
	if err := c.post(ctx, "/replay-jobs/claim", c.claimBody(), &out); err != nil {
		return replay.ReplayJob{}, false, err
	}
	return out.Job, out.OK, nil
}

func (c *HTTPArbiterClient) SubmitReplayAttestation(ctx context.Context, att replay.ReplayAttestation) error {
	return c.postAck(ctx, "/replay-attestations", att, "arbiter replay attestation was not acknowledged")
}

func (c *HTTPArbiterClient) ClaimByteSideScan(ctx context.Context) (ByteSideScanTask, bool, error) {
	var out struct {
		OK   bool             `json:"ok"`
		Task ByteSideScanTask `json:"task,omitempty"`
	}
	if err := c.post(ctx, "/byte-side-scans/claim", c.claimBody(), &out); err != nil {
		return ByteSideScanTask{}, false, err
	}
	return out.Task, out.OK, nil
}

func (c *HTTPArbiterClient) SubmitByteSideScan(ctx context.Context, result ByteSideScanResult) error {
	return c.postAck(ctx, "/byte-side-scans", result, "arbiter byte-side scan was not acknowledged")
}

func (c *HTTPArbiterClient) ClaimPromotion(ctx context.Context) (PromotionTask, bool, error) {
	var out struct {
		OK   bool          `json:"ok"`
		Task PromotionTask `json:"task,omitempty"`
	}
	if err := c.post(ctx, "/promotions/claim", c.claimBody(), &out); err != nil {
		return PromotionTask{}, false, err
	}
	return out.Task, out.OK, nil
}

func (c *HTTPArbiterClient) SubmitPromotionResult(ctx context.Context, result PromotionResult) (PromotionReceipt, error) {
	var receipt PromotionReceipt
	if err := c.post(ctx, "/promotions", result, &receipt); err != nil {
		return PromotionReceipt{}, err
	}
	if !receipt.OK {
		return PromotionReceipt{}, fmt.Errorf("arbiter promotion result was not acknowledged")
	}
	if receipt.Manifest.SnapshotID != "" {
		if err := receipt.Manifest.Validate(); err != nil {
			return PromotionReceipt{}, fmt.Errorf("validate published safe snapshot manifest: %w", err)
		}
	}
	return receipt, nil
}

func (c *HTTPArbiterClient) GetSafeWatermark(ctx context.Context) (SafeWatermark, error) {
	var out struct {
		OK        bool          `json:"ok"`
		Watermark SafeWatermark `json:"watermark,omitempty"`
	}
	if err := c.post(ctx, "/safe-watermark", struct{}{}, &out); err != nil {
		return SafeWatermark{}, err
	}
	if !out.OK {
		return SafeWatermark{}, fmt.Errorf("arbiter safe watermark was not acknowledged")
	}
	return out.Watermark, nil
}

func (c *HTTPArbiterClient) GetSafeSnapshot(ctx context.Context, snapshotID string) (replay.SafeSnapshotManifest, bool, error) {
	var out struct {
		OK       bool                        `json:"ok"`
		Manifest replay.SafeSnapshotManifest `json:"manifest,omitempty"`
	}
	err := c.post(ctx, "/safe-snapshots/get", struct {
		SnapshotID string `json:"snapshot_id"`
	}{SnapshotID: snapshotID}, &out)
	if err != nil {
		return replay.SafeSnapshotManifest{}, false, err
	}
	if out.OK {
		if err := out.Manifest.Validate(); err != nil {
			return replay.SafeSnapshotManifest{}, false, fmt.Errorf("validate safe snapshot manifest: %w", err)
		}
	}
	return out.Manifest, out.OK, nil
}

func (c *HTTPArbiterClient) CheckSafeRead(ctx context.Context, req SafeReadRequest) (SafeReadDecision, error) {
	var out struct {
		OK       bool             `json:"ok"`
		Decision SafeReadDecision `json:"decision,omitempty"`
	}
	if err := c.post(ctx, "/read-set/check", req, &out); err != nil {
		return SafeReadDecision{}, err
	}
	if !out.OK {
		return SafeReadDecision{}, fmt.Errorf("arbiter read-set check was not acknowledged")
	}
	return out.Decision, nil
}

func (c *HTTPArbiterClient) ClaimMutationTask(ctx context.Context) (MutationTask, bool, error) {
	var out struct {
		OK   bool         `json:"ok"`
		Task MutationTask `json:"task,omitempty"`
	}
	if err := c.post(ctx, "/mutation-tasks/claim", c.claimBody(), &out); err != nil {
		return MutationTask{}, false, err
	}
	return out.Task, out.OK, nil
}

func (c *HTTPArbiterClient) SubmitMutationClaim(ctx context.Context, claim MutationClaim) error {
	return c.postAck(ctx, "/mutation-claims", claim, "arbiter mutation claim was not acknowledged")
}

func (c *HTTPArbiterClient) ClaimMutationReplayTask(ctx context.Context) (MutationTask, bool, error) {
	var out struct {
		OK   bool         `json:"ok"`
		Task MutationTask `json:"task,omitempty"`
	}
	if err := c.post(ctx, "/mutation-replays/claim", c.claimBody(), &out); err != nil {
		return MutationTask{}, false, err
	}
	return out.Task, out.OK, nil
}

func (c *HTTPArbiterClient) SubmitMutationReplay(ctx context.Context, result MutationReplayResult) error {
	return c.postAck(ctx, "/mutation-replays", result, "arbiter mutation replay was not acknowledged")
}

func (c *HTTPArbiterClient) ClaimSafeAudit(ctx context.Context) (SafeAuditTask, bool, error) {
	var out struct {
		OK   bool          `json:"ok"`
		Task SafeAuditTask `json:"task,omitempty"`
	}
	if err := c.post(ctx, "/safe-audits/claim", c.claimBody(), &out); err != nil {
		return SafeAuditTask{}, false, err
	}
	return out.Task, out.OK, nil
}

func (c *HTTPArbiterClient) SubmitSafeAudit(ctx context.Context, vote SafeAuditVote) error {
	return c.postAck(ctx, "/safe-audits", vote, "arbiter safe audit was not acknowledged")
}

func (c *HTTPArbiterClient) ClaimRollback(ctx context.Context) (RollbackTask, bool, error) {
	var out struct {
		OK   bool         `json:"ok"`
		Task RollbackTask `json:"task,omitempty"`
	}
	if err := c.post(ctx, "/rollbacks/claim", c.claimBody(), &out); err != nil {
		return RollbackTask{}, false, err
	}
	return out.Task, out.OK, nil
}

func (c *HTTPArbiterClient) SubmitRollback(ctx context.Context, result RollbackResult) error {
	return c.postAck(ctx, "/rollbacks", result, "arbiter rollback result was not acknowledged")
}

func (c *HTTPArbiterClient) ClaimRepairSync(ctx context.Context) (RepairSyncTask, bool, error) {
	var out struct {
		OK   bool           `json:"ok"`
		Task RepairSyncTask `json:"task,omitempty"`
	}
	if err := c.post(ctx, "/repair-sync/claim", c.claimBody(), &out); err != nil {
		return RepairSyncTask{}, false, err
	}
	return out.Task, out.OK, nil
}

func (c *HTTPArbiterClient) SubmitRepairSync(ctx context.Context, result RepairSyncResult) error {
	return c.postAck(ctx, "/repair-sync", result, "arbiter repair/sync result was not acknowledged")
}

func (c *HTTPArbiterClient) ClaimCompaction(ctx context.Context) (CompactionTask, bool, error) {
	var out struct {
		OK   bool           `json:"ok"`
		Task CompactionTask `json:"task,omitempty"`
	}
	if err := c.post(ctx, "/compactions/claim", c.claimBody(), &out); err != nil {
		return CompactionTask{}, false, err
	}
	return out.Task, out.OK, nil
}

func (c *HTTPArbiterClient) SubmitCompaction(ctx context.Context, result CompactionResult) error {
	return c.postAck(ctx, "/compactions", result, "arbiter compaction result was not acknowledged")
}

func (c *HTTPArbiterClient) post(ctx context.Context, path string, in, out any) error {
	if c == nil || c.baseURL == "" {
		return fmt.Errorf("storage integrity arbiter endpoint is required")
	}
	primaryErr := postJSON(ctx, c.client, c.baseURL+c.primaryPrefix+path, in, out)
	if primaryErr == nil || c.fallbackPrefix == "" || !isLegacyControlPlaneFallback(primaryErr) {
		return primaryErr
	}
	return postJSON(ctx, c.client, c.baseURL+c.fallbackPrefix+path, in, out)
}

func (c *HTTPArbiterClient) postAck(ctx context.Context, path string, in any, notOK string) error {
	var ack Ack
	if err := c.post(ctx, path, in, &ack); err != nil {
		return err
	}
	if !ack.OK {
		return fmt.Errorf("%s", notOK)
	}
	return nil
}

func isLegacyControlPlaneFallback(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "status 404") || strings.Contains(msg, "status 405")
}

func postJSON(ctx context.Context, client *http.Client, url string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("post %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var payload map[string]string
		_ = json.NewDecoder(resp.Body).Decode(&payload)
		if msg := payload["error"]; msg != "" {
			return fmt.Errorf("post %s: status %s: %s", url, resp.Status, msg)
		}
		return fmt.Errorf("post %s: status %s", url, resp.Status)
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}
