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

type HTTPSequencerClient struct {
	baseURL string
	client  *http.Client
}

func NewHTTPSequencerClient(baseURL string) *HTTPSequencerClient {
	return &HTTPSequencerClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *HTTPSequencerClient) SubmitInsert(ctx context.Context, rec InsertRecord) error {
	var ack Ack
	if c == nil || c.baseURL == "" {
		return fmt.Errorf("storage integrity sequencer endpoint is required")
	}
	if err := postJSON(ctx, c.client, c.baseURL+"/v1/sequencer/inserts", rec, &ack); err != nil {
		return err
	}
	if !ack.OK {
		return fmt.Errorf("sequencer insert was not acknowledged")
	}
	return nil
}

func (c *HTTPSequencerClient) SubmitMutation(ctx context.Context, rec MutationRecord) error {
	var ack Ack
	if c == nil || c.baseURL == "" {
		return fmt.Errorf("storage integrity sequencer endpoint is required")
	}
	if err := postJSON(ctx, c.client, c.baseURL+"/v1/sequencer/mutations", rec, &ack); err != nil {
		return err
	}
	if !ack.OK {
		return fmt.Errorf("sequencer mutation was not acknowledged")
	}
	return nil
}

func (c *HTTPSequencerClient) ClaimReplayJob(ctx context.Context) (replay.ReplayJob, bool, error) {
	var out struct {
		OK  bool             `json:"ok"`
		Job replay.ReplayJob `json:"job,omitempty"`
	}
	if err := c.post(ctx, "/v1/sequencer/replay-jobs/claim", struct{}{}, &out); err != nil {
		return replay.ReplayJob{}, false, err
	}
	return out.Job, out.OK, nil
}

func (c *HTTPSequencerClient) SubmitReplayAttestation(ctx context.Context, att replay.ReplayAttestation) error {
	return c.postAck(ctx, "/v1/sequencer/replay-attestations", att, "sequencer replay attestation was not acknowledged")
}

func (c *HTTPSequencerClient) ClaimByteSideScan(ctx context.Context) (ByteSideScanTask, bool, error) {
	var out struct {
		OK   bool             `json:"ok"`
		Task ByteSideScanTask `json:"task,omitempty"`
	}
	if err := c.post(ctx, "/v1/sequencer/byte-side-scans/claim", struct{}{}, &out); err != nil {
		return ByteSideScanTask{}, false, err
	}
	return out.Task, out.OK, nil
}

func (c *HTTPSequencerClient) SubmitByteSideScan(ctx context.Context, result ByteSideScanResult) error {
	return c.postAck(ctx, "/v1/sequencer/byte-side-scans", result, "sequencer byte-side scan was not acknowledged")
}

func (c *HTTPSequencerClient) ClaimPromotion(ctx context.Context) (PromotionTask, bool, error) {
	var out struct {
		OK   bool          `json:"ok"`
		Task PromotionTask `json:"task,omitempty"`
	}
	if err := c.post(ctx, "/v1/sequencer/promotions/claim", struct{}{}, &out); err != nil {
		return PromotionTask{}, false, err
	}
	return out.Task, out.OK, nil
}

func (c *HTTPSequencerClient) SubmitPromotionResult(ctx context.Context, result PromotionResult) (PromotionReceipt, error) {
	var receipt PromotionReceipt
	if err := c.post(ctx, "/v1/sequencer/promotions", result, &receipt); err != nil {
		return PromotionReceipt{}, err
	}
	if !receipt.OK {
		return PromotionReceipt{}, fmt.Errorf("sequencer promotion result was not acknowledged")
	}
	if receipt.Manifest.SnapshotID != "" {
		if err := receipt.Manifest.Validate(); err != nil {
			return PromotionReceipt{}, fmt.Errorf("validate published safe snapshot manifest: %w", err)
		}
	}
	return receipt, nil
}

func (c *HTTPSequencerClient) GetSafeWatermark(ctx context.Context) (SafeWatermark, error) {
	var out struct {
		OK        bool          `json:"ok"`
		Watermark SafeWatermark `json:"watermark,omitempty"`
	}
	if err := c.post(ctx, "/v1/sequencer/safe-watermark", struct{}{}, &out); err != nil {
		return SafeWatermark{}, err
	}
	if !out.OK {
		return SafeWatermark{}, fmt.Errorf("sequencer safe watermark was not acknowledged")
	}
	return out.Watermark, nil
}

func (c *HTTPSequencerClient) GetSafeSnapshot(ctx context.Context, snapshotID string) (replay.SafeSnapshotManifest, bool, error) {
	var out struct {
		OK       bool                        `json:"ok"`
		Manifest replay.SafeSnapshotManifest `json:"manifest,omitempty"`
	}
	err := c.post(ctx, "/v1/sequencer/safe-snapshots/get", struct {
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

func (c *HTTPSequencerClient) CheckSafeRead(ctx context.Context, req SafeReadRequest) (SafeReadDecision, error) {
	var out struct {
		OK       bool             `json:"ok"`
		Decision SafeReadDecision `json:"decision,omitempty"`
	}
	if err := c.post(ctx, "/v1/sequencer/read-set/check", req, &out); err != nil {
		return SafeReadDecision{}, err
	}
	if !out.OK {
		return SafeReadDecision{}, fmt.Errorf("sequencer read-set check was not acknowledged")
	}
	return out.Decision, nil
}

func (c *HTTPSequencerClient) ClaimMutationTask(ctx context.Context) (MutationTask, bool, error) {
	var out struct {
		OK   bool         `json:"ok"`
		Task MutationTask `json:"task,omitempty"`
	}
	if err := c.post(ctx, "/v1/sequencer/mutation-tasks/claim", struct{}{}, &out); err != nil {
		return MutationTask{}, false, err
	}
	return out.Task, out.OK, nil
}

func (c *HTTPSequencerClient) SubmitMutationClaim(ctx context.Context, claim MutationClaim) error {
	return c.postAck(ctx, "/v1/sequencer/mutation-claims", claim, "sequencer mutation claim was not acknowledged")
}

func (c *HTTPSequencerClient) ClaimMutationReplayTask(ctx context.Context) (MutationTask, bool, error) {
	var out struct {
		OK   bool         `json:"ok"`
		Task MutationTask `json:"task,omitempty"`
	}
	if err := c.post(ctx, "/v1/sequencer/mutation-replays/claim", struct{}{}, &out); err != nil {
		return MutationTask{}, false, err
	}
	return out.Task, out.OK, nil
}

func (c *HTTPSequencerClient) SubmitMutationReplay(ctx context.Context, result MutationReplayResult) error {
	return c.postAck(ctx, "/v1/sequencer/mutation-replays", result, "sequencer mutation replay was not acknowledged")
}

func (c *HTTPSequencerClient) ClaimSafeAudit(ctx context.Context) (SafeAuditTask, bool, error) {
	var out struct {
		OK   bool          `json:"ok"`
		Task SafeAuditTask `json:"task,omitempty"`
	}
	if err := c.post(ctx, "/v1/sequencer/safe-audits/claim", struct{}{}, &out); err != nil {
		return SafeAuditTask{}, false, err
	}
	return out.Task, out.OK, nil
}

func (c *HTTPSequencerClient) SubmitSafeAudit(ctx context.Context, vote SafeAuditVote) error {
	return c.postAck(ctx, "/v1/sequencer/safe-audits", vote, "sequencer safe audit was not acknowledged")
}

func (c *HTTPSequencerClient) ClaimRollback(ctx context.Context) (RollbackTask, bool, error) {
	var out struct {
		OK   bool         `json:"ok"`
		Task RollbackTask `json:"task,omitempty"`
	}
	if err := c.post(ctx, "/v1/sequencer/rollbacks/claim", struct{}{}, &out); err != nil {
		return RollbackTask{}, false, err
	}
	return out.Task, out.OK, nil
}

func (c *HTTPSequencerClient) SubmitRollback(ctx context.Context, result RollbackResult) error {
	return c.postAck(ctx, "/v1/sequencer/rollbacks", result, "sequencer rollback result was not acknowledged")
}

func (c *HTTPSequencerClient) ClaimRepairSync(ctx context.Context) (RepairSyncTask, bool, error) {
	var out struct {
		OK   bool           `json:"ok"`
		Task RepairSyncTask `json:"task,omitempty"`
	}
	if err := c.post(ctx, "/v1/sequencer/repair-sync/claim", struct{}{}, &out); err != nil {
		return RepairSyncTask{}, false, err
	}
	return out.Task, out.OK, nil
}

func (c *HTTPSequencerClient) SubmitRepairSync(ctx context.Context, result RepairSyncResult) error {
	return c.postAck(ctx, "/v1/sequencer/repair-sync", result, "sequencer repair/sync result was not acknowledged")
}

func (c *HTTPSequencerClient) ClaimCompaction(ctx context.Context) (CompactionTask, bool, error) {
	var out struct {
		OK   bool           `json:"ok"`
		Task CompactionTask `json:"task,omitempty"`
	}
	if err := c.post(ctx, "/v1/sequencer/compactions/claim", struct{}{}, &out); err != nil {
		return CompactionTask{}, false, err
	}
	return out.Task, out.OK, nil
}

func (c *HTTPSequencerClient) SubmitCompaction(ctx context.Context, result CompactionResult) error {
	return c.postAck(ctx, "/v1/sequencer/compactions", result, "sequencer compaction result was not acknowledged")
}

func (c *HTTPSequencerClient) post(ctx context.Context, path string, in, out any) error {
	if c == nil || c.baseURL == "" {
		return fmt.Errorf("storage integrity sequencer endpoint is required")
	}
	return postJSON(ctx, c.client, c.baseURL+path, in, out)
}

func (c *HTTPSequencerClient) postAck(ctx context.Context, path string, in any, notOK string) error {
	var ack Ack
	if err := c.post(ctx, path, in, &ack); err != nil {
		return err
	}
	if !ack.OK {
		return fmt.Errorf("%s", notOK)
	}
	return nil
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
