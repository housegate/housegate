package plugin

import (
	"context"
	"errors"
	"fmt"
	"sync"

	chgo "github.com/ClickHouse/ch-go/proto"
	"github.com/housegate/housegate/pkg/chproto"
)

// SnapshotQueryHostAdmission is returned only after the executing host's
// injected source has independently authenticated its endpoint and validated
// the catalog/pin/profile proof. It carries local callbacks, never a caller
// supplied host marker or endpoint selection.
type SnapshotQueryHostAdmission struct {
	Run             func(context.Context) error
	CancelClient    func()
	MaxControlBytes uint64
}

// SnapshotQueryHostSource splits the nonblocking local recognition step from
// potentially blocking authenticated admission. IsSnapshotQuery must be pure
// local classification; AdmitSnapshotQueryAtHost runs inside QueryOnlyPlan.Run
// after Relay has claimed the active generation and started control polling.
type SnapshotQueryHostSource interface {
	IsSnapshotQuery(*chproto.Query) (bool, error)
	AdmitSnapshotQueryAtHost(context.Context, *chproto.Query) (SnapshotQueryHostAdmission, error)
}

// SnapshotQueryHostPlugin is injection-only and intentionally has no runtime
// construction path. It is the sole package owner allowed to create a
// QueryOnlyPlan, after its source has been selected explicitly by the host.
type SnapshotQueryHostPlugin struct{ source SnapshotQueryHostSource }

func NewSnapshotQueryHostPlugin(source SnapshotQueryHostSource) (*SnapshotQueryHostPlugin, error) {
	if source == nil {
		return nil, errors.New("snapshot query host source is required")
	}
	return &SnapshotQueryHostPlugin{source: source}, nil
}

func (p *SnapshotQueryHostPlugin) OnQuery(_ context.Context, qctx *QueryContext) error {
	if p == nil || p.source == nil || qctx == nil || qctx.Query == nil {
		return nil
	}
	if qctx.AgentPrepare != nil || qctx.QueryOnly != nil || qctx.DeferredInsert != nil || qctx.SuppressUpstreamExecution || qctx.AbortWithSuccess {
		return errors.New("snapshot query host: conflicting query ownership plan")
	}
	recognized, err := p.source.IsSnapshotQuery(qctx.Query)
	if err != nil {
		return fmt.Errorf("snapshot query host classify: %w", err)
	}
	if !recognized {
		return nil
	}
	query := cloneHostQuery(qctx.Query)
	var cancelMu sync.Mutex
	var admissionCancel func()
	var admissionCancelOnce sync.Once
	cancelAdmission := func() {
		cancelMu.Lock()
		cancel := admissionCancel
		cancelMu.Unlock()
		if cancel != nil {
			admissionCancelOnce.Do(cancel)
		}
	}
	qctx.QueryOnly = newHostQueryOnlyPlan(func(ctx context.Context) error {
		admission, err := p.source.AdmitSnapshotQueryAtHost(ctx, query)
		if err != nil {
			return fmt.Errorf("snapshot query host admission: %w", err)
		}
		if admission.Run == nil || admission.CancelClient == nil || admission.MaxControlBytes == 0 {
			return errors.New("snapshot query host admission is incomplete")
		}
		cancelMu.Lock()
		admissionCancel = admission.CancelClient
		cancelMu.Unlock()
		if err := ctx.Err(); err != nil {
			cancelAdmission()
			return err
		}
		return admission.Run(ctx)
	}, func() {
		// A cancellation before admission returns is carried by ctx. Once it
		// returns, the independent admission supplies its durable cancellation.
		cancelAdmission()
	}, 1024)
	return nil
}

func cloneHostQuery(q *chproto.Query) *chproto.Query {
	cpy := *q
	cpy.Settings = append([]chproto.Setting(nil), q.Settings...)
	cpy.Parameters = append([]chgo.Parameter(nil), q.Parameters...)
	cpy.OldSettings = append([]chproto.OldSetting(nil), q.OldSettings...)
	return &cpy
}

func (*SnapshotQueryHostPlugin) RunOnRouted() bool    { return true }
func (*SnapshotQueryHostPlugin) RunOnPeerTrust() bool { return true }
func (*SnapshotQueryHostPlugin) RunOnForward() bool   { return true }

var _ QueryPlugin = (*SnapshotQueryHostPlugin)(nil)
var _ RouteAware = (*SnapshotQueryHostPlugin)(nil)
var _ PeerTrustAware = (*SnapshotQueryHostPlugin)(nil)
var _ ForwardAware = (*SnapshotQueryHostPlugin)(nil)
