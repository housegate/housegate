package storageintegrity

import "fmt"

// SafeCutView is the read-only HouseGate projection of the ONE atomic safe cut
// the Arbiter commits in PublishMutationSafeCut (design section 4.8: manifest,
// global safe watermark, per-worker logical watermark, read-set membership, and
// route-cache epoch by a single atomic transition). HouseGate consumes it; it
// never commits the cut.
type SafeCutView struct {
	ManifestID         string
	GlobalWatermark    uint64
	PerWorkerWatermark map[string]uint64
	ReadSet            map[string]bool
	RouteCacheEpoch    uint64
	QuarantinedWorkers map[string]bool
}

// NewSafeCutView constructs a cut view, defensively copying every map so a caller
// mutating its inputs cannot change the committed view.
func NewSafeCutView(manifestID string, globalWatermark uint64, perWorkerWatermark map[string]uint64, readSet map[string]bool, routeCacheEpoch uint64, quarantined map[string]bool) SafeCutView {
	return SafeCutView{
		ManifestID:         manifestID,
		GlobalWatermark:    globalWatermark,
		PerWorkerWatermark: copyUint64Map(perWorkerWatermark),
		ReadSet:            copyBoolMap(readSet),
		RouteCacheEpoch:    routeCacheEpoch,
		QuarantinedWorkers: copyBoolMap(quarantined),
	}
}

// Valid fails closed on an incomplete cut: a blank manifest id, a zero global
// watermark or route-cache epoch, a nil read-set, or a read-set worker without a
// per-worker watermark (which could never be safely served).
func (v SafeCutView) Valid() error {
	if v.ManifestID == "" {
		return fmt.Errorf("safe cut: missing manifest id")
	}
	if v.GlobalWatermark == 0 {
		return fmt.Errorf("safe cut %s: zero global watermark", v.ManifestID)
	}
	if v.RouteCacheEpoch == 0 {
		return fmt.Errorf("safe cut %s: zero route-cache epoch", v.ManifestID)
	}
	if v.ReadSet == nil {
		return fmt.Errorf("safe cut %s: nil read-set", v.ManifestID)
	}
	for w := range v.ReadSet {
		if _, ok := v.PerWorkerWatermark[w]; !ok {
			return fmt.Errorf("safe cut %s: read-set worker %s has no per-worker watermark", v.ManifestID, w)
		}
	}
	return nil
}

// Clone deep-copies the cut view's maps.
func (v SafeCutView) Clone() SafeCutView {
	return SafeCutView{
		ManifestID:         v.ManifestID,
		GlobalWatermark:    v.GlobalWatermark,
		PerWorkerWatermark: copyUint64Map(v.PerWorkerWatermark),
		ReadSet:            copyBoolMap(v.ReadSet),
		RouteCacheEpoch:    v.RouteCacheEpoch,
		QuarantinedWorkers: copyBoolMap(v.QuarantinedWorkers),
	}
}

// GateDenyReason is the typed reason SafeReadGate refuses to serve.
type GateDenyReason int

const (
	GateAllowed GateDenyReason = iota
	GateDenyNotInReadSet
	GateDenyQuarantined
	GateDenyWorkerWatermarkBehind
	GateDenyGlobalWatermarkBehind
	GateDenyUnknownWorker
)

func (r GateDenyReason) String() string {
	switch r {
	case GateAllowed:
		return "Allowed"
	case GateDenyNotInReadSet:
		return "NotInReadSet"
	case GateDenyQuarantined:
		return "Quarantined"
	case GateDenyWorkerWatermarkBehind:
		return "WorkerWatermarkBehind"
	case GateDenyGlobalWatermarkBehind:
		return "GlobalWatermarkBehind"
	case GateDenyUnknownWorker:
		return "UnknownWorker"
	default:
		return "Unknown"
	}
}

// GateDecision is the answer to a MayServe query.
type GateDecision struct {
	Allowed bool
	Reason  GateDenyReason
	Detail  string
}

// SafeReadGate answers "may worker W serve at requested snapshot S?" against ONE
// committed safe cut. It consults only the committed read-set and watermarks —
// never a worker's local Applied=true state — so it forbids serving from a local
// apply or a single-worker ack before the cut committed (design section 5.2).
type SafeReadGate struct {
	cut SafeCutView
}

// NewSafeReadGate validates the cut and wraps it. Fail-closed on an invalid cut.
func NewSafeReadGate(cut SafeCutView) (SafeReadGate, error) {
	if err := cut.Valid(); err != nil {
		return SafeReadGate{}, err
	}
	return SafeReadGate{cut: cut.Clone()}, nil
}

// MayServe returns YES only when the worker is a member of the committed
// read-set, is not quarantined, and both its per-worker watermark and the global
// watermark cover the requested snapshot. Every NO carries a typed reason.
func (g SafeReadGate) MayServe(workerID string, requestedSnapshot uint64) GateDecision {
	if !g.cut.ReadSet[workerID] {
		return GateDecision{Reason: GateDenyNotInReadSet, Detail: fmt.Sprintf("worker %s is not in the committed read-set", workerID)}
	}
	if g.cut.QuarantinedWorkers[workerID] {
		return GateDecision{Reason: GateDenyQuarantined, Detail: fmt.Sprintf("worker %s is quarantined", workerID)}
	}
	wm, ok := g.cut.PerWorkerWatermark[workerID]
	if !ok {
		return GateDecision{Reason: GateDenyUnknownWorker, Detail: fmt.Sprintf("worker %s has no per-worker watermark", workerID)}
	}
	if wm < requestedSnapshot {
		return GateDecision{Reason: GateDenyWorkerWatermarkBehind, Detail: fmt.Sprintf("worker %s watermark %d < requested %d", workerID, wm, requestedSnapshot)}
	}
	if g.cut.GlobalWatermark < requestedSnapshot {
		return GateDecision{Reason: GateDenyGlobalWatermarkBehind, Detail: fmt.Sprintf("global watermark %d < requested %d", g.cut.GlobalWatermark, requestedSnapshot)}
	}
	return GateDecision{Allowed: true, Reason: GateAllowed}
}

// PublicationEquationInput carries the sets and readback digests the design
// section 4.8 publication equation is checked over. It is a dedicated input
// (adds AppliedEquivalentSet and readback digests) rather than a mutation of the
// driver's PublishMutationSafeCutInput.
type PublicationEquationInput struct {
	RequiredServingSet       []string
	RetainedServingSet       []string
	AppliedEquivalentSet     []string
	ExcludedBeforeCut        []string
	ServingAvailabilityFloor int
	CanonicalReadbackDigest  string
	RetainedReadbackDigests  map[string]string
}

// VerifyPublicationEquation is the pure predicate mirror of the design section
// 4.8 publication equation. HouseGate consults it; it does not commit the cut.
// It fails closed unless: RetainedServingSet ⊆ AppliedEquivalentSet;
// RequiredServingSet == RetainedServingSet ⊎ ExcludedBeforeCut (a disjoint union
// that set-equals Required); size(RetainedServingSet) >= the serving-availability
// floor; and every retained worker has a readback that equals the canonical
// input.
func VerifyPublicationEquation(in PublicationEquationInput) error {
	retained := stringSet(in.RetainedServingSet)
	applied := stringSet(in.AppliedEquivalentSet)
	excluded := stringSet(in.ExcludedBeforeCut)
	required := stringSet(in.RequiredServingSet)

	if missing, ok := subsetOf(retained, applied); !ok {
		return fmt.Errorf("publication equation: retained worker %s is not applied-equivalent", missing)
	}
	// Disjoint: no worker in both retained and excluded.
	for w := range retained {
		if excluded[w] {
			return fmt.Errorf("publication equation: worker %s is in both retained and excluded", w)
		}
	}
	// Coverage: retained ∪ excluded set-equals required.
	union := map[string]bool{}
	for w := range retained {
		union[w] = true
	}
	for w := range excluded {
		union[w] = true
	}
	if len(union) != len(required) {
		return fmt.Errorf("publication equation: retained∪excluded (%d) does not cover required (%d)", len(union), len(required))
	}
	for w := range required {
		if !union[w] {
			return fmt.Errorf("publication equation: required worker %s is neither retained nor excluded", w)
		}
	}
	for w := range union {
		if !required[w] {
			return fmt.Errorf("publication equation: worker %s is retained/excluded but not required", w)
		}
	}
	floor := in.ServingAvailabilityFloor
	if floor <= 0 {
		floor = MutationServingAvailabilityFloor
	}
	if len(retained) < floor {
		return fmt.Errorf("publication equation: retained serving set size %d below floor %d", len(retained), floor)
	}
	// Every retained worker's readback must equal the canonical input.
	for w := range retained {
		d, ok := in.RetainedReadbackDigests[w]
		if !ok {
			return fmt.Errorf("publication equation: retained worker %s has no readback digest", w)
		}
		if d != in.CanonicalReadbackDigest {
			return fmt.Errorf("publication equation: retained worker %s readback digest != canonical", w)
		}
	}
	return nil
}

func stringSet(ss []string) map[string]bool {
	s := make(map[string]bool, len(ss))
	for _, v := range ss {
		s[v] = true
	}
	return s
}

// subsetOf reports whether a ⊆ b, returning the first missing element otherwise.
func subsetOf(a, b map[string]bool) (string, bool) {
	// Deterministic missing element for a stable error.
	missing := ""
	for k := range a {
		if !b[k] {
			if missing == "" || k < missing {
				missing = k
			}
		}
	}
	if missing != "" {
		return missing, false
	}
	return "", true
}

func copyUint64Map(m map[string]uint64) map[string]uint64 {
	if m == nil {
		return nil
	}
	out := make(map[string]uint64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func copyBoolMap(m map[string]bool) map[string]bool {
	if m == nil {
		return nil
	}
	out := make(map[string]bool, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
