package storageintegrity

import "context"

// SIWriteEvent describes one storage-integrity write for billing (spec
// 2026-10-09 §6.9). Signer is the accountable writer bound into the
// statement; Owner is the validated operator relation (empty when the signer
// writes for itself) and Principal is Owner, else Signer. Rows is filled only
// by the host's promotion-time OnStatementSafe event.
type SIWriteEvent struct {
	StatementID  string
	Signer       string
	Owner        string
	Principal    string
	TableID      string
	Rows         uint64
	PayloadBytes uint64
	StatementSeq uint64
}

// WriteMeter is the billing extension point; nothing in HouseGate bills
// through it yet (spec D4, R11). OnStatementSequenced is called after the
// arbiter accepted a submission, asynchronously and best-effort: it never
// blocks or fails the write, and an idempotent ACK2 replay of the same
// statement may call it again, so implementations dedupe on StatementID.
type WriteMeter interface {
	OnStatementSequenced(ctx context.Context, ev SIWriteEvent)
}
