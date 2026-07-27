package housegate

import (
	"context"
	"errors"
	"fmt"
	"strings"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	siplugin "housegate/housegate/pkg/plugins/storageintegrity"
	sicore "housegate/housegate/pkg/storageintegrity"
)

// StorageIntegrityMergeGuard is the startup fail-closed guard for storage
// integrity tables. *storageintegrity.MergeGuard satisfies it; tests and hosts may
// inject narrower implementations at the library boundary.
type StorageIntegrityMergeGuard interface {
	AssertStopMerges(context.Context) error
}

// StorageIntegrityRuntimeOptions supplies the host-owned C1/P1e runtime ports.
// HouseGate can adapt arbiter-proto clients into its core ports, but the
// selected-SNode SourcePreparer remains host/companion-owned until that seam
// exists in arbiter/arbiter-proto.
type StorageIntegrityRuntimeOptions struct {
	ArbiterIngressClient pb.ArbiterIngressClient
	PayloadStoreClient   pb.PayloadStoreClient

	StatementSubmitter sicore.StatementSubmitter
	SourcePreparer     sicore.SourcePreparer
	StatusQuerier      sicore.IntakeStatusQuerier
	PayloadWriter      sicore.PayloadWriter
	MergeGuard         StorageIntegrityMergeGuard
}

func buildStorageIntegrityRuntimeConsumer(cfgExpectedSource string, opts StorageIntegrityRuntimeOptions) (siplugin.AdmissionConsumer, error) {
	expectedSource := strings.TrimSpace(cfgExpectedSource)

	submitter := opts.StatementSubmitter
	if submitter == nil && opts.ArbiterIngressClient != nil {
		submitter = sicore.NewArbiterStatementSubmitter(opts.ArbiterIngressClient)
	}
	payloadWriter := opts.PayloadWriter
	if payloadWriter == nil && opts.PayloadStoreClient != nil {
		payloadWriter = sicore.NewArbiterPayloadStoreWriter(opts.PayloadStoreClient)
	}

	var errs []error
	if submitter == nil {
		errs = append(errs, errors.New("storage_integrity.runtime.statement_submitter is required"))
	}
	if opts.SourcePreparer == nil {
		errs = append(errs, errors.New("storage_integrity.runtime.source_preparer is required"))
	}
	if payloadWriter == nil {
		errs = append(errs, errors.New("storage_integrity.runtime.payload_writer is required"))
	}
	if opts.MergeGuard == nil {
		errs = append(errs, errors.New("storage_integrity.runtime.merge_guard is required"))
	}
	if joined := errors.Join(errs...); joined != nil {
		return nil, joined
	}

	var orch *sicore.Orchestrator
	if opts.StatusQuerier != nil {
		orch = sicore.NewOrchestratorWithQuerier(submitter, opts.SourcePreparer, opts.StatusQuerier, sicore.OrchestratorConfig{ExpectedSource: expectedSource})
	} else {
		orch = sicore.NewOrchestrator(submitter, opts.SourcePreparer, sicore.OrchestratorConfig{ExpectedSource: expectedSource})
	}
	ingress, err := NewStorageIntegrityIngressWithPayloadWriter(orch, opts.MergeGuard, sicore.MaterializerNative, payloadWriter)
	if err != nil {
		return nil, fmt.Errorf("storage_integrity.runtime: %w", err)
	}
	return ingress, nil
}
