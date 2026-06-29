package storageintegrity

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"
)

// Runtime groups HouseGate-side storage-integrity workers. The P0 control
// plane is injected through the worker sinks/readers; this runtime owns local
// adapters and lifetime only.
type Runtime struct {
	Payloads     *MockPayloadStore
	Ingress      IngressSink
	Replay       *ReplayWorker
	Unsafe       *UnsafeValidationWorker
	Promotion    *PromotionWorker
	Rollback     *RollbackWorker
	SafeAudit    *SafeAuditWorker
	PollInterval time.Duration

	ReplayJobs  ReplayJobSource
	UnsafeTasks UnsafeValidationSource
	Promotions  PromotionSource
	Rollbacks   RollbackSource
	SafeAudits  SafeAuditSource
}

func (r *Runtime) Run(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)
	loops := 0
	if r.Replay != nil && r.ReplayJobs != nil {
		loops++
		g.Go(func() error {
			return r.poll(gctx, func(ctx context.Context) (bool, error) {
				job, ok, err := r.ReplayJobs.ClaimReplayJob(ctx)
				if err != nil || !ok {
					return ok, err
				}
				return true, r.Replay.VerifyAndSubmit(ctx, job)
			})
		})
	}
	if r.Unsafe != nil && r.UnsafeTasks != nil {
		loops++
		g.Go(func() error {
			return r.poll(gctx, func(ctx context.Context) (bool, error) {
				task, ok, err := r.UnsafeTasks.ClaimUnsafeValidation(ctx)
				if err != nil || !ok {
					return ok, err
				}
				return true, r.Unsafe.VerifyAndSubmit(ctx, task)
			})
		})
	}
	if r.Promotion != nil && r.Promotions != nil {
		loops++
		g.Go(func() error {
			return r.poll(gctx, func(ctx context.Context) (bool, error) {
				task, ok, err := r.Promotions.ClaimPromotion(ctx)
				if err != nil || !ok {
					return ok, err
				}
				return true, r.Promotion.Apply(ctx, task)
			})
		})
	}
	if r.Rollback != nil && r.Rollbacks != nil {
		loops++
		g.Go(func() error {
			return r.poll(gctx, func(ctx context.Context) (bool, error) {
				task, ok, err := r.Rollbacks.ClaimRollback(ctx)
				if err != nil || !ok {
					return ok, err
				}
				return true, r.Rollback.Apply(ctx, task)
			})
		})
	}
	if r.SafeAudit != nil && r.SafeAudits != nil {
		loops++
		g.Go(func() error {
			return r.poll(gctx, func(ctx context.Context) (bool, error) {
				task, ok, err := r.SafeAudits.ClaimSafeAudit(ctx)
				if err != nil || !ok {
					return ok, err
				}
				_, err = r.SafeAudit.Audit(ctx, task)
				return true, err
			})
		})
	}
	if loops == 0 {
		<-ctx.Done()
		return nil
	}
	return g.Wait()
}

func (r *Runtime) poll(ctx context.Context, claim func(context.Context) (bool, error)) error {
	interval := r.PollInterval
	if interval <= 0 {
		interval = time.Second
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		claimed, err := claim(ctx)
		if err != nil {
			return err
		}
		if claimed {
			continue
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}
