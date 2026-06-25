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
	Finality     *MockFinalityWatcher
	Replay       *ReplayWorker
	Promotion    *PromotionWorker
	SafeAudit    *SafeAuditWorker
	PollInterval time.Duration

	FinalityRequests FinalitySource
	ReplayJobs       ReplayJobSource
	Promotions       PromotionSource
	SafeAudits       SafeAuditSource
}

func (r *Runtime) Run(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)
	loops := 0
	if r.Finality != nil && r.FinalityRequests != nil {
		loops++
		g.Go(func() error {
			return r.poll(gctx, func(ctx context.Context) (bool, error) {
				req, ok, err := r.FinalityRequests.ClaimFinality(ctx)
				if err != nil || !ok {
					return ok, err
				}
				_, err = r.Finality.Finalize(ctx, req)
				return true, err
			})
		})
	}
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
