package housegate

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/housegate/housegate/pkg/log"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

var (
	storageIntegrityUnsafeParts = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "storage_integrity_unsafe_parts",
		Help: "Active parts per hg_unsafe table and logical partition (back-pressure input; cleanup is the only drain).",
	}, []string{"table", "partition"})
	storageIntegritySafeParts = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "storage_integrity_safe_parts",
		Help: "Active parts per hg_safe table and logical partition (grows by candidate parts per promotion; P4 compaction trigger).",
	}, []string{"table", "partition"})
	storageIntegrityBackpressureTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "storage_integrity_backpressure_total",
		Help: "Storage-integrity INSERT admissions refused with back-pressure (ClickHouse exception 252).",
	}, []string{"table"})
)

func init() {
	prometheus.MustRegister(storageIntegrityUnsafeParts)
	prometheus.MustRegister(storageIntegritySafeParts)
	prometheus.MustRegister(storageIntegrityBackpressureTotal)
}

// StorageIntegrityPartsPressure is the ingress-facing admission port; the
// supervisor and PartsPressureGuard both satisfy it.
type StorageIntegrityPartsPressure interface {
	Reserve(ctx context.Context, table string, partitionIDs []string) (sicore.PartsReservation, error)
	Invalidate()
}

// StorageIntegrityPartsPressureLifecycle is required for every runtime-owned
// pressure implementation, including injected ones. Startup performs a
// fail-fast refresh, Run maintains the snapshot, and Close cancels and joins
// that worker before its ClickHouse connection may be closed.
type StorageIntegrityPartsPressureLifecycle interface {
	StorageIntegrityPartsPressure
	Refresh(context.Context) error
	Run(context.Context)
}

// StorageIntegrityPartsPressureSupervisor polls the guard on an interval and
// promptly on invalidation, then mirrors each snapshot into Prometheus gauges.
type StorageIntegrityPartsPressureSupervisor struct {
	guard    *sicore.PartsPressureGuard
	interval time.Duration
	unsafeDB string
	safeDB   string
}

func NewStorageIntegrityPartsPressureSupervisor(guard *sicore.PartsPressureGuard, interval time.Duration, unsafeDB, safeDB string) *StorageIntegrityPartsPressureSupervisor {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	return &StorageIntegrityPartsPressureSupervisor{guard: guard, interval: interval, unsafeDB: unsafeDB, safeDB: safeDB}
}

func (s *StorageIntegrityPartsPressureSupervisor) Refresh(ctx context.Context) error {
	if s == nil || s.guard == nil {
		return errors.New("storage_integrity: parts pressure guard is required")
	}
	snap, err := s.guard.Refresh(ctx)
	if err != nil {
		return err
	}
	storageIntegrityUnsafeParts.Reset()
	storageIntegritySafeParts.Reset()
	for key, parts := range snap {
		switch key.Database {
		case s.unsafeDB:
			storageIntegrityUnsafeParts.WithLabelValues(key.Table, key.Partition).Set(float64(parts))
		case s.safeDB:
			storageIntegritySafeParts.WithLabelValues(key.Table, key.Partition).Set(float64(parts))
		}
	}
	return nil
}

func (s *StorageIntegrityPartsPressureSupervisor) Run(ctx context.Context) {
	if s == nil || s.guard == nil {
		return
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-s.guard.Invalidated():
		}
		if err := s.Refresh(ctx); err != nil && ctx.Err() == nil {
			log.WarnEveryN("storage_integrity.parts_pressure.refresh", 30, "storage_integrity parts snapshot failed; keeping last snapshot", "err", err)
		}
	}
}

func (s *StorageIntegrityPartsPressureSupervisor) Reserve(ctx context.Context, table string, partitionIDs []string) (sicore.PartsReservation, error) {
	return s.guard.Reserve(ctx, table, partitionIDs)
}

func (s *StorageIntegrityPartsPressureSupervisor) Invalidate() { s.guard.Invalidate() }
