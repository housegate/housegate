package housegate

import (
	"context"
	"errors"
	"fmt"

	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/schemaregistry"
	"github.com/housegate/housegate/pkg/sitable"
)

// resolveStorageIntegrityTableState selects the table-set source (spec
// 2026-09-24 §6.1). It returns (nil, nil, nil) when storage integrity is
// disabled. When enabled exactly one source is required: the configured
// storage_integrity.tables (served by sitable.Static, returned as the second
// value) or the host-injected Options.StorageIntegrityTableState.
func resolveStorageIntegrityTableState(opts Options, reg registry.Registry) (sitable.TableState, *sitable.Static, error) {
	cfg := opts.Config
	injected := opts.StorageIntegrityTableState
	if isNilInterface(injected) {
		injected = nil
	}
	hasTables := len(cfg.StorageIntegrity.Tables) > 0
	if !cfg.StorageIntegrity.IsEnabled() {
		if injected != nil {
			return nil, nil, errors.New("Options.StorageIntegrityTableState requires storage_integrity.enabled: true")
		}
		return nil, nil, nil
	}
	switch {
	case hasTables && injected != nil:
		return nil, nil, errors.New("storage_integrity: configure exactly one table-set source, storage_integrity.tables or Options.StorageIntegrityTableState, not both")
	case injected != nil:
		return injected, nil, nil
	case !hasTables:
		return nil, nil, errors.New("storage_integrity.enabled requires a table-set source: storage_integrity.tables or Options.StorageIntegrityTableState")
	}
	static := sitable.NewStatic(cfg.StorageIntegrity.Tables, staticStorageIntegritySchemas(opts, reg), cfg.StorageIntegrity.Ingress.NetworkID)
	return static, static, nil
}

// staticStorageIntegritySchemas loads the startup schemas of the configured
// tables for sitable.Static. The runtime's authoritative set wins; otherwise,
// with the ingress enabled, each table is loaded from the declared
// network-state schema, as the ingress used to do per query. A table that
// cannot be loaded stays schema-less and the ingress refuses writes to it.
func staticStorageIntegritySchemas(opts Options, reg registry.Registry) map[string]payloadexec.TableSchema {
	cfg := opts.Config
	out := map[string]payloadexec.TableSchema{}
	if len(opts.StorageIntegrityRuntime.TableSchemas) > 0 {
		for _, schema := range opts.StorageIntegrityRuntime.TableSchemas {
			out[schema.TableID] = schema
		}
		return out
	}
	if !cfg.StorageIntegrity.Ingress.Enabled {
		return out
	}
	source, err := resolveTableSchemas(opts, reg, "storage_integrity.ingress")
	if err != nil {
		log.Warnw("storage_integrity: no declared schema source; signed INSERTs will be refused", "error", err)
		return out
	}
	loader := schemaregistry.NewNetworkStateLoader(source, cfg.StorageIntegrity.Ingress.NetworkID)
	for _, id := range cfg.StorageIntegrity.Tables {
		db, table, _ := config.SplitStorageIntegrityTableID(id)
		schemas, err := loader.Load(context.Background(), []schemaregistry.TableRef{{
			TableID: id, Database: db, Table: table, LogicalDatabase: db, LogicalTable: table,
		}})
		if err != nil || len(schemas) != 1 {
			log.Warnw("storage_integrity: table has no declared schema at startup; signed INSERTs into it will be refused", "table", id, "error", err)
			continue
		}
		out[id] = schemas[0]
	}
	return out
}

// storageIntegrityTableStateLabel names the source for startup logs.
func storageIntegrityTableStateLabel(static *sitable.Static) string {
	if static != nil {
		return fmt.Sprintf("static (%d tables)", len(static.TableIDs()))
	}
	return "injected"
}
