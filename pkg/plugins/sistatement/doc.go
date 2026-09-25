// Package sistatement is the agent-mode storage-integrity statement plugin
// (spec 2026-08-18 signed envelope v2 §5.1). For every payload-local Native
// INSERT it first looks up the target table's status (spec 2026-09-24 §10);
// every table but an Active one passes through unsigned. For an Active table
// it verifies the registry schema against its schema_hash,
// asks Relay to answer the sample block locally (QueryContext.DeferredInsert),
// buffers and hashes the client's Data packets, and signs the
// housegate-statement-v2 token into SQL_x_statement_token before the Query is
// forwarded. It runs after materialize and before the agent auth signer.
package sistatement
