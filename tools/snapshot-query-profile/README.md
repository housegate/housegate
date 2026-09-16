# Snapshot query profile generator

`snapshot-query-profile` is an offline build and test tool. It measures final artifact files, creates one canonical `replay.QueryProfileFileV1`, and writes a separate unsigned provenance manifest. Running it does not qualify an analyzer, enable a runtime path, or add the generator itself to the native analyzer member set.

## Usage

```text
bazel run //tools/snapshot-query-profile -- \
  -recipe /absolute/path/recipe.json \
  -profile-out /absolute/path/query-profiles.json \
  -provenance-out /absolute/path/query-profiles.provenance.json
```

The recipe is strict JSON: all fields shown below are required, including the four empty measured digest fields; unknown, duplicate, missing, `null`, malformed, and trailing input is rejected. `platform` describes the SQL execution environment. Each `native_members[].platform` independently describes that analyzer member's host; the generator never infers a platform from a filename.

```json
{"version":1,"record":{"version":1,"clickhouse_build_digest":"","platform":"linux/amd64","native_analyzer_build_digest":"","grpc_analyzer_build_digest":"","tzdata_digest":"","settings":[{"name":"max_threads","value":"1"}],"scalar_operators":["column","literal"],"column_profile_id":"column-profile-v1","output_order_id":"output-order-v1","limits":{"max_sql_bytes":65536,"max_descriptor_bytes":65536,"max_output_rows":1048576,"max_output_bytes":268435456,"max_restore_bytes":1073741824,"max_sort_memory_bytes":134217728,"max_spill_bytes":1073741824,"max_execution_ms":30000}},"native_members":[{"platform":"linux/amd64","executable_path":"/artifacts/native-role","ffi_path":"/artifacts/librewriter.so"}],"grpc_executable_path":"/artifacts/rewriter-server","clickhouse_executable_path":"/artifacts/clickhouse","tzdata_artifact_path":"/artifacts/tzdata.zi"}
```

The four measured fields in `record` must be empty. The tool rejects precomputed values and replaces the empty fields only with measurements of the supplied files. It sorts copies of settings, scalar operators, and measured native tuples before canonical encoding; duplicates and invalid limits are rejected without changing the caller's recipe.

## Measurement and outputs

For every artifact the tool opens the supplied path, verifies the opened object is a regular file, reads its actual bytes once for that measurement, records the byte length, and uses `0x` followed by the lowercase 64-hex SHA-256 of those bytes. That is the raw artifact digest convention used for `clickhouse_build_digest`, each native executable/FFI digest, `grpc_analyzer_build_digest`, and `tzdata_digest`. N and Q use Housegate's existing domain-separated canonical APIs; the output profile digest in provenance is again raw SHA-256 of the canonical profile bytes.

The profile file contains no paths. The provenance manifest contains the exact input paths, explicit platforms, byte lengths, raw digests, N, Q, and profile output path/length/digest. Provenance, its paths, and the generator binary hash never enter Q. Identical bytes relocated to other paths therefore keep the same Q while changing provenance.

Both outputs are fully generated and validated before writing. Each is staged in its destination directory and renamed into place, so a profile path is never exposed with partial bytes.

Artifact immutability, semantic dependency closure, analyzer role membership, native/gRPC parity, and unchanged direct execution after measurement remain obligations of the later A4, A5, B, and D qualification gates. This tool's path-based input does not eliminate arbitrary concurrent file replacement or production TOCTOU; runtime loaders independently measure and retain the artifacts they actually load.
