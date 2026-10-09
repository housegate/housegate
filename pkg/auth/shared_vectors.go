package auth

// SharedStatementVectorsSHA256 is the SHA-256 of
// pkg/auth/testdata/statement_jws_v2.json — the statement-JWS conformance
// vectors this repo produces and the Arbiter FSM consumes verbatim.
//
// It is exported so the Arbiter, which already imports this package, can
// assert its committed copy is byte-identical without a Bazel filegroup or a
// module-cache lookup. Regenerating the vectors is therefore a coordinated
// wire change by construction: the constant, the two copies of the file, and
// the two releases move together or the downstream repo goes red the moment it
// bumps its housegate pin.
const SharedStatementVectorsSHA256 = "6af5c9cc34d6b083935d804799138e059ce7da99fb034a2e0332b3c7ce8737bc"

// SharedStatementLaneVectorsSHA256 is the SHA-256 of
// pkg/auth/testdata/statement_jws_v2_lanes.json — the client-lane statement
// JWS vectors (spec 2026-10-09 D10) the Arbiter FSM consumes verbatim. A
// separate file and constant keep SharedStatementVectorsSHA256 and the
// arbiter's existing copy unchanged.
const SharedStatementLaneVectorsSHA256 = "77c53046f6e932362e286f4cde4b5d3e5891a6bc756207887e6e4ccba595b516"
