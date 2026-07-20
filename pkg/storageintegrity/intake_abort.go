package storageintegrity

// This file holds the PR06 terminal-reject exact-cleanup contract: the sourcing
// of the exact candidate parts that abort hands to the AbortPreparedStatement
// seam. The abort control flow itself lives in intake.go's abort(); this file
// isolates the "which parts get cleaned" decision so the exact-parts invariant
// (design section 3.4 rules 3-4: cleanup only the journal's exact part names,
// never a whole partition) has a single, testable source.

// abortParts returns the exact candidate parts a terminal-reject cleanup may
// drop: the frozen CandidateParts from the record's cached prepared result, and
// nothing else. It is the single point that decides the cleanup surface, so the
// drop can never widen to a whole partition or to parts the prepare did not
// freeze. A record with no cached prepare (or an empty inventory) yields a nil
// slice — a legitimate no-op cleanup (a part not present is already clean).
//
// A defensive copy is returned so a caller (or the abort seam) mutating the
// slice cannot rewrite the record's frozen inventory, which a retry must reuse
// verbatim.
func (o *Orchestrator) abortParts(rec *intakeRecord) []CandidatePart {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !rec.hasPrepared || len(rec.prepared.CandidateParts) == 0 {
		return nil
	}
	return append([]CandidatePart(nil), rec.prepared.CandidateParts...)
}
