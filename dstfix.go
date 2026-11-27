package raft

// candidateAhead reports whether a candidate's last log (term, index) is
// ahead of r's: then r's latest configuration may be older than the
// candidate's, and can't be trusted to judge the candidate's membership.
// Used by the experimental switch candidate-check-unless-ahead
// (UPSTREAM-ISSUE-DRAFT.md).
func candidateAhead(r *Raft, term, index uint64) bool {
	lastIdx, lastTerm := r.getLastEntry()
	return term > lastTerm || (term == lastTerm && index > lastIdx)
}
