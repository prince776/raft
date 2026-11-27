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

// holdsEntry reports whether r holds the entry at index with term: in its
// log, or covered by its own snapshot (whose entries are committed, so
// they match any leader's). Used by the experimental switch
// snapshot-boundary-fix (DST.md 6.11).
func (r *Raft) holdsEntry(index, term uint64) bool {
	if snapIdx, _ := r.getLastSnapshot(); index <= snapIdx {
		return true
	}
	var l Log
	return r.logs.GetLog(index, &l) == nil && l.Term == term
}
