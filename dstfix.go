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

// logFollows reports whether the log holds every entry after index (or
// none at all): only then can raft restore a snapshot at index and replay
// the log from it. Used by the experimental switch
// snapshot-fallback-needs-log (DST.md 6.12).
func (r *Raft) logFollows(index uint64) bool {
	first, err1 := r.logs.FirstIndex()
	last, err2 := r.logs.LastIndex()
	if err1 != nil || err2 != nil {
		return false
	}
	return last <= index || first <= index+1
}

// dstVoted tells the simulation's "one vote per term" check of a vote raft
// granted (Config.dstOnVote; nil outside simulation tests).
func (r *Raft) dstVoted(term uint64, candidate []byte) {
	if f := r.config().dstOnVote; f != nil {
		f(term, candidate)
	}
}
