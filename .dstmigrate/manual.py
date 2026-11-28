# Hand edits after dstmigrate: where the tool can't know the World.
import re, sys

def edit(path, fn):
    s = open(path).read()
    t = fn(s)
    if t == s:
        sys.exit(f"manual.py: no change in {path}")
    # go-dst-kit's realworld.Default and dstbugs, wherever an edit uses them
    if 'realworld.Default()' in t:
        t = imports(t, 'github.com/prince776/go-dst-kit/realworld')
    if 'dstbugs.' in t:
        t = imports(t, 'github.com/prince776/go-dst-kit/dstbugs')
    open(path, 'w').write(t)

def func_range(s, name):
    i = s.index('func ' + name + '(')
    return i, s.index('\n}\n', i) + 3

def replace_func(s, name, new):
    i, j = func_range(s, name)
    return s[:i] + new + s[j:]

def imports(s, *paths):
    for p in paths:
        if '"%s"' % p not in s:
            s = s.replace('import (\n', 'import (\n\t"%s"\n' % p, 1)
    return s

# util.go: notify helpers need no World; timeouts and UUIDs take one.
def util(s):
    s = replace_func(s, 'asyncNotifyCh', '''func asyncNotifyCh(ch dst.Chan[struct{}]) {
	ch.TrySend(struct{}{})
}
''')
    s = replace_func(s, 'drainNotifyCh', '''func drainNotifyCh(ch dst.Chan[struct{}]) bool {
	_, _, got := ch.TryRecv()
	return got
}
''')
    s = replace_func(s, 'overrideNotifyBool', '''func overrideNotifyBool(ch dst.Chan[bool], v bool) {
	// ch is 1-buffered: empty, the send goes through; full, there is an old
	// value to take first.
	if ch.TrySend(v) {
		return // value sent, all done
	}
	ch.TryRecv() // channel had an old value
	if !ch.TrySend(v) {
		panic("race: channel was sent concurrently")
	}
}
''')
    s = replace_func(s, 'randomTimeout', '''func randomTimeout(w *dst.World, minVal time.Duration) dst.Chan[time.Time] {
	if minVal == 0 {
		return dst.Chan[time.Time]{}
	}
	extra := time.Duration(w.Rand().Int64()) % minVal
	return w.After(minVal + extra)
}
''')
    return s
edit('util.go', util)

edit('inmem_transport.go', lambda s: s.replace('ServerAddress(generateUUID(__w))', 'ServerAddress(generateUUID(realworld.Default()))'))

# Replace __w inside one function.
def in_func(name, world):
    def f(s):
        i, j = func_range(s, name)
        return s[:i] + s[i:j].replace('__w', world) + s[j:]
    return f

edit('inmem_transport.go', in_func('newInmemPipeline', 'trans.w'))
edit('net_transport.go', in_func('newNetPipeline', 'trans.w'))

# Transports: the World from the config (network), or the real one.
def inmem(s):
    s = s.replace('''func NewInmemTransportWithTimeout(addr ServerAddress, timeout time.Duration) (ServerAddress, *InmemTransport) {''',
        '''func NewInmemTransportWithTimeout(addr ServerAddress, timeout time.Duration) (ServerAddress, *InmemTransport) {
	return NewInmemTransportWithWorld(realworld.Default(), addr, timeout)
}

// NewInmemTransportWithWorld is NewInmemTransportWithTimeout in the World w.
func NewInmemTransportWithWorld(w *dst.World, addr ServerAddress, timeout time.Duration) (ServerAddress, *InmemTransport) {''')
    s = in_func('NewInmemTransportWithWorld', 'w')(s)
    return s.replace('''		consumerCh: dst.MakeChan[RPC](w, 16),''', '''		w:          w,
		consumerCh: dst.MakeChan[RPC](w, 16),''')
edit('inmem_transport.go', inmem)

# Snapshot stores: a World for the snapshot's timestamp (real by default).
def snapname(s):
    i = s.index('type FileSnapshotStore struct {'); j = s.index('\n}\n', i)
    s = s[:i] + s[i:j] + '\n\n\tw *dst.World // nil: the real World' + s[j:]
    return imports(s, 'github.com/prince776/go-dst-kit/dst') + '''
func (f *FileSnapshotStore) world() *dst.World {
	if f.w == nil {
		return realworld.Default()
	}
	return f.w
}
'''
edit('file_snapshot.go', snapname)

# FileSnapshotStore on a World's filesystem (phase 1 item 3). dstmigrate
# rewrote its os calls to f.world().FS(); the constructor (no receiver)
# takes the World explicitly. With none it is the real World
# (realworld.Default: plain os calls, so base is any ordinary path, as
# before, and the store recreates its directory if it is removed, as
# upstream does).
def filesnapworld(s):
    sig = 'func NewFileSnapshotStoreWithLogger(base string, retain int, logger hclog.Logger) (*FileSnapshotStore, error) {'
    i = s.index(sig); j = s.index('\n}\n', i)
    body = s[i:j]
    body = body.replace(sig, """func NewFileSnapshotStoreWithLogger(base string, retain int, logger hclog.Logger) (*FileSnapshotStore, error) {
	return NewFileSnapshotStoreWithWorld(realworld.Default(), base, retain, logger)
}

// NewFileSnapshotStoreWithWorld is NewFileSnapshotStoreWithLogger keeping
// the snapshots on w's filesystem, under base.
func NewFileSnapshotStoreWithWorld(w *dst.World, base string, retain int, logger hclog.Logger) (*FileSnapshotStore, error) {""")
    assert '__w' in body, body
    body = body.replace('__w', 'w')
    lit = """		logger: logger,
	}"""
    assert body.count(lit) == 1
    body = body.replace(lit, """		logger: logger,
		w:      w,
	}""")
    s = s[:i] + body + s[j:]
    return s
edit('file_snapshot.go', filesnapworld)
def inmemsnap(s):
    i = s.index('type InmemSnapshotStore struct {'); j = s.index('\n}\n', i)
    s = s[:i] + s[i:j] + '\n\n\tw *dst.World // nil: the real World' + s[j:]
    return imports(s, 'github.com/prince776/go-dst-kit/dst') + '''
func (m *InmemSnapshotStore) world() *dst.World {
	if m.w == nil {
		return realworld.Default()
	}
	return m.w
}
'''
edit('inmem_snapshot.go', inmemsnap)

# The test cluster runs in the real World.
def cluster(s):
    s = in_func('makeCluster', 'w')(s)
    return s.replace('''	c := &cluster{
		observationCh:''', '''	w := realworld.Default()
	c := &cluster{
		w:             w,
		observationCh:''', 1)
edit('testing.go', cluster)

# Map iteration that starts goroutines or sends: an order from the World
# (dst_maporder.go).
def maporder_raft(s):
    s = s.replace('for _, p := range r.leaderState.replState {', 'for _, p := range mapValues(r.w, r.leaderState.replState) {')
    s = s.replace('for future := range r.leaderState.notify {', 'for _, future := range futureOrder(r.w, r.leaderState.notify) {')
    s = s.replace('for serverID, repl := range r.leaderState.replState {', 'for _, serverID := range mapOrder(r.w, r.leaderState.replState) {\n\t\trepl := r.leaderState.replState[serverID]')
    s = s.replace('for _, repl := range r.leaderState.replState {', 'for _, repl := range mapValues(r.w, r.leaderState.replState) {')
    s = s.replace('for _, f := range r.leaderState.replState {', 'for _, f := range mapValues(r.w, r.leaderState.replState) {')
    return s
edit('raft.go', maporder_raft)
edit('replication.go', lambda s: s.replace('	for v := range n {\n		v.vote(leader)', '	for _, v := range futureOrder(s.w, n) {\n		v.vote(leader)'))
edit('net_transport.go', lambda s: s.replace('for k, e := range n.connPool {', 'for _, k := range mapOrder(n.w, n.connPool) {\n\t\te := n.connPool[k]'))
edit('observer.go', lambda s: s.replace('for _, or := range r.observers {', 'for _, or := range mapValues(r.w, r.observers) {'))
# verify futures get a creation sequence number, to order them.
def vseq(s):
    s = s.replace('	verifyFuture := &verifyFuture{}', '	verifyFuture := &verifyFuture{seq: r.verifySeq.Add(1)}')
    i = s.index('type Raft struct {'); j = s.index('\n}\n', i)
    return s[:i] + s[i:j] + '\n\n\t// verifySeq numbers verify futures, to order them (dst_maporder.go).\n\tverifySeq atomic.Uint64' + s[j:]
edit('api.go', lambda s: imports(vseq(s), 'sync/atomic'))
def vfield(s):
    i = s.index('type verifyFuture struct {'); j = s.index('\n}\n', i)
    return s[:i] + s[i:j] + '\n\n\tseq uint64 // creation order (dst_maporder.go)' + s[j:]
edit('future.go', vfield)

# Benchmark bug switches (dstbench.json; bug() in dstbugs*.go).
def in_func(head, a, b, n=1):
    def f(s):
        i = s.index(head)
        j = s.index('\n}\n', i)
        body = s[i:j]
        assert body.count(a) == n, (head, a, body.count(a))
        return s[:i] + body.replace(a, b) + s[j:]
    return f
edit('commitment.go', lambda s: s.replace('''	quorumMatchIndex := matched[(len(matched)-1)/2]

	if quorumMatchIndex > c.commitIndex && quorumMatchIndex >= c.startIndex {''', '''	quorumMatchIndex := matched[(len(matched)-1)/2]
	if dstbugs.Bug("quorum-off-by-one") {
		quorumMatchIndex = matched[len(matched)/2] // before 163a55f: half of an even cluster
	}

	if quorumMatchIndex > c.commitIndex && (quorumMatchIndex >= c.startIndex || dstbugs.Bug("commit-previous-term")) {''', 1))
edit('raft.go', in_func('func (r *Raft) requestVote(', 'if lastVoteTerm == req.Term && lastVoteCandBytes != nil {',
     'if lastVoteTerm == req.Term && lastVoteCandBytes != nil && !dstbugs.Bug("vote-twice") {'))
for head in ('func (r *Raft) requestVote(', 'func (r *Raft) requestPreVote('):
    edit('raft.go', in_func(head, 'if lastTerm > req.LastLogTerm {', 'if lastTerm > req.LastLogTerm && !dstbugs.Bug("vote-ignores-log") {'))
    edit('raft.go', in_func(head, 'if lastTerm == req.LastLogTerm && lastIdx > req.LastLogIndex {',
         'if lastTerm == req.LastLogTerm && lastIdx > req.LastLogIndex && !dstbugs.Bug("vote-ignores-log") {'))
edit('raft.go', in_func('func (r *Raft) appendEntries(', 'if a.Term < r.getCurrentTerm() {',
     'if a.Term < r.getCurrentTerm() && !dstbugs.Bug("accept-stale-leader") {'))
edit('raft.go', in_func('func (r *Raft) appendEntries(', 'if a.PrevLogTerm != prevLogTerm {',
     'if a.PrevLogTerm != prevLogTerm && !dstbugs.Bug("skip-prev-log-term") {'))
edit('snapshot.go', lambda s: s.replace('''	maxLog := min(snapIdx, lastLogIdx-trailingLogs)
''', '''	maxLog := min(snapIdx, lastLogIdx-trailingLogs)
	if dstbugs.Bug("compact-past-snapshot") {
		maxLog = lastLogIdx - trailingLogs // deletes entries the snapshot doesn't hold
	}
''', 1))

# Membership bug switches (dstbench.json).
edit('raft.go', lambda s: s.replace('''					if hasVote(r.configurations.latest, r.localID) {''',
    '''					if hasVote(r.configurations.latest, r.localID) || (dstbugs.Bug("nonvoter-can-lead") && inConfiguration(r.configurations.latest, r.localID)) {''', 1))
for head in ('func (r *Raft) requestVote(', 'func (r *Raft) requestPreVote('):
    edit('raft.go', in_func(head, 'if len(r.configurations.latest.Servers) > 0 && !hasVote(r.configurations.latest, candidateID) {',
         'if len(r.configurations.latest.Servers) > 0 && !hasVote(r.configurations.latest, candidateID) && !dstbugs.Bug("nonvoter-can-lead") {'))
edit('raft.go', in_func('func (r *Raft) checkLeaderLease(', 'if server.Suffrage == Voter {',
     'if server.Suffrage == Voter || dstbugs.Bug("lease-counts-nonvoters") {'))
edit('raft.go', in_func('func (r *Raft) configurationChangeChIfStable(', '''	if r.configurations.latestIndex == r.configurations.committedIndex &&
		r.getCommitIndex() >= r.leaderState.commitment.startIndex {''', '''	if (r.configurations.latestIndex == r.configurations.committedIndex || dstbugs.Bug("two-changes-in-flight")) &&
		(r.getCommitIndex() >= r.leaderState.commitment.startIndex || dstbugs.Bug("config-change-before-noop")) {'''))

# The candidate checks of 6b4e320 and the pre-vote code (a receiver
# rejects a candidate missing from, or not a voter in, its own latest
# configuration), off: raft before 6b4e320 (upstream #524's problem).
for head in ('func (r *Raft) requestVote(', 'func (r *Raft) requestPreVote('):
    edit('raft.go', in_func(head, '!inConfiguration(r.configurations.latest, candidateID) {',
         '!inConfiguration(r.configurations.latest, candidateID) && !dstbugs.Bug("no-candidate-config-check") {'))
    edit('raft.go', in_func(head, '&& !dstbugs.Bug("nonvoter-can-lead") {',
         '&& !dstbugs.Bug("nonvoter-can-lead") && !dstbugs.Bug("no-candidate-config-check") {'))

# Experiment: the candidate checks only when the candidate isn't ahead
# (dstfix.go; UPSTREAM-ISSUE-DRAFT.md's proposed fix).
for head in ('func (r *Raft) requestVote(', 'func (r *Raft) requestPreVote('):
    edit('raft.go', in_func(head, '&& !dstbugs.Bug("no-candidate-config-check") {',
         '&& !dstbugs.Bug("no-candidate-config-check") && !(dstbugs.Fix("candidate-check-unless-ahead") && candidateAhead(r, req.LastLogTerm, req.LastLogIndex)) {', 2))

# Experiment: BootstrapCluster appends the configuration entry before it
# saves the term, so a crash between the two leaves a node that can still
# bootstrap (HasExistingState sees the term first otherwise).
def bootstrap_log_first(s):
    a = '''	// Set current term to 1.
	if err := stable.SetUint64(keyCurrentTerm, 1); err != nil {
		return fmt.Errorf("failed to save current term: %v", err)
	}
'''
    assert s.count(a) == 1
    s = s.replace(a, '''	// Set current term to 1.
	if !dstbugs.Fix("bootstrap-log-first") {
		if err := stable.SetUint64(keyCurrentTerm, 1); err != nil {
			return fmt.Errorf("failed to save current term: %v", err)
		}
	}
''')
    b = '''	if err := logs.StoreLog(entry); err != nil {
		return fmt.Errorf("failed to append configuration entry to log: %v", err)
	}

	return nil'''
    assert s.count(b) == 1
    return s.replace(b, '''	if err := logs.StoreLog(entry); err != nil {
		return fmt.Errorf("failed to append configuration entry to log: %v", err)
	}
	if dstbugs.Fix("bootstrap-log-first") {
		if err := stable.SetUint64(keyCurrentTerm, 1); err != nil {
			return fmt.Errorf("failed to save current term: %v", err)
		}
	}

	return nil''')
edit('api.go', bootstrap_log_first)

# Experiment: entries at or below a follower's snapshot are committed, so
# (1) appendEntries treats a previous entry there as matching (the leader
# holds the same committed entry), and (2) installSnapshot acknowledges a
# snapshot older than what the follower applied without installing it.
# DST.md 6.11.
edit('raft.go', in_func('func (r *Raft) appendEntries(', '''		var prevLogTerm uint64
		if a.PrevLogEntry == lastIdx {
			prevLogTerm = lastTerm
		} else {''', '''		var prevLogTerm uint64
		snapIdx, _ := r.getLastSnapshot()
		if a.PrevLogEntry == lastIdx {
			prevLogTerm = lastTerm
		} else if dstbugs.Fix("snapshot-boundary-fix") && a.PrevLogEntry <= snapIdx {
			prevLogTerm = a.PrevLogTerm // committed: the leader's entry is ours
		} else {'''))
edit('raft.go', in_func('func (r *Raft) installSnapshot(', '''		reqConfigurationIndex = req.LastLogIndex
	}
	version := getSnapshotVersion(r.protocolVersion)''', '''		reqConfigurationIndex = req.LastLogIndex
	}
	if dstbugs.Fix("snapshot-boundary-fix") && r.holdsEntry(req.LastLogIndex, req.LastLogTerm) {
		// Figure 13, rule 6: we hold the snapshot's last entry, so we
		// continue from it without installing.
		resp.Success = true
		return
	}
	version := getSnapshotVersion(r.protocolVersion)'''))
edit('raft.go', in_func('func (r *Raft) appendEntries(', '''		lastLogIdx, _ := r.getLastLog()
		var newEntries []*Log
		for i, entry := range a.Entries {
			if entry.Index > lastLogIdx {''', '''		lastLogIdx, _ := r.getLastLog()
		snapshotIdx, _ := r.getLastSnapshot()
		var newEntries []*Log
		for i, entry := range a.Entries {
			if dstbugs.Fix("snapshot-boundary-fix") && entry.Index <= snapshotIdx {
				continue // committed, and in our snapshot
			}
			if entry.Index > lastLogIdx {'''))

# Bug for the FSM's index check (dst_kv_test.go): an entry handed to the
# FSM twice. With clients' retries deduplicated by ID, only that check sees it.
edit('raft.go', in_func('func (r *Raft) processLogs(', '''			batch = append(batch, preparedLog)

			// If we have filled up a batch, send it to the FSM''', '''			batch = append(batch, preparedLog)
			if dstbugs.Bug("apply-entry-twice") && idx%50 == 0 {
				batch = append(batch, preparedLog)
			}

			// If we have filled up a batch, send it to the FSM'''))

# Experimental fix (DST.md 6.12): restoreSnapshot falls back to an older
# snapshot when the newest fails to restore, but NewRaft then replays the
# log from the older snapshot's index and panics where the log was
# compacted past it. An older snapshot is usable only if the log holds
# every entry after it.
edit('api.go', in_func('func (r *Raft) restoreSnapshot() error {', '''	for _, snapshot := range snapshots {
		if success := r.tryRestoreSingleSnapshot(snapshot); !success {''', '''	for _, snapshot := range snapshots {
		if dstbugs.Fix("snapshot-fallback-needs-log") && !r.logFollows(snapshot.Index) {
			r.logger.Error("not restoring a snapshot the log was compacted past", "id", snapshot.ID, "last-index", snapshot.Index)
			continue
		}
		if success := r.tryRestoreSingleSnapshot(snapshot); !success {'''))
# ...and, the general guard, wherever the snapshot went missing (a
# fallback above, or FileSnapshotStore's List skipping a snapshot whose
# metadata it couldn't read): NewRaft returns an error instead of
# replaying a log that doesn't follow the restored snapshot.
edit('api.go', lambda s: s.replace('''	// Scan through the log for any configuration change entries.
	snapshotIndex, _ := r.getLastSnapshot()''', '''	// Scan through the log for any configuration change entries.
	snapshotIndex, _ := r.getLastSnapshot()
	if dstbugs.Fix("snapshot-fallback-needs-log") && !r.logFollows(snapshotIndex) {
		first, _ := r.logs.FirstIndex()
		return nil, fmt.Errorf("the log starts at %d, past the restored snapshot's index %d: a later snapshot couldn't be read", first, snapshotIndex)
	}''', 1))

# Phase 2: a white-box hook for the "one vote per term" check. raft calls
# Config.dstOnVote (nil outside the simulation) whenever it grants a vote,
# its own included, after persisting it.
edit('config.go', lambda s: s.replace('''	// skipStartup allows NewRaft() to bypass all background work goroutines
	skipStartup bool''', '''	// skipStartup allows NewRaft() to bypass all background work goroutines
	skipStartup bool

	// dstOnVote, if set (simulation tests), is told of every vote raft
	// grants: the term and the candidate (its encoded address).
	dstOnVote func(term uint64, candidate []byte)''', 1))
edit('raft.go', in_func('func (r *Raft) requestVote(', '''			r.logger.Warn("duplicate requestVote from", "candidate", candidate)
			resp.Granted = true''', '''			r.logger.Warn("duplicate requestVote from", "candidate", candidate)
			resp.Granted = true
			r.dstVoted(req.Term, candidateBytes)'''))
edit('raft.go', in_func('func (r *Raft) requestVote(', '''	resp.Granted = true
	r.setLastContact()''', '''	resp.Granted = true
	r.dstVoted(req.Term, candidateBytes)
	r.setLastContact()'''))
edit('raft.go', in_func('func (r *Raft) electSelf(', '''				if err := r.persistVote(req.Term, req.Addr); err != nil {
					r.logger.Error("failed to persist vote", "error", err)
					return (dst.Chan[*voteResult]{})

				}''', '''				if err := r.persistVote(req.Term, req.Addr); err != nil {
					r.logger.Error("failed to persist vote", "error", err)
					return (dst.Chan[*voteResult]{})

				}
				r.dstVoted(req.Term, req.Addr)'''))
# Bug for "terms never go back": a restarted node forgets its term.
edit('api.go', in_func('func NewRaft(', '''	currentTerm, err := stable.GetUint64(keyCurrentTerm)''', '''	currentTerm, err := stable.GetUint64(keyCurrentTerm)
	if dstbugs.Bug("forget-term-on-restart") {
		currentTerm = 0
	}'''))

# Experimental fix (DST.md 6.13): processHeartbeat runs appendEntries on
# the transport's connection goroutine, concurrently with the main loop,
# though appendEntries "must only be called from the main thread". A
# heartbeat that passed the term check while the main loop moved to a new
# term (electSelf) then steps the node down to the heartbeat's older term:
# the current term goes back, on disk too. With the fix, a heartbeat that
# would change the term or the state goes to the main loop instead.
edit('raft.go', lambda s: s.replace('''	case *AppendEntriesRequest:
		r.appendEntries(rpc, cmd)
	default:
		r.logger.Error("expected heartbeat, got"''', '''	case *AppendEntriesRequest:
		r.appendEntriesOn(rpc, cmd, true)
	default:
		r.logger.Error("expected heartbeat, got"''', 1))
edit('raft.go', lambda s: s.replace('''func (r *Raft) appendEntries(rpc RPC, a *AppendEntriesRequest) {
	defer metrics.MeasureSince([]string{"raft", "rpc", "appendEntries"}, r.w.Now())''', '''func (r *Raft) appendEntries(rpc RPC, a *AppendEntriesRequest) { r.appendEntriesOn(rpc, a, false) }

// appendEntriesOn: heartbeat is set on processHeartbeat's fast path (not
// the main thread).
func (r *Raft) appendEntriesOn(rpc RPC, a *AppendEntriesRequest, heartbeat bool) {
	defer metrics.MeasureSince([]string{"raft", "rpc", "appendEntries"}, r.w.Now())
	forwarded := false''', 1))
edit('raft.go', in_func('func (r *Raft) appendEntriesOn(', '''	defer func() {
		rpc.Respond(resp, rpcErr)
	}()''', '''	defer func() {
		if !forwarded {
			rpc.Respond(resp, rpcErr)
		}
	}()'''))
edit('raft.go', in_func('func (r *Raft) appendEntriesOn(', '''	if a.Term > r.getCurrentTerm() || (r.getState() != Follower && !r.candidateFromLeadershipTransfer.Load()) {
		// Ensure transition to follower''', '''	if a.Term > r.getCurrentTerm() || (r.getState() != Follower && !r.candidateFromLeadershipTransfer.Load()) {
		if heartbeat && dstbugs.Fix("heartbeat-on-main-thread") {
			// Changing the term or the state is the main loop's job.
			forwarded = true
			r.rpcCh.Send(rpc)
			return
		}
		// Ensure transition to follower'''))

# Phase 2 step 2's plant: a follower ignores the leader's commit index, so
# it never applies (caught only by "every member catches up").
edit('raft.go', in_func('func (r *Raft) appendEntriesOn(', 'if a.LeaderCommitIndex > 0 && a.LeaderCommitIndex > r.getCommitIndex() {',
     'if a.LeaderCommitIndex > 0 && a.LeaderCommitIndex > r.getCommitIndex() && !dstbugs.Bug("follower-ignores-commit") {'))

# newSeed (only upstream's util_test calls it) stays on crypto/rand:
# dstmigrate now maps crypto/rand.Reader to the World's, and no World is
# in scope there.
edit('util.go', lambda s: s.replace('crand.Int(dst.CryptoReader(__w), big.NewInt(math.MaxInt64))', 'crand.Int(crand.Reader, big.NewInt(math.MaxInt64))', 1))

# Phase 2 step 3's plant (#696's class): the leadership-transfer flag is
# never cleared, so the leader refuses writes after its first transfer.
edit('raft.go', in_func('func (r *Raft) leaderLoop(', '''				r.w.Go(func() {
					defer r.setLeadershipTransferInProgress(false)''', '''				r.w.Go(func() {
					defer func() {
						if !dstbugs.Bug("transfer-flag-sticks") {
							r.setLeadershipTransferInProgress(false)
						}
					}()'''))

# Experimental fix (DST.md 6.14, upstream #696): leadershipTransfer's send
# on the replication goroutine's one-slot back channel isn't in the select
# with stopCh. An earlier transfer's request left unread fills the slot; the
# next transfer blocks on the send, and when the leader steps down nothing
# drains the channel: the transfer goroutine never finishes, the "transfer
# in progress" flag is never cleared, and the node, re-elected, refuses
# every write. With the fix the send gives way to stopCh.
edit('raft.go', in_func('func (r *Raft) leadershipTransfer(id ServerID', '''		repl.triggerDeferErrorCh.Send(err)
''', '''		if dstbugs.Fix("transfer-send-stoppable") {
			stopped := false
			dst.Select(r.w,
				dst.OnSend(repl.triggerDeferErrorCh, err, func() {}),
				dst.OnRecv(stopCh, func(_ struct{}, _ bool) { stopped = true }),
			)
			if stopped {
				doneCh.Send(nil)
				return
			}
		} else {
			repl.triggerDeferErrorCh.Send(err)
		}
'''))

# Experimental fix (DST.md 6.16, upstream #612): when raft's pipeline
# decoder stops (a rejected or failed append), nothing reads the
# transport's doneCh any more; netPipeline.decodeResponses blocks handing
# over the next response, so it stops taking from inprogressCh, and the
# replication goroutine, inside pipelineSend, blocks handing over its next
# request (both channels unbuffered at the default MaxRPCsInFlight). It
# never reaches the select that would see the decoder finished, so
# pipeline.Close (deferred after that loop) never runs: replication to the
# follower stops for good while heartbeats go on. With the fix the decoder
# closes the pipeline as it exits, which unblocks both.
edit('replication.go', in_func('func (r *Raft) pipelineDecode(', '''	defer finishCh.Close()
''', '''	defer finishCh.Close()
	if dstbugs.Fix("pipeline-close-on-abort") {
		defer p.Close()
	}
'''))

# Experimental fix (DST.md 6.17, upstream #697's rule-7 half): installSnapshot
# keeps the follower's log as it is (compacting by TrailingLogs), even when
# the log's entry at the snapshot's last index has another term, so
# entries that conflict with the snapshot stay, and the next append lands
# after them (Log Matching broken in the store). Figure 13 rule 7: if the
# log holds the snapshot's last entry (same index and term), keep what
# follows it; otherwise discard the entire log.
edit('raft.go', in_func('func (r *Raft) installSnapshot(', '''	// Clear old logs if r.logs is a MonotonicLogStore. Otherwise compact the
	// logs. In both cases, log any errors and continue.''', '''	if dstbugs.Fix("snapshot-install-rule7") {
		var l Log
		if err := r.logs.GetLog(req.LastLogIndex, &l); err != nil || l.Term != req.LastLogTerm {
			// Figure 13 rule 7: the log doesn't hold the snapshot's last
			// entry, so none of it can be trusted: discard it all.
			first, err1 := r.logs.FirstIndex()
			last, err2 := r.logs.LastIndex()
			if err1 == nil && err2 == nil && first > 0 && last >= first {
				if err := r.logs.DeleteRange(first, last); err != nil {
					r.logger.Error("failed to discard a log the snapshot conflicts with", "error", err)
					rpcErr = err
					return
				}
			}
			r.setLastLog(req.LastLogIndex, req.LastLogTerm)
		}
	}
	// Clear old logs if r.logs is a MonotonicLogStore. Otherwise compact the
	// logs. In both cases, log any errors and continue.'''))

# Simulation speed (go-dst-kit performance pass): each accepted connection
# used to allocate a fresh 256KB bufio reader. Connections churn far more in
# a simulation (every crash, reset and partition redials), and zeroing and
# returning those buffers to the OS took a large share of the CPU. The
# transport keeps the readers of closed connections and Resets them onto new
# ones: a Reset reader behaves exactly as a new one, so no run changes. Only
# the server's reader: one goroutine (handleConn) owns it for the whole
# connection. Not the client's writer: a pipeline can Release its connection
# while another goroutine is still writing through it. The list is per
# transport (one node), never process-wide, so nothing crosses seeds. A plain
# mutex: no scheduling point is added.
def bufreuse(s):
    s = s.replace('''type NetworkTransport struct {
	connPool     map[ServerAddress][]*netConn''', '''type NetworkTransport struct {
	bufMu   sync.Mutex //dst:ok a plain lock: no scheduling point, see spareReader
	readers []*bufio.Reader

	connPool     map[ServerAddress][]*netConn''', 1)
    s = s.replace('''func (n *netConn) Release() error {
	return n.conn.Close()
}''', '''func (n *netConn) Release() error {
	return n.conn.Close()
}

// spareReader returns a receive buffer of connReceiveBufferSize on conn,
// reusing one a closed connection gave back (Reset, so it behaves as new).
func (n *NetworkTransport) spareReader(conn net.Conn) *bufio.Reader {
	n.bufMu.Lock()
	defer n.bufMu.Unlock()
	if k := len(n.readers); k > 0 {
		r := n.readers[k-1]
		n.readers = n.readers[:k-1]
		r.Reset(conn)
		return r
	}
	return bufio.NewReaderSize(conn, connReceiveBufferSize)
}

func (n *NetworkTransport) putReader(r *bufio.Reader) {
	r.Reset(nil)
	n.bufMu.Lock()
	n.readers = append(n.readers, r)
	n.bufMu.Unlock()
}''', 1)
    s = s.replace('''	defer func() { _ = conn.Close() }()
	r := bufio.NewReaderSize(conn, connReceiveBufferSize)''', '''	defer func() { _ = conn.Close() }()
	r := n.spareReader(conn)
	defer n.putReader(r)''', 1)
    return imports(s, 'sync')
edit('net_transport.go', bufreuse)

# Experimental fix (DST.md 6.18): appendConfigurationEntry adopts the new
# configuration after dispatchLogs even when storing it failed (dispatchLogs
# then answers the future with the error and steps down to Follower). The
# node keeps a configuration no log holds; once a later leader's entry fills
# that index and commits, it is the node's committed configuration too.
edit('raft.go', lambda s: s.replace('''	r.dispatchLogs([]*logFuture{&future.logFuture})
	index := future.Index()
	r.setLatestConfiguration(configuration, index)''', '''	r.dispatchLogs([]*logFuture{&future.logFuture})
	if dstbugs.Fix("config-only-if-stored") && r.getState() != Leader {
		// Storing the entry failed: dispatchLogs answered the future with
		// the error and stepped down. The configuration never reached the
		// log, so it isn't this node's.
		return
	}
	index := future.Index()
	r.setLatestConfiguration(configuration, index)''', 1))

# tcp_transport.go: TCP stream layers have no World (the simulation uses
# its own); dstmigrate now maps net.Listen and net.DialTimeout, so they
# go through the real World, which is exactly net's.
edit('tcp_transport.go', lambda s: s.replace('__w.Net().Listen(', 'realworld.Default().Net().Listen(').replace('dst.DialTimeout(__w, ', 'dst.DialTimeout(realworld.Default(), '))
