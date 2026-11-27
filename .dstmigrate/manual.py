# Hand edits after dstmigrate: where the tool can't know the World.
import re, sys

def edit(path, fn):
    s = open(path).read()
    t = fn(s)
    if t == s:
        sys.exit(f"manual.py: no change in {path}")
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
    s = s.replace('func generateUUID() string {', 'func generateUUID(w *dst.World) string {').replace('__w.Rand().CryptoRead(buf)', 'w.Rand().CryptoRead(buf)')
    s += '''
// theRealWorld is the real World, for code with no World of its own (a
// default, or a test helper).
var theRealWorld = sync.OnceValue(func() *dst.World { return realworld.New(realworld.Options{}) })
'''
    return imports(s, 'sync', 'github.com/prince776/go-dst-kit/realworld')
edit('util.go', util)

for f in ['raft.go', 'replication.go', 'snapshot.go']:
    edit(f, lambda s: s.replace('randomTimeout(r.config()', 'randomTimeout(r.w, r.config()')
         .replace('randomTimeout(hbTimeout)', 'randomTimeout(r.w, hbTimeout)')
         .replace('randomTimeout(electionTimeout)', 'randomTimeout(r.w, electionTimeout)'))
edit('inmem_transport.go', lambda s: s.replace('ServerAddress(generateUUID())', 'ServerAddress(generateUUID(theRealWorld()))'))
edit('util_test.go', lambda s: s.replace('randomTimeout(time.Millisecond)', 'randomTimeout(theRealWorld(), time.Millisecond)')
     .replace('randomTimeout(0)', 'randomTimeout(theRealWorld(), 0)').replace('generateUUID()', 'generateUUID(theRealWorld())'))

# NewRaft: the World from Config.World, else the real one.
def newraft(s):
    i = s.index('func NewRaft(conf *Config')
    j = s.index('\n}\n', i)
    b = s[i:j].replace('''	// Validate the configuration.''', '''	// The World raft runs in: the real one unless the config says.
	w := conf.World
	if w == nil {
		w = theRealWorld()
	}

	// Validate the configuration.''', 1).replace('__w', 'w').replace('''	r.conf.Store(*conf)''', '''	r.w = w
	r.conf.Store(*conf)''', 1)
    return s[:i] + b + s[j:]
edit('api.go', newraft)

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
	return NewInmemTransportWithWorld(theRealWorld(), addr, timeout)
}

// NewInmemTransportWithWorld is NewInmemTransportWithTimeout in the World w.
func NewInmemTransportWithWorld(w *dst.World, addr ServerAddress, timeout time.Duration) (ServerAddress, *InmemTransport) {''')
    s = in_func('NewInmemTransportWithWorld', 'w')(s)
    return s.replace('''		consumerCh: dst.MakeChan[RPC](w, 16),''', '''		w:          w,
		consumerCh: dst.MakeChan[RPC](w, 16),''')
edit('inmem_transport.go', inmem)

def nettrans(s):
    i, j = func_range(s, 'NewNetworkTransportWithConfig')
    b = s[i:j].replace('__w', 'w').replace('''	trans := &NetworkTransport{''', '''	w := config.World
	if w == nil {
		w = theRealWorld()
	}
	trans := &NetworkTransport{
		w:                       w,''', 1)
    return s[:i] + b + s[j:]
edit('net_transport.go', nettrans)

# Metrics, the restore monitor, FSM restore: a World parameter.
def logm(s):
    s = s.replace('func emitLogStoreMetrics(s LogStore,', 'func emitLogStoreMetrics(w *dst.World, s LogStore,')
    return in_func('emitLogStoreMetrics', 'w')(s)
edit('log.go', logm)
edit('raft.go', lambda s: s.replace('emitLogStoreMetrics(goArg', 'emitLogStoreMetrics(r.w, goArg')
     .replace('startSnapshotRestoreMonitor(r.logger,', 'startSnapshotRestoreMonitor(r.w, r.logger,'))
edit('log_test.go', lambda s: s.replace('emitLogStoreMetrics(s, ', 'emitLogStoreMetrics(theRealWorld(), s, '))

def sat(s):
    s = s.replace('func newSaturationMetric(name []string,', 'func newSaturationMetric(w *dst.World, name []string,')
    s = in_func('newSaturationMetric', 'w')(s)
    return s.replace('''		lastReport:     w.Now(),''', '''		w:              w,
		lastReport:     w.Now(),''')
edit('saturation.go', sat)
edit('saturation_test.go', lambda s: s.replace('newSaturationMetric([]string', 'newSaturationMetric(theRealWorld(), []string'))

def prog(s):
    s = re.sub(r'func startSnapshotRestoreMonitor\(\n', 'func startSnapshotRestoreMonitor(\n\tw *dst.World,\n', s)
    s = in_func('startSnapshotRestoreMonitor', 'w')(s)
    return s.replace('''		doneCh:          dst.MakeChan[struct{}](w, 0),''', '''		w:               w,
		doneCh:          dst.MakeChan[struct{}](w, 0),''')
edit('progress.go', prog)

def fsm(s):
    s = s.replace('func fsmRestoreAndMeasure(logger hclog.Logger,', 'func fsmRestoreAndMeasure(w *dst.World, logger hclog.Logger,')
    s = in_func('fsmRestoreAndMeasure', 'w')(s)
    return (s.replace('startSnapshotRestoreMonitor(logger,', 'startSnapshotRestoreMonitor(w, logger,')
             .replace('fsmRestoreAndMeasure(snapLogger, r.fsm,', 'fsmRestoreAndMeasure(r.w, snapLogger, r.fsm,')
             .replace('newSaturationMetric([]string{"raft", "thread", "fsm"', 'newSaturationMetric(r.w, []string{"raft", "thread", "fsm"'))
edit('fsm.go', fsm)
edit('api.go', lambda s: s.replace('fsmRestoreAndMeasure(snapLogger, r.fsm,', 'fsmRestoreAndMeasure(r.w, snapLogger, r.fsm,')
     .replace('newSaturationMetric([]string{"raft", "thread", "main"', 'newSaturationMetric(w, []string{"raft", "thread", "main"')
     .replace('startSnapshotRestoreMonitor(snapLogger, crc,', 'startSnapshotRestoreMonitor(conf.worldOrReal(), snapLogger, crc,'))
edit('config.go', lambda s: s + '''
// worldOrReal is the config's World, or the real one.
func (c *Config) worldOrReal() *dst.World {
	if c.World != nil {
		return c.World
	}
	return theRealWorld()
}
''')

# Snapshot stores: a World for the snapshot's timestamp (real by default).
def snapname(s):
    s = s.replace('func snapshotName(term, index uint64) string {', 'func snapshotName(w *dst.World, term, index uint64) string {')
    s = in_func('snapshotName', 'w')(s)
    s = s.replace('	name := snapshotName(term, index)', '	name := snapshotName(f.world(), term, index)')
    i = s.index('type FileSnapshotStore struct {'); j = s.index('\n}\n', i)
    s = s[:i] + s[i:j] + '\n\n\tw *dst.World // nil: the real World' + s[j:]
    return imports(s, 'github.com/prince776/go-dst-kit/dst') + '''
func (f *FileSnapshotStore) world() *dst.World {
	if f.w == nil {
		return theRealWorld()
	}
	return f.w
}
'''
edit('file_snapshot.go', snapname)

# FileSnapshotStore on a World's filesystem (phase 1 item 3). dstmigrate
# rewrote its os calls to f.world().FS(); the constructor (no receiver)
# takes the World explicitly. With none it is the real World rooted at
# the filesystem's root, so base may be any ordinary path, as before, and
# the store recreates its directory if it is removed (as upstream does).
def filesnapworld(s):
    sig = 'func NewFileSnapshotStoreWithLogger(base string, retain int, logger hclog.Logger) (*FileSnapshotStore, error) {'
    i = s.index(sig); j = s.index('\n}\n', i)
    body = s[i:j]
    body = body.replace(sig, """func NewFileSnapshotStoreWithLogger(base string, retain int, logger hclog.Logger) (*FileSnapshotStore, error) {
	w, rel, err := realWorldAt(base)
	if err != nil {
		return nil, fmt.Errorf("snapshot path not accessible: %v", err)
	}
	return NewFileSnapshotStoreWithWorld(w, rel, retain, logger)
}

// NewFileSnapshotStoreWithWorld is NewFileSnapshotStoreWithLogger keeping
// the snapshots on w's filesystem, under base (relative to its root).
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
    s = imports(s, 'github.com/prince776/go-dst-kit/realworld')
    return s + """
// realWorldAt returns the real World rooted at the filesystem's root and
// dir's path in it. dir is created if missing and its symbolic links
// resolved (a World's root refuses absolute ones, such as macOS's /var).
func realWorldAt(dir string) (*dst.World, string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil { //dst:ok the real World's root
		return nil, "", err
	}
	abs, err := filepath.Abs(dir)
	if err == nil {
		abs, err = filepath.EvalSymlinks(abs)
	}
	if err != nil {
		return nil, "", err
	}
	root := filepath.VolumeName(abs) + string(filepath.Separator)
	rel, err := filepath.Rel(root, abs)
	if err != nil {
		return nil, "", err
	}
	return realworld.New(realworld.Options{Dir: root}), rel, nil
}
"""
edit('file_snapshot.go', filesnapworld)
def inmemsnap(s):
    s = s.replace('	name := snapshotName(term, index)', '	name := snapshotName(m.world(), term, index)')
    i = s.index('type InmemSnapshotStore struct {'); j = s.index('\n}\n', i)
    s = s[:i] + s[i:j] + '\n\n\tw *dst.World // nil: the real World' + s[j:]
    return imports(s, 'github.com/prince776/go-dst-kit/dst') + '''
func (m *InmemSnapshotStore) world() *dst.World {
	if m.w == nil {
		return theRealWorld()
	}
	return m.w
}
'''
edit('inmem_snapshot.go', inmemsnap)

# The test cluster runs in the real World.
def cluster(s):
    s = in_func('makeCluster', 'w')(s)
    return s.replace('''	c := &cluster{
		observationCh:''', '''	w := theRealWorld()
	c := &cluster{
		w:             w,
		observationCh:''', 1)
edit('testing.go', cluster)

# Tests that build transports by struct literal: the real World.
edit('inmem_transport_test.go', lambda s: s.replace('''	t1 := &InmemTransport{
''', '''	t1 := &InmemTransport{
		w: theRealWorld(),
''').replace('''	t2 := &InmemTransport{
''', '''	t2 := &InmemTransport{
		w: theRealWorld(),
'''))

# More test literals of types holding a World.
edit('net_transport_test.go', lambda s: s.replace('''	transport := NetworkTransport{
''', '''	transport := NetworkTransport{
		w:          theRealWorld(),
'''))
edit('raft_test.go', lambda s: s.replace('Raft{leaderState: leaderState{}', 'Raft{raftState: raftState{w: theRealWorld()}, leaderState: leaderState{}')
     .replace('&followerReplication{nextIndex: idx}', '&followerReplication{w: theRealWorld(), nextIndex: idx}')
     .replace('&followerReplication{}, stopCh', '&followerReplication{w: theRealWorld()}, stopCh')
     .replace('''	r := &Raft{
		trans:  transport,''', '''	r := &Raft{
		raftState: raftState{w: theRealWorld()},
		trans:     transport,'''))

# The leader's per-follower replication state runs in the leader's World.
edit('raft.go', lambda s: s.replace('''			s = &followerReplication{
''', '''			s = &followerReplication{
				w:                   r.w,
''', 1))

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
	if bug("quorum-off-by-one") {
		quorumMatchIndex = matched[len(matched)/2] // before 163a55f: half of an even cluster
	}

	if quorumMatchIndex > c.commitIndex && (quorumMatchIndex >= c.startIndex || bug("commit-previous-term")) {''', 1))
edit('raft.go', in_func('func (r *Raft) requestVote(', 'if lastVoteTerm == req.Term && lastVoteCandBytes != nil {',
     'if lastVoteTerm == req.Term && lastVoteCandBytes != nil && !bug("vote-twice") {'))
for head in ('func (r *Raft) requestVote(', 'func (r *Raft) requestPreVote('):
    edit('raft.go', in_func(head, 'if lastTerm > req.LastLogTerm {', 'if lastTerm > req.LastLogTerm && !bug("vote-ignores-log") {'))
    edit('raft.go', in_func(head, 'if lastTerm == req.LastLogTerm && lastIdx > req.LastLogIndex {',
         'if lastTerm == req.LastLogTerm && lastIdx > req.LastLogIndex && !bug("vote-ignores-log") {'))
edit('raft.go', in_func('func (r *Raft) appendEntries(', 'if a.Term < r.getCurrentTerm() {',
     'if a.Term < r.getCurrentTerm() && !bug("accept-stale-leader") {'))
edit('raft.go', in_func('func (r *Raft) appendEntries(', 'if a.PrevLogTerm != prevLogTerm {',
     'if a.PrevLogTerm != prevLogTerm && !bug("skip-prev-log-term") {'))
edit('snapshot.go', lambda s: s.replace('''	maxLog := min(snapIdx, lastLogIdx-trailingLogs)
''', '''	maxLog := min(snapIdx, lastLogIdx-trailingLogs)
	if bug("compact-past-snapshot") {
		maxLog = lastLogIdx - trailingLogs // deletes entries the snapshot doesn't hold
	}
''', 1))

# Membership bug switches (dstbench.json).
edit('raft.go', lambda s: s.replace('''					if hasVote(r.configurations.latest, r.localID) {''',
    '''					if hasVote(r.configurations.latest, r.localID) || (bug("nonvoter-can-lead") && inConfiguration(r.configurations.latest, r.localID)) {''', 1))
for head in ('func (r *Raft) requestVote(', 'func (r *Raft) requestPreVote('):
    edit('raft.go', in_func(head, 'if len(r.configurations.latest.Servers) > 0 && !hasVote(r.configurations.latest, candidateID) {',
         'if len(r.configurations.latest.Servers) > 0 && !hasVote(r.configurations.latest, candidateID) && !bug("nonvoter-can-lead") {'))
edit('raft.go', in_func('func (r *Raft) checkLeaderLease(', 'if server.Suffrage == Voter {',
     'if server.Suffrage == Voter || bug("lease-counts-nonvoters") {'))
edit('raft.go', in_func('func (r *Raft) configurationChangeChIfStable(', '''	if r.configurations.latestIndex == r.configurations.committedIndex &&
		r.getCommitIndex() >= r.leaderState.commitment.startIndex {''', '''	if (r.configurations.latestIndex == r.configurations.committedIndex || bug("two-changes-in-flight")) &&
		(r.getCommitIndex() >= r.leaderState.commitment.startIndex || bug("config-change-before-noop")) {'''))

# The candidate checks of 6b4e320 and the pre-vote code (a receiver
# rejects a candidate missing from, or not a voter in, its own latest
# configuration), off: raft before 6b4e320 (upstream #524's problem).
for head in ('func (r *Raft) requestVote(', 'func (r *Raft) requestPreVote('):
    edit('raft.go', in_func(head, '!inConfiguration(r.configurations.latest, candidateID) {',
         '!inConfiguration(r.configurations.latest, candidateID) && !bug("no-candidate-config-check") {'))
    edit('raft.go', in_func(head, '&& !bug("nonvoter-can-lead") {',
         '&& !bug("nonvoter-can-lead") && !bug("no-candidate-config-check") {'))

# Experiment: the candidate checks only when the candidate isn't ahead
# (dstfix.go; UPSTREAM-ISSUE-DRAFT.md's proposed fix).
for head in ('func (r *Raft) requestVote(', 'func (r *Raft) requestPreVote('):
    edit('raft.go', in_func(head, '&& !bug("no-candidate-config-check") {',
         '&& !bug("no-candidate-config-check") && !(bug("candidate-check-unless-ahead") && candidateAhead(r, req.LastLogTerm, req.LastLogIndex)) {', 2))

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
	if !bug("bootstrap-log-first") {
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
	if bug("bootstrap-log-first") {
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
		} else if bug("snapshot-boundary-fix") && a.PrevLogEntry <= snapIdx {
			prevLogTerm = a.PrevLogTerm // committed: the leader's entry is ours
		} else {'''))
edit('raft.go', in_func('func (r *Raft) installSnapshot(', '''		reqConfigurationIndex = req.LastLogIndex
	}
	version := getSnapshotVersion(r.protocolVersion)''', '''		reqConfigurationIndex = req.LastLogIndex
	}
	if bug("snapshot-boundary-fix") && r.holdsEntry(req.LastLogIndex, req.LastLogTerm) {
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
			if bug("snapshot-boundary-fix") && entry.Index <= snapshotIdx {
				continue // committed, and in our snapshot
			}
			if entry.Index > lastLogIdx {'''))

# Bug for the FSM's index check (dst_kv_test.go): an entry handed to the
# FSM twice. With clients' retries deduplicated by ID, only that check sees it.
edit('raft.go', in_func('func (r *Raft) processLogs(', '''			batch = append(batch, preparedLog)

			// If we have filled up a batch, send it to the FSM''', '''			batch = append(batch, preparedLog)
			if bug("apply-entry-twice") && idx%50 == 0 {
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
		if bug("snapshot-fallback-needs-log") && !r.logFollows(snapshot.Index) {
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
	if bug("snapshot-fallback-needs-log") && !r.logFollows(snapshotIndex) {
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
	if bug("forget-term-on-restart") {
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
		if heartbeat && bug("heartbeat-on-main-thread") {
			// Changing the term or the state is the main loop's job.
			forwarded = true
			r.rpcCh.Send(rpc)
			return
		}
		// Ensure transition to follower'''))

# Phase 2 step 2's plant: a follower ignores the leader's commit index, so
# it never applies (caught only by "every member catches up").
edit('raft.go', in_func('func (r *Raft) appendEntriesOn(', 'if a.LeaderCommitIndex > 0 && a.LeaderCommitIndex > r.getCommitIndex() {',
     'if a.LeaderCommitIndex > 0 && a.LeaderCommitIndex > r.getCommitIndex() && !bug("follower-ignores-commit") {'))

# newSeed (only upstream's util_test calls it) stays on crypto/rand:
# dstmigrate now maps crypto/rand.Reader to the World's, and no World is
# in scope there.
edit('util.go', lambda s: s.replace('crand.Int(dst.CryptoReader(__w), big.NewInt(math.MaxInt64))', 'crand.Int(crand.Reader, big.NewInt(math.MaxInt64))', 1))
