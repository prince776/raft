package raft

// The simulated cluster for deterministic simulation tests (go-dst-kit):
// machines, their stores, raft, and how a node boots. The workloads (the
// key-value service, dst_kv_test.go; the membership operator,
// dst_operator_test.go) and the checks (dst_checks_test.go) are composed
// onto it by newCluster; the sweeps are in dst_test.go.
//
// Each server is a real raft node on its own simulated machine, talking
// through the real NetworkTransport over the simulated network
// (dst_stream_test.go), keeping its log and stable state in a file on the
// machine's crash-prone simulated disk (dstLogStore). Snapshots live in
// memory outside the simulation (for now) and survive crashes whole.

import (
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
)

// clusterSpec says what to simulate.
type clusterSpec struct {
	voters   int // servers bootstrapped as voters
	spares   int // machines that start empty, outside the cluster, for the operator to add
	scenario dstScenario
	// operator runs membership changes (dst_operator_test.go), and takes
	// tickets for machines the runner loses for good (Faults.Loss).
	operator bool
	// defaultTimeouts runs raft with its own default timeouts (heartbeat
	// and election 1s, lease 500ms, commit 50ms) instead of a fifth of
	// them. Raft needs round trips well under its election timeout; slow
	// links (Faults.Gray) can take longer than a fifth of the defaults.
	defaultTimeouts bool
	// drawConfig draws raft's configuration per seed (raftKnobs), the
	// same on every node; otherwise it is dstConfig's, fixed.
	drawConfig bool
	// preVoteOff: elections without the pre-vote round (fixed
	// configuration; a drawn one draws it).
	preVoteOff bool
	// noTransfers: no leadership transfers even when the seed draws them
	// (scripts that need the leader to stay put).
	noTransfers bool
	// oneSnapshot keeps one snapshot (FileSnapshotStore's retain), so a
	// node never falls back to an older one at startup (DST.md 6.12).
	oneSnapshot bool
	// clients is the number of clients on their own machines, reaching
	// the cluster through each node's server (dst_service_test.go).
	clients int
	// forceTrailing, with drawConfig, fixes TrailingLogs to
	// forceTrailingLogs (experiments).
	forceTrailing     bool
	forceTrailingLogs uint64
}

// raftKnobs is the part of raft's configuration a seed draws: options an
// operator may set, each changing which code paths run.
type raftKnobs struct {
	// Timeouts: raft's defaults (heartbeat 1s, election 1s, lease 500ms,
	// commit 50ms) times scale; with defaultTimeouts (gray failure), scale
	// is 1 or 2 and the lease a full heartbeat. The
	// election timeout is the heartbeat timeout times electionX (raft
	// requires at least 1), the lease the heartbeat timeout times leaseX
	// (at most 1).
	scale, electionX, leaseX float64
	preVoteDisabled          bool   // elections without the pre-vote round
	maxAppend                int    // MaxAppendEntries: entries per AppendEntries (raft's default 64)
	trailingLogs             uint64 // entries kept after a snapshot: fewer means more snapshot installs
	threshold                uint64 // SnapshotThreshold: entries between snapshots
	retain                   int    // snapshots FileSnapshotStore keeps (Consul: 2)
	batchApply               bool   // BatchApplyCh
}

func (k raftKnobs) String() string {
	return fmt.Sprintf("timeouts x%.2f (election x%.1f, lease x%.2f), pre-vote off %v, max append %d, trailing logs %d, snapshot threshold %d, batch apply %v, snapshots retained %d",
		k.scale, k.electionX, k.leaseX, k.preVoteDisabled, k.maxAppend, k.trailingLogs, k.threshold, k.batchApply, k.retain)
}

// drawKnobs draws a configuration from its own stream (so the rest of the
// run's randomness is unchanged by it).
func drawKnobs(h *dsttest.Harness, defaultTimeouts bool) raftKnobs {
	r := h.World().Rand().Stream("raft config")
	pick := func(xs ...float64) float64 { return xs[r.IntN(len(xs))] }
	k := raftKnobs{
		scale:           pick(0.2, 0.2, 0.5, 1),
		electionX:       pick(1, 1, 2),
		leaseX:          pick(0.5, 0.5, 1),
		preVoteDisabled: r.IntN(3) == 0,
		maxAppend:       []int{64, 64, 1, 4}[r.IntN(4)],
		// TrailingLogs 0 exposes a raft bug (DST.md 6.11), so the
		// sweeps leave it to TestDSTSnapshotBoundary.
		trailingLogs: []uint64{32, 4}[r.IntN(2)],
		threshold:    []uint64{64, 16}[r.IntN(2)],
		batchApply:   r.IntN(2) == 0,
		retain:       1 + r.IntN(2),
	}
	if defaultTimeouts {
		// Gray failure: slow links add up to ~450ms each way, and raft's
		// timeouts must outlast a quorum round trip (a 500ms lease, raft's
		// default, made every leader step down within it on some seeds:
		// configuration, not a bug). Configure it as for a slow network:
		// raft's defaults or twice them, the lease a full heartbeat.
		k.scale = pick(1, 2)
		k.leaseX = 1
	}
	return k
}

func (k raftKnobs) apply(c *Config) {
	d := DefaultConfig()
	hb := time.Duration(float64(d.HeartbeatTimeout) * k.scale)
	c.HeartbeatTimeout = hb
	c.ElectionTimeout = time.Duration(float64(hb) * k.electionX)
	c.LeaderLeaseTimeout = time.Duration(float64(hb) * k.leaseX)
	c.CommitTimeout = time.Duration(float64(d.CommitTimeout) * k.scale)
	c.PreVoteDisabled = k.preVoteDisabled
	c.MaxAppendEntries = k.maxAppend
	c.TrailingLogs = k.trailingLogs
	c.SnapshotThreshold = k.threshold
	c.BatchApplyCh = k.batchApply
}

// dstScenario switches weaker modes on, each to show a check catches what
// it breaks: the store's (noSync, splitBatches: see dstLogStore), and
// naiveReadIndex, a client serving ReadIndex reads without first committing
// an entry in the leader's term.
type dstScenario struct{ noSync, splitBatches, naiveReadIndex bool }

// simCluster is a simulated cluster: its nodes, and what runs on them.
type simCluster struct {
	h     *dsttest.Harness
	spec  clusterSpec
	nodes []*dstNode // the bootstrapped voters first, then the spares
	kv    *kvWorkload
	op    *operator  // nil unless spec.operator
	knobs *raftKnobs // the drawn configuration (nil unless spec.drawConfig)
	inv   *invariants
	// transfer: the leader hands leadership on now and then (a per-seed
	// feature, dst_transfer_test.go).
	transfer bool
}

// dstNode is one machine. Its log and stable store live in a file on its
// disk (what a crash keeps is what was fsynced); its raft, FSM and
// workloads are rebuilt at each boot.
type dstNode struct {
	name    string
	initial bool // bootstrapped as a voter (else a spare)
	logs    *dstLogStore
	snaps   *dstSnapshots
	fsm     *kvFSM
	raft    *Raft
	m       *dsttest.Machine
	// wipeOnBoot: the next boot starts from an empty disk under the same
	// identity. Outside raft's fault model (a node forgetting what it
	// acknowledged); only TestDSTQuorumOffByOneScripted uses it.
	wipeOnBoot bool
}

func (nd *dstNode) id() ServerID        { return ServerID(nd.name) }
func (nd *dstNode) addr() ServerAddress { return ServerAddress(nd.name + ":7000") }

// up reports whether the node's raft is running now.
func (nd *dstNode) up() bool { return nd.raft != nil && !nd.m.Dead() }

func dstConfig(w *dst.World, name string) *Config {
	c := DefaultConfig()
	c.LocalID = ServerID(name)
	c.World = w
	c.Logger = hclog.NewNullLogger()
	if os.Getenv("DST_RAFT_LOG") != "" { // debugging one replay: raft's own log
		c.Logger = hclog.New(&hclog.LoggerOptions{Name: name, Level: hclog.Debug, Output: os.Stderr})
	}
	c.HeartbeatTimeout = 200 * time.Millisecond
	c.ElectionTimeout = 200 * time.Millisecond
	c.LeaderLeaseTimeout = 100 * time.Millisecond
	c.CommitTimeout = 10 * time.Millisecond
	// Snapshot often, so runs reach compaction and snapshot installs.
	c.SnapshotThreshold = 64
	c.SnapshotInterval = 500 * time.Millisecond
	c.TrailingLogs = 32
	return c
}

// newCluster builds the cluster spec describes on h, with its workloads
// and checks.
func newCluster(h *dsttest.Harness, spec clusterSpec) *simCluster {
	c := &simCluster{h: h, spec: spec}
	if spec.drawConfig {
		k := drawKnobs(h, spec.defaultTimeouts)
		if spec.forceTrailing {
			k.trailingLogs = spec.forceTrailingLogs
		}
		c.knobs = &k
		if k.preVoteDisabled {
			h.Sometimes("raft config: pre-vote off")
		}
		if k.maxAppend < 64 {
			h.Sometimes("raft config: small append batches")
		}
		if k.trailingLogs < 32 {
			h.Sometimes("raft config: few trailing logs")
		}
	}
	if spec.drawConfig {
		h.Expect("raft config: pre-vote off", "raft config: small append batches", "raft config: few trailing logs")
	}
	var initial []Server
	for i := range spec.voters + spec.spares {
		nd := &dstNode{name: fmt.Sprintf("n%d", i), initial: i < spec.voters}
		c.nodes = append(c.nodes, nd)
		if nd.initial {
			initial = append(initial, Server{ID: nd.id(), Address: nd.addr()})
		}
	}
	c.kv = newKVWorkload(c)
	c.transfer = h.Feature("leadership transfers") && !spec.noTransfers
	if spec.operator {
		c.op = newOperator(c, initial)
	}
	for _, nd := range c.nodes {
		nd.m = h.Machine(nd.name, func(w *dst.World) { c.boot(nd, w, initial) })
	}
	for i := range spec.clients {
		h.Machine(fmt.Sprintf("c%d", i), func(w *dst.World) { c.kv.remoteClient(w, i) }, dsttest.NeverCrash)
	}
	if spec.clients > 0 {
		h.Expect("a remote client's operation succeeded", "a remote client followed a redirect", "a remote client retried")
	}
	// raft panics when it can't persist its term or vote (fail-stop): with
	// disk errors that is the process dying, restarted by its supervisor.
	// Only disk-error panics: any other panic is still a failure.
	h.OnPanic(func(v any) bool { return diskError(fmt.Errorf("%v", v)) })
	c.inv = newInvariants()
	c.checks()
	c.invariantChecks()
	return c
}

// boot starts a node: its store, transport and raft (bootstrapping the
// initial voters the first time), then its workloads. It returns when the
// fault phase ends; raft keeps running.
func (c *simCluster) boot(nd *dstNode, w *dst.World, initial []Server) {
	h := c.h
	if nd.wipeOnBoot {
		nd.wipeOnBoot = false
		if err := w.FS().Remove("raft.log"); err != nil {
			h.Fail(err)
			return
		}
		if err := w.FS().RemoveAll("snap"); err != nil {
			h.Fail(err)
			return
		}
		h.Sometimes("a node lost its disk")
		c.inv.forget(nd.name)
	}
	// Startup steps that hit the disk retry on a disk error (Faults.
	// DiskErrors: a full disk or EIO passes), as a supervised service does.
	retry := func(what string, fn func() error) bool {
		for {
			err := fn()
			if err == nil {
				return true
			}
			if !diskError(err) {
				h.Fail(fmt.Errorf("%s: %v", what, err))
				return false
			}
			h.Sometimes("a node's startup failed on the disk")
			w.Sleep(50 * time.Millisecond)
		}
	}
	var logs *dstLogStore
	if !retry("open the log store", func() (err error) { logs, err = openDSTLogStore(w.FS(), "raft.log"); return err }) {
		return
	}
	logs.onStore = func(s *dstLogStore, l []*Log) { c.inv.stored(nd.name, s, l) }
	logs.noSync, logs.splitBatches = c.spec.scenario.noSync, c.spec.scenario.splitBatches
	nd.logs = logs
	stream, err := newDSTStream(w, string(nd.addr()))
	if err != nil {
		h.Fail(err)
		return
	}
	conf := dstConfig(w, nd.name)
	if c.spec.preVoteOff {
		conf.PreVoteDisabled = true
	}
	if c.spec.defaultTimeouts {
		d := DefaultConfig()
		conf.HeartbeatTimeout, conf.ElectionTimeout, conf.LeaderLeaseTimeout, conf.CommitTimeout = d.HeartbeatTimeout, d.ElectionTimeout, d.LeaderLeaseTimeout, d.CommitTimeout
	}
	// The transport's I/O timeout: 500ms for the fixed configuration (the
	// seeds its tests assert on depend on it); with a drawn configuration,
	// ten heartbeat timeouts (Consul uses 10s with raft's 1s defaults). A
	// timeout shorter than a slow, lossy link's round trip strands the
	// follower behind it for good: every RPC to it times out, including
	// its rejections, so the leader never backs off to catch it up.
	transTimeout := 500 * time.Millisecond
	if c.knobs != nil {
		c.knobs.apply(conf)
		transTimeout = 10 * conf.HeartbeatTimeout
	}
	trans := NewNetworkTransportWithConfig(&NetworkTransportConfig{Stream: stream, MaxPool: 3, Timeout: transTimeout,
		Logger: hclog.NewNullLogger(), World: w})
	retain := 1
	if c.knobs != nil && !c.spec.oneSnapshot {
		retain = c.knobs.retain
	}
	if !retry("open the snapshot store", func() (err error) { nd.snaps, err = openDSTSnapshots(w, retain, conf.Logger); return err }) {
		return
	}
	if nd.initial {
		bootstrap := func() error {
			if ok, _ := HasExistingState(nd.logs, nd.logs, nd.snaps); ok {
				return nil
			}
			return BootstrapCluster(conf, nd.logs, nd.logs, nd.snaps, trans, Configuration{Servers: initial})
		}
		if !retry("bootstrap", bootstrap) {
			return
		}
	}
	nd.fsm = newKVFSM(h)
	// Restoring a snapshot can fail on a read error; raft then reports
	// "failed to load any existing snapshots" without the cause, so that
	// message retries too (a snapshot that is really corrupt never loads,
	// and the run fails its liveness checks instead).
	conf.dstOnVote = func(term uint64, cand []byte) { c.inv.voted(nd.name, term, cand) }
	var r *Raft
	if !retry("start raft", func() (err error) {
		r, err = NewRaft(conf, nd.fsm, nd.logs, nd.logs, nd.snaps, trans)
		if err != nil && (strings.Contains(err.Error(), "failed to load any existing snapshots") ||
			strings.Contains(err.Error(), "a later snapshot couldn't be read")) && c.h.Swarm().ReadErrProb > 0 {
			return fmt.Errorf("%w (%v)", syscall.EIO, err)
		}
		return err
	}) {
		return
	}
	nd.raft = r
	if c.op != nil {
		w.Go(func() { c.op.run(w, nd) })
	}
	if c.transfer {
		w.Go(func() { c.transfers(w, nd) })
	}
	if c.spec.clients > 0 {
		kc := &kvClient{h: h, w: w, r: r, fsm: nd.fsm, naive: c.spec.scenario.naiveReadIndex}
		w.Go(func() { c.serve(w, nd, kc) })
	}
	c.kv.run(w, nd)
}

// node is the node with id, or nil.
func (c *simCluster) node(id ServerID) *dstNode {
	for _, nd := range c.nodes {
		if nd.id() == id {
			return nd
		}
	}
	return nil
}

// leader is a running node that believes it leads, the one with the
// highest term if several do (a deposed one may not know yet), or nil.
func (c *simCluster) leader() *dstNode {
	var best *dstNode
	for _, nd := range c.nodes {
		if nd.up() && nd.raft.getState() == Leader && (best == nil || nd.raft.getCurrentTerm() > best.raft.getCurrentTerm()) {
			best = nd
		}
	}
	return best
}

// dstCluster and dstClusterWith are fixed clusters: n voters, no spares,
// no membership changes, dstConfig's configuration. sweepCluster is the
// same with the configuration drawn per seed, for the sweeps.
func dstCluster(h *dsttest.Harness, n int) []*dstNode { return dstClusterWith(h, n, dstScenario{}) }

func sweepCluster(h *dsttest.Harness, n int) *simCluster {
	return newCluster(h, clusterSpec{voters: n, drawConfig: true, clients: 2})
}

func dstClusterWith(h *dsttest.Harness, n int, sc dstScenario) []*dstNode {
	return newCluster(h, clusterSpec{voters: n, scenario: sc}).nodes
}

// patience is how long the liveness checks wait for the cluster: ten
// election timeouts, at least 5s. A cluster whose timeouts are drawn long
// (up to twice raft's defaults) needs seconds for one failed election.
func (c *simCluster) patience() time.Duration {
	if c.knobs == nil {
		return 5 * time.Second
	}
	k := c.knobs
	election := time.Duration(float64(DefaultConfig().HeartbeatTimeout) * k.scale * k.electionX)
	if p := 10 * election; p > 5*time.Second { // raft's max shadows the builtin
		return p
	}
	return 5 * time.Second
}

// dstSnapshots is raft's FileSnapshotStore on the node's simulated disk
// (in "snap"), so crashes and disk faults reach snapshots as they reach
// the log. It records the index of the latest snapshot it holds, for
// checks between scheduling steps, which can't use the World: found on
// disk at boot, raised when a sink's Close succeeds (snapshots taken and
// installed alike). Between a crash and the next boot it may over-report
// what survived, which can only hide a violation, never invent one.
type dstSnapshots struct {
	*FileSnapshotStore
	latest uint64
}

func openDSTSnapshots(w *dst.World, retain int, logger hclog.Logger) (*dstSnapshots, error) {
	fss, err := NewFileSnapshotStoreWithWorld(w, "snap", retain, logger.Named("snapshot"))
	if err != nil {
		return nil, err
	}
	s := &dstSnapshots{FileSnapshotStore: fss}
	if list, err := fss.List(); err == nil && len(list) > 0 {
		s.latest = list[0].Index
	}
	return s, nil
}

func (s *dstSnapshots) Create(version SnapshotVersion, index, term uint64, configuration Configuration,
	configurationIndex uint64, trans Transport) (SnapshotSink, error) {
	sink, err := s.FileSnapshotStore.Create(version, index, term, configuration, configurationIndex, trans)
	if err != nil {
		return nil, err
	}
	return &dstSnapshotSink{SnapshotSink: sink, s: s, index: index}, nil
}

type dstSnapshotSink struct {
	SnapshotSink
	s     *dstSnapshots
	index uint64
}

func (k *dstSnapshotSink) Close() error {
	err := k.SnapshotSink.Close()
	if err == nil && k.index > k.s.latest {
		k.s.latest = k.index
	}
	return err
}

// covers reports whether the node's latest snapshot includes index.
func (s *dstSnapshots) covers(index uint64) bool {
	return s != nil && s.latest >= index
}
