# Deterministic simulation testing of hashicorp/raft

This fork (`prince776/raft`, branch `dst`) runs hashicorp/raft under
[go-dst-kit](https://github.com/prince776/go-dst-kit): every goroutine,
channel, select, timer, clock read, lock, random draw, file and socket goes
through a `*dst.World`, real in production and simulated in tests, so a
whole cluster runs in one process, deterministically, under injected
faults, and any failing run replays exactly from its seed.

Status as of 2026-11-27. Phase 1 (the simulated world and its faults) is
in progress: items 1 and 2 of 7 are done. Phases 2 to 4 (better tests, then plain
exploration, then guided exploration) haven't started.

## Contents

1. [How raft was adapted](#1-how-raft-was-adapted)
2. [The simulated world](#2-the-simulated-world)
3. [The workloads](#3-the-workloads)
4. [The checks](#4-the-checks)
5. [The tests](#5-the-tests)
6. [Findings](#6-findings)
7. [The bug benchmark](#7-the-bug-benchmark)
8. [Guided exploration so far](#8-guided-exploration-so-far)
9. [Known weaknesses](#9-known-weaknesses)
10. [Plan and open decisions](#10-plan-and-open-decisions)
11. [How to run](#11-how-to-run)

## 1. How raft was adapted

- `Config.World` (nil: the real World) chooses the World raft runs in;
  `NetworkTransportConfig.World` does the same for the network transport.
  Public channel-typed APIs became `dst.Chan`.
- Most of the rewrite was done by go-dst-kit's `dstmigrate` (about 120
  channel sites and 66 selects); the rest is hand edits in
  `.dstmigrate/manual.py` (which World a constructor or helper uses, the
  notify helpers, the bug switches). `.dstmigrate/run.sh` re-runs the whole
  migration from the base commit `f9cf2a8`. Adapting raft found 11 gaps in
  the tool, each fixed and tested in go-dst-kit.
- Raft ranges over maps while starting goroutines and sending, and Go
  randomizes map order: `dst_maporder.go` keeps Go's order in the real
  World and, in a simulated one, shuffles sorted keys from the World's
  randomness (deterministic, and orders still explored).
- Raft's own test suite passes on the real World (about 130s).
  `TestRaft_ProtocolVersion_Upgrade_2_3` fails about 1 run in 20, on the
  unmigrated upstream code (`dd4346d`) as well.

## 2. The simulated world

A run is one seed; everything below is drawn from it.

**The cluster.** 1 to 7 real raft nodes, each on its own simulated
machine with its own disk, using raft's real `NetworkTransport` over the
simulated network's TCP-like streams (`dst_stream_test.go`). Each node
keeps its log, term and vote in `dstLogStore` (`dst_logstore_test.go`), a
file on its disk. Snapshots are an in-memory store outside the simulation:
they survive everything (not realistic yet; phase 1 item 3). Raft's
timeouts are its defaults divided by 5 (heartbeat and election 200ms,
lease 100ms, commit 10ms), to fit more leader changes into a 10s fault
phase; drawing them per seed is planned. A run has a 10s fault phase and a
10s recovery phase without faults.

**The faults** (rates per seed; go-dst-kit's swarm):

| Class | What happens |
|---|---|
| Crash and restart | A machine loses power; unsynced writes are lost; it restarts after a random downtime |
| Disk | Torn writes; power loss inside an I/O call; writes reaching the disk out of order (or in order, per seed). Stored data is never damaged afterwards |
| Partition | Machines (the test machine included) shuffled and split at a random point, both ways or one way; heals after a random time. Blocked traffic is held, not dropped, and flows after the heal; raft's 500ms connection deadlines break connections meanwhile |
| Network | Latency: a floor plus an exponential tail capped at 10x its mean; connection resets |
| Pause | A machine freezes (a GC or VM stall), then resumes |
| Clocks | Always on: offset up to ±10ms, rate ±100ppm per machine (go-dst-kit's defaults, not raft's limits); start times near day, month and year boundaries |
| Scheduling | Goroutine interleavings, uniform or PCT |
| Loss | A machine dies for good, with its disk; the test is told (a ticket) and replaces it |
| Gray (new) | Gray failure, shares drawn per seed: slow links (+5ms to 300ms per write), lossy links (1% to 20% of segments lost, each stalling the stream for a retransmission timeout, 200ms doubling to 3.2s), connections reset when a partition heals, slow machines (stalls of 2 to 50ms every 10 to 100ms), slow disks (fsync 10 to 100x slower). Slow links and disks stay slow in the recovery phase; the rest stop |

**The store's contract** (`dstLogStore`). Raft's `LogStore` and
`StableStore` interfaces don't say what a store must guarantee, so we
stated one from the caller's side, then weakened it one guarantee at a
time to learn which raft needs (6.6):

1. Durable on return: once a call returns without error, its effect
   survives any later crash. Raft needs this.
2. A crash during a call keeps a prefix of the call's entries (all, some
   first ones, or none), never a gap. Raft needs no more: keeping a batch
   whole isn't required.
3. Reads see every completed call; a missing key reads as 0 and an empty
   log has first and last index 0 (raft relies on both; its docs don't
   say so).

The store provides these by fsyncing before it returns and writing each
call as one checksummed record (a torn one is cut off on open). The
weaker modes `noSync` and `splitBatches` break 1 and the whole-batch part
of 2.

**Raft's fault model, and what's out of it.** hashicorp/raft assumes
crash-recovery with reliable storage: a node's storage holds everything it
acknowledged, or the node is gone for good (and replaced through a
membership change). Data corruption, and a node coming back empty under
its old identity (amnesia), are outside it; corruption belongs to testing
storage engines (raft-wal), later.

## 3. The workloads

- **The key-value service** (`dst_kv_test.go`): an FSM over keys a, b, c,
  and on each node, while it leads, a client issuing an operation every 5
  to 25ms: puts through the log, gets through the log or by ReadIndex
  (a Barrier once per term, then commit index, VerifyLeader, wait for the
  FSM). The client lives in the server's process (as an application
  embedding raft would call Apply); clients on their own machines, with
  forwarding and retries, are phase 1 item 4.
- **The membership operator** (`dst_operator_test.go`), as autopilot or a
  person: acts while its node leads, one change at a time (sometimes two
  at once, as two operators would); replaces servers the runner loses for
  good; keeps the voter count by adding spares as non-voters and promoting
  them once caught up; random adds, promotions, demotions, removals (half
  the removed servers are decommissioned). It never voluntarily goes below
  3 voters (see 6.5).

## 4. The checks

| Check | When | What it catches |
|---|---|---|
| At most one leader per term | after every step | split brain |
| Only voters are elected | after every step | non-voters leading |
| The cluster recovers | at the end: a leader, and a probe write committing, within 5s | lost liveness |
| The clients' history is linearizable | at the end (go-dst-kit's checker, KV model) | stale reads, lost or reordered writes |
| Acknowledged writes survive | at the end, on members of the final configuration | durability |
| The nodes applied the same sequence | at the end | divergent logs, double applies |
| The members agree on the configuration | at the end, running members | divergent membership |
| Membership changes are linearizable | at the end, against raft's own semantics of each change | lost or reordered changes |

Each check has been shown to fail on a bug meant for it (5).

## 5. The tests

Default build (`go test -run TestDST`):

| Test | Setup | Result |
|---|---|---|
| `TestDSTSmoke` | 3 nodes, scheduling only | 50 seeds pass |
| `TestDSTCrashes` | 3 nodes, crashes and disk | 50 pass; snapshots persisted and restored |
| `TestDSTPartitions` | 3 nodes, partitions and network | 50 pass |
| `TestDSTEverything` | 5 nodes, all of the above and pauses | 50 pass |
| `TestDSTFourNodes` | 4 nodes, crashes, disk, partitions, network | 50 pass |
| `TestDSTMembership` | 1/3/5/7 voters per seed, 3 spares, the operator, crashes, disk, partitions, network, losses | 200 pass |
| `TestDSTGray` | 5 nodes, crashes, disk, partitions, network, pauses, gray failure; raft's default timeouts | 200 pass |
| `TestDSTLogStoreContract` | one store through crashes against a model | 200 pass; catches `noSync` (159/200 seeds) and `splitBatches` (93/200) |
| `TestDSTCatchesUnsyncedStore` | cluster on a store that skips fsync | caught (17/50) |
| `TestDSTCatchesNaiveReadIndex` | ReadIndex without the barrier | caught (3/50) |

With `-tags dstbugs`:

| Test | What it shows |
|---|---|
| `TestDSTChecksCatchBugs` | vote-twice → one leader per term (9/50); skip-prev-log-term → same sequence (1/50); accept-stale-leader → linearizability (5/50) |
| `TestDSTQuorumOffByOneScripted` | quorum-off-by-one loses writes along its one sequence, which needs amnesia: correct raft 20/20 pass, with the bug 10/20 fail |
| `TestDSTStaleConfigStall` | the liveness bug in 6.1: correct raft stalls on 15/20 seeds; without the candidate checks, or with the proposed fix, all 20 recover |
| `TestBootstrapCrash` | the bootstrap crash in 6.2: correct raft can't bootstrap again; with the fix it can |
| `TestDSTMembershipGrayFixed` | the membership sweep under gray failure, with both proposed fixes on: 200 pass (correct raft fails 2 of 200, one per bug) |

## 6. Findings

### 6.1 A live majority that can never elect a leader (liveness bug)

Inside raft's fault model (one partition, one removal, one server lost for
good), a cluster can end with a live majority of its voters and no leader,
for good:

1. Voters {A, B, C}, A leading. C is cut off (by chance; unrelated to
   what follows).
2. Meanwhile A replaces B with a new server D, the usual way: add D as a
   non-voter, promote it once caught up ({A, B, C, D}, committed by A, B
   and D), then remove B ({A, C, D}, committed by A and D). C misses all
   of it.
3. A is lost for good. C and D are two of {A, C, D}'s three voters: D can
   only win with C's vote.
4. C is reconnected; its latest configuration is still {A, B, C}, without
   D.

D, the most up to date, asks C for a pre-vote; C logs `rejecting pre-vote
request since node is not a voter` (D isn't one in C's stale
configuration). C campaigns in its own configuration, where A is gone and
B rejects it (`our last index is greater`). Nobody can win, ever. Plain
Raft has no such check: C would grant D its vote (D's log is ahead), D
would win and catch C up.

The checks (in `requestVote` and `requestPreVote`: reject a candidate
missing from, or not a voter in, the receiver's latest configuration) came
with `6b4e320` (#526, 2022), to stop nodes that missed a configuration
change from disrupting elections (#524). The symptom matches upstream
issue [#472](https://github.com/hashicorp/raft/issues/472) (a cluster stuck
in Candidate, open since 2021, cause unknown).

Evidence: `TestDSTStaleConfigStall` (`dst_bugs_test.go`); correct raft
stalls on 15 of 20 seeds, and with only those checks off (switch
`no-candidate-config-check`) all 20 recover; raft's own log shows the
rejections. It was first seen in `TestDSTMembership`. A draft upstream
issue is in [UPSTREAM-ISSUE-DRAFT.md](UPSTREAM-ISSUE-DRAFT.md) (not posted).

A proposed fix: apply the membership checks only when the candidate's log
isn't ahead of the receiver's (a receiver can trust its configuration to
judge a candidate only then). Switch `candidate-check-unless-ahead`
(`dstfix.go`): the sequence recovers on 20 of 20 seeds, and the membership
sweep still passes 200 seeds. Caveat: the sweep also passes with the checks
removed entirely, so it doesn't exercise the disruption the checks were
added for (#524); that the fix keeps that protection is argued, not
measured. A scenario for #524 (a node that missed configuration changes,
campaigning) belongs to phase 2.

**Upstream-ready:** branch `fix/stale-config-vote` (based on upstream
`main`, `dd4346d`; nothing from the simulation) with the fix and two tests
in raft's style: `TestRaft_StaleVoterCanElectNewerVoter` (the sequence on
`MakeCluster`, with and without pre-vote; fails 3 of 3 before the fix,
passes 20 of 20 after) and
`TestRaft_VoteGranted_WhenCandidateLogAheadOfStaleConfiguration` (direct
RPCs: same log, behind, ahead by index, ahead by term). Upstream's suite
passes. A write-up with diagrams: `docs/dst-bugs.html`.

It also happens without a scripted partition: under gray failure
(the membership sweep under gray failure, seed 175), a voter lagging behind lossy links kept
a configuration without the candidate and rejected it (`node is not in
configuration`) after the leader was lost. Both the fix and the checks
removed recover that seed.

### 6.2 A crash during BootstrapCluster leaves a node that can never lead

`BootstrapCluster` saves term 1, then appends the configuration entry. If
the node crashes between the two, on restart `HasExistingState` sees the
term and says the node has state, so bootstrapping again fails with
`ErrCantBootstrap`; and the node has no configuration, so it never
campaigns. In a multi-voter cluster that heals itself (a leader elected by
the others replicates the log and configuration to it); it is permanent
where the node is needed for a majority, above all a single-voter
cluster, until someone wipes the node's data. The window is short (one
write) and once per node, so low severity.

Found by the gray sweep (seed 54: a slow disk widened the window, and the
cluster's only voter crashed in it). `TestBootstrapCrash` shows it directly
(a store whose StoreLog fails after the term was saved). Upstream-ready:
branch `fix/bootstrap-crash` with `TestBootstrapCluster_CrashBetweenWrites`
(fails before the fix). Fix: write the
configuration entry first, then the term (switch `bootstrap-log-first`): a
crash between them leaves a log whose configuration raft uses on start;
the seed recovers and the test passes. It is in the upstream draft as a
second item.

### 6.3 A demoted leader shuts down

A leader that loses its vote, demoted as well as removed, steps down and,
with `ShutdownOnRemove` (the default), shuts down (`raft.go`,
`leaderLoop`: `!hasVote(...)` sets `stepDown`), logging `removed ourself,
shutting down`. It stays in the configuration as a non-voter that no longer
takes part. A follower that's demoted doesn't. Not a safety problem;
possibly unintended.

### 6.4 quorum-off-by-one (upstream 163a55f) is invisible inside raft's model

The bug computes the commit index as the median of the sorted match
indexes rounded up, so an even configuration commits on half its voters (2
of 4; with 2 voters, the leader alone). Odd configurations run identical
code. Measured: it is active in every 4-node run (an instrument counting a
leader's commit index against what a majority stores: ahead in 50 of 50
runs with the bug, none without), and no sweep ever failed. The reason: a
commit quorum of N/2 still intersects every election quorum of N/2+1, and
a voter holding the entry refuses a candidate without it, so Leader
Completeness holds. Under the faults raft protects against it costs
durability margin, not correctness; it loses writes only if a node forgets
what it acknowledged (`TestDSTQuorumOffByOneScripted`, using amnesia).

Earlier I said permanent loss plus replacement would expose it; that was
wrong (replacing a lost server takes a leader, whose election needs the
other holder's vote). A white-box check, "a leader never commits an entry
before a majority stores it", would catch it in every run (open decision,
10).

### 6.5 Two voters is a hazard of single-server changes (not a bug)

With voters {A, B}, if A appends its own demotion or removal and fails
before replicating it, A (a non-voter in its own latest configuration)
won't campaign, and B, behind A, can't get A's vote. That's inherent to
Raft's one-at-a-time changes. The sweep found it through the operator;
the operator now keeps at least 3 voters, as autopilot's minimum quorum
does.

### 6.6 Earlier findings

- **ReadIndex needs a barrier per term.** My first ReadIndex client read
  from a new leader's commit index before it committed an entry of its own
  term (Raft paper §8); the linearizability check caught the stale read.
  The fixed client issues a Barrier once per term; the naive one is a
  switch the checks must catch.
- **Raft needs durability on return, not atomic batches.** With fsync
  skipped, crashes lose acknowledged writes (17/50 seeds). With a StoreLogs
  batch written as one record per entry (a crash keeps a prefix), nothing
  failed in 200 crash seeds and 73,292 committed writes. A store that could
  keep a later entry of a batch without an earlier one isn't tested.

### 6.7 Mistakes of mine the sweeps caught

Worth knowing when reading results: the checks caught harness bugs as
readily as raft ones.

- The recovery probe retried a write under the same ID ("probe applied
  twice"); each attempt now has its own.
- The operator re-added servers whose raft had shut down on removal,
  promoted non-voters that hadn't caught up, and shrank clusters to 2
  voters; each stalled a cluster and looked at first like a raft bug.
- I twice named the wrong cause of the stall in 6.1 before raft's own log
  showed the pre-vote path.

### 6.8 Raft's timeouts must outlast a slow path (configuration, not a bug)

On a fifth of raft's default timeouts (what the other sweeps use, for more
elections per run), 6 of 200 gray seeds never elected a leader again: with
slow links adding up to about 300ms per write, a round trip outlasts a
200ms election timeout. All 6 recovered on raft's defaults (1s), and the
gray sweeps use those. The same lesson appeared in go-dst-kit's own
examples: three of them lose liveness under gray failure, by fixed
timeouts shorter than a slow path.

### 6.9 A configuration entry can commit by the old configuration (minor, not a bug in raft's model)

The Raft thesis (§4.1) says a server uses a configuration as soon as it is
in its log, so the entry that changes the configuration is committed by a
majority of the *new* configuration. hashicorp's leader stores the entry
and counts its own write (`dispatchLogs` calls `commitment.match`) before
it switches the commit tracker to the new configuration
(`setConfiguration`). With one voter, that count already makes a majority
of the old configuration: adding a second voter to a one-voter cluster
commits at once, held by the leader alone, while the new configuration
says two voters. With more voters the leader's own write is never a
majority, so only this case is affected. Within raft's model it is
harmless: if the leader is lost the cluster is dead either way (it was its
only voter), and every later entry is counted by the new configuration.
Found by the white-box commit check (6.10), which accepts it.

### 6.10 The commit-rule check

`a leader commits only what a majority stores` (dst_checks_test.go): when
a leader moves its commit index to i, a majority of the voters of a
configuration it held since its last commit move must durably store its
entry i (or a snapshot covering it). It catches quorum-off-by-one from the
first seeds of the 4-node and membership sweeps; every default sweep
passes. Its first three versions flagged correct raft, each time the
check's fault: the commit index reaches the leader loop steps after the
count (the configuration can change in between); 6.9; and a new leader's
commit index is learned while following, so only its own moves are
judged.

### 6.11 A follower mishandles its own snapshot boundary (liveness, then a panic)

Found by phase 1 item 7 (configuration per seed) with `TrailingLogs` 0,
which raft accepts. Seed 11 of `TestDSTEverything`:

1. Follower n0 applies up to 207, takes a snapshot and, keeping no
   trailing entries, compacts its log down to entry 208 alone.
2. The leader's AppendEntries follows 207. `appendEntries` looks the
   previous entry up in the log unless it is the last entry (208), and
   207 is gone, though n0's snapshot proves it committed:
   `failed to get previous log: previous-index=207 last-index=208`.
3. The leader backs off to 204 and sends its own snapshot, older than
   n0's state. n0 installs it: its applied index goes back from 207 to
   204 and its state machine with it; 208 stays (`no logs to truncate`).
4. Every AppendEntries after 204 fails the same way and the leader sends
   the snapshot again, round after round: Antithesis's #697 livelock,
   reached another way. When the commit index moves past 204, applying 205
   finds no entry and raft panics (`log not found`).

Fix (switch `snapshot-boundary-fix`; branch `fix/snapshot-boundary` on
upstream `main`): entries at or below a follower's snapshot are committed,
and a leader holds every committed entry, so (1) a previous entry at or
below the snapshot matches and incoming entries it covers are skipped, and
(2) a follower that already holds a snapshot's last entry (in its log
with the same term, or under its own snapshot) doesn't install it: Figure
13, rule 6. My first version of (2) skipped any snapshot ending at or
before the applied index, and the gray membership sweep found it could
livelock: a follower whose applied index ran ahead of both its log and
its own snapshot (an installed snapshot later replaced by an older local
one, another oddity) acknowledged every snapshot without being able to
continue from its end. The bug also shows with TrailingLogs 4, not only 0. Evidence: `TestDSTSnapshotBoundary` (TrailingLogs 0, 400
seeds: correct raft failed 2 when found, none since snapshots moved to the
simulated disk; the fix 0); `TestSnapshotBoundaryScripted` (dstbugs:
both cases fail on correct raft every run, pass with the fix); upstream-style tests
`TestRaft_AppendEntries_PrevLogAtSnapshotBoundary` and
`TestRaft_InstallSnapshot_IgnoresSnapshotOlderThanApplied` fail every run
on upstream and pass with the fix; upstream's suite passes. Part (2) needs
no TrailingLogs 0: any follower handed an older snapshot rolls its state
machine back (the test shows 20 entries going back to 10). Upstream's
attempted fix for #697 (PR #704, closed) handles only a conflicting entry
at the snapshot's index, not these.

### 6.12 An unreadable newest snapshot makes raft panic at startup (availability)

Found by phase 1 item 5 (disk errors), seeds 99 and 190 of
`TestDSTDiskErrors`. At startup `restoreSnapshot` tries the snapshots
newest first and silently falls back to an older one when one fails to
restore; and `FileSnapshotStore.List` silently skips a snapshot whose
metadata it can't read (meant for half-written directories). Either way
raft restores an older snapshot, or none, and `NewRaft` then replays the
log from that index and panics (`log not found`) where the newest
snapshot had compacted it away: with two snapshots retained (Consul's
setting) and `TrailingLogs` below a snapshot interval, the usual case.

1. Seed 99: reading the newest snapshot's `state.bin` failed (EIO); raft
   restored the one before (83), and entry 84 was gone: panic.
2. Seed 190: reading the only snapshot's `meta.json` failed (EIO); List
   returned nothing, raft started without a snapshot and found no entry 1.

A transient error heals when the process restarts. A snapshot that stays
unreadable (bit rot, a corrupt file) crash-loops the node forever, where a
clean error was due; its state can't be recovered from that node either
way, since the entries are gone.

Fix (switch `snapshot-fallback-needs-log`): an older snapshot is restored
only if the log holds every entry after it, and `NewRaft` returns an error
instead of replaying a log that doesn't follow the restored snapshot.
Evidence: `TestSnapshotFallbackGap` (raft's own helpers, the real
filesystem: two snapshots, a log compacted past the older, the newest's
`state.bin` removed) panics on correct raft every run and returns
"failed to load any existing snapshots" with the fix;
`TestDSTDiskErrorsFixed` (drawn retention, every fix on) passes 200 of
200. Also noticed: `NewFileSnapshotStoreWithLogger` wraps its errors with
`%v`, so callers can't tell a full disk (`errors.Is(err,
syscall.ENOSPC)`) from other failures.

## 7. The bug benchmark

`dstbench.json`: bugs behind switches (`bug(name)` in `dstbugs*.go`, only
under `-tags dstbugs`, chosen by `$DSTBUG`), each run against the sweeps
that should catch it; a baseline runs the sweeps with no bug (0 failures
in 500 runs). Two are held out (not run until a final check).

**Plain sweeps, 100 seeds per cell** (distinct failing seeds):

| Bug | Kind | Crashes | Partitions | Everything | 4 nodes | Membership |
|---|---|---|---|---|---|---|
| vote-twice | classic | 0 | 7 | 27 | 18 | |
| vote-ignores-log | classic | 10 | 3 | 8 | 3 | |
| accept-stale-leader | classic | 0 | 7 | 5 | 2 | |
| skip-prev-log-term | classic | 1 | 1 | 5 | 3 | |
| readindex-without-barrier | client | 9 | 1 | 6 | 5 | |
| quorum-off-by-one | upstream 163a55f | n/a | n/a | n/a | 0 (with the commit-rule check: 300/300) | 0 (with it: 273/300) |
| nonvoter-can-lead | upstream 656e6c0 + 6b4e320 | | | | | 0 |
| lease-counts-nonvoters | upstream 6e41709 | | | | | 0 |
| two-changes-in-flight | classic | | | | | 1 |
| commit-previous-term | held out (Raft Figure 8) | | | | | |
| compact-past-snapshot | held out | | | | | |
| config-change-before-noop | held out (raft-dev, 2015) | | | | | |

(n/a: odd clusters run identical code.) The membership bugs are nearly
invisible with the careful operator; with the earlier careless one,
nonvoter-can-lead failed 7 of 100 (by "only voters are elected"). That's
input for phase 2 (scenarios and checks): lease-counts-nonvoters needs a
check that a leader cut off from a voter majority steps down within its
lease.

## 8. Guided exploration so far

Built in go-dst-kit (signals: markers, marker order, interleaving pairs;
failure signatures for reporting). First measurement on raft, every
signal, 100 runs per cell, distinct failing seeds against the sweep:
vote-twice 5/13/13 vs 7/27/18 (partitions/everything/4 nodes);
vote-ignores-log 9/4/7/2 vs 10/3/8/3; accept-stale-leader 8/4/1 vs 7/5/2;
skip-prev-log-term 2/2/3/3 vs 1/1/5/3; readindex-without-barrier 6/1/6/3
vs 9/1/6/5. No gain. Per the plan, exploration waits until the world, the
tests and plain exploration are better.

## 9. Known weaknesses

- Snapshots are perfect (in memory, outside the simulation).
- Clients live in the servers and only use their own node; no forwarding,
  redirects or retries.
- Clock skew is far below what raft should tolerate.
- Timeouts are fixed (defaults / 5, or the defaults in the gray sweeps),
  never drawn.
- Not measured: how often a message arrives after a term change (gray
  failure makes it likely, but nothing counts it).
- Membership under gray failure runs only with the proposed fixes on
  (dstbugs build): the default suite has no membership-plus-gray sweep.
- Several tests are caught on few seeds (skip-prev-log-term 1/50 in its
  test; the naive ReadIndex 3/50): stable, since seeds are fixed, but thin.

## 10. Plan and open decisions

Phases (Prince, 2026-11-27): 1. the simulated world and faults; 2. the
tests themselves (scenarios, assertions, checks); 3. how useful plain
exploration is; 4. guided exploration. Phase 1 items:

1. ~~Permanent loss and replacement through membership changes~~ (done)
2. ~~Slow and stale components~~ (done: design and what was built below)
3. ~~Snapshots on the simulated disk~~ (done: below; errors are item 5)
4. ~~Realistic clients on their own machines~~ (done: below)
5. ~~Store calls that fail (I/O error, disk full)~~ (done: below)
6. ~~Clock extremes~~ (done: below)
7. ~~Timeouts drawn per seed, raft's defaults included~~ (done: the
   configuration per seed, below; done before 3 to 6 by agreement, since 3
   waits for dstmigrate's file I/O from the rosedb work)

### Antithesis's open reports: a benchmark from outside

Antithesis filed four reports on hashicorp/raft in 2026, all open, so the
bugs should be in the code we test. Finding them with our sweeps is the
benchmark that matters most: real bugs, found by someone else, not
re-inserted by us. None of our sweeps finds them yet, and each points at
a gap:

| Report | Bug | Why we miss it today | Needed |
|---|---|---|---|
| [#695](https://github.com/hashicorp/raft/issues/695) | Heartbeats handled on the transport goroutine bump the term concurrently with the leader loop: logs diverge (safety) | Our checks would see divergence; the interleaving is rare under seed sweeps | Phase 2: Raft's invariants checked every step (Log Matching, Leader Completeness); phase 3-4: exploration |
| [#696](https://github.com/hashicorp/raft/issues/696) | An interrupted leadership transfer leaves "leadership transfer in progress" set: the re-elected leader refuses writes (liveness) | The workload never transfers leadership | Phase 2: leadership transfers in the workload |
| [#697](https://github.com/hashicorp/raft/issues/697) | InstallSnapshot keeps a conflicting log suffix (Figure 13 rule 7): a follower loops on snapshots (liveness) | "The cluster recovers" only needs the leader to commit; "same sequence" accepts a follower stuck on a prefix | Phase 2: every node converges after the faults; phase 1 item 3: snapshots on the simulated disk |
| [#746](https://github.com/hashicorp/raft/issues/746) | `persistVote` writes term and candidate as two writes; a crash between them lets a stale vote skip the log check (safety) | Needs pre-vote off; our config never turns it off | Phase 1 item 7: draw the configuration per seed (pre-vote on or off); the crash class is the one that found 6.2 |

Plan: these join the benchmark as "live" entries (no switch: correct raft
should fail), measured after each phase.

### Open decisions for Prince

- ~~White-box check "a leader never commits before a majority stores
  it"~~: approved (2026-11-27), white-box checks in general welcome for
  phase 2.
- File the upstream issue for 6.1 ([draft](UPSTREAM-ISSUE-DRAFT.md))? And
  6.2, in it or separately?
- ~~go-dst-kit's examples under gray failure~~: approved, Gray goes into
  AllFaults and the examples get fixed.
- Membership under gray failure lives in the dstbugs build with both
  fixes on; keep it there, or put it in the default suite (red until
  upstream fixes the bugs)?
- The commit-rule check (6.10) takes quorum-off-by-one from 0 to 300 of
  300 four-node seeds and 273 of 300 membership seeds (dstbench).
- quorum-off-by-one end to end: an operator doing manual outage recovery
  (`RecoverCluster`, as `peers.json` in Consul and Nomad) after losing a
  majority for good would expose it within raft's model (with the bug, the
  survivors may lack an acknowledged write). Proposed as a phase 2
  scenario.

### Item 4: clients on their own machines (as built)

Every sweep now also runs two clients on machines of their own
(`dst_service_test.go`), which partitions and gray failure reach like any
machine. Each node serves the key-value service on port 8000 (one JSON
request per connection): the leader executes it; a follower redirects to
the leader it knows or, on seeds with the feature "servers forward to
the leader", forwards it there (as Consul forwards RPCs). A client keeps
the last leader it learned, follows redirects, and retries with the same
operation ID (timeouts doubling while requests fail) until it gets an
answer or 2s pass; an operation that may have taken effect is recorded
as such in the history. The FSM applies each ID once (Raft's client
sessions, thesis §6.3): retries of committed writes were deduplicated
1,700 to 2,400 times per 200-seed sweep. Since dedup could hide a raft that
applies an entry twice, the FSM also checks it sees each log index once,
in order (`TestDSTCatchesEntryAppliedTwice`: a planted bug, caught on 20
of 20 seeds).

Adding the clients shifted timing and exposed four harness weaknesses,
each found as a failing seed and fixed in the harness, not in raft:

- raft's in-memory snapshot store publishes a snapshot when it is
  created, so a crash mid-write left an empty one ("failed to load any
  existing snapshots"). Replaced by `dstSnapshotStore`, which publishes on
  Close (as a temporary file and a rename would), until item 3.
- The transport's 500ms I/O timeout was shorter than a slow, lossy link's
  round trip: every RPC to the follower behind it timed out, rejections
  included, so the leader never caught it up. With a drawn configuration
  it is now ten heartbeat timeouts (Consul: 10s).
- Under gray failure a 500ms lease (raft's default) is shorter than a
  quorum round trip over slow links: every leader stepped down within it.
  Gray sweeps now configure raft as for a slow network (timeouts 1 or 2
  times the defaults, the lease a full heartbeat).
- The liveness checks' 5s deadlines were sized for 200ms timeouts; they now
  wait ten election timeouts (at least 5s), and the membership check
  waits for the members to converge instead of looking once.

### Item 3: snapshots on the simulated disk (as built)

Every node now keeps its snapshots with raft's own `FileSnapshotStore`, in
`snap/` on its simulated disk, so crashes, power loss and disk faults reach
snapshots as they reach the log. dstmigrate rewrites the store's `os` file
I/O to the World's FS (`-real-fs` keeps it in raft's tests and
`peersjson.go`, which use real paths); `NewFileSnapshotStoreWithWorld`
takes the World, and with none the store uses the real World rooted at
the filesystem's root, so upstream's tests pass unchanged (including the
one that deletes the store's directory). Snapshots retained are drawn per
seed (1 or 2).

`TestDSTSnapshotStoreContract`: through crashes, a snapshot whose Close
returned is durable and reads back byte for byte, and a crash inside
Close leaves the new snapshot or the previous one; 400 seeds pass, and a
store with fsync off is caught on 161 of 200. The store fsyncs the state
file, the metadata and, after the rename, the parent directory, but never
the snapshot's own directory: safe on the disk model's journaled
directories (as on ext4 and XFS), unsafe only under bare POSIX rules, a
hardening note for upstream rather than a finding. A snapshot directory
without `meta.json` seen after a crash was a reap that power cut between
removing the file and the directory; raft skips it, correctly.

The new timing exposed two harness weaknesses (fixed in the harness) and
moved one known bug into the default sweeps:
- The operator lost a server for good when the leader's latest
  configuration had a majority without it, though that configuration had
  reached nobody: losing the leader lost the only copy (a one-voter
  cluster of a lost voter). It now requires a majority without the server
  in the committed configuration too.
- Membership seed 148 stalled on the stale-configuration bug (6.1). The
  default build's `TestDSTMembership` no longer loses servers for good;
  `TestDSTMembershipFixed` (dstbugs) does, with the fixes on: 400 seeds
  pass, as `TestDSTMembershipGrayFixed` does.
- The snapshot-boundary sweep no longer hits 6.11 on correct raft (0 of
  400), so `TestSnapshotBoundaryScripted` (raft's test helpers, as on the
  upstream branch) shows the bug on every run; the sweep stays as the
  fix's regression test.

### Item 5: disk errors (as built)

go-dst-kit's `Faults.DiskErrors` (now in `AllFaults`): a disk fills at a
drawn time and stays full until the faults stop (writes that don't fit
fail with ENOSPC, freeing space makes room), and fsyncs, writes and reads
fail with EIO, a failed fsync losing the file's unsynced writes for good
though the next fsync succeeds, as on Linux. raft answers a failure to
persist its term or vote by panicking (fail-stop): the harness declares
disk-error panics a process death (`Harness.OnPanic`), and the node
restarts as a supervisor would restart it; any other panic still fails
the run. Startup steps (opening the stores, bootstrap, `NewRaft`) retry
on disk errors.

`TestDSTDiskErrors` (crashes, disk faults, disk errors; five nodes, one
snapshot kept): 200 of 200 seeds pass, with 143 fail-stop panics in the
first 50. The new faults found, in order:

- **A bug in the harness's log store** (seed 199, a non-linearizable
  history: two acknowledged writes lost). After a failed write the store
  left the record's bytes in the file to overwrite next time; a failed
  fsync's data is never durable but stays readable, so a process
  restarted after a kill (here a fail-stop panic) replayed the record as
  stored, and records fsynced after it then sat after a hole that a power
  loss opened: recovery stopped there. The store now cuts a failed record
  from the file at once (`dstLogStore.failed`), and its contract test runs
  with kills and disk errors (it failed before the fix: entries 17–37
  recovered where 17–35 were acknowledged). raft's own FileSnapshotStore
  passes the same contract under the same faults.
- **raft finding 6.12** (seeds 99 and 190): above.

### Item 6: clock extremes (as built)

`TestDSTClocks`: five nodes, every machine's clock offset by up to an hour
and running up to 20% fast or slow (go-dst-kit's defaults are 10ms and
100ppm), with crashes, disk faults, partitions, network faults and
pauses, configuration drawn per seed: 200 seeds pass. An experiment at
1%, 5%, 10%, 20% and 40% drift (100 seeds each) found nothing either; the
drawn clocks were checked to apply (rates 0.6 to 1.4 at 40%). That fits
hashicorp/raft's design: no safety rule depends on time (reads verify
leadership with a round of heartbeats, not a lease), and timeouts drawn
between T and 2T absorb drift.

Not built: clock *jumps*. An NTP step moves the wall clock but not Go's
monotonic clock, which `time.Since`, timers and deadlines use; Go's
`time.Time` offers no way to move one without the other, so a faithful
jump can't be expressed with the standard type. The jumps the monotonic
clock does see (a VM or process frozen, then resumed) are the Pause
fault. Also not shown: a check that the clock fault can make fail. The
natural negative is a lease read (a leader serving reads from its own
clock), a phase 2 scenario.

### Item 7: raft's configuration per seed (as built)

The sweeps (not the scripted and "catches" tests, which keep dstConfig)
draw one configuration per seed, the same on every node, from their own
stream (`raftKnobs` in dst_cluster_test.go): timeouts at raft's defaults
times 0.2, 0.5 or 1 (always 1 under gray failure); election timeout 1 or
2 times the heartbeat; lease 0.5 or 1 times it; pre-vote off on a third of
seeds; MaxAppendEntries 64, 1 or 4; TrailingLogs 32 or 4 (0 found 6.11 and
is left to its test); SnapshotThreshold 64 or 16; BatchApplyCh on or off.
Each sweep `Expect`s pre-vote off, small append batches and few trailing
logs. All sweeps pass 200 seeds with it; the failure report shows the
seed's configuration.

### Item 2: slow and stale components (design)

Goal: the gray failures that cause real outages, inside raft's model.
Library (go-dst-kit) or harness, by whether a second system needs it:

| Fault | Where | What |
|---|---|---|
| Slow link | library (simnet) | per-link extra latency, drawn per seed for some links, sometimes one direction only |
| Lossy link | library (simnet) | per-link packet loss shown as TCP does: a lost segment stalls the stream for a retransmission timeout (200ms, doubling), so latency spikes and head-of-line blocking |
| Long partitions break connections | library (simnet) | held traffic is dropped and connections reset if a partition outlasts a timeout (as TCP gives up after retransmissions), so stale data doesn't flow after a long heal |
| Slow node | library (sched) | a machine whose goroutines run slower: each step costs it extra virtual time |
| Slow disk | library (simfs) | one machine's fsync latency far above the others' |

Checks: unchanged (safety must hold; the cluster must recover after the
faults). Sanity: baseline sweeps pass with each on; each fault is shown to
do something (markers, and a test where it changes an outcome: e.g. a
lossy link makes a leader step down, a long partition resets
connections).

**As built** (go-dst-kit `Faults.Gray`, local commit `60e196c`), where it
differs from the design:

- Long partitions: connections that crossed a partition are reset at the
  heal with a per-seed chance, not by how long the partition lasted (a
  middlebox forgetting the connection; simpler, and the held data is
  still exact when it isn't reset).
- Slow node: repeated short stalls (the machine paused for 2 to 50ms
  every 10 to 100ms), not extra virtual time per step: it reuses pauses,
  and it is what a starved CPU looks like from outside.
- Slow disk: a per-machine fsync latency multiplier.
- Slow links: both directions drawn independently (so sometimes one way).
- Laws, each broken by hand once: `[links-per-seed]`, `[loss-delays]`,
  `[heal-may-reset]` (simnet), `[gray-explores]` (dsttest).
- Not shown: a test where a gray fault alone changes an outcome. Raft's
  sweeps did show it (6.1's second shape, 6.2, 6.8).
- In raft, one 200-seed `TestDSTGray` run exercised them: 6,353 stalls of
  slow machines, 746 lost segments, 169 resets at heal, 138 slow links,
  23 slow disks.

## 11. How to run

The fork builds against a local go-dst-kit through a `go.work` (not
committed) next to it. Go 1.25 (`GOTOOLCHAIN=go1.25.0`).

Checks come in two tiers (`dst-check.sh`): `quick` after every change
(about 75s: vet, small sweeps, the contract, planted-bug and scripted-bug
tests) and `full` before a commit, in the background (every sweep at 200
seeds, the fixed sweeps at 400). Both cap sweeps at 4 cores.

```sh
./dst-check.sh quick                             # after every change
./dst-check.sh full                              # before a commit
go test -run TestDST .                           # every sweep
go test -run TestDSTMembership . -dst.seeds=200  # more seeds
go test -run '^TestDSTMembership$' . -dst.seed=6 -dst.v   # replay one seed, with the event log
DST_RAFT_LOG=1 go test -run ... -dst.seed=N .   # with raft's own log
go test -tags dstbugs -run 'TestDSTChecksCatchBugs|TestDSTStaleConfigStall|TestBootstrapCrash|TestDSTMembershipGrayFixed' -v .
DSTBUG=a,b go test -tags dstbugs ...             # several switches at once
DSTBUG=vote-twice go test -tags dstbugs -run TestDSTPartitions .
dstbench -seeds 100                              # the benchmark (go-dst-kit/cmd/dstbench)
.dstmigrate/run.sh                               # re-run the migration from f9cf2a8
```
