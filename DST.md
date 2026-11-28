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
added for (#524). Phase 2 step 4 measured it: the fix keeps that
protection (`TestRemovedServerCampaigns`, below).

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

### 6.4 quorum-off-by-one (upstream 163a55f) is invisible inside raft's model (until an operator recovers an outage: phase 2 step 5)

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

### 6.13 A heartbeat on the transport's goroutine sets the term back (safety; upstream #695)

Found by phase 2's "terms never go back" invariant, in ordinary sweeps:
13 of 250 seeds of the five default sweeps (Smoke, Everything,
Membership, Gray, DiskErrors at 50 each), each replaying exactly.

`processHeartbeat` runs `appendEntries` on the transport's connection
goroutine (the heartbeat fast path), though `appendEntries` "must only be
called from the main thread". Gray seed 49: n3, a candidate in term 2,
gets a heartbeat from the term-2 leader; the heartbeat passes the "older
term?" check; raft's main loop starts the election (`electSelf`: term 3,
persisted, n3 votes for itself); then the heartbeat goroutine, seeing a
non-follower, runs `setState(Follower); setCurrentTerm(2)`. The current
term goes back from 3 to 2, on disk too (DiskErrors seed 19: from 28 to
21, the heartbeat having waited behind failing disk writes). Only atomics
are touched, so `-race` can't see it.

This is the "alternate manifestation" of Antithesis's
[#695](https://github.com/hashicorp/raft/issues/695) (open): there a
heartbeat races a RequestVote; here it races the node's own election.
Its main manifestation (a leader appending under a term it doesn't lead,
logs diverging) wasn't seen: "logs match" holds on every seed. The fix
proposed in [#701](https://github.com/hashicorp/raft/pull/701) (closed
unmerged) serializes `appendEntries`'s step-down with a mutex; the main
loop's own term changes (`electSelf`, `requestVote`) don't take it, so
this variant would remain.

Fix (switch `heartbeat-on-main-thread`, as #695's reporter suggested): a
heartbeat that would change the term or the state is handed to the main
loop (`rpcCh`) instead of handled on the transport's goroutine. All six
seeds examined pass with it; with every known fix on, the term check is
strict (below). Not yet shown outside the simulation (it needs the
interleaving; a scripted test is a todo).

### 6.14 A transfer goroutine stuck on a send: the re-elected leader refuses writes for good (liveness; upstream #696)

Found by phase 2 step 3 (leadership transfers in the workload), 3 of 250
default seeds (Everything 24, Membership 47 and one more), each replaying
exactly. Everything seed 24: n0 leads term 8 and starts a transfer; the
target lags, so `leadershipTransfer` asks the replication goroutine to
catch it up through `repl.triggerDeferErrorCh` (one slot); the transfer
times out and stops, leaving its request in the slot, unread (the
replication goroutine is stuck on a slow follower). The next transfer
blocks on the full slot: that send is outside the `select` with `stopCh`.
n0 fails to contact a quorum, steps down, wins term 9; nothing drains the
channel now, so the transfer goroutine never sends on `doneCh`, the
wrapper goroutine never returns, and its deferred
`setLeadershipTransferInProgress(false)` never runs. n0 refuses every
write ("leadership transfer in progress") until it restarts: 265 refusals
before the run ends.

This is Antithesis's [#696](https://github.com/hashicorp/raft/issues/696)
(open: "the re-elected leader refuses to service client requests with
the error message 'leadership transfer in progress'", after a transfer
interrupted by a step-down), here with its mechanism traced.

Fix (switch `transfer-send-stoppable`): the send selects on `stopCh` too,
so a stopped transfer always finishes and clears the flag. Both seeds
examined pass with it; it is in `knownFixes`, and with it on a write
refused for good fails "the cluster recovers"; without it the refusal is
counted (`a re-elected leader refused writes for good (6.14)`). Planted
`transfer-flag-sticks` (the flag never cleared) is caught by "the cluster
recovers" in half the seeds that draw transfers.

### 6.15 RecoverCluster keeps the server's stored term (a hazard, not a bug)

Found while building phase 2 step 5. `RecoverCluster` forces a
configuration on the server it runs on but leaves its current term as
stored, so the recovered cluster's first election may reuse a term a lost
server led, and write different entries under it (seen on 2 of 50 seeds:
two leaders of term 3; entry 13 of term 3 with two payloads). Harmless
while the lost servers stay lost, which is the procedure's premise. If one
was only unreachable and comes back (an operator's mistake), its log and
the cluster's agree on an (index, term) with different entries: Log
Matching breaks, and raft can't notice. A defensive option for upstream:
let `RecoverCluster` take a term at least as high as any the operator saw
(or bump the stored term by a margin), so recovered terms never collide.

### 6.16 A rejected pipelined append deadlocks replication to that follower for good (liveness; upstream #612)

Found by the strict disk-errors sweep at 400 seeds (seed 240). A
follower's `StoreLogs` fails once (here a failed fsync); it rejects that
pipelined append, and the next ones ("failed to get previous log").
raft's `pipelineDecode` sees the rejection and returns, so nothing reads
the transport's `doneCh` any more. `netPipeline.decodeResponses` blocks
handing over the next response and stops taking requests from
`inprogressCh`; the replication goroutine, inside `pipelineSend`, blocks
handing over its next request (both channels are unbuffered at the
default `MaxRPCsInFlight` of 2, capacity `maxInFlight-2`). It never gets
back to the `select` that would see the decoder finished, so
`pipeline.Close`, deferred after that loop, never runs. The leader never
sends that follower entries again, while heartbeats (their own
goroutine) go on: the follower stays at its last index, 20s after the
faults (stacks taken from the simulation's goroutines confirm each
step).

This is the root cause of [#612](https://github.com/hashicorp/raft/issues/612)
(open since 2024-08: "Leader stopped sending appendEntries to the
follower, but heartbeat was working", after a MySQL-backed log store
failed one `StoreLogs` with a deadlock error, then two "failed to get
previous log"; the maintainers hadn't seen it before). One difference:
our run logs "aborting pipeline replication"; the reporter found no such
line.

Fix (switch `pipeline-close-on-abort`, in `knownFixes`): the decoder closes
the pipeline as it exits, which unblocks both sides; replication falls
back to plain RPCs. Seed 240 passes with it.

Outside the simulation, `TestPipelineStall` (`dst_boundary_scripted_test.go`,
raft's own pieces: three servers over real TCP, pipelined replication at
the default `MaxRPCsInFlight`, entries applied without pause, one
follower's `StoreLogs` failing once): the follower stalls for good in 4 of
5 tries on correct raft, and in each of them the leader's `Shutdown` never
returns either (it waits for the stuck replication goroutine); the
goroutine dump shows `pipelineSend` inside `netPipeline.AppendEntries` and
the same pipeline's `decodeResponses`, both blocked a minute. With the fix:
0 of 5.

### 6.17 InstallSnapshot keeps a log that conflicts with the snapshot (safety; upstream #697, rule 7)

Found by "logs match" in the strict membership sweep at 400 seeds (seed
266). n3 holds entry 210 from term 4; the leader installs a snapshot
ending at 210, term 5. `installSnapshot` only compacts by `TrailingLogs`
("no logs to truncate"), so the conflicting 210 stays, and the next append
stores 211 (term 5) after it: two logs hold 211 after different entries.
Figure 13 rule 7: keep the entries following the snapshot only if the log
holds the snapshot's last entry (same index and term); otherwise discard
the whole log. This is the half of Antithesis's
[#697](https://github.com/hashicorp/raft/issues/697) ("InstallSnapshot
keeps a conflicting log suffix (Figure 13 rule 7)") that 6.11 (rule 6)
didn't cover; an end-to-end check saw it only once a stale entry was
served.

Fix (switch `snapshot-install-rule7`, in `knownFixes`): discard the log
when it doesn't hold the snapshot's last entry, and set the last log to the
snapshot. Seed 266 passes with it.

### 6.18 A leader keeps a configuration its log never got (safety risk; new)

Found by phase 3's combined sweep: the 300-seed no-bug hunt failed seed
149 ("the members agree on the configuration"), with every other fix on.
n3, the leader, demoted n6. Its log store's write of that entry failed
(an fsync EIO): `dispatchLogs` answers the future with the error and steps
down to Follower. But `appendConfigurationEntry` then adopts the new
configuration anyway (`setLatestConfiguration` after `dispatchLogs`, with
no check). The node now holds a configuration no log holds. A later
leader fills that index with other entries; once it commits, n3's
committed configuration is the phantom too. At the end all eight nodes
hold the same log to index 48 (term 7), and n3 alone thinks n6 is a
non-voter. Raft counts votes and commits against its configuration, so a
node acting on a phantom configuration could, as leader, count a quorum of
the wrong servers.

`TestFailedConfigWrite` (`dst_boundary_scripted_test.go`, raft's in-memory
pieces, no simulation): the leader's log store fails the DemoteVoter
entry's write once. On upstream the leader's latest configuration holds
the demotion and no entry in its log does; with the fix it doesn't.

Fix (switch `config-only-if-stored`, in `knownFixes`): adopt the
configuration only if the leader is still leader after `dispatchLogs` (a
failed store stepped it down). Seed 149 passes with it, and the 300-seed
hunt passes. Its rate in the combined sweep is in phase 3's baseline
(10b).

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
| [#695](https://github.com/hashicorp/raft/issues/695) | Heartbeats handled on the transport goroutine bump the term concurrently with the leader loop: logs diverge (safety) | **Found in phase 2** (6.13): "terms never go back" catches its alternate manifestation on 13 of 250 default seeds; divergence itself not seen yet | Phase 3-4: exploration for the divergence |
| [#696](https://github.com/hashicorp/raft/issues/696) | An interrupted leadership transfer leaves "leadership transfer in progress" set: the re-elected leader refuses writes (liveness) | **Found in phase 2** (6.14): with transfers in the workload, on 3 of 250 default seeds; root cause and a fix | — |
| [#697](https://github.com/hashicorp/raft/issues/697) | InstallSnapshot keeps a conflicting log suffix (Figure 13 rule 7): a follower loops on snapshots (liveness) | **Found**: its rule-6 relative in phase 1 (6.11), its rule-7 mechanism in phase 2 by "logs match" (6.17) | — |
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
- ~~quorum-off-by-one end to end~~: done in phase 2 step 5
  (`TestDSTManualRecovery`): with manual outage recovery it loses
  acknowledged writes inside raft's model.

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

## 10a. Phase 2: checks (in progress)

Approved order: Raft's core invariants; convergence after the faults;
leadership transfers (#696); a removed server campaigning (#524); later,
a manual-recovery operator and a planted lease read.

### Step 1: Raft's core invariants (as built)

`dst_invariants_test.go`, four checks after every step, each with a bug
planted for it (`TestDSTChecksCatchBugs`, caught within 10–30 seeds):

| Check | How | Planted bug that breaks it |
|---|---|---|
| logs match | as each store writes: (index, term) maps to one payload and one previous term, so two logs holding an entry agree on all before it | `skip-prev-log-term` |
| leaders hold every committed entry | when a node is first seen leading a term, against every entry a leader committed (a snapshot covers what it compacted) | `vote-ignores-log` |
| terms never go back | a node's current term, across crashes and restarts | `forget-term-on-restart` (new) |
| one vote per term | a hook in raft's vote grants (`Config.dstOnVote`, its own vote included), across restarts | `vote-twice` |

They now catch first what the end-to-end checks caught before
(`vote-twice` by a second vote before a second leader, `accept-stale-leader`
by a leader missing a committed entry). A node that lost its disk
(`wipeOnBoot`) starts afresh in the last two. "Terms never go back" found
6.13 at once; since upstream breaks it, it fails a run only with that fix
on (`knownFixes`), and counts the regressions otherwise
(`a node's term went back (6.13)`). Cost: small (the default sweeps' speed
is unchanged within noise).

### Step 2: every member catches up (as built)

`catchesUp`, a final check after "the cluster recovers": every member
(in the committed configuration, not lost) applies the leader's commit
index within the patience bound (5s, or ten election timeouts with a
drawn configuration). "The cluster recovers" needs only a leader that
commits, and "the same sequence" accepts a member stuck on a prefix, so
neither could see a follower that never catches up. Planted bug
`follower-ignores-commit` (a follower ignores the leader's commit index):
caught on 10 of 10 seeds, by this check alone. A member whose raft shut
itself down (6.3, demoted or removed with `ShutdownOnRemove`) is skipped
and counted, as the operator's agreement check already does: the first
sweep's three failures (Membership seeds 9, 28, 50) were all that.
Membership then passes 200 of 200.

### Step 3: leadership transfers (as built)

A per-seed feature, `leadership transfers` (`dst_transfer_test.go`): during
the faults the leader now and then hands leadership on, to a voter of its
choosing or raft's. Each transfer must resolve within patience; a stuck
flag shows as "the cluster recovers" failing. Found 6.14 (#696) at once.
Scripts that need the leader to stay put opt out (`noTransfers`).

Transfers made the linearizability checker give up (search budget) on
about 1 in 15 seeds: every write in flight when leadership moves ends
unknown, and the checker must try each at every place and as never
happening. A bigger budget only tripped the real-time watchdog. Two
sound reductions fixed it: a read whose result never came is recorded as
having no effect (it changes nothing), and, since the check runs after
every member caught up, an unknown write that no node applied is removed
(it never took effect; removing it can only make the check stricter).
Seeds that took 10s and gave up now pass in 2–7s.

### Step 4: a removed server campaigning (#524) (as built)

`TestRemovedServerCampaigns` (`dst_boundary_scripted_test.go`): a voter
is cut off and removed (or demoted) without hearing of it, campaigns
alone, and comes back just as the leader crashes, so the voters hold an
election while it campaigns with its inflated term. 20 seeds per row:

| Server | Raft | Pre-vote | Disrupted |
|---|---|---|---|
| removed | correct | off | 0 |
| removed | correct | on | 0 |
| removed | bug 1's fix (`candidate-check-unless-ahead`) | off | 0 |
| removed | #526's checks removed (`no-candidate-config-check`) | off | 18 |
| demoted | correct, the fix, or the checks removed | off/on | 0 |

For a removed server, "disrupted" is a voter taking its term; the checks
are what prevent it, and bug 1's fix keeps them (the returning server's
log, having missed the removal, isn't ahead). Without the crash nothing
is disrupted even with the checks removed: a voter that has a leader
refuses every vote ("we have a leader"), so only an election shows what
the checks protect. For a demoted server one term jump is by design
(requestVote: "step down and update term, but reject the vote", #526, so
the cluster can replicate to it); the test requires a voter leader that
then holds its term for 2s and the demoted server caught up as a
non-voter. With pre-vote on, nothing is disrupted in any row: pre-vote
alone covers #524 when the transport supports it.

### Step 5a: manual outage recovery (as built)

`TestDSTManualRecovery` (`dst_bugs_test.go`, the dstbugs build): an even
cluster (4 or 6 voters, drawn configuration, clients on their own
machines) runs under crashes and network faults; midway it splits in half
(the leader's half with the clients) and that half is then lost for good,
a site lost. An operator recovers by RecoverCluster's safe procedure: the
most up to date survivor (last term, then index) recovered as the sole
voter (`peers.json`), the other survivors rejoining with clean disks.
Losing half of an even cluster is the case where recovery is needed
(survivors short of a quorum) and every acknowledged write is still
guaranteed (each is on a quorum, n/2+1, and the most up to date survivor
holds every committed entry); for odd n the two never coincide.

- Correct raft: 50 of 50 seeds keep every acknowledged write.
- quorum-off-by-one (the white-box checks left out, `noWhiteBox`): 27 of
  50 seeds lose acknowledged writes or break linearizability. Until now
  the bug needed amnesia to do harm (6.4).

Two lessons on the way. Recovering every survivor from its own log (the
doc's "typical" way) commits each one's uncommitted tail, and can diverge
their state machines, as the doc warns (1 of 50 seeds). And a recovery
reuses terms (6.15), so the checks with history (one leader per term,
logs match) start afresh at a manual recovery.

### Step 5b: a lease read (not built, by decision)

Prince's decision (2026-11-28): skip it. hashicorp/raft has no lease
reads (VerifyLeader runs a round of heartbeats), so a lease-read check
would test a planted feature, not raft; and the simulated clock can't
break a lease anyway (drift is capped, and the jumps that would need a
wall clock moving apart from the monotonic one aren't modelled). Clock
jumps went on go-dst-kit's Todo instead. Phase 2 is closed.

## 10b. Phase 3: how useful plain exploration is

`TestDSTPhase3` (`dst_phase3_test.go`, dstbugs, `DST_PHASE3=1`): one
combined sweep, not one per fault class. Every fault kind is on
(`AllFaults` plus kills and losses; the swarm switches each off for a
third of seeds), and each seed draws its cluster: 3, 4 or 5 fixed voters,
or a membership cluster whose lost servers are replaced (a quarter of
seeds), on raft's default timeouts whenever the seed drew gray failure.
Each known bug runs until 5 seeds fail or its budget runs out; a real bug
runs with its fix off (and `strict-checks`, so the two checks that only
count upstream's 6.13 and 6.14 behaviour fail on it). A no-bug hunt of 300
seeds passes, and reaches every fault and workload marker (crashes, kills,
losses and replacements, partitions, pauses, gray links and machines,
disk-full and EIO, torn writes and holes, snapshots, membership changes,
transfers, client redirects and retries).

A first design ran every bug through every fault class (about 250 cells,
2-2.5 hours); Prince stopped it: most cells only confirmed which faults a
bug needs. This run takes minutes.

| Bug | Seeds to first failure (failing / run) |
|---|---|
| apply-entry-twice, follower-ignores-commit, forget-term-on-restart, quorum-off-by-one, vote-twice | 1 (8–25 of 25) |
| readindex-without-barrier, transfer-flag-sticks, vote-ignores-log | 7–11 (5–6 of 25–75) |
| skip-prev-log-term, accept-stale-leader | 20–24 (3–5 of 75–100) |
| compact-past-snapshot | 171 (5 of 350) |
| nonvoter-can-lead, two-changes-in-flight, config-change-before-noop, commit-previous-term | 294–467 (1 of 500) |
| lease-counts-nonvoters | 90 (1 of 100) |
| real: #695 heartbeat off the main loop (6.13) | 7 (4 of 300) |
| real: #612 pipeline deadlock (6.16) | 50 (2 of 100) |
| real: unreadable newest snapshot (6.12) | 208 (2 of 500) |
| real: crash inside bootstrap (6.2) | 500 (1 of 500) |
| real: stale-config stall (6.1), #696 stuck transfer (6.14), #697 rule 7 (6.17) | not found in 300–500 |
| real: snapshot boundary (6.11) | not found in 500: it needs TrailingLogs 0, which the drawn configuration never uses (a gap in the world, not in exploration) |

**The frozen baseline** (the mix above, plus TrailingLogs 0 on a quarter
of seeds and membership clusters on half; each target to its 5th failure
or 3000 seeds; seed 2399, an inconclusive linearizability check with every
fix on, not counted as a detection):

| Target | Plain: failing seeds / run (first) |
|---|---|
| compact-past-snapshot | 5 / 50 (1) |
| nonvoter-can-lead | 5 / 950 (43) |
| 6.18 phantom configuration | 5 / 650 (149) |
| two-changes-in-flight | 3 / 3000 (719) |
| config-change-before-noop | 1 / 3000 |
| 6.1 stale-config stall | 1 / 3000 (2170) |
| 6.14 #696 stuck transfer | 1 / 3000 (1906) |
| commit-previous-term | 0 / 3000 |
| 6.11 snapshot boundary | 0 / 3000 |
| 6.17 #697 rule 7 | 0 / 3000 |

Harness gap found by it: "membership changes are linearizable" can run out
of search budget (seed 2399), a verdict-less failure; the tally now counts
it apart. Todo: the same sound reductions the KV check got.

So plain exploration finds the shallow planted bugs at once, and the
membership and snapshot ones rarely (1 in 300–500 seeds). It finds
only 4 of the 8 real bugs within 500 seeds, while dedicated per-class
sweeps found them at 1 in 50 to 1 in 250 (the combined sweep spreads its
seeds over every fault kind). Phase 4's targets: 6.1, 6.14 (#696), 6.17
(#697), and the rare membership plants; 6.11 needs TrailingLogs 0 drawn
first. Not yet measured: the swarm itself (drawn per seed against
pinned).

## 10c. Phase 4: guided exploration (first measurement: no gain)

The same combined sweep as phase 3, used as a hunt: 500 plain seeds
against 500 guided runs (`dsttest.Explore`), on A (upstream raft: every
known fix off, strict-checks on, so every real bug found so far is
present) and B (every fix on). `TestDSTPhase4` (`DST_PHASE4=1`). New
signal: the cluster's abstract state (`dst_state_test.go`, go-dst-kit's
`Harness.State`): capped counts of leaders, candidates, nodes down,
configuration changes in flight, disagreeing latest configurations,
non-voters, logs overlapping or gapped after a snapshot, lagging commit
indexes. Chosen from the protocol, not the bugs. (A per-node list was
tried first: 5000 states in 40 runs, none recurring; counts give about 400
in 500 runs.)

A, distinct failure kinds (check plus signature) and abstract states
reached, per 500 runs:

| First seed | Plain | Guided, all signals | Guided, states only |
|---|---|---|---|
| 1 | 4 kinds, 387 states, 97 s cpu | 5 kinds, 398 states, 394 s | 3 kinds, 422 states, 126 s |
| 1001 | 7 kinds, 426 states, 82 s | | 4 kinds, 457 states, 171 s |
| 2001 | 11 kinds, 421 states, 68 s | | 5 kinds, 392 states, 74 s |

B: no failure in any arm (plain 391 states, all signals 369, states 411).

Reading:
- No gain. From seed 1 guided looked better (6.18's "members agree" at runs
  40 and 70 against none by plain; "every member catches up" at run 44
  against 179). The two repeats reverse it: plain finds more kinds, and
  each kind about as early or earlier.
- Guided runs fail more often on the commonest bug (6.13, terms going back:
  28 runs against 16, 18 against 12) and find fewer kinds. Forks of a
  failing root fail the same way, as in go-dst-kit's earlier measurements.
- Guided runs don't reach more abstract states (+9%, +7%, -7%). Steering
  toward rare states doesn't widen coverage, so the explorer isn't doing
  what it's for here. A likely reason: a fork keeps its root's cluster
  shape, configuration and fault mix (drawn in setup), and most of the
  state space, and these bugs, depend on those; only fresh seeds vary them.
- Cost: all signals 4x plain's CPU (recording interleaving pairs: a stack
  walk per scheduling point); states only 1.1-2x.
- "log not found" panics in A (1-6 per 500, both arms) aren't attributed to
  one bug yet; every phase 3 single-bug cell missed them, so they may need
  two fixes off. Todo.

**Second measurement** (Prince: don't overexplore one root; 500 may be too
few). go-dst-kit c020e49: half the runs fresh, a root forked at most 8
times without finding anything new, a feature's carriers from distinct
roots. 2000 runs per arm, A, by states; the run each kind (by check) first
failed, - for not found:

| | Seed 1 plain | Seed 1 guided | Seed 1001 plain | Seed 1001 guided |
|---|---|---|---|---|
| terms never go back (6.13) | 94 | 93 | 2 | 2 |
| every member catches up | 179 | 50 | 127 | 237 |
| members agree on the configuration (6.18) | 780 | 138 | 152 | 157 |
| the cluster recovers | 295 | 525 | 221 | 424 |
| "log not found" panic | 462 | 865 | 372 | 42 |
| run never finishes | 576 | 1080 | 1739 | - |
| only voters are elected | - | - | 1873 | - |
| panic in a raft goroutine | - | - | 1934 | - |
| kinds / abstract states | 6 / 531 | 6 / 615 | 8 / 533 | 5 / 592 |

Forks now spread (209 and 221 roots, at most 15 forks of one) and guided
runs reach 11-16% more abstract states, but find no more kinds: some
earlier (6.18 at 138 vs 780 once), some later, and the singletons plain
found late (only voters, a raft-goroutine panic, a hang) not at all. A
guided exploration has half the fresh roots, and most of these bugs depend
on what a root draws (shape, configuration, fault mix).

**Diagnostics** (TestDSTPhase4Forks, 1000 plain seeds of upstream raft).
Forks of a failing run, 5 per root per point, failing the same way:

| Kind | Fresh | Step 1 | 10% | 25% | 50% | 75-90% |
|---|---|---|---|---|---|---|
| every member catches up | 1.5% | 8% | 8% | 28-32% | 100% | 100% |
| members agree (6.18) | 0.3% | 0/15 | 0/15 | 33% | 93% | 100% |
| terms never go back (6.13) | 2.0% | 4% | 12% | 8-20% | 16-20% | 32-36% |
| cluster recovers; never finishes | 0.1% each | 0/5 | 5/5 | 5/5 | 5/5 | 5/5 |
| "log not found" panic | 0.1% | 0/5 | 0/5 | 0-1/5 | 0/5 | 0/5 |

A fork at step 1 keeps what setup drew (shape, configuration, fault mix)
and re-draws the rest: about the fresh rate. So the root's draws don't
decide these bugs (my reading above was wrong); the state the run reaches
does, between 10% and 50% of the way in, and from there forks fail 30-100%
of the time. Forking works; choosing what to fork doesn't.

Does abstract-state rarity mark those runs? States reached by at most 5 of
the 1000 runs, per run: passing 0.43; 6.18 6.67 (rarest reached by 16 runs
against 42); catches up 1.0; 6.13 0.30; the rest 0. Only 6.18's path is
rare in this abstraction, which fits guided finding it early once.

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
