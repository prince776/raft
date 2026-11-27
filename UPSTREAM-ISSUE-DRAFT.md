<!-- DRAFT for hashicorp/raft. Not posted; Prince decides. -->

# A live majority of voters can stay leaderless forever: candidates rejected by a voter's stale configuration

## Summary

Since #526 (`6b4e320`), a node rejects a (pre-)vote request from a
candidate that isn't in, or isn't a voter in, the receiver's **latest
configuration**. When the receiver is behind, its latest configuration is
stale, and it can reject the one candidate that could win. Combined with a
lost server, this leaves a cluster with a live majority of its voters and
no leader, permanently. The sequence uses only ordinary operations and
faults: one partition, one routine server replacement, one server that
never comes back.

## Sequence

Voters `{A, B, C}`, `A` leading, a new server `D`:

1. `C` is partitioned from the others.
2. Meanwhile `B` is replaced by `D` (add the new server, then remove the
   old): `AddNonvoter(D)`; once `D` has caught up, `AddVoter(D)`
   (configuration `{A, B, C, D}`, committed by `A`, `B`, `D`); then
   `RemoveServer(B)` (`{A, C, D}`, committed by `A`, `D`). `C` misses all of
   it.
3. `A` dies and doesn't come back. `C` and `D` are two of the three voters
   of `{A, C, D}`, so `D` can only win with `C`'s vote.
4. The partition heals. `C`'s latest configuration is still `{A, B, C}`.

## What happens

`D` (the most up-to-date node) campaigns; `C` rejects its pre-vote:

```
[WARN]  C: rejecting pre-vote request since node is not a voter: from=D
```

`C` campaigns in its own configuration `{A, B, C}`: `A` is gone, and `B`
(removed; if still running) rejects it:

```
[WARN]  B: rejecting pre-vote request since our last index is greater: candidate=C
```

Neither can ever win. `C` never learns the newer configuration, because
no leader exists to send it.

## Expected

Raft's election restriction alone would let `C` vote for `D`: `D`'s log is
at least as up to date as `C`'s. `D` wins, replicates, and `C` learns
`{A, C, D}`. With the candidate checks disabled (`requestVote` and
`requestPreVote`: both the "not in configuration" and the "not a voter"
checks), the same sequence recovers every time.

## Reproduction

Found with deterministic simulation (go-dst-kit: the whole cluster in one
process, raft's real NetworkTransport over a simulated network, seeded
faults): <https://github.com/prince776/raft/tree/dst>, test
`TestDSTStaleConfigStall` in `dst_bugs_test.go`. On 20 seeds, correct raft
stays leaderless on 15; with the candidate checks off, all 20 elect a
leader. Each seed replays exactly.

It also happens without a scripted partition: in a randomized sweep with
slow and lossy links, a voter that lagged behind kept a configuration
without the candidate and rejected it (`rejecting pre-vote request since
node is not in configuration`) after the leader was lost.

A reproduction with raft's own test helpers, and the fix below, are on
branch `fix/stale-config-vote` of <https://github.com/prince776/raft>
(`TestRaft_StaleVoterCanElectNewerVoter` fails every run before the fix,
with and without pre-vote).

Base: `dd4346d` (main as of 2026-11).

## Possible fix

The checks exist so that a node that missed configuration changes doesn't
disrupt the cluster with rising terms (#524). A receiver can only trust
its configuration to judge a candidate if its own log is at least as up to
date as the candidate's. So: apply the membership checks only when the
candidate's last log (term, index) is not ahead of the receiver's. A
candidate whose log is ahead may know a newer configuration than the
receiver; the election restriction already protects safety in that case.
A node stuck on a stale log (the #524 case) is never ahead of the current
members, so it is still rejected.

Tested in the simulation (switch `candidate-check-unless-ahead`): the
sequence above recovers on all 20 seeds, and the membership sweep (200
seeds: 1 to 7 voters, adds, promotions, demotions, removals, servers lost
for good, crashes, partitions) still passes. One caveat: that sweep also
passes with the checks removed entirely, so it doesn't exercise #524's
disruption, and that the fix keeps #524's protection is an argument here,
not a measurement.

## Possibly related

- #472: "Leadership election stuck in 3 node cluster in Candidate state"
  (open since 2021): a server whose configuration doesn't list the other
  live server.
- #524, #526: the change that introduced the checks.

## Also noticed (separate)

**A crash inside `BootstrapCluster` leaves a node that can never lead.**
It saves term 1, then appends the configuration entry. After a crash
between the two, `HasExistingState` reports state (the term), so
bootstrapping again returns `ErrCantBootstrap`, and the node has no
configuration, so it never campaigns; a single-node cluster needs its data
wiped. Appending the configuration entry before saving the term closes
the window (a crash between them leaves a log whose configuration raft
uses on start). Low severity (one write, once per node; in a multi-node
cluster a leader elected by the others repairs the node); maybe a separate
issue. Fix and test: branch `fix/bootstrap-crash` on
<https://github.com/prince776/raft>.

**A demoted leader shuts down.**
A leader that is demoted (`DemoteVoter`, not removed) steps down and, with
`ShutdownOnRemove`, shuts down (`leaderLoop`: `!hasVote(committed,
localID)` sets `stepDown`; log: `removed ourself, shutting down`), though
it remains a member as a non-voter. A demoted follower keeps running. Is
that intended?

---

<!-- SECOND DRAFT, a separate issue. Not posted; Prince decides. -->

# A follower mishandles its own snapshot boundary: rejected appends, state rolled back, livelock, panic (related to #697)

## Summary

Two gaps at a follower's snapshot boundary, both reachable with ordinary
settings:

1. **`appendEntries` rejects a previous entry the follower's snapshot
   covers.** It looks `PrevLogEntry` up in the log unless it equals the
   last entry. With few trailing logs (`TrailingLogs` 0 is accepted), a
   follower holding a snapshot up to S and a log of only S+1 rejects an
   append whose previous entry is S (or below S with entries crossing it),
   though everything at or below its snapshot is committed and so matches
   any leader's log.
2. **`installSnapshot` installs a snapshot the follower already has
   past.** Handed a snapshot older than its applied state, the follower
   restores it: its applied index and state machine move back in time.
   Raft's Figure 13, rule 6, says a follower that holds the snapshot's last
   entry keeps its log and doesn't reset.

Together: the leader, seeing the rejection, backs off and sends its own
(older) snapshot; the follower installs it and rolls back; the entries
between the two snapshots now exist nowhere on the follower, so every
later append fails and the leader resends the snapshot, round after round
(the livelock of #697, by another path). When the commit index passes the
older snapshot, applying the next entry finds no log and raft panics
(`log not found`). Part 2 alone needs no special setting: any follower
sent an older snapshot rolls back.

## Reproduction

Branch `fix/snapshot-boundary` of <https://github.com/prince776/raft>
(base `dd4346d`) adds two tests using raft's own helpers; both fail every
run on `main` and pass with the fix:

- `TestRaft_AppendEntries_PrevLogAtSnapshotBoundary`: an append whose
  previous entry is the follower's snapshot index, or inside the snapshot
  with entries crossing it.
- `TestRaft_InstallSnapshot_IgnoresSnapshotOlderThanApplied`: an older
  snapshot sent to an up-to-date follower (its state machine goes from 20
  entries back to 10 on `main`).

Found with deterministic simulation (go-dst-kit), seed 11 of a sweep with
crashes, disk faults and partitions that drew `TrailingLogs` 0; the
sequence above is from raft's own log.

## Possible fix (on the branch)

1. In `appendEntries`, a previous entry at or below the follower's last
   snapshot index matches (committed entries match the leader's), and
   incoming entries at or below it are skipped rather than compared.
2. In `installSnapshot`, after validating the request: if the follower
   holds the snapshot's last entry (in its log with the same term, or
   covered by its own snapshot), acknowledge without installing.

A first version of (2) skipped any snapshot ending at or before the
applied index; simulation found it could livelock (a follower whose
applied index was ahead of both its log and its snapshot acknowledged
every snapshot without being able to continue from it), so the branch
follows rule 6 exactly. Upstream's suite passes with the fix.

## Related

- #697 (Antithesis): InstallSnapshot livelock. PR #704 (closed) addressed
  a conflicting entry at the snapshot's index, not these two cases.

---

<!-- THIRD DRAFT, a separate issue. Not posted; Prince decides. -->

# NewRaft panics when the newest snapshot can't be read and the log was compacted past an older one

## Summary

At startup, `restoreSnapshot` tries the snapshots newest first and, if one
fails to restore, silently falls back to the next. `FileSnapshotStore.List`
likewise skips a snapshot whose `meta.json` it can't read. Either way raft
may restore an older snapshot, or none, while the log was compacted past
it by the newest snapshot. `NewRaft` then replays the log from the
restored index and panics (`log not found`).

With `retain` 2 (Consul's setting) and `TrailingLogs` below the snapshot
interval this is the usual layout, so one unreadable newest snapshot (an
I/O error while reading it, or a corrupt file) turns into a startup panic.
A transient error heals on the next start; a snapshot that stays
unreadable crash-loops the node, where a clean error was due (the node
can't recover from its own data either way, since the entries are gone).

## Reproduction

A plain test with raft's helpers and the real filesystem: one node,
`TrailingLogs` 0, `FileSnapshotStore` with retain 2; apply 10 entries,
snapshot, apply 10, snapshot, apply 1; shut down; delete the newest
snapshot's `state.bin`; `NewRaft` with the same stores panics with
`log not found` (every run). Branch: (to be cut, like
`fix/snapshot-boundary`, from `TestSnapshotFallbackGap` on
<https://github.com/prince776/raft/tree/dst>).

Found with deterministic simulation (go-dst-kit) injecting disk errors:
seed 99 (reading `state.bin` failed: raft restored the older snapshot 83,
then entry 84 was gone) and seed 190 (reading `meta.json` failed: List
returned nothing, raft started without a snapshot and found no entry 1).

## Possible fix

1. In `restoreSnapshot`, restore an older snapshot only if the log holds
   every entry after it (`FirstIndex <= index+1`, or the log ends at or
   before it).
2. In `NewRaft`, before replaying the log, return an error if the log
   doesn't follow the restored snapshot (or the absence of one) instead
   of panicking.

Also: `NewFileSnapshotStoreWithLogger` wraps errors with `%v`, so callers
can't detect, say, a full disk with `errors.Is(err, syscall.ENOSPC)`.

---

<!-- FOURTH DRAFT: a comment on #695, not a new issue. Not posted; Prince decides. -->

# Comment on #695: the same race sets the term back when it meets the node's own election

A deterministic simulation (go-dst-kit, with a check that a node's current
term never decreases) hits a variant of the alternate manifestation on
about 5% of seeds with crashes, partitions and slow links, each seed
replaying exactly:

1. Node n3 is a candidate in term 2 (its pre-vote won); the term-2 leader's
   heartbeat reaches its transport goroutine and passes the "older term?"
   check in `appendEntries`.
2. n3's main loop runs `electSelf`: `setCurrentTerm(3)` (persisted), votes
   for itself in term 3.
3. The heartbeat goroutine resumes: the node isn't a follower, so it runs
   `setState(Follower); setCurrentTerm(2)`. The term goes back to 2, in
   the stable store too. (In one seed it went from 28 to 21: the heartbeat
   had waited behind slow, failing disk writes.)

Note on #701's approach: a mutex around `appendEntries`'s step-down
doesn't cover this, since `electSelf` (and `requestVote`) change the term
on the main loop without it. Handing any heartbeat that would change the
term or the state to the main loop (`rpcCh`), as suggested above, fixes
every seed we have; the fast path keeps handling the common heartbeat
(same term, follower).
