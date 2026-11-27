<!-- DRAFT for hashicorp/raft. Not posted; Prince decides. -->

# A live majority of voters can stay leaderless forever: candidates rejected by a voter's stale configuration

## Summary

Since #526 (`6b4e320`), a node rejects a (pre-)vote request from a
candidate that isn't in, or isn't a voter in, the receiver's **latest
configuration**. When the receiver is behind, its latest configuration is
stale, and it can reject the one candidate that could win. Combined with a
lost server, this leaves a cluster with a live majority of its voters and
no leader, permanently. The sequence uses only ordinary operations and
faults: one partition, one `RemoveServer`, one server that never comes
back.

## Sequence

Voters `{A, B, C}`, `A` leading, a new server `D`:

1. `C` is partitioned from the others.
2. `A`: `AddNonvoter(D)`; once `D` has caught up, `AddVoter(D)`.
   Configuration `{A, B, C, D}`, committed by `A`, `B`, `D`.
3. `A`: `RemoveServer(B)`. Configuration `{A, C, D}`, committed by `A`, `D`.
4. `A` dies and doesn't come back. `C` and `D` are two of the three voters
   of `{A, C, D}`.
5. The partition heals. `C`'s latest configuration is still `{A, B, C}`.

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
leader. Each seed replays exactly. We can also write a reproduction with
raft's own test helpers (`MakeCluster`, partitions via `InmemTransport`
disconnects) if that's more useful.

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

A leader that is demoted (`DemoteVoter`, not removed) steps down and, with
`ShutdownOnRemove`, shuts down (`leaderLoop`: `!hasVote(committed,
localID)` sets `stepDown`; log: `removed ourself, shutting down`), though
it remains a member as a non-voter. A demoted follower keeps running. Is
that intended?
