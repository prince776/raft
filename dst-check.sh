#!/bin/sh
# The simulation's checks, in two tiers (DST.md, "How to run").
#   ./dst-check.sh quick   after every change: vet, small sweeps, the
#                          contract, planted-bug and scripted-bug tests
#   ./dst-check.sh full    before a commit, in the background: every sweep
#                          at 200 seeds, the fixed sweeps at 400
# Sweeps use at most 4 cores (GOMAXPROCS), so two agents' sweeps can share
# the machine.
set -e
export GOTOOLCHAIN=go1.25.0 GOMAXPROCS=${GOMAXPROCS:-4}
cd "$(dirname "$0")"
scripted='TestDSTChecksCatchBugs|TestDSTStaleConfigStall|TestDSTQuorumOffByOneScripted|TestBootstrapCrash|TestSnapshotBoundaryScripted|TestDSTCatchesEntryAppliedTwice'
case "${1:-quick}" in
quick)
	go vet . && go vet -tags dstbugs .
	go test -count=1 -run '^TestDST(Smoke|Everything|Membership|Gray)$' . -dst.seeds=20
	go test -count=1 -run '^TestDST(LogStoreContract|SnapshotStoreContract|CatchesUnsyncedStore|CatchesNaiveReadIndex)$' . -dst.seeds=50
	go test -tags dstbugs -count=1 -run "^($scripted)\$" .
	;;
full)
	go vet . && go vet -tags dstbugs .
	go test -count=1 -timeout 0 -run 'TestDST' . -dst.seeds=200
	go test -tags dstbugs -count=1 -timeout 0 -run "^($scripted)\$" .
	go test -tags dstbugs -count=1 -timeout 0 -run '^(TestDSTMembershipFixed|TestDSTMembershipGrayFixed|TestDSTSnapshotBoundary)$' . -dst.seeds=400
	;;
*)
	echo "usage: $0 [quick|full]" >&2
	exit 2
	;;
esac
