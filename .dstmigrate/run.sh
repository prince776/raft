#!/bin/sh
# Re-run the raft migration from the pre-migration commit (f9cf2a8): the
# tool, then the hand edits. The simulation tests (dst_*_test.go, written
# against the migrated code) are set aside meanwhile.
# Usage: .dstmigrate/run.sh  (from the repo root)
set -e
# dstmigrate: $DSTMIGRATE, or built from the go-dst-kit checkout next to
# this one (the go.work's).
if [ -z "$DSTMIGRATE" ]; then
	DSTMIGRATE=$(mktemp -d)/dstmigrate
	(cd ../go-dst-kit/dstlint && GOTOOLCHAIN=go1.25.0 go build -o "$DSTMIGRATE" ./cmd/dstmigrate)
fi
BASE=f9cf2a8
mkdir -p .dstmigrate/aside
for f in dst_*_test.go dst_test.go dst_maporder.go dstfix.go dstbugs.go dstbugs_on.go; do [ -f $f ] && mv $f .dstmigrate/aside/; done
rm -f dst_recv.go
git checkout -q $BASE -- $(git ls-tree -r --name-only $BASE | grep '\.go$')
GOTOOLCHAIN=go1.25.0 "$DSTMIGRATE" -tests -test-world 'theRealWorld()' -real-fs '*_test.go,peersjson.go,testing.go' \
  -world 'Raft=$.w,raftState=$.w,NetworkTransport=$.w,netPipeline=$.trans.w,InmemTransport=$.w,inmemPipeline=$.trans.w,cluster=$.w,snapshotRestoreMonitor=$.w,deferError=$.w,followerReplication=$.w,saturationMetric=$.w,FileSnapshotStore=$.world(),FileSnapshotSink=$.store.world()' . 2>&1 | grep -v '^rewrote' || true
python3 .dstmigrate/manual.py
mv .dstmigrate/aside/*.go . 2>/dev/null || true
gofmt -w $(git ls-files "*.go") dst_recv.go
git reset -q  # run.sh checked files out of $BASE, which staged them; unstage
