//go:build dstbugs

package raft

// The simulator's performance baseline (go-dst-kit performance pass): a
// fixed set of seeds from four plain classes, every fix on. It writes each
// seed's fingerprint and step count, and reports the CPU time the sweeps
// took. An optimization of the simulator must leave every fingerprint as
// it was (it may not change what any seed does) and should cut the CPU.
//
// Skipped unless DST_PERF_OUT names the file to write.

import (
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dsttest"
)

func cpuTime() time.Duration {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func TestDSTPerfBaseline(t *testing.T) {
	out := os.Getenv("DST_PERF_OUT")
	if out == "" {
		t.Skip("set DST_PERF_OUT")
	}
	defer func() { activeBug = "" }()
	activeBug = knownFixes
	f, err := os.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	seeds := map[string]int{"Everything": 40, "Membership": 20, "DiskErrors": 20, "Gray": 20}
	var total time.Duration
	var steps uint64
	for _, c := range phase3Classes() {
		n, ok := seeds[c.name]
		if !ok {
			continue
		}
		o := c.opts()
		o.FirstSeed, o.Seeds, o.KeepGoing, o.DeterminismChecks = 1, n, true, -1
		cpu0, wall0 := cpuTime(), time.Now()
		rep := dsttest.Sweep(o, c.sweeps)
		cpu := cpuTime() - cpu0
		total += cpu
		var st uint64
		for _, r := range rep.Results {
			fmt.Fprintf(f, "%s\t%d\t%016x\t%d\t%v\n", c.name, r.Seed, r.Fingerprint, r.Steps, r.Err != nil)
			st += r.Steps
		}
		steps += st
		t.Logf("%-11s %3d seeds: cpu %6.1fs wall %6.1fs, %9d steps, %5.2fµs cpu/step, %d failed",
			c.name, n, cpu.Seconds(), time.Since(wall0).Seconds(), st, float64(cpu.Microseconds())/float64(st), len(rep.Failed))
	}
	t.Logf("total cpu %.1fs, %d steps, %.2fµs cpu/step", total.Seconds(), steps, float64(total.Microseconds())/float64(steps))
}
