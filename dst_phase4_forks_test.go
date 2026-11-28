//go:build dstbugs

package raft

// Phase 4 diagnostic (DST.md 10c): does forking help these bugs at all?
// A plain sweep of upstream raft (every fix off, strict-checks); for up to
// five failing roots per kind, forks at 25%, 50%, 75% and 90% of the steps
// to the failure, five each with fresh randomness. If forks fail the same
// way far more often than fresh seeds do, the state a run is in decides
// the bug and forking can work; if at about the fresh rate, the root's
// draws (shape, configuration, fault mix) decide it, and no choice of fork
// point helps. Skipped unless DST_PHASE4_FORKS=1 (DST_PHASE4_RUNS seeds,
// default 1000).

import (
	"maps"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dsttest"
)

func phase4Kind(err error) string {
	k := phase3Why(err)
	if strings.HasPrefix(k, "other: ") {
		k = "other: " + dsttest.Signature(err)
		if len(k) > 90 {
			k = k[:90]
		}
	}
	return k
}

func TestDSTPhase4Forks(t *testing.T) {
	if os.Getenv("DST_PHASE4_FORKS") != "1" {
		t.Skip("set DST_PHASE4_FORKS=1")
	}
	defer func() { activeBug = "" }()
	activeBug = "strict-checks"
	n := 1000
	if v, err := strconv.Atoi(os.Getenv("DST_PHASE4_RUNS")); err == nil && v > 0 {
		n = v
	}
	o := dsttest.Options{Faults: phase3Faults, FaultPhase: 10 * time.Second, RecoveryPhase: 10 * time.Second,
		FirstSeed: 1, Seeds: n, KeepGoing: true, DeterminismChecks: -1}
	rep := dsttest.Sweep(o, phase3Setup)
	fresh := map[string]int{}
	roots := map[string][]dsttest.Result{}
	for _, r := range rep.Results {
		if r.Err == nil || strings.Contains(r.Err.Error(), "linearizability inconclusive") {
			continue
		}
		k := phase4Kind(r.Err)
		fresh[k]++
		if len(roots[k]) < 5 {
			roots[k] = append(roots[k], r)
		}
	}
	// 0: a fork at step 1 keeps only what setup drew from the root (shape,
	// configuration, fault mix) and re-draws everything after: the control
	// that tells the root's draws from the state reached.
	// Does rarity predict failure? Per run: how many of the states it
	// reached are rare (reached by at most 5 runs of the sweep), and the
	// rarest one's count, failing runs against passing ones.
	reach := map[uint64]int{}
	for _, r := range rep.Results {
		for k := range r.FirstStates {
			reach[k]++
		}
	}
	type agg struct{ runs, rare, rarest int }
	by := map[string]*agg{}
	for _, r := range rep.Results {
		k := "passing"
		if r.Err != nil {
			k = "failing: " + phase4Kind(r.Err)
		}
		a := by[k]
		if a == nil {
			a = &agg{}
			by[k] = a
		}
		a.runs++
		rarest := 1 << 30
		for st := range r.FirstStates {
			if reach[st] <= 5 {
				a.rare++
			}
			if reach[st] < rarest {
				rarest = reach[st]
			}
		}
		a.rarest += rarest
	}
	names := make([]string, 0, len(by))
	for k := range by {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		a := by[k]
		t.Logf("rarity: %-62.62s %4d runs: %.2f rare states per run, rarest reached by %.1f runs on average", k, a.runs, float64(a.rare)/float64(a.runs), float64(a.rarest)/float64(a.runs))
	}
	// Does each progress quantity predict failure? The failure rate among
	// the tenth of runs that pushed it highest, against the overall rate
	// (importance splitting needs a score failures are likelier after).
	failed := 0
	for _, r := range rep.Results {
		if r.Err != nil {
			failed++
		}
	}
	for _, q := range slices.Sorted(maps.Keys(rep.Progress)) {
		rs := slices.Clone(rep.Results)
		sort.SliceStable(rs, func(i, j int) bool { return rs[i].ProgressMax[q] > rs[j].ProgressMax[q] })
		top := rs[:len(rs)/10]
		f := 0
		for _, r := range top {
			if r.Err != nil {
				f++
			}
		}
		t.Logf("progress: %-45s top tenth (>= %d) fail %d/%d (%.1f%%), overall %.1f%%", q, top[len(top)-1].ProgressMax[q], f, len(top),
			100*float64(f)/float64(len(top)), 100*float64(failed)/float64(len(rep.Results)))
	}
	if os.Getenv("DST_PHASE4_NOFORK") == "1" {
		return
	}
	fracs := []float64{0, 0.1, 0.25, 0.5}
	const per = 5
	type job struct {
		kind string
		frac int
		path dsttest.Path
	}
	var jobs []job
	for k, rs := range roots {
		for _, r := range rs {
			for fi, f := range fracs {
				at := max(uint64(float64(r.Steps)*f), 1) // raft's uint64 max
				for i := range per {
					jobs = append(jobs, job{k, fi, r.Path.Then(dsttest.Fork{Step: at, Seed: r.Seed*1000 + uint64(fi*per+i) + 1})})
				}
			}
		}
	}
	same := make([]bool, len(jobs))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	for i, j := range jobs {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if res := dsttest.RunPath(j.path, o, phase3Setup, nil); res.Err != nil && phase4Kind(res.Err) == j.kind {
				same[i] = true
			}
		}()
	}
	wg.Wait()
	kinds := make([]string, 0, len(roots))
	for k := range roots {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	for _, k := range kinds {
		hits := make([]int, len(fracs))
		tot := make([]int, len(fracs))
		for i, j := range jobs {
			if j.kind != k {
				continue
			}
			tot[j.frac]++
			if same[i] {
				hits[j.frac]++
			}
		}
		var b strings.Builder
		for fi := range fracs {
			b.WriteString(" ")
			b.WriteString(strconv.Itoa(int(fracs[fi] * 100)))
			b.WriteString("%: ")
			b.WriteString(strconv.Itoa(hits[fi]))
			b.WriteString("/")
			b.WriteString(strconv.Itoa(tot[fi]))
		}
		t.Logf("%-60.60s fresh %3d/%d (%.1f%%); forks failing the same way:%s", k, fresh[k], rep.Runs, 100*float64(fresh[k])/float64(rep.Runs), b.String())
	}
}
