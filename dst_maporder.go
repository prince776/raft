package raft

import (
	"cmp"
	"maps"
	"slices"

	"github.com/prince776/go-dst-kit/dst"
)

// Map iteration order is random in Go, and several of raft's loops start
// or stop goroutines or send in that order. In a simulated World the order
// must come from the World's randomness (so a run replays) and still vary
// (so order-dependent bugs can be found): these helpers sort the keys and
// shuffle them with the World's "map-order" stream (drawing from it shifts
// no other random choice). In the real World they keep Go's order.

// mapOrder is m's keys in the order a range over m visits them.
func mapOrder[K cmp.Ordered, V any](w *dst.World, m map[K]V) []K {
	if w == nil || !w.Simulated() {
		return slices.Collect(maps.Keys(m))
	}
	keys := slices.Sorted(maps.Keys(m))
	shuffle(w, keys)
	return keys
}

// mapValues is m's values in mapOrder.
func mapValues[K cmp.Ordered, V any](w *dst.World, m map[K]V) []V {
	keys := mapOrder(w, m)
	vals := make([]V, len(keys))
	for i, k := range keys {
		vals[i] = m[k]
	}
	return vals
}

// futureOrder is a set of verify futures in range order: by creation
// (seq), shuffled, in a simulated World.
func futureOrder(w *dst.World, m map[*verifyFuture]struct{}) []*verifyFuture {
	fs := slices.Collect(maps.Keys(m))
	if w == nil || !w.Simulated() {
		return fs
	}
	slices.SortFunc(fs, func(a, b *verifyFuture) int { return cmp.Compare(a.seq, b.seq) })
	shuffle(w, fs)
	return fs
}

func shuffle[T any](w *dst.World, s []T) {
	w.Rand().Stream("map-order").Shuffle(len(s), func(i, j int) { s[i], s[j] = s[j], s[i] })
}
