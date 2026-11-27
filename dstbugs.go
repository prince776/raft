//go:build !dstbugs

package raft

// bug reports whether the known bug name is re-inserted, for the
// deterministic simulation benchmark (dstbench.json). Only a build with
// the dstbugs tag can re-insert one: here it is always false.
func bug(name string) bool { return false }
