package raft

import "github.com/prince776/go-dst-kit/dst"

// dstRecv is <-c where only the value is used (written by dstmigrate).
func dstRecv[T any](c dst.Chan[T]) T { v, _ := c.Recv(); return v }
