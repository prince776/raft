package raft

// The key-value service's server and its remote clients: what an
// application built on raft exposes to clients on other machines. Each
// node serves requests on port 8000 (one JSON request and response per
// connection). The leader executes them; a follower answers with a
// redirect to the leader it knows, or, on seeds with the feature "servers
// forward to the leader", forwards the request there itself (as Consul
// forwards RPCs). Clients on their own machines keep the last leader they
// learned, follow redirects and retry, with the same operation ID, until
// they get an answer; the FSM applies an ID once, so a retried write that
// had committed isn't applied again.

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/prince776/go-dst-kit/check"
	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
)

const svcPort = "8000"

// svcAddr is the service address of the node named name.
func svcAddr(name string) string { return name + ":" + svcPort }

// svcReq is a client's request: Op "put", "get" (through the log) or
// "rget" (ReadIndex). Forwarded marks a request a follower forwarded.
type svcReq struct {
	ID, Op, Key string
	Forwarded   bool
}

// svcResp is the server's answer. Status "ok" (done), "not-applied" (no
// effect), "unknown" (may take effect), or "redirect" (not the leader;
// Leader is the one this node knows, or empty).
type svcResp struct {
	Status, Leader, Value string
	Found                 bool
}

// serve runs nd's server until its listener closes (the machine crashes).
func (c *simCluster) serve(w *dst.World, nd *dstNode, kc *kvClient) {
	ln, err := w.Net().Listen("tcp", svcAddr(nd.name))
	if err != nil {
		c.h.Fail(err)
		return
	}
	forward := c.h.Feature("servers forward to the leader")
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		w.Go(func() {
			defer conn.Close()
			conn.SetDeadline(w.Now().Add(3 * time.Second))
			var req svcReq
			line, err := bufio.NewReader(conn).ReadString('\n')
			if err != nil || json.Unmarshal([]byte(line), &req) != nil {
				return
			}
			resp := c.execute(w, nd, kc, req, forward)
			b, _ := json.Marshal(resp)
			conn.Write(append(b, '\n'))
		})
	}
}

// execute runs req on nd: on the leader, or by redirecting or forwarding.
func (c *simCluster) execute(w *dst.World, nd *dstNode, kc *kvClient, req svcReq, forward bool) svcResp {
	if nd.raft.State() != Leader {
		leader := ""
		if addr, _ := nd.raft.LeaderWithID(); addr != "" {
			leader, _, _ = strings.Cut(string(addr), ":")
		}
		if forward && leader != "" && !req.Forwarded {
			req.Forwarded = true
			c.h.Sometimes("a server forwarded a request to the leader")
			resp, sent, err := svcCall(w, svcAddr(leader), req, time.Second)
			switch {
			case err == nil:
				return resp
			case sent:
				return svcResp{Status: "unknown"}
			}
		}
		return svcResp{Status: "redirect", Leader: leader}
	}
	var res kvResult
	var o kvOutcome
	switch req.Op {
	case "put":
		o = kc.put(req.ID, req.Key)
	case "get":
		res, o = kc.logGet(req.ID, req.Key)
	default:
		res, o = kc.readIndex(req.Key)
	}
	status := map[kvOutcome]string{kvDone: "ok", kvNotApplied: "not-applied", kvUnknown: "unknown"}[o]
	return svcResp{Status: status, Value: res.Value, Found: res.Found}
}

// svcCall sends req to addr and reads the answer, within timeout. sent
// reports whether the request may have reached the server (so it may have
// taken effect).
func svcCall(w *dst.World, addr string, req svcReq, timeout time.Duration) (resp svcResp, sent bool, err error) {
	ctx, cancel := dst.WithTimeout(w, context.Background(), timeout)
	defer cancel()
	conn, err := w.Net().DialContext(ctx, "tcp", addr)
	if err != nil {
		return svcResp{}, false, err
	}
	defer conn.Close()
	dl, _ := ctx.Deadline()
	conn.SetDeadline(dl)
	b, _ := json.Marshal(req)
	if _, err := conn.Write(append(b, '\n')); err != nil {
		return svcResp{}, true, err
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return svcResp{}, true, err
	}
	if err := json.Unmarshal([]byte(line), &resp); err != nil {
		return svcResp{}, true, err
	}
	return resp, true, nil
}

// remoteClient is client i on its own machine: sequential operations
// until the fault phase ends, each retried until it gets an answer or two
// seconds pass.
func (k *kvWorkload) remoteClient(w *dst.World, i int) {
	h := k.c.h
	id := 1000 + i // history client IDs: apart from the embedded clients'
	var voters []string
	for _, nd := range k.c.nodes {
		if nd.initial {
			voters = append(voters, nd.name)
		}
	}
	target := voters[w.Rand().IntN(len(voters))]
	timeout := 200 * time.Millisecond
	for n := 0; h.Phase() == dsttest.Faulty; n++ {
		opID := fmt.Sprintf("r%d-%d", i, n)
		key := k.keys[w.Rand().IntN(len(k.keys))]
		x := w.Rand().IntN(4)
		req := svcReq{ID: opID, Key: key, Op: []string{"put", "put", "get", "rget"}[x]}
		in := check.KVInput{Op: check.KVGet, Key: key}
		if req.Op == "put" {
			in = check.KVInput{Op: check.KVPut, Key: key, Value: opID}
		}
		call := k.hist.Invoke(id, in)
		maybe := false // an attempt may have taken effect
		outcome, res := kvUnknown, kvResult{}
		deadline := w.Now().Add(2 * time.Second)
	attempts:
		for w.Now().Before(deadline) {
			resp, sent, err := svcCall(w, svcAddr(target), req, timeout)
			if err != nil {
				maybe = maybe || sent
				if timeout *= 2; timeout > 2*time.Second { // raft's min shadows the builtin
					timeout = 2 * time.Second
				}
				target = voters[w.Rand().IntN(len(voters))]
				h.Sometimes("a remote client retried")
				w.Sleep(20 * time.Millisecond)
				continue
			}
			if timeout = timeout * 3 / 4; timeout < 200*time.Millisecond {
				timeout = 200 * time.Millisecond
			}
			switch resp.Status {
			case "ok":
				outcome, res = kvDone, kvResult{Value: resp.Value, Found: resp.Found}
				break attempts
			case "not-applied":
				if !maybe {
					outcome = kvNotApplied
					break attempts
				}
			case "unknown":
				maybe = true
			case "redirect":
				if resp.Leader != "" && resp.Leader != target {
					target = resp.Leader
					h.Sometimes("a remote client followed a redirect")
					continue
				}
				target = voters[w.Rand().IntN(len(voters))]
			}
			w.Sleep(20 * time.Millisecond)
		}
		endCall(call, in, res, outcome, opID, k.acked)
		if outcome == kvDone {
			h.Sometimes("a remote client's operation succeeded")
		}
		w.Sleep(time.Duration(5+w.Rand().IntN(20)) * time.Millisecond)
	}
}
