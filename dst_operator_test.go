package raft

// The membership operator of a simulated cluster (dst_cluster_test.go):
// what Consul's autopilot or a person does with raft's membership API.
// It runs on every node and acts while its node leads, one change at a
// time: it replaces servers the runner lost for good (Faults.Loss; it is
// told by a ticket, as a person is by an alert), keeps the voter count at
// the cluster's size by adding spares as non-voters and promoting them
// once healthy (up and caught up, as autopilot does), and otherwise makes
// random changes: add a spare, promote, demote, gracefully remove (then,
// half the time, lose the removed server's machine: decommissioned; else
// it stays, its raft shut down by the removal, ShutdownOnRemove, until a
// crash restarts it and it can be a spare again).
//
// Every change and its outcome (applied, failed, or unknown) goes in a
// history that is checked for linearizability against raft's own
// semantics of the change (memberModel), with the final configuration read
// at the end; and the members must agree on that configuration.

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/prince776/go-dst-kit/check"
	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
)

// memberIn is a change to one server's membership (Command), or a read of
// it (Read).
type memberIn struct {
	Command ConfigurationChangeCommand
	Read    bool
	ID      ServerID
}

// memberOut is what a read found: "voter", "nonvoter", or "" (not a member).
type memberOut struct{ State string }

// memberModel is one server's membership under raft's changes
// (nextConfiguration): AddVoter makes it a voter; AddNonvoter makes it a
// non-voter unless it is already a voter; DemoteVoter makes a member a
// non-voter; RemoveServer removes it. Partitioned by server.
func memberModel() check.Model[string, memberIn, memberOut] {
	return check.Model[string, memberIn, memberOut]{
		Init: func() string { return "" },
		Step: func(s string, in memberIn, out memberOut, known bool) (bool, string) {
			if in.Read {
				return !known || out.State == s, s
			}
			switch in.Command {
			case AddVoter:
				return true, "voter"
			case AddNonvoter:
				if s == "voter" {
					return true, s
				}
				return true, "nonvoter"
			case DemoteVoter:
				if s == "" {
					return true, s
				}
				return true, "nonvoter"
			case RemoveServer:
				return true, ""
			}
			return false, s
		},
		Partition: func(in memberIn) string { return string(in.ID) },
		Describe: func(in memberIn, out memberOut, known bool) string {
			if in.Read {
				return fmt.Sprintf("read(%s) -> %q", in.ID, out.State)
			}
			mark := ""
			if !known {
				mark = " ?"
			}
			return fmt.Sprintf("%v(%s)%s", in.Command, in.ID, mark)
		},
	}
}

type operator struct {
	c       *simCluster
	hist    *check.History[memberIn, memberOut]
	tickets []ServerID // servers lost for good, to remove, oldest first
	clients int
	seen    map[ServerID]bool // every server a change or the bootstrap named
}

func newOperator(c *simCluster, initial []Server) *operator {
	o := &operator{c: c, hist: check.NewHistory[memberIn, memberOut](c.h.World()), seen: map[ServerID]bool{}}
	// The bootstrap configuration, as changes that returned at once.
	for _, s := range initial {
		o.seen[s.ID] = true
		o.hist.Invoke(0, memberIn{Command: AddVoter, ID: s.ID}).Return(memberOut{})
	}
	c.h.OnLoss(o.canLose, o.lost)
	return o
}

// canLose keeps losses survivable: one at a time (the last one replaced
// first), and a voter only while more than half the voters would still be
// alive. It reads the configuration of the current leader (none: no loss).
func (o *operator) canLose(m *dsttest.Machine) bool {
	l := o.c.leader()
	if l == nil || len(o.tickets) > 0 {
		return false
	}
	nd := o.c.node(ServerID(m.Name()))
	// The survivors must hold a majority of every configuration that may
	// be in effect: the latest, and the committed one, since the latest
	// may not have reached anyone yet (losing a leader whose additions
	// were still unreplicated left a cluster of one lost voter).
	for _, cfg := range []Configuration{l.raft.configurations.latest, l.raft.configurations.committed} {
		if !hasVote(cfg, nd.id()) {
			continue // a non-voter or a machine outside it
		}
		voters, alive := 0, 0
		for _, s := range cfg.Servers {
			if s.Suffrage == Voter {
				voters++
				if n := o.c.node(s.ID); n != nil && !n.m.Lost() {
					alive++
				}
			}
		}
		if alive-1 <= voters/2 {
			return false
		}
	}
	return true
}

// lost takes the runner's ticket for a machine it lost for good.
func (o *operator) lost(m *dsttest.Machine) {
	o.tickets = append(o.tickets, ServerID(m.Name()))
	o.c.h.Sometimes("a server was lost for good")
}

// run is one boot's operator on nd: while the node leads, a change every
// 100 to 400ms, until the fault phase ends.
func (o *operator) run(w *dst.World, nd *dstNode) {
	h := o.c.h
	o.clients++
	cid := o.clients
	for h.Phase() == dsttest.Faulty {
		w.Sleep(time.Duration(100+w.Rand().IntN(300)) * time.Millisecond)
		if h.Phase() != dsttest.Faulty || nd.raft.State() != Leader {
			continue
		}
		f := nd.raft.GetConfiguration()
		if f.Error() != nil {
			continue
		}
		o.step(w, nd, cid, f.Configuration())
	}
}

// step makes one change, chosen from cfg (the leader's latest).
func (o *operator) step(w *dst.World, nd *dstNode, cid int, cfg Configuration) {
	var voters, nonvoters, spares []ServerID
	for _, s := range cfg.Servers {
		if s.Suffrage == Voter {
			voters = append(voters, s.ID)
		} else {
			nonvoters = append(nonvoters, s.ID)
		}
	}
	// A spare is a machine outside the cluster with raft running (a
	// removed server shuts its raft down, ShutdownOnRemove, until its
	// machine restarts).
	for _, n := range o.c.nodes {
		if !n.m.Lost() && !inConfiguration(cfg, n.id()) && n.up() && n.raft.getState() != Shutdown {
			spares = append(spares, n.id())
		}
	}
	// A non-voter is promoted only once healthy, as autopilot does: up,
	// and caught up to the leader's commit index.
	commit := nd.raft.getCommitIndex()
	var healthy []ServerID
	for _, id := range nonvoters {
		if n := o.c.node(id); n.up() && n.raft.getState() != Shutdown && n.raft.getLastIndex() >= commit {
			healthy = append(healthy, id)
		}
	}
	pick := func(ids []ServerID) ServerID { return ids[w.Rand().IntN(len(ids))] }
	// A voluntary demotion or removal of a voter keeps a minimum quorum, as
	// autopilot does: at least 3 voters remain (a cluster that started
	// smaller may grow, but doesn't shrink back), and a live majority of
	// them. (Below three voters a change that the
	// leader fails to commit can stall the cluster for good: with voters
	// {A, B}, A appends "demote A" and fails; A, a non-voter in its own
	// latest configuration, won't campaign, and B, behind A, can't get A's
	// vote. That's Raft's single-server changes, not a bug.)
	canDrop := func(id ServerID) bool {
		alive := 0
		for _, v := range voters {
			if v != id && !o.c.node(v).m.Lost() {
				alive++
			}
		}
		return len(voters)-1 >= 3 && alive > (len(voters)-1)/2
	}
	// A lost server first: remove it.
	for len(o.tickets) > 0 {
		id := o.tickets[0]
		if !inConfiguration(cfg, id) {
			o.tickets = o.tickets[1:]
			o.c.h.Sometimes("a lost server was removed")
			continue
		}
		o.change(nd, cid, RemoveServer, id)
		return
	}
	switch target := o.c.spec.voters; {
	case len(voters) < target && len(healthy) > 0:
		o.change(nd, cid, AddVoter, pick(healthy)) // promote
	case len(voters) < target && len(nonvoters) == 0 && len(spares) > 0:
		o.change(nd, cid, AddNonvoter, pick(spares)) // to promote once added
	default:
		// A random change, sometimes none.
		switch x := w.Rand().IntN(10); {
		case x == 0 && len(spares) > 0:
			o.change(nd, cid, AddNonvoter, pick(spares))
		case x == 1 && len(healthy) > 0:
			o.change(nd, cid, AddVoter, pick(healthy))
		case x == 2 && len(voters) > 0:
			if id := pick(voters); canDrop(id) {
				o.change(nd, cid, DemoteVoter, id)
			}
		case x == 4 && len(spares) >= 2:
			// Two changes at once (two operators, or autopilot and a
			// person): add two voters. Raft takes one at a time; the other
			// waits, and may time out.
			a, b := spares[0], spares[1]
			o.together(w, nd, cid, func(id int) { o.change(nd, id, AddVoter, a) }, func(id int) { o.change(nd, id, AddVoter, b) })
		case x == 5 && len(voters) >= 2:
			// Remove two voters at once, if both may go.
			a, b := voters[0], voters[len(voters)-1]
			if a == nd.id() || b == nd.id() || !canDrop(a) || len(voters)-2 < 3 {
				return
			}
			o.together(w, nd, cid, func(id int) { o.change(nd, id, RemoveServer, a) }, func(id int) { o.change(nd, id, RemoveServer, b) })
		case x == 3:
			all := append(slices.Clone(voters), nonvoters...)
			if len(all) == 0 {
				return
			}
			id := pick(all)
			if hasVote(cfg, id) && !canDrop(id) {
				return
			}
			if o.change(nd, cid, RemoveServer, id) && w.Rand().IntN(2) == 0 {
				// Decommission it: a removed server shut down for good.
				// (The other half keep running outside the cluster.)
				o.c.h.Sometimes("a removed server was shut down")
				o.c.node(id).m.Lose()
			}
		}
	}
}

// together runs two changes at once, as two clients, and waits for both.
func (o *operator) together(w *dst.World, nd *dstNode, cid int, first, second func(client int)) {
	o.clients++
	other := o.clients
	done := dst.MakeChan[struct{}](w, 1)
	w.Go(func() {
		second(other)
		done.Send(struct{}{})
	})
	first(cid)
	done.Recv()
	o.c.h.Sometimes("two membership changes at once")
}

// change asks nd's raft for one change, records it with its outcome, and
// reports whether it was applied.
func (o *operator) change(nd *dstNode, cid int, cmd ConfigurationChangeCommand, id ServerID) bool {
	h, r := o.c.h, nd.raft
	o.seen[id] = true
	call := o.hist.Invoke(cid, memberIn{Command: cmd, ID: id})
	const timeout = 500 * time.Millisecond
	var f IndexFuture
	switch cmd {
	case AddVoter:
		f = r.AddVoter(id, o.c.node(id).addr(), 0, timeout)
	case AddNonvoter:
		f = r.AddNonvoter(id, o.c.node(id).addr(), 0, timeout)
	case DemoteVoter:
		f = r.DemoteVoter(id, 0, timeout)
	case RemoveServer:
		f = r.RemoveServer(id, 0, timeout)
	}
	err := f.Error()
	switch {
	case err == nil:
		call.Return(memberOut{})
		h.Sometimes("membership: " + cmd.String())
		return true
	case changeNotApplied(err):
		call.Fail()
		h.Sometimes("a membership change was refused")
	default:
		call.Abandon() // it may still commit
		h.Sometimes("a membership change's outcome is unknown")
	}
	return false
}

// changeNotApplied: errors raft returns before a change enters the log.
func changeNotApplied(err error) bool {
	if errors.Is(err, ErrNotLeader) || errors.Is(err, ErrEnqueueTimeout) || errors.Is(err, ErrRaftShutdown) {
		return true
	}
	msg := err.Error()
	for _, s := range []string{"configuration changed since", "need at least one voter", "found duplicate", "empty ID", "empty address"} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// final is the configuration at the end: the leader's committed one.
func (o *operator) final() (Configuration, error) {
	l := o.c.leader()
	if l == nil {
		return Configuration{}, errors.New("no leader at the end")
	}
	return l.raft.configurations.committed, nil
}

// agree: every running member's committed configuration is the leader's
// (a member whose raft shut down doesn't take part, so isn't asked).
// agree waits, up to the cluster's patience, for every running member to
// hold the leader's committed configuration: a follower behind a slow link
// lags by its round trips, so a single look would fail a correct cluster.
func (o *operator) agree(w *dst.World) error {
	deadline := w.Now().Add(o.c.patience())
	for {
		err := o.agreeNow()
		if err == nil || w.Now().After(deadline) {
			return err
		}
		w.Sleep(50 * time.Millisecond)
	}
}

func (o *operator) agreeNow() error {
	cfg, err := o.final()
	if err != nil {
		return err
	}
	for _, s := range cfg.Servers {
		nd := o.c.node(s.ID)
		if nd == nil || !nd.up() {
			continue
		}
		if nd.raft.getState() == Shutdown {
			// A leader that loses its vote shuts down with ShutdownOnRemove,
			// demoted as well as removed (raft.go, leaderLoop): it is still
			// listed, as a non-voter, but no longer takes part.
			o.c.h.Sometimes("a member's raft was shut down")
			continue
		}
		if got := nd.raft.configurations.committed; !slices.Equal(got.Servers, cfg.Servers) {
			return fmt.Errorf("%s's committed configuration %v isn't the leader's %v:\n%s", nd.name, got.Servers, cfg.Servers, o.c.describe())
		}
	}
	return nil
}

// linearizable reads every server's final membership into the history,
// then checks it.
func (o *operator) linearizable() error {
	cfg, err := o.final()
	if err != nil {
		return err
	}
	for _, id := range slices.Sorted(maps.Keys(o.seen)) {
		state := ""
		for _, s := range cfg.Servers {
			if s.ID == id {
				state = map[ServerSuffrage]string{Voter: "voter", Nonvoter: "nonvoter"}[s.Suffrage]
			}
		}
		o.hist.Invoke(-1, memberIn{Read: true, ID: id}).Return(memberOut{State: state})
	}
	return check.Linearizable(memberModel(), o.hist.Ops(), check.Options{}).Err()
}
