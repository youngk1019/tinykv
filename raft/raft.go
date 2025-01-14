// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout       int
	actualElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		log.Errorf("storage initial state error :%s", err.Error())
		panic(err)
	}
	prs := make(map[uint64]*Progress)
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	raftLog := newLog(c.Storage)
	lastIndex := raftLog.LastIndex()
	for _, peer := range c.peers {
		prs[peer] = &Progress{
			Match: 0,
			Next:  lastIndex + 1,
		}
	}

	return &Raft{
		id:                    c.ID,
		Term:                  hardState.Term,
		Vote:                  hardState.Vote,
		RaftLog:               raftLog,
		Prs:                   prs,
		State:                 StateFollower,
		votes:                 make(map[uint64]bool),
		msgs:                  make([]pb.Message, 0),
		Lead:                  None,
		heartbeatTimeout:      c.HeartbeatTick,
		electionTimeout:       c.ElectionTick,
		actualElectionTimeout: c.ElectionTick + rand.Intn(c.ElectionTick),
	}
}

func (r *Raft) lastIndex() uint64 {
	return r.RaftLog.LastIndex()
}

func (r *Raft) lastTerm() uint64 {
	lastTerm, err := r.RaftLog.Term(r.lastIndex())
	if err != nil {
		log.Errorf("raft log term :%s", err.Error())
		panic(err)
	}
	return lastTerm
}

func (r *Raft) lastIndexAndTerm() (uint64, uint64) {
	lastIndex := r.lastIndex()
	lastTerm, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		log.Errorf("raft log term :%s", err.Error())
		panic(err)
	}
	return lastIndex, lastTerm
}

func (r *Raft) isFollower() bool {
	return r.State == StateFollower
}

func (r *Raft) isCandidate() bool {
	return r.State == StateCandidate
}

func (r *Raft) isLeader() bool {
	return r.State == StateLeader
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	preIndex := r.Prs[to].Next - 1
	preLogTerm, err := r.RaftLog.Term(preIndex)
	if err != nil {
		r.sendSnapshot(to)
		return false
	}
	entries, err := r.RaftLog.EntriesFrom(preIndex + 1)
	if err != nil {
		r.sendSnapshot(to)
		return false
	}
	sendEntries := make([]*pb.Entry, len(entries))
	for i := range entries {
		sendEntries[i] = &entries[i]
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: preLogTerm,
		Index:   preIndex,
		Entries: sendEntries,
		Commit:  r.RaftLog.committed,
	})
	return true
}

func (r *Raft) sendSnapshot(to uint64) {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		Snapshot: &snapshot,
	})
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

func (r *Raft) sendAppendResponse(to uint64, reject bool) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
		Index:   r.lastIndex(),
	})
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  min(r.RaftLog.committed, r.Prs[to].Match),
	})
}

func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	})
}

func (r *Raft) sendRequestVote(to, logTerm, index uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: logTerm,
		Index:   index,
	})
}

func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	})
}

func (r *Raft) isMoreUpToDate(logTerm, index uint64) bool {
	lastIndex, lastTerm := r.lastIndexAndTerm()
	if lastTerm > logTerm || (lastTerm == logTerm && lastIndex > index) {
		return true
	}
	return false
}

func (r *Raft) updateCommit() {
	updated := false
	for i := r.RaftLog.committed + 1; i <= r.lastIndex(); i++ {
		matchCnt := 0
		for _, peer := range r.Prs {
			if peer.Match >= i {
				matchCnt++
			}
		}
		term, _ := r.RaftLog.Term(i)
		if matchCnt > len(r.Prs)/2 && term == r.Term {
			r.RaftLog.committed = i
			updated = true
		}
	}
	if updated {
		r.handleBroadcastAppend()
	}
}

func (r *Raft) refreshElectionTimeout() {
	r.actualElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

func (r *Raft) resetTick() {
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.refreshElectionTimeout()
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.tickElection()
	case StateLeader:
		r.tickHeartbeat()
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed += 1
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		err := r.Step(pb.Message{
			MsgType: pb.MessageType_MsgBeat,
		})
		if err != nil {
			log.Errorf("step message beat :%s", err.Error())
			panic(err)
		}
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.actualElectionTimeout {
		r.electionElapsed = 0
		err := r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
		})
		if err != nil {
			log.Errorf("step message hup :%s", err.Error())
			panic(err)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = None
	r.resetTick()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.Lead = None
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.resetTick()
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	lastIndex := r.RaftLog.LastIndex()
	for _, p := range r.Prs {
		p.Match = 0
		p.Next = lastIndex + 1
	}
	r.resetTick()
	// propose a noop entry
	_ = r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		To:      r.id,
		From:    r.id,
		Term:    r.Term,
		Entries: []*pb.Entry{
			{
				EntryType: pb.EntryType_EntryNormal,
				Term:      r.Term,
				Index:     lastIndex + 1,
				Data:      nil,
			},
		},
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleDoElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleDoElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		r.handleBroadcastHeartbeat()
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	}
	return nil
}

func (r *Raft) handleDoElection() {
	r.becomeCandidate()
	lastIndex, lastTerm := r.lastIndexAndTerm()
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendRequestVote(peer, lastTerm, lastIndex)
	}
}

func (r *Raft) handleBroadcastHeartbeat() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendHeartbeat(peer)
	}
}

func (r *Raft) handleBroadcastAppend() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
	}
	// Reply false if term < currentTerm (§5.1)
	if r.Term > m.Term {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	if r.Vote != None && r.Vote != m.From {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	if r.isMoreUpToDate(m.LogTerm, m.Index) {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	r.Vote = m.From
	r.sendRequestVoteResponse(m.From, false)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if r.Term > m.Term {
		return
	}
	r.votes[m.From] = !m.Reject
	grant, reject := 0, 0
	for _, v := range r.votes {
		if v {
			grant++
		} else {
			reject++
		}
	}
	if grant > len(r.Prs)/2 {
		r.becomeLeader()
	} else if reject > len(r.Prs)/2 {
		r.becomeFollower(r.Term, None)
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	offset := r.lastIndex() + 1
	for i, e := range m.Entries {
		e.Index = offset + uint64(i)
		e.Term = r.Term
	}
	r.RaftLog.AppendEntries(m.Entries...)
	lastIndex := r.lastIndex()
	r.Prs[r.id].Next = lastIndex + 1
	r.Prs[r.id].Match = lastIndex
	r.handleBroadcastAppend()
	r.updateCommit()
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// Reply false if term < currentTerm (§5.1)
	if r.Term > m.Term {
		r.sendAppendResponse(m.From, true)
		return
	}
	r.becomeFollower(m.Term, m.From)
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		r.sendAppendResponse(m.From, true)
		return
	}
	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	if len(m.Entries) > 0 {
		idx := 0
		lastIndex := r.lastIndex()
		for i, ent := range m.Entries {
			idx = i
			if ent.Index > lastIndex {
				break
			}
			entTerm, err := r.RaftLog.Term(ent.Index)
			if err != nil || entTerm != ent.Term {
				r.RaftLog.Truncate(ent.Index)
				r.RaftLog.stabled = min(r.RaftLog.stabled, ent.Index-1)
				r.RaftLog.committed = min(r.RaftLog.committed, ent.Index-1)
				r.RaftLog.applied = min(r.RaftLog.applied, ent.Index-1)
				break
			}
		}
		if m.Entries[idx].Index > r.lastIndex() {
			r.RaftLog.AppendEntries(m.Entries[idx:]...)
		}
	}
	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if m.Commit > r.RaftLog.committed {
		lastNewEntryIndex := m.Index
		if len(m.Entries) > 0 {
			lastNewEntryIndex = m.Entries[len(m.Entries)-1].Index
		}
		r.RaftLog.committed = min(m.Commit, lastNewEntryIndex)
	}
	r.sendAppendResponse(m.From, false)
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
	}
	if m.Reject {
		r.Prs[m.From].Next -= 1
		r.sendAppend(m.From)
		return
	} else {
		r.Prs[m.From].Next = m.Index + 1
		r.Prs[m.From].Match = m.Index
		r.updateCommit()
	}

}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// Reply false if term < currentTerm (§5.1)
	if r.Term > m.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	r.becomeFollower(m.Term, m.From)
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	r.RaftLog.committed = min(m.Commit, r.lastIndex())
	r.sendHeartbeatResponse(m.From, false)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if r.isMoreUpToDate(m.LogTerm, m.Index) {
		r.sendAppend(m.From)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	meta := m.Snapshot.Metadata
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, true)
	} else if r.RaftLog.committed >= meta.Index {
		r.sendAppendResponse(m.From, false)
	} else {
		r.becomeFollower(m.Term, m.From)
		r.RaftLog.firstIndex = meta.Index + 1
		r.RaftLog.committed = meta.Index
		r.RaftLog.applied = meta.Index
		r.RaftLog.stabled = meta.Index
		r.RaftLog.pendingSnapshot = m.Snapshot
		r.RaftLog.entries = make([]pb.Entry, 0)
		r.Prs = make(map[uint64]*Progress)
		for _, id := range meta.ConfState.Nodes {
			r.Prs[id] = &Progress{Next: r.RaftLog.LastIndex() + 1}
		}
		r.sendAppendResponse(m.From, false)
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
