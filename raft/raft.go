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
	"log"
	mrand "math/rand"
	"sort"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0
const DEBUG bool = true

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
	electionTimeout int

	//
	randomElectionTimeout int

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
	r := &Raft{
		id:               c.ID,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		State:            StateFollower,
		votes:            make(map[uint64]bool),
	}

	//这里有读取配置的
	hardSt, confSt, _ := r.RaftLog.storage.InitialState()

	if c.peers == nil {
		c.peers = confSt.Nodes
	}

	lastIndex := r.RaftLog.LastIndex()

	//[question]为什么需要区别对待
	for _, p := range c.peers {
		if p != r.id {
			r.Prs[p] = &Progress{Match: lastIndex, Next: lastIndex + 1}
		} else {
			r.Prs[p] = &Progress{Next: lastIndex + 1}
		}
	}

	r.becomeFollower(0, None)
	r.randomElectionTimeout = r.electionTimeout + mrand.Intn(r.electionTimeout)
	r.Term = hardSt.GetTerm()
	r.Vote = hardSt.GetVote()
	r.RaftLog.committed = hardSt.GetCommit()
	return r
}

//sendSnapshot
func (r *Raft) sendSnapshot(to uint64) bool {

	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		r.DebugPrint("snapshot is not ready yet. %v", err)
		return false
	}
	r.DebugPrint("Sending meta snapshot to %d", to)

	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snapshot,
	}
	r.msgs = append(r.msgs, msg)
	return false
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).

	prevIndex := r.Prs[to].Match
	prevTerm, err := r.RaftLog.Term(prevIndex)

	if err != nil {
		if err == ErrCompacted {
			r.sendSnapshot(to)
			return false
		}
		log.Panicln(err)
	}

	entries := []*pb.Entry{}

	var n uint64
	n = uint64(len(r.RaftLog.entries))

	//impossible for err not to be nil
	for i, err := r.RaftLog.GetEntrieIndex(prevIndex + 1); i < n; i++ {
		if err != nil {
			log.Panicln(err)
		}
		entries = append(entries, &r.RaftLog.entries[i])
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: prevTerm,
		Index:   prevIndex,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}

	r.msgs = append(r.msgs, msg)
	r.DebugPrint("send append request to %d,logTerm:%d,lastindex:%d,entries:%d", to, prevTerm, prevIndex,
		len(entries))

	return true
}

//send transfer leader msg
func (r *Raft) sendTransferLeader(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).

	//// heartbeat dont need the following info while electionRequest do.
	// prevIndex := r.RaftLog.LastIndex()
	//impossible to get index which can't locate its term
	// prevTerm, err := r.RaftLog.Term(prevIndex)

	// if err != nil {
	// 	log.Panicln(err)
	// }

	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}

	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
		Term:    r.Term,
		From:    r.id,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)

}

func (r *Raft) sendRequestVote(to, lastIndex, lastLogTerm uint64) {

	msg := pb.Message{

		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: lastLogTerm,
		Index:   lastIndex,
	}

	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {

	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendAppendResponse(to, term, index uint64, reject bool) {

	//返回term和index作为快速定位日志的逻辑

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: term,
		Index:   index,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) appendEntries(entries []*pb.Entry) {

	if r.leadTransferee != None {
		r.DebugPrint("transfering leadership. Ignore all proposal")
		return
	}

	index := r.Prs[r.id].Next
	for _, entry := range entries {
		entry.Index = index
		entry.Term = r.Term
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		index++
	}

	r.DebugPrint("append log to raftLog.  Before:Match:%d,Next:%d After:Match:%d,Next:%d",
		r.Prs[r.id].Match, r.Prs[r.id].Next, r.RaftLog.LastIndex(), r.Prs[r.id].Match+1)

	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	r.broadcastAppend()
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

func (r *Raft) broadcastHeartbeat() {
	for ind := range r.Prs {
		if ind != r.id {
			r.sendHeartbeat(ind)
		}
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.tickElection()
	case StateCandidate:
		r.tickElection()
	case StateLeader:
		r.tickHeartBeat()

	}
}

func (r *Raft) tickHeartBeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {

	// Your Code Here (2A).

	//称为follower都伴随着term的增大，因此可以在这个阶段进行状态的重置
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = None
	r.leadTransferee = None

}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Lead = None
	r.Term++
	r.Vote = r.id

	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.DebugPrint("me becomes candidate. term increases to %d", r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id

	lastIndex := r.RaftLog.LastIndex()

	//match是什么意思？之前的实践当中没有遇到的match的
	for ind := range r.Prs {
		if ind != r.id {
			r.Prs[ind].Next = lastIndex + 1
		} else {
			r.Prs[ind].Next = lastIndex + 2
			r.Prs[ind].Match = lastIndex + 1
		}
		// heartBeat等超时进行发送？
		// r.sendHeartbeat(ind)
	}
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: lastIndex + 1})
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}

	r.DebugPrint("New leader. Append new empty entry.")
	r.broadcastAppend()
	r.heartbeatElapsed = 0

}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

	// //如果peers为0，是新创建的节点，需要快照
	// //检查是否是合理的id，
	// if len(r.Prs) > 0 {
	// 	if _, ok := r.Prs[r.id]; !ok {
	// 		return nil
	// 	}
	// }

	//检查term是否是最新的，否则降级为follower
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
	}

	switch r.State {
	case StateFollower:
		r.FollowerStep(m)
	case StateCandidate:
		r.CandidateStep(m)
	case StateLeader:
		r.LeaderStep(m)
	}
	return nil
}

func (r *Raft) FollowerStep(m pb.Message) error {

	switch m.MsgType {

	//超时开始选举
	case pb.MessageType_MsgHup:
		r.doElection()
	case pb.MessageType_MsgPropose:
		//心跳处理
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:

		//处理投票处理
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:

	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		// r.handleRequestVote(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
		//compact log
	case pb.MessageType_MsgCompactLog:
		r.hanldeCompactLog(m)
	case pb.MessageType_MsgTimeoutNow, pb.MessageType_MsgTransferLeader:
		r.DebugPrint("leadership transfered from %d", m.From)
		r.doElection()
	}

	return nil

}

func (r *Raft) CandidateStep(m pb.Message) error {

	switch m.MsgType {

	case pb.MessageType_MsgHup:
		r.doElection()
	case pb.MessageType_MsgPropose:
		//心跳处理
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)

	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)

	case pb.MessageType_MsgAppend:
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:

	}
	return nil
}

func (r *Raft) LeaderStep(m pb.Message) error {

	switch m.MsgType {

	//心跳处理--但是可能可以在上层进行拦截
	case pb.MessageType_MsgHup:

	case pb.MessageType_MsgPropose:
		r.appendEntries(m.Entries)

	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	case pb.MessageType_MsgBeat:
		r.broadcastHeartbeat()
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgCompactLog:
		r.hanldeCompactLog(m)
	}

	return nil

}

func (r *Raft) doElection() {

	r.becomeCandidate()
	r.heartbeatTimeout = 0
	r.randomElectionTimeout = r.electionTimeout + mrand.Intn(r.electionTimeout)

	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}

	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)

	for peer := range r.Prs {
		if peer != r.id {
			r.DebugPrint("send vote request to peer %d", peer)
			r.sendRequestVote(peer, lastIndex, lastTerm)
		}
	}
	r.DebugPrint("send vote request.%v", r.msgs)

}

func (r *Raft) broadcastAppend() {
	for ind := range r.Prs {
		if ind != r.id {
			r.sendAppend(ind)
		}
	}
}

func (r *Raft) leaderCommit() {

	if len(r.Prs) == 0 {
		return
	}

	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
		return
	}

	match := make(uint64Slice, len(r.Prs))

	i := 0
	for _, prs := range r.Prs {
		match[i] = prs.Match
		i++
	}

	sort.Sort(match)
	n := match[(len(r.Prs)-1)/2]
	if n > r.RaftLog.committed {
		logterm, err := r.RaftLog.Term(n)
		if err != nil {
			log.Panic(err)
		}
		if logterm == r.Term {
			r.DebugPrint("more than half of server updated status,change commited from %d to %d", r.RaftLog.committed, n)
			r.RaftLog.committed = n
			r.broadcastAppend()
		}
	}

}

func (r *Raft) handleTransferLeader(m pb.Message) {
	newLeader := m.From
	process, ok := r.Prs[newLeader]

	if newLeader == r.id || !ok {
		r.DebugPrint("skip transfer msg.")
		return
	}

	r.leadTransferee = newLeader

	if process.Match != r.RaftLog.LastIndex() {
		r.DebugPrint("follower %d is not up to date, begin to send log.", newLeader)
		r.sendAppend(newLeader)
	} else {
		r.DebugPrint("follower %d is not up to date, begin to send log.", newLeader)
		r.sendTransferLeader(newLeader)
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {

	reject := true
	// m.term小于当前的term，拒绝
	if m.Term != None && m.Term < r.Term {
		r.sendRequestVoteResponse(m.From, reject)
		r.DebugPrint("reject vote request from %d,my term is higher.", m.From)
		return
	}

	if r.Vote != None && r.Vote != m.From {
		r.sendRequestVoteResponse(m.From, reject)
		r.DebugPrint("reject vote request from %d, has already voted for %d",
			m.From, r.Vote)
		return
	}

	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)

	if lastTerm > m.LogTerm || (lastTerm == m.LogTerm && lastIndex > m.Index) {
		r.sendRequestVoteResponse(m.From, reject)
		r.DebugPrint("reject vote request from %v,my log is newer.", m.From)
		return
	}

	r.Vote = m.From
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + mrand.Intn(r.electionTimeout)
	reject = false
	r.sendRequestVoteResponse(m.From, reject)
	r.DebugPrint("grant vote to %d", m.From)

}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {

	if m.Term < r.Term {
		r.DebugPrint("msg's term is older,skip.")
		return
	}

	r.votes[m.From] = !m.Reject
	var grant int
	var count int

	for _, v := range r.votes {
		grant++
		if v {
			count++
		}
	}
	if count > len(r.Prs)/2 {
		r.becomeLeader()
	} else if grant-count > len(r.Prs)/2 {
		r.becomeFollower(r.Term, None)
	}
	r.DebugPrint("After checking response from peer:%d,getVotes:%d,recievedVotes:%d/[total %d],my state %v",
		m.From, count, grant, len(r.Prs), r.State.String())
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).

	// // debug
	// if m.Entries != nil {
	// 	//not empty
	// 	r.DebugPrint("debug")
	// }

	reject := true
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, None, None, reject)
		r.DebugPrint("reject append requet from %d,my term is newer.", m.From)
		return
	}

	//每次接收到append的请求的时候，对于符合条件的，进行相关的修改
	//只有一个最新的term，且只有一个主，因此，只要不少于本节点的term的都
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + mrand.Intn(r.electionTimeout)
	r.Lead = m.From
	lastIndex := r.RaftLog.LastIndex()

	//对于lastIndex位置不匹配的，需要回应，重新调整的lastIndex的位置
	if m.Index > lastIndex {
		r.sendAppendResponse(m.From, None, lastIndex+1, reject)
		r.DebugPrint("lastindex %d from %d doesn't exit in my log.", m.Index, m.From)
		return
	}

	if m.Index >= r.RaftLog.firstIndex {
		logterm, err := r.RaftLog.Term(m.Index)
		if err != nil {
			log.Panic(err)
		}
		if logterm != m.LogTerm {
			sliceIndex, _ := r.RaftLog.GetEntrieIndex(m.Index + 1)
			// if err != nil {
			// 	log.Panic(err)
			// }
			sliceIndexReal := sort.Search(int(sliceIndex), func(i int) bool {
				return r.RaftLog.entries[i].Term == logterm
			})
			sliceIndexReal += int(r.RaftLog.firstIndex)
			r.sendAppendResponse(m.From, logterm, uint64(sliceIndexReal), reject)
			r.DebugPrint("response to %d with conflict term %d,conflict index %d", m.From, logterm, sliceIndexReal)
			return
		}
	}

	//小于firstindex的一定是正确的，只有commit之后的日志才会压缩的，而raft协议是不会修改达成共识的数据的

	for i, entry := range m.Entries {

		if entry.Index < r.RaftLog.firstIndex {
			continue
		}
		if entry.Index <= r.RaftLog.LastIndex() {
			logterm, err := r.RaftLog.Term(entry.Index)
			if err != nil {
				log.Panic(err)
			}
			if logterm != entry.Term {
				ind, err := r.RaftLog.GetEntrieIndex(entry.Index)
				if err != nil {
					log.Panic(err)
				}
				r.RaftLog.entries[ind] = *entry
				r.RaftLog.entries = r.RaftLog.entries[:ind+1]

				//master也拥有的日志，就是所谓的stable的
				r.RaftLog.stabled = min(r.RaftLog.stabled, entry.Index-1)
			}

		} else {
			n := len(m.Entries)
			for j := i; j < n; j++ {
				r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[j])
			}
			break
		}

	}

	if m.Commit > r.RaftLog.committed {
		r.DebugPrint("commited index update,change from %v to %v", r.RaftLog.committed, min(m.Commit, m.Index+uint64(len(m.Entries))))
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}
	reject = false

	r.sendAppendResponse(m.From, r.Term, r.RaftLog.LastIndex(), reject)
	//位置匹配，需要查看是否已经应用

	//heartbeat
	// r.DebugPrint("append entries successfully.")

}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {

	if m.Term < r.Term {
		return
	}

	if m.Reject {
		index := m.Index
		if m.LogTerm != None {
			logTerm := m.LogTerm
			l := r.RaftLog
			sliceIndex := sort.Search(len(l.entries), func(i int) bool {
				return l.entries[i].Term > logTerm
			})
			if sliceIndex > 0 && l.entries[sliceIndex-1].Term == logTerm {
				index = uint64(sliceIndex) + l.firstIndex
			}
		}
		if index >= r.RaftLog.GetFirstIndex() {
			r.Prs[m.From].Next = index
			r.Prs[m.From].Match = index - 1
			r.sendAppend(m.From)
		} else {
			r.sendSnapshot(m.From)
		}
	} else {
		if m.Index != r.Prs[m.From].Match {
			r.DebugPrint("get positive reply from server:%d,change Match:%d,Next:%d to Match:%d,Next:%d",
				m.From, r.Prs[m.From].Match, r.Prs[m.From].Next, m.Index, m.Index+1)
			r.Prs[m.From].Match = m.Index
			r.Prs[m.From].Next = m.Index + 1
		}
		r.leaderCommit()

		//to deal with situation when follower sucessfully make itself uptodate
		if m.From == r.leadTransferee {
			r.handleTransferLeader(pb.Message{From: m.From})
		}
	}

}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	reject := true
	if m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, reject)
		return
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + mrand.Intn(r.electionTimeout)
	reject = false
	r.sendHeartbeatResponse(m.From, reject)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// Your Code Here (2A).
	r.sendAppend(m.From)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).

	//因为是上层进行安装，一旦信息到了这一层，只需要修改元数据即可
	//一定不存在过期的快照？这个先留个疑问,从测试来看，还是需要自行判断
	snapshotIndex := m.Snapshot.Metadata.Index

	if snapshotIndex <= r.RaftLog.committed {
		r.sendAppendResponse(m.From, None, r.RaftLog.LastIndex(), true)
		return
	}

	r.DebugPrint("begin to install snapshot metadata")

	r.becomeFollower(max(r.Term, m.Term), m.From)
	if len(r.RaftLog.entries) > 0 {
		r.RaftLog.entries = nil
	}

	r.RaftLog.snapshotTerm = m.Snapshot.Metadata.Term
	r.RaftLog.firstIndex = snapshotIndex + 1
	r.RaftLog.committed = snapshotIndex
	r.RaftLog.applied = snapshotIndex
	r.RaftLog.stabled = snapshotIndex

	// 更新node的信息
	for _, p := range m.Snapshot.Metadata.ConfState.Nodes {
		if p != r.id {
			r.Prs[p] = &Progress{Match: snapshotIndex, Next: snapshotIndex + 1}
		} else {
			r.Prs[p] = &Progress{Next: snapshotIndex + 1}
		}
	}

	r.RaftLog.pendingSnapshot = m.Snapshot
	r.sendAppendResponse(m.From, None, r.RaftLog.LastIndex(), false)

}

func (r *Raft) hanldeCompactLog(m pb.Message) {
	lo := len(r.RaftLog.entries)
	r.RaftLog.maybeCompact(m.Term, m.Index)
	ln := len(r.RaftLog.entries)
	r.DebugPrint("after compaction, entry length changes from %d to %d", lo, ln)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	_, ok := r.Prs[id]
	if !ok {
		r.Prs[id] = &Progress{}
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	_, ok := r.Prs[id]
	if ok {
		delete(r.Prs, id)
		r.leaderCommit()
	}
}

func (r *Raft) DebugPrint(format string, a ...interface{}) {
	if DEBUG {
		log.SetFlags(log.Lmicroseconds | log.Lshortfile | log.LstdFlags)
		var para []interface{}
		var str string
		str = "[id:%d,term:%d,fIdx:%d,cIdx:%d,len:%d,role:%s,t:%d]"
		para = append(para, []interface{}{r.id, r.Term, r.RaftLog.firstIndex, r.RaftLog.committed,
			len(r.RaftLog.entries), r.State.String(), r.randomElectionTimeout}...)
		para = append(para, a...)
		log.Printf(str+format, para...)
	}
}

func (r *Raft) getSortState() SoftState {
	return SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) getHardState() pb.HardState {
	return pb.HardState{
		Term:                 r.Term,
		Vote:                 r.Vote,
		Commit:               r.RaftLog.committed,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     []byte{},
		XXX_sizecache:        0,
	}
}
