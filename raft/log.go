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
	"log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).

	// 是storage中第一个日志的位置的index号
	firstIndex uint64

	// 先作为一个变量直接存起来
	snapshotTerm uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).

	//need to modify
	l, _ := storage.FirstIndex()
	h, _ := storage.LastIndex()

	//raftLog's entries seem it dones't contain compact info. no dummy entries.
	entries, err := storage.Entries(l, h+1)
	if err != nil {
		log.Panicln(err)
	}

	r := &RaftLog{
		storage:    storage,
		entries:    entries,
		stabled:    h,
		applied:    l - 1,
		firstIndex: l,
	}

	return r
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact(snapshotTerm, snapshotIndex uint64) bool {
	// Your Code Here (2C).
	if snapshotTerm >= l.snapshotTerm && snapshotIndex >= l.firstIndex {
		if snapshotIndex <= l.applied {
			l.entries = l.entries[snapshotIndex-l.firstIndex+1:]
			l.firstIndex = snapshotIndex + 1
			l.snapshotTerm = snapshotTerm
			return true
		} else {
			log.Fatalf("unexpected situation. applyid %d smaller than snapshotID %d", l.applied, snapshotIndex)
		}
	}

	return false

}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).

	if len(l.entries) > 0 {
		return l.entries[l.stabled-l.firstIndex+1:]
	}

	return nil

}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.applied-l.firstIndex+1 : l.committed-l.firstIndex+1]
	}

	return nil
}

// // get Entries[begin:end)
// func (l *RaftLog) getEnts(begin, end uint64) (ents []pb.Entry) {
// 	ents, err := l.storage.Entries(begin, end)
// 	switch err {
// 	case ErrCompacted:
// 		log.Panicln("uncommited log has been compacted. wrong status")
// 	case ErrUnavailable:
// 		log.Println("only the dummy entries,no log need to be committed.")
// 	}
// 	return ents
// }

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).

	var ind uint64
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}

	ind, err := l.storage.LastIndex()

	if err != nil {
		log.Panic(err)
	}

	//如果meta数据修改了，但是raftdb没有修改出现lastindex小于firstIndex的情况
	if ind < l.GetFirstIndex() {
		ind = l.GetFirstIndex() - 1
	}

	return ind
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).

	if len(l.entries) > 0 && l.firstIndex <= i {
		return l.entries[i-l.firstIndex].Term, nil
	}

	//之前的写法判断比较繁琐，不够清晰
	term, err := l.storage.Term(i)
	if err == ErrUnavailable && !IsEmptySnap(l.pendingSnapshot) {
		if i == l.pendingSnapshot.Metadata.Index {
			term = l.pendingSnapshot.Metadata.Term
			err = nil
		} else {
			err = ErrCompacted
		}

	}

	return term, err
}

//返回某一个index在当前entry中的物理下标
func (l *RaftLog) GetEntrieIndex(i uint64) (uint64, error) {

	var index uint64
	if i < l.firstIndex {
		return index, ErrCompacted
	}

	index = i - l.firstIndex

	//如果输入的长度超过了可能的长度，返回entries长度，其实就是
	//下一个日志的位置
	if i-l.firstIndex+1 > uint64(len(l.entries)) {
		index = uint64(len(l.entries))
		return index, ErrUnavailable
	}

	return index, nil

}

func (l *RaftLog) GetFirstIndex() uint64 {
	return l.firstIndex
}

func (l *RaftLog) GetSnapshotTerm() uint64 {
	return l.snapshotTerm
}
