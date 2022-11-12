package raft

import "fmt"

//
// example InstallSnapshot RPC arguments structure.
// field names must start with capital letters!
// 参考fig2的InstallSnapshotRpc
//
type InstallSnapshotArgs struct {
	Term        int //请求候选人的term
	CandidateId int //请求候选人
	LastLog     *Entry
	Data        []byte
}

//
//根据节点生成 InstallSnapshotArgs
//
func (rf *Raft) genInstallSnapshotArgs() *InstallSnapshotArgs {
	return &InstallSnapshotArgs{Term: rf.curTerm, CandidateId: rf.me,
		LastLog: rf.entry[0], Data: rf.persister.ReadSnapshot()}
}

//
// example InstallSnapshot RPC reply structure.
// field names must start with capital letters!
//
type InstallSnapshotReply struct {
	Term int //本节点的term
}

func (args *InstallSnapshotArgs) log() string {
	return fmt.Sprintf("[from[%v][%v]|[last log] %+v]", args.CandidateId, args.Term, *args.LastLog)
}

//
// example InstallSnapshot RPC handler.
//
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.curTerm
	if args.Term < rf.curTerm {
		DPrintf("[InstallSnapshot] %v|snatshot term old args %v", rf.LogPrefix(), args.log())
		return
	}
	if args.Term > rf.curTerm {
		rf.curTerm, rf.voteFor = args.Term, -1
		rf.persist()
		DPrintf("[InstallSnapshot] %v|larger term %+v", rf.LogPrefix(), args.log())
	}
	rf.status = Status_Follower
	rf.electionTimer.Reset(RandomElectionTimeout())
	DPrintf("[InstallSnapshot] %v|receive heartbreak reset  %+v reset ticker", rf.LogPrefix(), args.log())

	//快照index太小,小于commitIndex,随着日志的提交，会到达snatshot
	if args.LastLog.Index <= rf.commitIndex {
		DPrintf("[InstallSnapshot] %v|snatshot index too old  %+v commitindex %v",
			rf.LogPrefix(), *args, rf.commitIndex)
		return
	}
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			SnapshotTerm:  args.LastLog.Term,
			SnapshotIndex: args.LastLog.Index,
			Snapshot:      args.Data,
		}

	}()

}

//
//处理结果
//
func (rf *Raft) HandleInstallSnapshotResp(peer int, req *InstallSnapshotArgs, resp *InstallSnapshotReply) {
	//成为follower
	if resp.Term > rf.curTerm {
		DPrintf("%v [HandleInstallSnapshotResp]|[%v]  less term,resp %+v", rf.LogPrefix(), peer, *resp)
		rf.status, rf.voteFor = Status_Follower, -1
		rf.curTerm = resp.Term
		rf.persist()
		return
	}
	//处理成功
	DPrintf("%v [HandleInstallSnapshotResp]|[%v]  succ term args %+v", rf.LogPrefix(), peer, req)
	rf.matchIndex[peer] = req.LastLog.Index
	rf.nextIndex[peer] = rf.matchIndex[peer] + 1
	rf.AddCommitIndex()

}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	//!!!记得加锁
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//快照index太小,小于commitIndex,随着日志的提交，会到达snatshot
	//TODO ： 为什么不需要检查term是否相同
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("[CondInstallSnapshot] %v|snapshot too old lastIncludeIndex[%v]", rf.LogPrefix(), lastIncludedIndex)
		return false
	}

	if rf.GetLastIndex() < lastIncludedIndex {
		rf.entry = make([]*Entry, 1)
		rf.entry[0] = &Entry{}
	} else {
		index := rf.GetPosByIndex(lastIncludedIndex)
		rf.entry = ShrinkSlice(rf.entry[index:])
	}
	rf.entry[0].Command = nil
	rf.entry[0].Term = lastIncludedTerm
	rf.entry[0].Index = lastIncludedIndex
	DPrintf("[CondInstallSnapshot] %v|change to snatshot %v", rf.LogPrefix(), lastIncludedIndex)
	rf.commitIndex, rf.applIndex = lastIncludedIndex, lastIncludedIndex
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.entry[0].Index {
		DPrintf("[Snapshot] %v|snapshot %v less than %v", rf.LogPrefix(), index, rf.entry[0].Index)
		return
	}
	preLogStats := rf.logState()

	if rf.GetLastIndex() < index {
		DPrintf("[Snapshot] %v|snapshot %v large than %v", rf.LogPrefix(), index, rf.GetLastIndex())

		rf.entry = make([]*Entry, 1)
		rf.entry[0] = &Entry{}

		rf.entry[0].Term = rf.curTerm
		rf.entry[0].Index = index
	} else {
		pos := rf.GetPosByIndex(index)
		rf.entry = ShrinkSlice(rf.entry[pos:])
		rf.entry[0].Command = nil
	}
	DPrintf("[Snapshot] %v|snapshot  change %v to pre(%v|%v)", rf.LogPrefix(), index, preLogStats, rf.logState())

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)

}

//
//clone slice，减少内存泄露
//
func ShrinkSlice(ori []*Entry) []*Entry {
	x := make([]*Entry, len(ori))
	copy(x, ori)
	ori = nil
	return x
}

func (rf *Raft) logState() string {
	return fmt.Sprintf("[%v|%v]", rf.entry[0].Index, rf.GetLastIndex())
}
