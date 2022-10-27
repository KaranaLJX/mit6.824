package raft

//
//获取Index的位置
//
func (rf *Raft) GetPosByIndex(index int) int {
	firstIndex := rf.entry[0].Index
	return index - firstIndex
}

//
//获取最后一个index
//
func (rf *Raft) GetLastIndex() int {
	return rf.entry[len(rf.entry)-1].Index
}

//
//  复制协程
//
func (rf *Raft) Replicator(peer int) {
	rf.replicateCond[peer].L.Lock()
	defer rf.replicateCond[peer].L.Lock()
	rf.mu.RLock()
	if !(rf.status == Status_Leader && rf.matchIndex[peer] >= rf.GetLastIndex()) {
		rf.mu.RUnlock()
		rf.replicateCond[peer].Wait()
	}
	// rf.mu.RLock()
	// defer rf.mu.RUnlock()
	//TODO: 这里如何加锁
	for rf.status == Status_Leader && rf.matchIndex[peer] >= rf.commitIndex {
		//这里不应该go 出去，因为广播发送结束后才可能
		rf.SendOneBroadcast(peer)
	}
}

//
//提交日志协程
//
func (rf *Raft) ApplyEntry() {
	rf.applyCond.L.Lock()
	defer rf.applyCond.L.Unlock()
	rf.mu.Lock()
	if rf.commitIndex <= rf.applIndex {
		rf.applyCond.Wait()
		rf.mu.Unlock()
	}
	// rf.mu.RLock()
	// defer rf.mu.RUnlock()
	//TODO: 这里如何加锁
	for rf.applIndex < rf.commitIndex {
		var entryToApply []Entry
		rf.mu.RLock()
		entryToApply = rf.entry[rf.GetPosByIndex(rf.applIndex):]
		rf.mu.RLock()
		//这一块耗时很高，需要并发???
		for _, e := range entryToApply {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				CommandIndex: e.Index,
				Command:      e.Command,
			}
		}
		rf.mu.Lock()
		rf.applIndex = rf.commitIndex
		rf.mu.Unlock()
	}

}

//
//发送日志同步广播
//
func (rf *Raft) Broadcast(isHeartBreak bool) {
	for !rf.killed() && rf.status == Status_Leader {
		for peer := range rf.peers {
			if isHeartBreak {
				go rf.SendOneBroadcast(peer)
			} else {
				rf.replicateCond[peer].Signal()
			}
		}
	}
}

//
// leader添加提交日志
//
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term          int     //请求候选人的term
	LeaderID      int     //领导者id
	PreLogIndex   int     //上一次同步的日志索引
	PreLogTerm    int     //上一次同步日的任期
	Entries       []Entry //同步的日志
	LeaderComimit int     //领导者的已提交的日志的最高索引

}

//
//根据节点生成 genAppendEntriesArgs
//
func (rf *Raft) genAppendEntriesArgs() *AppendEntriesArgs {
	return &AppendEntriesArgs{Term: rf.curTerm, LeaderID: rf.me, LeaderComimit: rf.commitIndex}
}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
// 参考fig2的AppendEntriesRpc
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term         int  //当前任期号
	Success      bool //是否同步成功
	ConflitIndex int  //冲突index
	ConflitTerm  int  //冲突Iterm
}

//
//发送一次广播,同步日志
//
func (rf *Raft) SendOneBroadcast(peer int) {
	//
	rf.mu.RLock()
	req := rf.genAppendEntriesArgs()
	req.PreLogIndex = rf.nextIndex[peer] - 1
	pos := rf.GetPosByIndex(rf.nextIndex[peer] - 1)
	if pos < 0 {
		//leader太超前 TODO，发送snatshot
		rf.mu.RUnlock()
		return
	}
	if pos >= len(rf.entry) {
		//leader落后 TODO 怎么处理？
		rf.mu.RUnlock()
		return
	}
	req.PreLogTerm = rf.entry[pos].Term
	req.Entries = rf.entry[pos:]
	resp := &AppendEntriesReply{}
	if rf.sendAppendEntries(peer, req, resp) {
		rf.mu.Lock()
		defer rf.mu.Lock()
		rf.HandleBroadCastResp(peer, req, resp)
	}

}

//
//处理结果
//
func (rf *Raft) HandleBroadCastResp(peer int, req *AppendEntriesArgs, resp *AppendEntriesReply) {
	//成为候选人
	if resp.Term > rf.curTerm {
		rf.status = Status_Candidate
		rf.voteFor = -1
		rf.curTerm = resp.Term
		return
	}
	//处理成功
	if resp.Success && resp.Term == rf.curTerm {
		rf.matchIndex[peer] = req.PreLogIndex + len(req.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		rf.AddCommitIndex()
		return
	}
	//处理冲突,回退nextIndex
	if resp.Term == rf.curTerm {
		cPos := rf.GetPosByIndex(resp.ConflitIndex)
		if resp.ConflitTerm != -1 {
			//找到最后一个term跟冲突term相同的pos
			for cPos = len(rf.entry) - 1; cPos >= 0; cPos-- {
				if rf.entry[cPos].Term == resp.ConflitTerm {
					break
				}
			}
		}
		if cPos < 0 {
			//TODO如何处理conflitIndex不存在
			return
		}
		rf.nextIndex[peer] = rf.entry[cPos].Index + 1

	}
}

//
//检查过半投票，并且步进commitIndex
//
func (rf *Raft) AddCommitIndex() {
	for i := len(rf.entry) - 1; i >= 0; i++ {
		index := rf.entry[i].Index
		votes := 1
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			if rf.matchIndex[peer] >= index {
				votes++
				if votes > (len(rf.peers) / 2) {
					break
				}
			}
		}
		if votes > (len(rf.peers) / 2) {
			rf.commitIndex = index
			break
		}
	}
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	if reply == nil {
		reply = &AppendEntriesReply{}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//term 太小，不接受心跳
	if args.Term < rf.curTerm {
		DPrintf("%s leader term [%+v] less than curterm",
			rf.LogPrefix(), args)
		reply.Term = rf.curTerm
		return
	}
	//term相等，但是commitIndex太小，不接受心跳
	if args.Term == rf.curTerm && args.LeaderComimit < rf.commitIndex {
		DPrintf("%s commit index too small args %+v", rf.LogPrefix(), *args)
	}
	//接受心跳，重设election timer
	if args.Term > rf.curTerm {
		rf.curTerm, rf.voteFor = args.Term, -1
	}

	rf.status = Status_Follower
	isReset := rf.electionTimer.Reset(RandomElectionTimeout())
	DPrintf("%s receive heartbreak from leader [%+v] reset %v", rf.LogPrefix(), args, isReset)

	//日志同步
	SyncEntry(args, reply)

}

//
//peer日志同步leader
//
func SyncEntry(args *AppendEntriesArgs, reply *AppendEntriesReply) {

}
