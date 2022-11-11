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
//发送日志同步广播
//
func (rf *Raft) Broadcast(isHeartBreak bool) {
	if !rf.killed() && rf.status == Status_Leader {
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
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
	Term          int      //请求候选人的term
	LeaderID      int      //领导者id
	PreLogIndex   int      //上一次同步的日志索引
	PreLogTerm    int      //上一次同步日的任期
	Entries       []*Entry //同步的日志
	LeaderComimit int      //领导者的已提交的日志的最高索引

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
	rf.mu.RLock()
	req := rf.genAppendEntriesArgs()
	req.PreLogIndex = rf.nextIndex[peer] - 1
	pos := rf.GetPosByIndex(req.PreLogIndex)
	if pos < 0 {
		DPrintf("ERR[SendOneBroadcast]%s PreLog %v miss", rf.LogPrefix(), req.PreLogIndex)
		rf.mu.RUnlock()
		return
	}
	if pos >= len(rf.entry) {
		//leader落后 TODO 怎么处理？

		DPrintf("ERR[SendOneBroadcast]%s PreLog %v exceed %v", rf.LogPrefix(), pos, len(rf.entry))
		rf.mu.RUnlock()
		return
	}

	req.Entries = rf.entry[pos+1:]
	req.PreLogTerm = rf.entry[pos].Term

	resp := &AppendEntriesReply{}
	rf.mu.RUnlock()
	if rf.sendAppendEntries(peer, req, resp) {
		rf.mu.Lock()
		rf.HandleBroadCastResp(peer, req, resp)
		rf.mu.Unlock()

	}

}

//
//处理结果
//
func (rf *Raft) HandleBroadCastResp(peer int, req *AppendEntriesArgs, resp *AppendEntriesReply) {
	//成为候选人
	defer rf.persist()
	if resp.Term > rf.curTerm {
		DPrintf("%v [HandleBroadCastResp]|[%v]  less term,resp %+v", rf.LogPrefix(), peer, *resp)

		rf.status = Status_Candidate
		rf.voteFor = -1
		rf.curTerm = resp.Term
		return
	}
	//处理成功
	if resp.Success && resp.Term == rf.curTerm {
		if len(req.Entries) > 0 {
			DPrintf("%v [HandleBroadCastResp]|[%v]  success sync %+v", rf.LogPrefix(), peer, *req)
			rf.matchIndex[peer] = req.PreLogIndex + len(req.Entries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			rf.AddCommitIndex()
		}
		return
	}
	//处理冲突,回退nextIndex
	if resp.Term == rf.curTerm {
		DPrintf("%v [HandleBroadCastResp]|[%v]  conflict req %+v resp %+v", rf.LogPrefix(), peer, *req, *resp)
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
	for i := len(rf.entry) - 1; i >= 0; i-- {
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
	if rf.commitIndex > rf.applIndex {
		rf.applyCond.Signal()
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
	defer rf.persist()
	//term 太小，不接受心跳
	if args.Term < rf.curTerm {
		DPrintf("%s leader term [%+v] less than curterm",
			rf.LogPrefix(), args)
		reply.Term = rf.curTerm
		reply.ConflitTerm = -1
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
	reply.Term = rf.curTerm
	rf.status = Status_Follower
	isReset := rf.electionTimer.Reset(RandomElectionTimeout())
	DPrintf("%s receive heartbreak from leader [%+v] reset %v", rf.LogPrefix(), args, isReset)

	//日志同步
	rf.SyncEntry(args, reply)

}

//
//peer日志同步leader日志
//
func (rf *Raft) SyncEntry(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	defer rf.persist()
	pos := rf.GetPosByIndex(args.PreLogTerm)
	//prelog找不到 TODO:how to handle
	if pos < 0 || pos >= len(rf.entry) {
		DPrintf("[SyncEntry] %v| preLogTerm not found req %+v", rf.LogPrefix(), *args)
		reply.ConflitTerm = 0
		return
	}
	//不match
	if rf.entry[pos].Term != args.PreLogTerm {
		DPrintf("[SyncEntry] %v| req %+v preLogTerm %+v conflit %+v", rf.LogPrefix(), *args, rf.entry[pos].Term, args.PreLogTerm)
		reply.ConflitIndex = args.PreLogIndex
		reply.ConflitTerm = rf.entry[pos].Term
		for i := 0; i < len(rf.entry); i++ {
			if rf.entry[i].Term == reply.ConflitTerm {
				reply.ConflitIndex = rf.entry[i].Index
				break
			}
		}
		return
	}

	if len(args.Entries) > 0 {
		//没有任何日志匹配
		lPos := rf.GetPosByIndex(args.Entries[0].Index)
		rPos := rf.GetPosByIndex(args.Entries[len(args.Entries)-1].Index)
		//如有不同，全部替换
		curPos := lPos
		for ; curPos <= rPos && curPos < len(rf.entry); curPos++ {
			if rf.entry[curPos].Term != args.Entries[curPos-lPos].Term {
				break
			}
		}
		if curPos <= rPos {
			rf.entry = append(rf.entry[:curPos], args.Entries[curPos-lPos:]...)
		}
		DPrintf("[SyncEntry] %v|success sync req %+v ", rf.LogPrefix(), *args)
	}

	reply.Success = true
	rf.commitIndex = args.LeaderComimit
	if rf.commitIndex > rf.applIndex {
		rf.applyCond.Signal()
	}
}

//
//
//
