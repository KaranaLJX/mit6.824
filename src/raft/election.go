package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
// 参考fig2的RequestVoteRpc
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int //请求候选人的term
	CandidateId int //请求候选人
	LastLog     *Entry
}

//
//根据节点生成 ReqVoteArgs
//
func (rf *Raft) genRequestVoteArgs() *RequestVoteArgs {
	return &RequestVoteArgs{Term: rf.curTerm, CandidateId: rf.me,
		LastLog: rf.GetLastLog()}
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
// 参考fig2的RequestVoteRpc
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //本节点的term
	VoteGranted bool //是否投票
}

//args.Term
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//DPrintf("%s receive vote from %+v", rf.LogPrefix(), *args)
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.curTerm || (args.Term == rf.curTerm && rf.voteFor != -1 && rf.voteFor != args.CandidateId) {
		//DPrintf("%s reject vote %+v", rf.LogPrefix(), *args)
		reply.Term, reply.VoteGranted = rf.curTerm, false
		return
	}
	//DPrintf("%s agree vote %+v", rf.LogPrefix(), *args)

	if args.Term > rf.curTerm {
		rf.status = Status_Follower
		rf.curTerm, rf.voteFor = args.Term, -1
	}

	//如果不是最新的log，不投票
	if !rf.IsLogUpToDate(args.LastLog) {
		reply.Term, reply.VoteGranted = rf.curTerm, false
		return
	}
	rf.voteFor = args.CandidateId
	rf.status = Status_Follower
	rf.curTerm = args.Term
	//}
	reply.Term, reply.VoteGranted = rf.curTerm, true
}

func (rf *Raft) GetLastLog() *Entry {
	return rf.entry[len(rf.entry)-1]
}

//
//是否lastlog较新
//
func (rf *Raft) IsLogUpToDate(e *Entry) bool {
	lastLog := rf.GetLastLog()
	return e.Term > lastLog.Term || (e.Term == lastLog.Term && e.Index >= lastLog.Index)
}
