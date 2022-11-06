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
}

//
//根据节点生成 ReqVoteArgs
//
func (rf *Raft) genRequestVoteArgs() *RequestVoteArgs {
	return &RequestVoteArgs{Term: rf.curTerm, CandidateId: rf.me}
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

	// if args.Term  > rf.curTerm {
	rf.status = Status_Follower
	rf.curTerm = args.Term
	//}
	reply.Term, reply.VoteGranted = rf.curTerm, true
}
