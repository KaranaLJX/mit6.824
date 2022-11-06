package raft

//
//  复制协程
//
func (rf *Raft) Replicator(peer int) {
	//为什么是在外层加条件锁
	rf.replicateCond[peer].L.Lock()
	defer rf.replicateCond[peer].L.Unlock()
	for !rf.killed() {
		if !rf.needReplicate(peer) {
			DPrintf("[Replicator] %v | peer %v do not need to Replicate", rf.LogPrefix(), peer)
			rf.replicateCond[peer].Wait()
		}
		//这里不应该go 出去，因为广播发送结束后才可能
		DPrintf("[Replicator] %v | peer %v match %v last %v", rf.LogPrefix(), peer, rf.matchIndex[peer], rf.GetLastIndex())
		rf.SendOneBroadcast(peer)
	}
}

//
//判断peer是否需要同步日志 为了defer释放锁，单独起一个func
//
func (rf *Raft) needReplicate(peer int) bool {
	//如果
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.status == Status_Leader && rf.matchIndex[peer] < rf.GetLastIndex()
}
