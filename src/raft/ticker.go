package raft

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		//2A
		case <-rf.heartBreakTimer.C:
			{
				rf.mu.Lock()
				if rf.status == Status_Leader {
					//DPrintf("%s leader heartbreak timeout", rf.LogPrefix())
					//rf.electionTimer.Reset(RandomElectionTimeout())
					rf.Broadcast(true)
				}
				rf.heartBreakTimer.Reset(heartBreakTimeout * timeoutUint)
				rf.mu.Unlock()
			}
		case <-rf.electionTimer.C:
			{

				//转换状态 term+1 异步开启选举 重置timeout
				rf.mu.Lock()
				//DPrintf("%s election timeout", rf.LogPrefix())
				rf.status = Status_Candidate
				rf.curTerm = rf.curTerm + 1
				rf.StartElection()
				rf.electionTimer.Reset(RandomElectionTimeout())
				rf.mu.Unlock()
			}
		}

	}
}
