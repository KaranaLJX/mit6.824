package raft

//
//提交日志协程
//
func (rf *Raft) ApplyEntry() {
	for !rf.killed() {
		rf.mu.Lock() ///same  rf.applyCond.L.Lock()
		if rf.commitIndex <= rf.applIndex {
			DPrintf("[ApplyEntry] %v |  no need to apply", rf.LogPrefix())
			rf.applyCond.Wait()
		}
		DPrintf("[ApplyEntry] %v | log %v", rf.LogPrefix(), rf.logState())
		entryToApply := make([]*Entry, rf.commitIndex-rf.applIndex)
		cIndex := rf.commitIndex
		r := rf.GetPosByIndex(rf.commitIndex)
		l := rf.GetPosByIndex(rf.applIndex) + 1
		copy(entryToApply, rf.entry[l:r+1])
		rf.mu.Unlock()
		//这一块耗时很高，需要并发???
		for _, e := range entryToApply {
			DPrintf("[ApplyEntry] %v apply %v", rf.LogPrefix(), e)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				CommandIndex: e.Index,
				Command:      e.Command,
				CommandTerm:  e.Term,
			}
		}
		rf.mu.Lock()
		rf.applIndex = max(rf.applIndex, cIndex)
		rf.mu.Unlock()
	}

}

func max(i, j int) int {
	if i > j {
		return i
	}
	return j
}
