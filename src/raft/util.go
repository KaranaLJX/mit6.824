package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
func (rf *Raft) LogPrefix() string {

	// Your code here (2D).

	return fmt.Sprintf("[%v]|[t%v]|[L%v]", rf.me, rf.curTerm, rf.status == Status_Leader)
}
