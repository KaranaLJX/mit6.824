package raft

import (
	"fmt"
	"log"
	"os"
)

func init() {
	if IsLogFile {
		f, err := os.OpenFile(LogFile, os.O_WRONLY|os.O_CREATE|os.O_SYNC|os.O_APPEND, 0755)
		if err != nil {
			panic(err)
		}
		f.Truncate(0)
		log.SetOutput(f)
	}

}

// Debugging
const Debug = true
const LogFile = "/Users/bytedance/go/src/mit6.824/src/raft/log/log1"
const IsLogFile = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
func (rf *Raft) LogPrefix() string {
	return fmt.Sprintf("[%v]|[t%v]|[L%v]|[c%v][a%v]", rf.me, rf.curTerm, rf.status == Status_Leader, rf.commitIndex, rf.applIndex)
}
