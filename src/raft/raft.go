package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	//2A
	status  RaftStatus //Raft Node 状态
	curTerm int        //当前term
	voteFor int        //给哪个candidateID投过票

	heartBreakTimer *time.Timer //心跳timer
	electionTimer   *time.Timer //选举timer

	//2B
	applyCh       chan ApplyMsg
	entry         []*Entry     //日志
	commitIndex   int          //最后一个提交的日志索引
	applIndex     int          //最后一个已应用的日志索引
	applyCond     *sync.Cond   //提交日志的cond
	matchIndex    []int        //各个peer的matchIndex
	nextIndex     []int        //各个peer的nextIndex
	replicateCond []*sync.Cond //同步peer日志的条件变量

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

//
type RaftStatus int32

const (
	Status_Follower  = 0
	Status_Leader    = 1
	Status_Candidate = 2
)

const (
	electionTimeout   = 1000
	heartBreakTimeout = 125
	timeoutUint       = time.Millisecond
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.curTerm
	isleader = rf.status == Status_Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {

	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
//日志
//
type Entry struct {
	Index   int         //日志索引
	Term    int         //日志Term
	Command interface{} //命令内容
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != Status_Leader {
		return index, term, false
	}
	if len(rf.entry) == 0 {
		index = 1
	} else {
		index = rf.GetLastIndex() + 1
	}
	term = rf.curTerm
	rf.entry = append(rf.entry, &Entry{index, term, command})
	DPrintf("[Start] %s receive command index %v", rf.LogPrefix(), index)
	go rf.Broadcast(false)
	return index, term, true
	// Your code here (2B).

}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	DPrintf("%s is killed", rf.LogPrefix())
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
//开启选举，见论文$5.2 To Begin a election ...
//先投自己，异步询问其他peers
//当自己仍是候选人，且获得票数过半，转为leader
//当其他人的term大于当前term，转为follwer
//
func (rf *Raft) StartElection() {
	//rf.mu.Lock()不能在主协程加锁，cuz外部有锁
	//DPrintf("%s start election", rf.LogPrefix())
	rf.voteFor = rf.me
	grantCnt := 1
	req := rf.genRequestVoteArgs()
	for peer := range rf.peers {

		if peer == rf.me {
			continue
		}
		go func(peer int) {
			//异步请求投票，可以加锁

			resp := new(RequestVoteReply)
			if rf.sendRequestVote(peer, req, resp) {
				//当且仅当发送请求成功后才加锁，不然发送失败后，会阻塞锁
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.curTerm == resp.Term && rf.status == Status_Candidate {
					if resp.VoteGranted {
						//DPrintf("%s recevice agree from %v  reply %+v", rf.LogPrefix(), peer, *resp)

						grantCnt++
						if grantCnt > len(rf.peers)/2 {
							rf.status = Status_Leader
							//TODO: 发送广播
							rf.Broadcast(true)
							DPrintf("[election] %s become leader", rf.LogPrefix())

						}
					}
					if resp.Term > req.Term {
						rf.status = Status_Follower
						rf.curTerm = resp.Term
						rf.voteFor = -1
					}
				}
			}
		}(peer)
	}
}

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
					rf.electionTimer.Reset(RandomElectionTimeout())
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

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,
		applyCh:   applyCh,
		mu:        sync.RWMutex{},
	}

	// Your initialization code here (2A, 2B, 2C).
	//2A
	rf.voteFor = -1
	rf.heartBreakTimer = time.NewTimer(heartBreakTimeout * timeoutUint)
	rf.electionTimer = time.NewTimer(RandomElectionTimeout())
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.replicateCond = make([]*sync.Cond, len(peers))
	rf.applyCond = &sync.Cond{L: &rf.mu}
	rf.entry = []*Entry{{0, 0, nil}} //第一条填充

	//2B
	//开启n个同步日志routinue
	for peer := range peers {
		rf.nextIndex[peer] = rf.GetLastIndex() + 1
		if peer != rf.me {
			rf.replicateCond[peer] = &sync.Cond{L: &sync.Mutex{}}
			go rf.Replicator(peer)
		}
	}

	//开启提交日志协程
	go rf.ApplyEntry()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	DPrintf("%s peer server start", rf.LogPrefix())
	return rf
}

func RandomElectionTimeout() time.Duration {
	return time.Duration((electionTimeout + rand.Intn(electionTimeout)) * int(timeoutUint))
}
