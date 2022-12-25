package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false
const ReqTimeout = 3 * time.Second

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}
type Command struct {
	ClientID  int64
	CommandID int64
	Op        string
	Key       string
	Value     string
}

type CommandReply struct {
	CommandID int64
	Value     string
	Err       Err
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big
	//TODO: 如果客户端的commandID比当前的id小，怎么取
	lastReply  map[int64]CommandReply
	notifyChan map[int]chan CommandReply
	st         *store
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	c := Command{Op: OpGet, Key: args.Key}
	index, _, isLeader := kv.rf.Start(c)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	select {
	case r := <-kv.notifyChan[index]:
		reply.Value = r.Value
		reply.Err = r.Err
	case <-time.After(ReqTimeout):
		reply.Err = ErrTimeout

	}
	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if kv.lastReply[args.ClientID].CommandID == args.CommandID {
		DPrintf("client %v command %v dup", args.ClientID, args.CommandID)
		reply.Err = kv.lastReply[args.ClientID].Err
		return
	}
	kv.mu.Unlock()
	// Your code here.
	c := Command{ClientID: args.ClientID, CommandID: args.CommandID, Op: args.Op}
	index, _, isLeader := kv.rf.Start(c)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	select {
	case r := <-kv.notifyChan[index]:
		reply.Err = r.Err
	case <-time.After(ReqTimeout):
		reply.Err = ErrTimeout
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) isDup(clientID, commandID int64) bool {
	return kv.lastReply[clientID].CommandID == clientID
}

func (kv *KVServer) applier() {
	for !kv.killed() {

		msg := <-kv.applyCh
		r := CommandReply{}
		if msg.CommandValid {
			c := msg.Command.(Command)
			if c.Op == OpGet {
				v, ok := kv.st.Get(c.Key)
				if !ok {
					r.Err = ErrNoKey
				} else {
					r.Value = v
				}
			} else {
				kv.mu.Lock()
				dup := kv.isDup(c.ClientID, c.CommandID)
				kv.mu.Unlock()
				if dup {
					DPrintf("client %v command %v already apply", c.ClientID, c.CommandID)
					r.Err = kv.lastReply[c.ClientID].Err
				} else {
					if c.Op == OpAppend {
						ok := kv.st.Append(c.Key, c.Value)
						if !ok {
							r.Err = ErrNoKey
						}
					} else {
						kv.st.Put(c.Key, c.Value)
					}
				}
			}
			ch := kv.notifyChan[msg.CommandIndex]
			ch <- r
		} else {
			DPrintf("command %+v invalid", msg)
		}

	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastReply = map[int64]CommandReply{}
	kv.notifyChan = make(map[int]chan CommandReply)
	kv.st = &store{}
	// You may need initialization code here.
	go kv.applier()

	return kv
}
