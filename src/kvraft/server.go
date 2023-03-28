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

const Debug = true
const ReqTimeout = 3 * time.Second

func DPrintf(format string, a ...interface{}) (n int, err error) {

	log.Printf(format, a...)

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

func (kv *KVServer) GetNotifyCh(index int) chan CommandReply {
	if _, ok := kv.notifyChan[index]; !ok {
		kv.notifyChan[index] = make(chan CommandReply)
	}
	ch := kv.notifyChan[index]
	return ch
}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("server[%v] get GET req %v", kv.me, *args)
	c := Command{Op: OpGet, Key: args.Key}
	reply.LeaderID = -1
	index, _, isLeader := kv.rf.Start(c)
	if !isLeader {
		DPrintf("no leader server[%v] get Get req %+v", kv.me, *args)

		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := kv.GetNotifyCh(index)
	kv.mu.Unlock()
	select {
	case r := <-ch:
		reply.Value = r.Value
		reply.Err = r.Err
	case <-time.After(ReqTimeout):
		reply.Err = ErrTimeout

	}
	go func() {
		kv.mu.Lock()
		delete(kv.notifyChan, index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("server[%v] get PutAppend req %v", kv.me, *args)
	reply.LeaderID = -1
	kv.mu.Lock()
	isDup := kv.isDup(args.ClientID, args.CommandID)
	kv.mu.Unlock()
	if isDup {
		DPrintf("client %+v command %+v dup", args.ClientID, args.CommandID)
		reply.Err = kv.lastReply[args.ClientID].Err
		return
	}

	// Your code here.
	c := Command{ClientID: args.ClientID, CommandID: args.CommandID, Op: args.Op, Key: args.Key, Value: args.Value}
	index, _, isLeader := kv.rf.Start(c)
	if !isLeader {
		DPrintf("no leader server[%v] get PutAppend req %+v", kv.me, *args)

		reply.Err = ErrWrongLeader
		return
	}
	ch := kv.GetNotifyCh(index)
	select {
	case r := <-ch:
		DPrintf("server|[%v] PutAppend req %+v get apply reply %+v", kv.me, *args, r)
		reply.Err = r.Err

	case <-time.After(ReqTimeout):
		DPrintf("server|[%v] PutAppend req %+v timeout", kv.me, *args)
		reply.Err = ErrTimeout
	}
	go func() {
		kv.mu.Lock()
		delete(kv.notifyChan, index)
		kv.mu.Unlock()
	}()
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
	dup := kv.lastReply[clientID].CommandID == commandID
	return dup
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		r := CommandReply{}
		DPrintf("server|[%v] receive apply command %+v", kv.me, msg)
		if msg.CommandValid {
			kv.mu.Lock()
			c := msg.Command.(Command)
			if c.Op == OpGet {
				v, ok := kv.st.Get(c.Key)
				if !ok {
					r.Err = ErrNoKey
				} else {
					r.Value = v
				}
			} else {
				if kv.isDup(c.ClientID, c.CommandID) {
					DPrintf("client %+vcommand %+valready apply", c.ClientID, c.CommandID)
					r = kv.lastReply[c.ClientID]
				} else {
					if c.Op == OpAppend {
						kv.st.Append(c.Key, c.Value)
					} else {
						kv.st.Put(c.Key, c.Value)
					}
				}
			}
			DPrintf("server|[%v] success apply %+vto state", kv.me, msg)
			r.CommandID = c.CommandID
			if c.Op != OpGet {
				kv.lastReply[c.ClientID] = r
			}
			if _, isLeader := kv.rf.GetState(); isLeader {
				DPrintf("server|[%v] notify  goroutine  with msg %+vand reply %+v", kv.me, msg, r)
				nf := kv.GetNotifyCh(msg.CommandIndex)
				nf <- r
			} else {
				DPrintf("server|[%v] not  leader,no need to notify %v", kv.me, msg)

			}
			kv.mu.Unlock()
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
	labgob.Register(Command{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastReply = map[int64]CommandReply{}
	kv.notifyChan = make(map[int]chan CommandReply)
	kv.st = &store{mu: sync.RWMutex{}, value: map[string]string{}}
	DPrintf("make server|[%v] all peer num %v", me, len(servers))
	// You may need initialization code here.
	go kv.applier()

	return kv
}
