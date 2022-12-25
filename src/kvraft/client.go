package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
	"6.824/raft"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	leaderID  int
	clientID  int64
	commandID int64
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientID = nrand()
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	args := &GetArgs{Key: key}
	reply := &GetReply{}
	//TODO: which server to call
	for {
		ok := ck.servers[ck.leaderID].Call("KVServer.Get", &args, &reply)
		if !ok {
			raft.DPrintf("call server GET system err")
			continue
		}
		if reply.Err != "" {
			raft.DPrintf("call server GET err: %v", reply.Err)
			if reply.Err == ErrWrongLeader {
				if reply.LeaderID < len(ck.servers) {
					ck.leaderID = reply.LeaderID
				} else {
					ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
				}

			}
			continue
		}
		return reply.Value
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.commandID++
	args := &PutAppendArgs{Key: key, Value: value, Op: op, ClientID: ck.clientID, CommandID: ck.commandID}
	reply := &PutAppendReply{}
	//TODO: which server to call
	for {
		ok := ck.servers[ck.leaderID].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			raft.DPrintf("call server PutAppend system err")
			continue
		}
		if reply.Err != "" {
			raft.DPrintf("call server PutAppend err: %v", reply.Err)
			if reply.Err == ErrWrongLeader {
				if reply.LeaderID < len(ck.servers) {
					ck.leaderID = reply.LeaderID
				} else {
					ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
				}

			}
			continue
		}
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
