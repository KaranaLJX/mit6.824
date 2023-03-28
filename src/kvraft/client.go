package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
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
	DPrintf("make client|[%v]", ck.clientID)
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

	//TODO: which server to call
	for {
		DPrintf("client|[%v] call server|[%v]  GET with %+vargs", ck.clientID, ck.leaderID, *args)
		reply := &GetReply{}
		ok := ck.servers[ck.leaderID].Call("KVServer.Get", args, reply)
		if !ok {
			DPrintf("client|[%v] call GET %+v server|[%v]server GET system err", ck.clientID, *args, ck.leaderID)
		}
		if !ok || (reply.Err == ErrWrongLeader || reply.Err == ErrTimeout) {
			DPrintf("client|[%v] call GET %+v server|[%v] GET err: %v", ck.clientID, *args, ck.leaderID, reply.Err)
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)

			continue
		}

		DPrintf("client|[%v] call server|[%v] suncc  GET with %+vargs get reply %+v",
			ck.clientID, ck.leaderID, *args, *reply)
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
	//TODO: which server to call
	for {
		DPrintf("client|[%v] call server|[%v] PutAppend with %+vargs", ck.clientID, ck.leaderID, *args)
		reply := &PutAppendReply{}
		ok := ck.servers[ck.leaderID].Call("KVServer.PutAppend", args, reply)
		if !ok {
			DPrintf("client|[%v] call server|[%v] PutAppend get systm err", ck.clientID, ck.leaderID)
		}

		if (!ok) || (reply.Err == ErrWrongLeader || reply.Err == ErrTimeout) {
			DPrintf("client|[%v] call server|[%v] PutAppend %+v get err %v", ck.clientID, ck.leaderID, *args, reply.Err)

			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)

			continue
		}
		DPrintf("client|[%v] call server|[%v] suncc  PutAppend with %+v args get reply %+v",
			ck.clientID, ck.leaderID, *args, *reply)
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}
