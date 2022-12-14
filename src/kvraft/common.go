package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	OpGet          = "Get"
	OpPut          = "Put"
	OpAppend       = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	ClientID  int64
	CommandID int64
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err      Err
	LeaderID int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err      Err
	Value    string
	LeaderID int
}
