package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	ReqId     int64  //请求ID，如何生成？
	ReqOp     Optype //请求类型
	ReqTaskId int    //任务Id，为什么请求体里面要任务ID(告诉协调器任务完成)
}

type ExampleReply struct {
	RepId      int64
	Repop      Optype
	RepTaskId  int
	RepnMap    int //这是什么 Map任务的数量？
	RepnReduce int
	RepContent string //字符串传什么？
}

type Optype int

const (
	TaskReq Optype = iota //这是什么写法？为什么后面都不用声明类型？
	TaskMap
	TaskReduce
	TaskReduceDone
	TaskMapDone
	TaskDone
	TaskWait
)

type TaskState int

const (
	Running TaskState = iota
	Stopped
	Rebooting
	Terminated
)

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
