package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mapTasks     chan int //map任务 为什么要用chan？
	reduceTasks  chan int //reduce任务
	nReduce      int      //reduce任务量 跟reduceCnt有什么区别？
	nMap         int
	mapRuning    []int64 //正在跑的map任务
	reduceRuning []int64
	tasks        []string //为什么要用string？
	mapCnt       int
	reduceCnt    int
	taskDone     bool       //整个任务完成
	mutex        *sync.Cond //为什么要用条件变量？
}

// Your code here -- RPC handlers for the worker to call.
//响应client请求任务
func (c *Coordinator) handleTaskReq(args *ExampleArgs, reply *ExampleReply) error {
	//为什么取长度不需要加锁
	//先响应map 任务
	if len(c.mapTasks) > 0 {
		reply.RepId = args.ReqId
		reply.Repop = TaskMap
		reply.RepTaskId = <-c.mapTasks
		reply.RepnMap = c.nMap
		reply.RepContent = c.tasks[reply.RepTaskId]
		reply.RepnReduce = c.nReduce
		c.mutex.L.Lock()
		c.mapRuning[reply.RepTaskId] = args.ReqId
		c.mutex.L.Unlock()
		go func(taskId int) {
			time.Sleep(10 * time.Second)
			c.mutex.L.Lock()
			if c.mapRuning[taskId] != 1 {
				c.mapTasks <- taskId
			} else {
				c.mapCnt--
			}
			c.mutex.L.Lock()
			c.mutex.L.Unlock()
		}(reply.RepTaskId)
		return nil
	}
	//如果队列中的map任务为0
	if len(c.mapTasks) == 0 {
		c.mutex.L.Lock()
		mapCurr := c.mapCnt
		//reduceCurr := c.reduceCnt
		c.mutex.L.Unlock()
		//仍然有未完成的map任务
		if mapCurr > 0 {
			reply.RepId = args.ReqId
			reply.Repop = TaskWait
			return nil
		}
		//如果队列中的reduce任务大于0
		if len(c.reduceTasks) > 0 {
			reply.RepId = args.ReqId
			reply.Repop = TaskReduce
			reply.RepTaskId = <-c.reduceTasks
			reply.RepnMap = c.nMap
			reply.RepnReduce = c.nReduce
			c.mutex.L.Lock()
			c.reduceRuning[reply.RepTaskId] = args.ReqId
			c.mutex.L.Unlock()
			go func(taskId int) {
				time.Sleep(10 * time.Second)
				c.mutex.L.Lock()
				if c.reduceRuning[taskId] != 1 {
					c.reduceTasks <- taskId
					return
				}
				c.reduceCnt--
				if c.reduceCnt == 0 {
					c.taskDone = true
				}
				c.mutex.L.Unlock()
			}(reply.RepTaskId)
			//函数退出后，子协程后退出吗？？?main函数和普通函数
			return nil

		}
		//仍然有reduce任务未完成
		reply.RepId = args.ReqId
		reply.Repop = TaskWait

		return nil

	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	//reply.Y = args.X + 1
	c.mutex.L.Lock()
	allDone := c.taskDone
	c.mutex.L.Unlock()
	//任务完成
	if allDone {
		reply.RepId = args.ReqId
		reply.Repop = TaskDone
		return nil
	}
	switch args.ReqOp {
	case TaskReq:
		return c.handleTaskReq(args, reply)
	case TaskMapDone:
		c.mutex.L.Lock()
		if c.mapRuning[args.ReqTaskId] == args.ReqId {
			c.mapRuning[args.ReqTaskId] = 1
		}
		c.mutex.L.Unlock()
	case TaskReduceDone:
		c.mutex.L.Lock()
		if c.reduceRuning[args.ReqTaskId] == args.ReqId {
			c.reduceRuning[args.ReqTaskId] = 1
		}
		c.mutex.L.Unlock()
	default:
		return nil
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mutex.L.Lock() //条件变量的结构？
	ret := c.taskDone
	c.mutex.L.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapTasks = make(chan int, len(files))
	c.reduceTasks = make(chan int, nReduce)
	c.nReduce = nReduce
	c.nMap = len(files)
	c.mapRuning = make([]int64, c.nMap) //为什么还没分配就有数量
	c.reduceRuning = make([]int64, c.nReduce)
	c.tasks = make([]string, c.nMap)
	c.mapCnt = c.nMap
	c.reduceCnt = nReduce
	c.mutex = sync.NewCond(&sync.Mutex{})

	for i := 0; i < len(files); i++ {
		c.tasks[i] = files[i]
		c.mapRuning[i] = 0
		c.mapTasks <- i
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks <- i
		c.reduceRuning[i] = 0
	}
	c.server()
	return &c
}
