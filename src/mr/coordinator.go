package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type taskState int

const (
	Idle taskState = iota
	InProgress
	Completed
)

type Task struct {
	Type      string // "map" , "reduce" , "wait" or "exit"
	State     taskState
	ID        int
	NMap int
	NReduce int
	InputFile string
	StartTime int64
}

type Coordinator struct {
	mu            sync.Mutex
	mapTasks      []Task
	reduceTasks   []Task
	nMap          int
	nReduce       int
	allMapDone    bool
	allReduceDone bool
}

func (c *Coordinator) RequestTask(args *RequestArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.allMapDone {
		//检查是否有map任务
		for i := 0; i < c.nMap; i++ {
			//检测超时任务
			if c.mapTasks[i].State == InProgress && time.Now().Unix()-c.mapTasks[i].StartTime > 10 {
				c.mapTasks[i].State = Idle
			}

			//返回一个空闲的map任务
			if c.mapTasks[i].State == Idle {
				c.mapTasks[i].State = InProgress
				c.mapTasks[i].StartTime = time.Now().Unix()
				reply.Task = c.mapTasks[i]
				log.Printf("Sending task: %+v, nMap: %d, nReduce: %d", reply.Task, reply.Task.NMap, reply.Task.NReduce)
				return nil
			}
		}

		//暂时没有空闲任务，通知worker等待
		reply.Task = Task{
			Type: "wait",
		}
		return nil
	}
	if !c.allReduceDone {
		//检查是否有reduce任务
		for i := 0; i < c.nReduce; i++ {
			//检测超时任务
			if c.reduceTasks[i].State == InProgress && time.Now().Unix()-c.reduceTasks[i].StartTime > 10 {
				c.reduceTasks[i].State = Idle
			}

			//返回一个空闲的reduce任务
			if c.reduceTasks[i].State == Idle {
				c.reduceTasks[i].State = InProgress
				c.reduceTasks[i].StartTime = time.Now().Unix()
				reply.Task = c.reduceTasks[i]
				return nil
			}
		}

		//暂时没有空闲任务，通知worker等待
		reply.Task = Task{
			Type: "wait",
		}
		return nil
	}
	//所有任务都完成,通知worker退出
	reply.Task = Task{
		Type: "exit",
	}
	return nil
}

// TaskDone 告诉coordinator任务已经完成
func (c *Coordinator) TaskDone(args *RequestArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == "map" {
		c.mapTasks[args.TaskID].State = Completed
	} else if args.TaskType == "reduce" {
		c.reduceTasks[args.TaskID].State = Completed
	}

	//检查是否所有map任务都完成
	c.allMapDone = true
	for i := 0; i < c.nMap; i++ {
		if c.mapTasks[i].State != Completed {
			c.allMapDone = false
			break
		}
	}

	//检查是否所有reduce任务都完成
	c.allReduceDone = true
	for i := 0; i < c.nReduce; i++ {
		if c.reduceTasks[i].State != Completed {
			c.allReduceDone = false
			break
		}
	}

	if c.allMapDone && c.allReduceDone {
		log.Println("all tasks done")
		time.Sleep(3 * time.Second)
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	err := rpc.Register(c)
	if err != nil {
		return
	}
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	_ = os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		err := http.Serve(l, nil)
		if err != nil {

		}
	}()
}

func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	//当所有map和reduce任务都完成时，返回true
	return c.allMapDone && c.allReduceDone
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
		nMap:        len(files),
		nReduce:     nReduce,
	}

	//初始化map任务
	for i, file := range files {
		c.mapTasks[i] = Task{
			Type:      "map",
			State:     Idle,
			ID:        i,
			InputFile: file,
			NMap:c.nMap,
			NReduce:c.nReduce,
		}
	}

	//初始化reduce任务
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			Type:    "reduce",
			State:   Idle,
			ID:      i,
			NMap:c.nMap,
			NReduce:c.nReduce,
		}
	}

	c.server()
	return &c
}
