package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var DeregisterChan chan *WorkerArgs

type Coordinator struct {
	// Your definitions here.
	FileNames    []string //文件名
	TotalFileNum int

	MapPointer int
	MapStatus  []int //0无人做 1在做 2做完了

	ReducePointer int
	ReduceStatus  []int
	NReduceCount  int

	Workers []*WorkerArgs //记录是哪个worker

}
type WorkerArgs struct {
	WorkerNum int //worker编号
	//FileName      string//这是不必要的
	WorkSerial    int //文件编号
	NSerial       int
	HeartBeatChan chan bool
	Stage         int //1Map 2Reduce
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//	func (c *Coordinator) Example(args *WorkerArgs, reply *WorkerReply) error {
//		reply.Y = args.X + 1
//		return nil
//	}

func (c *Coordinator) Worker(args *RPCArgs, reply *RPCReply) error {
	log.Printf("worker:%v called.", args.WorkerNum)

	var mu sync.Mutex
	mu.Lock()
	switch args.Status {
	case 0: //初次
		if c.MapPointer != c.TotalFileNum {
			isExistTodoFlag := false
			for i := c.MapPointer; i < c.TotalFileNum; i++ {
				if c.MapStatus[i] == 0 {
					isExistTodoFlag = true
				}
				if c.MapStatus[i] == 0 {
					reply.WorkSerial = i
					c.MapStatus[i] = 1
					break
				}
			}
			if !isExistTodoFlag {
				reply.BaseMsg = NewBaseMsg(500, "")
				break
			}
			reply.BaseMsg = NewBaseMsg(200, "")
			reply.FileName = c.FileNames[reply.WorkSerial]
			reply.NReduceCount = c.NReduceCount
			reply.WorkCount = c.TotalFileNum

			workerArgs := &WorkerArgs{
				WorkerNum:  args.WorkerNum,
				WorkSerial: reply.WorkSerial,
				Stage:      1,
				//FileName:   c.FileNames[reply.WorkSerial],
			}
			workerArgs.HeartBeatChan = make(chan bool)
			c.Workers = append(c.Workers, workerArgs)
			//mu.Unlock()
			go workerArgs.HeartbeatCheck()

		} else if c.ReducePointer != c.NReduceCount {
			//reduce work
			isExistTodoFlag := false
			for i := c.ReducePointer; i < c.NReduceCount; i++ {
				if c.ReduceStatus[i] == 0 {
					isExistTodoFlag = true
				} //assign reduce work
				if c.ReduceStatus[i] == 0 {
					reply.NSerial = i
					c.ReduceStatus[i] = 1
					break
				}
			}
			if !isExistTodoFlag {
				reply.BaseMsg = NewBaseMsg(500, "")
				break
			}

			reply.BaseMsg = NewBaseMsg(300, "")
			reply.NReduceCount = c.NReduceCount
			reply.WorkCount = c.TotalFileNum

			workerArgs := &WorkerArgs{
				WorkerNum: args.WorkerNum,
				NSerial:   reply.NSerial,
				Stage:     2,
			}
			workerArgs.HeartBeatChan = make(chan bool)
			c.Workers = append(c.Workers, workerArgs)
			go workerArgs.HeartbeatCheck()
		} else if c.ReducePointer == c.NReduceCount {
			reply.BaseMsg = NewBaseMsg(400, "")
		} else {
			reply.BaseMsg = NewBaseMsg(500, "") //wait
		}
	case 1: //完成后再次
		if args.Stage == 1 {
			if c.MapStatus[args.WorkSerial] != 2 {
				c.MapStatus[args.WorkSerial] = 2
				fmt.Printf("map %v done\n", args.WorkSerial)
			}
		} else {
			if c.ReduceStatus[args.NSerial] != 2 {
				c.ReduceStatus[args.NSerial] = 2
				fmt.Printf("reduce %v done\n", args.NSerial)
			}
		}

		if c.MapPointer != c.TotalFileNum {
			//assign map work
			isExistTodoFlag := false
			for i := c.MapPointer; i < c.TotalFileNum; i++ {
				if c.MapStatus[i] == 0 {
					isExistTodoFlag = true
				}
				if c.MapStatus[i] == 0 {
					reply.WorkSerial = i
					c.MapStatus[i] = 1
					break
				}
			}
			if !isExistTodoFlag {
				reply.BaseMsg = NewBaseMsg(500, "")
				break
			}
			reply.FileName = c.FileNames[reply.WorkSerial]
			reply.NReduceCount = c.NReduceCount
			reply.WorkCount = c.TotalFileNum

			for _, worker := range c.Workers {
				if args.WorkerNum == worker.WorkerNum {
					worker.WorkSerial = reply.WorkSerial
					worker.Stage = 1
				}
				reply.BaseMsg = NewBaseMsg(200, "")
			}
		} else if c.ReducePointer != c.NReduceCount {
			isExistTodoFlag := false
			for i := c.ReducePointer; i < c.NReduceCount; i++ {
				if c.ReduceStatus[i] == 0 {
					isExistTodoFlag = true
				} //assign reduce work
				if c.ReduceStatus[i] == 0 {
					reply.NSerial = i
					c.ReduceStatus[i] = 1
					break
				}
			}
			if !isExistTodoFlag {
				reply.BaseMsg = NewBaseMsg(500, "")
				break
			}
			reply.NReduceCount = c.NReduceCount
			reply.WorkCount = c.TotalFileNum

			for _, worker := range c.Workers {
				if args.WorkerNum == worker.WorkerNum {
					worker.NSerial = reply.NSerial
					worker.Stage = 2
				}
				reply.BaseMsg = NewBaseMsg(300, "")
			}
		} else if c.ReducePointer == c.NReduceCount {
			reply.BaseMsg = NewBaseMsg(400, "")
		} else {
			reply.BaseMsg = NewBaseMsg(500, "") //wait
		}
	}
	//mu.Lock()
	//pointer指向目前连续已完成文件的最后一个文件(为了失败补救，同时确认是否进入reduce阶段)
	//todo:problem ann
	if c.MapPointer != c.TotalFileNum {
		allDone := true
		for i := 0; i < c.TotalFileNum; i++ {
			if c.MapStatus[i] != 2 {
				allDone = false
				c.MapPointer = i
				break
			}
		}
		if allDone {
			c.MapPointer = c.TotalFileNum
		}
	} else if c.ReducePointer != c.NReduceCount {
		allDone := true
		for i := 0; i < c.NReduceCount; i++ {
			if c.ReduceStatus[i] != 2 {
				allDone = false
				c.ReducePointer = i
				break
			}
		}
		if allDone {
			c.ReducePointer = c.NReduceCount
		}
	}
	//fmt.Println(c.ReduceStatus)
	//fmt.Println(c.ReducePointer)
	mu.Unlock()

	return nil
}

func (c *Coordinator) ReceiveHeartbeat(args *RPCArgs, reply *RPCReply) error {

	for _, worker := range c.Workers {
		if args.WorkerNum == worker.WorkerNum {
			log.Printf("%v heartbeat\n", args.WorkerNum)
			worker.HeartBeatChan <- true
		}
		reply.BaseMsg = NewBaseMsg(200, "")
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	//time.Sleep(time.Second * 4)
	if c.ReducePointer == c.NReduceCount {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	//init
	c := Coordinator{
		FileNames:    files,
		MapStatus:    make([]int, len(files)),
		TotalFileNum: len(files),
		//Stage:        0,
		MapPointer:    0,
		ReducePointer: 0,
		ReduceStatus:  make([]int, nReduce),
		NReduceCount:  nReduce,
	}
	for i := 0; i < len(files); i++ {
		c.MapStatus[i] = 0
		c.ReduceStatus[i] = 0
	}
	for i := len(files); i < nReduce; i++ {
		c.ReduceStatus[i] = 0
	}

	DeregisterChan = make(chan *WorkerArgs)
	go c.DeRegister()

	c.server()
	return &c
}

func (args *WorkerArgs) HeartbeatCheck() {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			DeregisterChan <- args //didn't work
			return
		case <-args.HeartBeatChan:
			ticker.Reset(10 * time.Second)
		}
	}
}

func (c *Coordinator) DeRegister() {
	for {
		select {
		case t := <-DeregisterChan:
			if c.ReducePointer != c.NReduceCount {
				switch t.Stage {
				case 1:
					log.Printf("%v dead,map work %v release", t.WorkerNum, t.WorkSerial)
					c.MapStatus[t.WorkSerial] = 0
				case 2:
					log.Printf("%v dead,reduce work %v release", t.WorkerNum, t.NSerial)
					c.ReduceStatus[t.NSerial] = 0
				}
			} else {
				log.Printf("worker %v release", t.WorkerNum)
			}

			//todo:release c.Workers
		}
	}
}
