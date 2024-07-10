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
	Intermediates []*[]KeyValue

	Workers []*WorkerArgs //记录是哪个worker

}
type WorkerArgs struct {
	WorkerNum int //worker编号
	//FileName      string//这是不必要的
	WorkSerial    int //文件编号
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
		if c.MapPointer != c.TotalFileNum-1 {
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
				reply.BaseMsg = NewBaseMsg(400, "")
				break
			}
			reply.BaseMsg = NewBaseMsg(200, "")
			reply.FileName = c.FileNames[reply.WorkSerial]
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

		} else if c.ReducePointer != (c.TotalFileNum-1) && c.MapPointer == (c.TotalFileNum-1) {
			//reduce work
			reply.BaseMsg = NewBaseMsg(300, "")

			for i := c.ReducePointer; i < c.TotalFileNum; i++ { //assign reduce work
				if c.ReduceStatus[i] == 0 {
					reply.WorkSerial = i
					c.ReduceStatus[i] = 1
					break
				}
			}
			//reply.Intermediate = c.Intermediates[reply.WorkSerial]

			workerArgs := &WorkerArgs{
				WorkerNum:  args.WorkerNum,
				WorkSerial: reply.WorkSerial,
				Stage:      2,
			}
			workerArgs.HeartBeatChan = make(chan bool)
			c.Workers = append(c.Workers, workerArgs)
			mu.Unlock()
			go workerArgs.HeartbeatCheck()
		} else if c.ReducePointer == c.TotalFileNum-1 {
			//todo:done
		} else {
			reply.BaseMsg = NewBaseMsg(400, "") //wait
		}
	case 1: //完成后再次
		if args.Stage == 1 {
			c.MapStatus[args.WorkSerial] = 2
			//c.Intermediates[args.WorkSerial] = args.Intermediate
			fmt.Printf("map %v done\n", args.WorkSerial)
		} else {
			c.ReduceStatus[args.WorkSerial] = 2
			fmt.Printf("reduce %v done\n", args.WorkSerial)
		}

		if c.MapPointer != c.TotalFileNum-1 {
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
				reply.BaseMsg = NewBaseMsg(400, "")
				break
			}
			reply.FileName = c.FileNames[reply.WorkSerial]

			for _, worker := range c.Workers {
				if args.WorkerNum == worker.WorkerNum {
					worker.WorkSerial = reply.WorkSerial
					worker.Stage = 1
					//worker.FileName = c.FileNames[reply.WorkSerial]
				}
				reply.BaseMsg = NewBaseMsg(200, "")
			}

			//workerArgs.HeartBeatChan = make(chan bool)
			//c.Workers = append(c.Workers, workerArgs)
			//mu.Unlock()

			//go workerArgs.HeartbeatCheck()

		} else if c.ReducePointer != (c.TotalFileNum-1) && c.MapPointer == (c.TotalFileNum-1) {
			for i := c.ReducePointer; i < c.TotalFileNum; i++ { //assign reduce work
				if c.ReduceStatus[i] == 0 {
					reply.WorkSerial = i
					c.ReduceStatus[i] = 1
					break
				}
			}
			//reply.Intermediate = c.Intermediates[reply.WorkSerial]
			for _, worker := range c.Workers {
				if args.WorkerNum == worker.WorkerNum {
					worker.WorkSerial = reply.WorkSerial
					worker.Stage = 2
					//worker.FileName = c.FileNames[reply.WorkSerial]
				}
				reply.BaseMsg = NewBaseMsg(300, "")
			}
		} else if c.ReducePointer == c.TotalFileNum-1 {
			//todo:done
		} else {
			reply.BaseMsg = NewBaseMsg(400, "") //wait
		}
	}
	//mu.Lock()
	//pointer指向目前连续已完成文件的最后一个文件(为了失败补救，同时确认是否进入reduce阶段)
	if c.MapPointer != c.TotalFileNum-1 {
		for i := 0; i < c.TotalFileNum; i++ {
			if c.MapStatus[i] != 2 {
				c.MapPointer = i
				break
			}
		}
	} else if c.ReducePointer != c.TotalFileNum-1 {
		for i := 0; i < c.TotalFileNum; i++ {
			if c.ReduceStatus[i] != 2 {
				c.ReducePointer = i
				break
			}
		}
	}
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
	if c.ReducePointer == c.TotalFileNum-1 {
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
		ReduceStatus:  make([]int, len(files)),
		Intermediates: make([]*[]KeyValue, len(files)),
	}
	for i := 0; i < len(files); i++ {
		c.MapStatus[i] = 0
		c.ReduceStatus[i] = 0
		c.Intermediates[i] = nil
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
			if t.WorkSerial != -1 {
				var mu sync.Mutex
				mu.Lock()
				switch t.Stage {
				case 1:
					log.Printf("%v dead,map work %v release", t.WorkerNum, t.WorkSerial)
					c.MapStatus[t.WorkSerial] = 0
				case 2:
					log.Printf("%v dead,reduce work %v release", t.WorkerNum, t.WorkSerial)
					c.ReduceStatus[t.WorkSerial] = 0
				}
				mu.Unlock()
			} else {
				log.Printf("worker %v release", t.WorkerNum)
			}

			//todo:release c.Workers
		}
	}
}
