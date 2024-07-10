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
	FileStatus   []int    //0无人做 1在做 2做完了
	TotalFileNum int
	Workers      []*WorkerArgs //记录是哪个worker
	Stage        int
	Pointer      int
}
type WorkerArgs struct {
	WorkerNum     int //worker编号
	FileName      string
	WorkSerial    int //文件编号
	HeartBeatChan chan bool
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

	//pointer指向目前连续已完成文件的最后一个文件(为了失败补救)
	for i := 0; i < c.TotalFileNum; i++ {
		fmt.Println(c.FileStatus[i])
		if c.FileStatus[i] != 2 {
			c.Pointer = i
			break
		}
	}

	switch args.Status {
	case 0:
		if c.Pointer < c.TotalFileNum {
			var mu sync.Mutex
			mu.Lock()
			reply.BaseMsg = NewBaseMsg(200, "success to assign")
			for i := c.Pointer; i < c.TotalFileNum; i++ { //assign map work
				if c.FileStatus[i] == 0 {
					reply.WorkSerial = i
					break
				}
			}
			reply.FileName = c.FileNames[reply.WorkSerial]
			workerArgs := &WorkerArgs{
				WorkerNum:  args.WorkerNum,
				WorkSerial: reply.WorkSerial,
				FileName:   c.FileNames[reply.WorkSerial],
			}
			workerArgs.HeartBeatChan = make(chan bool)
			c.Workers = append(c.Workers, workerArgs)
			mu.Unlock()

			go workerArgs.HeartbeatCheck()

		} else {
			//reduce work
		}
	case 1:
		c.FileStatus[args.WorkSerial] = 2
		fmt.Printf("work %v done\n", args.WorkSerial)

		if c.Pointer < c.TotalFileNum {
			var mu sync.Mutex
			mu.Lock()
			//assign map work
			for i := c.Pointer; i < c.TotalFileNum; i++ {
				if c.FileStatus[i] == 0 {
					reply.WorkSerial = i
					break
				}
			}
			//reply.WorkSerial++
			reply.FileName = c.FileNames[reply.WorkSerial]

			for _, worker := range c.Workers {
				if args.WorkerNum == worker.WorkerNum {
					worker.WorkSerial = reply.WorkSerial
					worker.FileName = c.FileNames[reply.WorkSerial]
				}
				reply.BaseMsg = NewBaseMsg(200, "")
			}

			//workerArgs.HeartBeatChan = make(chan bool)
			//c.Workers = append(c.Workers, workerArgs)
			mu.Unlock()

			//go workerArgs.HeartbeatCheck()

		} else {
			reply.BaseMsg = NewBaseMsg(400, "")
		}
	}

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

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		FileNames:    files,
		FileStatus:   make([]int, len(files)),
		TotalFileNum: len(files),
		Stage:        0,
		Pointer:      0,
	}
	for i := 0; i < len(files); i++ {
		c.FileStatus[i] = 0
	} //init
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
			log.Printf("%v dead,%v release", t.WorkerNum, t.WorkSerial)
			c.FileStatus[t.WorkSerial] = 0
			//todo:release c.Workers
		}
	}
}
