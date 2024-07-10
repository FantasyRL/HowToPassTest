package mr

import (
	"fmt"
	"io/ioutil"
	rand2 "math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	workerNum := rand2.Int()
	// Your worker implementation here.
	args := RPCArgs{
		//Status:    0,
		WorkerNum: workerNum,
	}

	//发送args 接收reply
	reply := RPCReply{}
	ok := call("Coordinator.Worker", &args, &reply)
	if ok {
		go SendHeartbeat(workerNum)
		switch reply.BaseMsg.Code {
		case 200:
			Map(reply.FileName, args.WorkerNum, reply.WorkSerial, mapf, reducef)

		case 400:
			log.Println("nothing to do")
			return
		}
	} else {
		fmt.Printf("call failed!\n")
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	forever := make(chan bool)
	<-forever
}
func SendHeartbeat(workerNum int) {
	ticker := time.NewTicker(time.Second * 3)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			args := RPCArgs{
				WorkerNum: workerNum,
			}
			reply := RPCReply{}
			ok := call("Coordinator.ReceiveHeartbeat", &args, &reply)
			if !ok {
				log.Printf("heartbeat coordinator failed!\n")
			}
		}
	}
}
func SendMapDoneMsg(workerNum int, workSerial int,
	mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	args := RPCArgs{
		Status:     1,
		WorkerNum:  workerNum,
		WorkSerial: workSerial,
	}
	reply := RPCReply{}
	ok := call("Coordinator.Worker", &args, &reply)
	if ok {
		switch reply.BaseMsg.Code {
		case 200:
			Map(reply.FileName, args.WorkerNum, reply.WorkSerial, mapf, reducef)

		case 400:
			log.Println("nothing to do")
			return
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//func CallExample() {
//
//	// declare an argument structure.
//	args := WorkerArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := WorkerReply{}
//
//	// send the RPC request, wait for the reply.
//	// the "Coordinator.Example" tells the
//	// receiving server that we'd like to call
//	// the Example() method of struct Coordinator.
//	ok := call("Coordinator.Example", &args, &reply)
//	if ok {
//		reply.Y should be 100.
//		fmt.Printf("reply.Y %v\n", reply.Y)
//	} else {
//		fmt.Printf("call failed!\n")
//	}
//}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func Map(fileName string, workerNum int, workSerial int,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	intermediate := []KeyValue{}
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	intermediate = append(intermediate, kva...)

	SendMapDoneMsg(workerNum, workSerial, mapf, reducef) //WIP

	//reduce
	sort.Sort(ByKey(intermediate)) //通过规则进行sort

	oname := strings.Join([]string{"mrtmp", strconv.Itoa(workSerial), strconv.Itoa(ihash(strconv.Itoa(workSerial)))}, "-")
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}
