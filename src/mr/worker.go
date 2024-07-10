package mr

import (
	"encoding/json"
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
	for {
		ok := call("Coordinator.Worker", &args, &reply)
		if ok {
			go SendHeartbeat(workerNum)
			switch reply.BaseMsg.Code {
			case 200:
				Map(reply.FileName, args.WorkerNum, reply.WorkSerial, mapf, reducef)
				break
			case 300:
				//fmt.Println(*reply.Intermediate)
				Reduce(args.WorkerNum, reply.WorkSerial, reducef)
				break
			case 400:
				time.Sleep(time.Second)
				continue
			}
		} else {
			fmt.Printf("call failed!\n")
			break
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	//forever := make(chan bool)
	//<-forever
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
func SendMapDoneMsg(workerNum int, workSerial int, intermediate *[]KeyValue,
	mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	args := RPCArgs{
		Status:       1,
		WorkerNum:    workerNum,
		WorkSerial:   workSerial,
		Intermediate: intermediate,
		Stage:        1,
	}
	reply := RPCReply{}
	for {

		ok := call("Coordinator.Worker", &args, &reply)
		if ok {
			switch reply.BaseMsg.Code {
			case 200:
				Map(reply.FileName, args.WorkerNum, reply.WorkSerial, mapf, reducef)
				break
			case 300:
				Reduce(args.WorkerNum, reply.WorkSerial, reducef)
				break
			case 400:
				time.Sleep(time.Second)
				continue
			}
		}
		time.Sleep(time.Second * 1)
	}

}
func SendReduceDoneMsg(workerNum int, workSerial int,
	reducef func(string, []string) string) {
	args := RPCArgs{
		Status:     1,
		WorkerNum:  workerNum,
		WorkSerial: workSerial,
		Stage:      2,
	}
	reply := RPCReply{}
	ok := call("Coordinator.Worker", &args, &reply)
	if ok {
		switch reply.BaseMsg.Code {
		//no 200 此时已无Map任务
		case 300:
			Reduce(args.WorkerNum, reply.WorkSerial, reducef)
		case 400:
			log.Println("nothing to do")
			os.Exit(0)
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

	//单体式直接把Map后的中间结果临时保存在了一个切片内，但是分布式显然
	//不能这么做，分布式系统通过Map产生的中间结果一定不能相互干扰
	intermediate := []KeyValue{}
	// 对txt文件进行Map，将获得的key value 切片合并
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))      //Map 函数生成一组中间结果（键值对）
	intermediate = append(intermediate, kva...) //一个txt文件的key value
	sort.Sort(ByKey(intermediate))              //通过规则进行sort

	//不是很懂mr-X-Y代表的什么
	tmpName := strings.Join([]string{"mr", strconv.Itoa(workSerial), strconv.Itoa(ihash(strconv.Itoa(workSerial)))}, "-")
	tmpFile, _ := os.Create(tmpName)
	enc := json.NewEncoder(tmpFile)
	for _, kv := range intermediate {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal("map create tmp file error")
		}
	}

	SendMapDoneMsg(workerNum, workSerial, &intermediate, mapf, reducef)

}

func Reduce(workerNum int, workSerial int, reducef func(string, []string) string) {
	kva := []KeyValue{}
	//reduce
	//每个 Reduce 函数接收来自 Map 步骤的中间结果，并进行汇总、聚合或其他计算。
	//Reduce 函数生成最终的输出结果。
	//intermediate := []KeyValue{} //tmp
	tmpName := strings.Join([]string{"mr", strconv.Itoa(workSerial), strconv.Itoa(ihash(strconv.Itoa(workSerial)))}, "-")
	tmpFile, _ := os.Open(tmpName)
	dec := json.NewDecoder(tmpFile)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	tmpFile.Close()
	os.Remove(tmpName)

	i := 0
	//单体式通过一个比较巧妙的循环分割了reduce任务，分布式的reduce任务又应该怎么划分？
	oname := strings.Join([]string{"mr", "out", strconv.Itoa(workSerial)}, "-")
	ofile, _ := os.Create(oname)
	for i < len(kva) {
		// 对相同的单词进行计数，保存到values切片，再进行Reduce
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	ofile.Close()
	SendReduceDoneMsg(workerNum, workSerial, reducef)
}
