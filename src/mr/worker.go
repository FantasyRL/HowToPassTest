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
		Status:    0,
		WorkerNum: workerNum,
	}

	//发送args 接收reply
	reply := RPCReply{}
	go SendHeartbeat(workerNum)
	for {
		ok := call("Coordinator.Worker", &args, &reply)
		if ok {
			switch reply.BaseMsg.Code {
			case 200:
				Map(reply.FileName, args.WorkerNum, reply.WorkSerial, reply.NReduceCount, mapf, reducef)
				break
			case 300:
				Reduce(args.WorkerNum, reply.WorkCount, reply.NSerial, reducef) //crash
				break
			case 500:
				time.Sleep(time.Second)
				continue
			case 400:
				os.Exit(0)
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
	time.Sleep(time.Second * 2)
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
		//Intermediate: intermediate,
		Stage: 1,
	}
	reply := RPCReply{}
	for {
		ok := call("Coordinator.Worker", &args, &reply)
		if ok {
			switch reply.BaseMsg.Code {
			case 200:
				Map(reply.FileName, args.WorkerNum, reply.WorkSerial, reply.NReduceCount, mapf, reducef)
				break
			case 300:
				Reduce(args.WorkerNum, reply.WorkCount, reply.NSerial, reducef)
				break
			case 500:
				time.Sleep(time.Second)
				continue
			case 400:
				os.Exit(0)
			}
		} else {
			fmt.Printf("call failed!\n")
			break
		}
	}

}
func SendReduceDoneMsg(workerNum int, nSerial int,
	reducef func(string, []string) string) {
	args := RPCArgs{
		Status:    1,
		WorkerNum: workerNum,
		NSerial:   nSerial,
		Stage:     2,
	}
	reply := RPCReply{}
	for {
		ok := call("Coordinator.Worker", &args, &reply)
		if ok {
			switch reply.BaseMsg.Code {
			//no 200 此时已无Map任务
			case 300:
				Reduce(args.WorkerNum, reply.WorkCount, reply.NSerial, reducef)
			case 500:
				time.Sleep(time.Second)
				continue
			case 400:
				os.Exit(0)
			}
		} else {
			fmt.Printf("call failed!\n")
			break
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

func Map(filename string, workerNum int, workSerial int, nReduceCount int,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	//单体式直接把Map后的中间结果临时保存在了一个切片内，但是分布式显然
	//不能这么做，分布式系统通过Map产生的中间结果一定不能相互干扰
	//intermediate := []KeyValue{}
	// 对txt文件进行Map，将获得的key value 切片合并
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	contents, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(contents)) //Map 函数生成一组中间结果（键值对）
	//sort.Sort(ByKey(kva))//unnecessary

	//nReduce
	//intermediate = append(intermediate, kva...) //一个txt文件的key value
	//按照key，也就是单词进行排序，将相同的单词聚集在一起
	intermediate := make([][]KeyValue, nReduceCount)
	for _, kv := range kva {
		intermediate[ihash(kv.Key)%nReduceCount] = append(intermediate[ihash(kv.Key)%nReduceCount], kv)
	}
	for i := 0; i < nReduceCount; i++ {
		tmpName := strings.Join([]string{"mr", strconv.Itoa(workSerial), strconv.Itoa(i)}, "-")
		os.Remove(tmpName)
		tmpFile, _ := os.Create(tmpName)
		enc := json.NewEncoder(tmpFile)
		for _, kv := range intermediate[i] {
			enc.Encode(kv)
			if err != nil {
				log.Fatal("map create tmp file error")
			}
		}
		tmpFile.Close()
	}

	//incorrect
	//for i := 0; i < nReduceCount; i++ {
	//	tmpName := strings.Join([]string{"mr", strconv.Itoa(workSerial), strconv.Itoa(ihash(strconv.Itoa(i)))}, "-")
	//	tmpFile, _ := os.Create(tmpName)
	//	enc := json.NewEncoder(tmpFile)
	//	for j := i * (len(kva)/nReduceCount + 1); j < (i+1)*(len(kva)/nReduceCount+1) && j < len(kva); j++ {
	//		err := enc.Encode(&kva[j])
	//		if err != nil {
	//			log.Fatal("map create tmp file error")
	//		}
	//	}
	//	tmpFile.Close()
	//}

	SendMapDoneMsg(workerNum, workSerial, mapf, reducef)

}

func Reduce(workerNum int, workCount int, n int, reducef func(string, []string) string) {
	//reduceFileNum := response.TaskId
	//intermediate := shuffle(response.FileSlice)
	//dir, _ := os.Getwd()
	//tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	//if err != nil {
	//	log.Fatal("Failed to create temp file", err)
	//}
	//i := 0
	//for i < len(intermediate) {
	//	j := i + 1
	//	for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
	//		j++
	//	}
	//	var values []string
	//	for k := i; k < j; k++ {
	//		values = append(values, intermediate[k].Value)
	//	}
	//	output := reducef(intermediate[i].Key, values)
	//	fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
	//	i = j
	//}
	//tempFile.Close()
	//fn := fmt.Sprintf("mr-out-%d", reduceFileNum)
	//os.Rename(tempFile.Name(), fn)
	//reduce
	//每个 Reduce 函数接收来自 Map 步骤的中间结果，并进行汇总、聚合或其他计算。
	//Reduce 函数生成最终的输出结果。
	//intermediate := []KeyValue{} //tmp
	kva := make([]KeyValue, 0)
	for i := 0; i < workCount; i++ {
		tmpName := strings.Join([]string{"mr", strconv.Itoa(i), strconv.Itoa(n)}, "-")
		tmpFile, _ := os.Open(tmpName)
		dec := json.NewDecoder(tmpFile)
		defer tmpFile.Close()
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		//os.Remove(tmpName)
	}
	sort.Sort(ByKey(kva))

	i := 0
	//单体式通过一个比较巧妙的循环分割了reduce任务，分布式的reduce任务又应该怎么划分？
	oname := strings.Join([]string{"mr", "out", strconv.Itoa(n)}, "-")
	os.Remove(oname)
	ofile, _ := os.Create(oname)
	defer ofile.Close()
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

	for i := 0; i < workCount; i++ {
		tmpName := strings.Join([]string{"mr", strconv.Itoa(i), strconv.Itoa(n)}, "-")
		os.Remove(tmpName)
	}
	SendReduceDoneMsg(workerNum, n, reducef)
}
