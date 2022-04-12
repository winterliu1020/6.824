package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

/* worker当中用到的一些方法 */
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func intermediateFilename(numMapTask int, numReduceTask int) string {
	return fmt.Sprintf("mr-%v-%v", numMapTask, numReduceTask)
}

// 把一维度键值对数组存储到文件
func storeIntermediateFile(kva []KeyValue, filename string) {
	file, err := os.Create(filename)
	defer file.Close()

	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	enc := json.NewEncoder(file)
	if enc == nil {
		log.Fatalf("cannot create encoder")
	}
	for _, kv := range kva {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encoder")
		}
	}
}

func finishTask(task MapReduceTask) {
	args := RpcRequestArgs{RequestType: "finish", Task: task}
	reply := RpcReply{}
	call("Master.MapReduceHandler", &args, &reply)
}

// 读取文件中的数据到一维数据
func loadIntermediateFile(filename string) []KeyValue {
	var kva []KeyValue
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("loadIntermediateFile cannot open %v", filename)
	}
	dec := json.NewDecoder(file)
	for {
		kv := KeyValue{}
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}

func mapTask(mapf func(string, string) []KeyValue, task MapReduceTask) {
	// 这个map函数：其实就是把一个file里所有键值对，根据hash分散到不同子文件中
	filename := task.MapFile

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("mapTask cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("mapTask cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))
	kvaa := make([][]KeyValue, task.NumReduce)

	for _, kv := range kva {
		idx := ihash(kv.Key) % task.NumReduce // 子文件的reduce编号
		kvaa[idx] = append(kvaa[idx], kv)
	}

	for i := 0; i < task.NumReduce; i++ {
		// 把kvaa[i]这个一维数组的值存在这个文件中
		storeIntermediateFile(kvaa[i], intermediateFilename(task.TaskId, i))
	}

	defer finishTask(task)

}

func reduceTask(reducef func(string, []string) string, task MapReduceTask) {
	// 把数据从该reduceTask对于的子文件中读取到一维数组
	var intermediate []KeyValue
	for _, filename := range task.ReduceFiles {
		intermediate = append(intermediate, loadIntermediateFile(filename)...)
	}
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%v", task.TaskId)
	ofile, _ := os.Create(oname)

	// 统计当前分区中包含的所有子文件key, value情况
	i := 0
	for i < len(intermediate) {
		// 遍历当前分区对应的数据，也就是intermediate这个一维数组
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		// j指向了下一个key的第一个元素位置
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		// 然后我要去指向reduce函数，统计当前key出现的总的次数
		out := reducef(intermediate[i].Key, values)

		// 把当前key的统计结果写到文件中
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, out)

		i = j
	}

	ofile.Close()

	defer finishTask(task)
}

func waitTask() {
	// master让worker等待
	time.Sleep(time.Second)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// worker要做哪些事情呢：其实就是不断的去向master发请求，根据响应包的类型进行执行
	for {
		args := RpcRequestArgs{RequestType: "request"}
		reply := RpcReply{}

		response := call("Master.MapReduceHandler", &args, &reply)
		//log.Println("worker send a request....")
		if !response {
			//log.Println("shibai........")
			break
		}

		switch reply.Task.TaskType {
		case "Map":
			mapTask(mapf, reply.Task)
			//log.Println("get a map task...")
		case "Reduce":
			reduceTask(reducef, reply.Task)
			//log.Println("get a reduce task...")
		case "Wait":
			waitTask()
			//log.Println("get a wait task...")
		}
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
