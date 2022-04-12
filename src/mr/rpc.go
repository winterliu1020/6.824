package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
// worker去向master取任务（map task/ reduce task），是通过rpc request，而且发一个rpc 请求可能是：取一个任务/完成了一个任务然后给master一个消息说完成了，所以rpc request有两种类型
type MapReduceTask struct {
	TaskId int // task的唯一标识符

	TaskType string // Map, Reduce, Wait
	//TaskStatus string // Unassigned, Assigned, Finished

	// 因为我用链表，所以不需要TaskNum
	//TaskNum int

	MapFile     string   // map任务对应的file路径
	ReduceFiles []string // reduce任务对应的许多个文件

	NumReduce int // 也就是该task所在的MapReduce系统中定义了几个reduce，因为等会执行map任务的时候，产生的key/value会根据hash（这里就是去模这个NumReduce）放到对应的reduce中间文件
	NumMap    int // MapReduce系统中定义了几个map，这个其实没啥用，input文件的个数决定了map的个数

}

type RpcRequestArgs struct {
	RequestType string
	Task        MapReduceTask
}
type RpcReply struct {
	Task MapReduceTask
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
