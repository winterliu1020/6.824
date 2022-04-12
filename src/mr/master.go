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

type Master struct {
	// Your definitions here.
	// 因为输入只有已知数量的文件，reduce的个数；
	// 规则是：每一个input文件对应一个map，而每一个map会产生reduce个数的中间文件，
	// 所以mrmaster的输入知道了，则有多少个map任务，多少个reduce任务（就是输入确定的nreduce）就知道了
	// 因为实验是worker通过rpc向master去取map任务，或者reduce任务，因为这两种任务都得worker去做，master是不执行任务的，所以所有的任务数据先存在master数据结构中，等待worker来拿

	// 用一个双向链表来存储任务，如果取出来的任务10s后没有执行成功，则重新放回链表末尾
	//NumMap int
	//NumReduce int

	MapTasks    *LinkedList
	ReduceTasks *LinkedList

	// 存储已分配的但是还没收到finish的task
	notFinishMap    map[int]*Node
	notFinishReduce map[int]*Node

	mu sync.Mutex
}

/* master 的一些方法*/

// master对于每一个task开启一个协程，10s后看一下这个task的状态
func (m *Master) checkTimeout(taskType string, num int, timeout int) {
	time.Sleep(time.Second * time.Duration(timeout))
	m.mu.Lock()
	defer m.mu.Unlock()
	if taskType == "Map" {
		// 相当于10s后看一下这个节点的状态，10s后节点已经被放到hashmap了，hashmap中放的都是已分配但没有结束的node
		mapTask, okMap := m.notFinishMap[num]
		if okMap {
			//if mapTask.data.TaskStatus == "Assigned" {
			//	mapTask.data.TaskType = "Unassigned" // 如果10s后还是Assigned，就得改成Unassigned，并把节点重新放回MapTasks链表中等待重新分配
			//
			//}
			addLast(m.MapTasks, mapTask)
			delete(m.notFinishMap, mapTask.data.TaskId)
		}
	} else {
		reduceTask, okReduce := m.notFinishMap[num]
		if okReduce {
			//if reduceTask.data.TaskStatus == "Assigned" {
			//	reduceTask.data.TaskType = "Unassigned" // 如果10s后还是Assigned，就得改成Assigned
			//
			//}
			addLast(m.ReduceTasks, reduceTask) // 10s后还在map里面，说明这个task没有成功完成，所以放回list中
			delete(m.notFinishReduce, reduceTask.data.TaskId)
		}
	}
}

type LinkedList struct {
	head *Node
	tail *Node
}

/* LinkedList的一些方法 */
func addLast(list *LinkedList, aNode *Node) {
	var temp = list.tail.pre
	temp.next = aNode
	aNode.pre = temp
	aNode.next = list.tail
	list.tail.pre = aNode
}

func removeFirst(list *LinkedList) *Node {
	if list.head.next == list.tail {
		// 空链表
		return nil
	}
	var node = list.head.next
	list.head.next = node.next
	node.next.pre = list.head

	node.pre = nil
	node.next = nil

	return node
}

type Node struct {
	data MapReduceTask
	pre  *Node
	next *Node
}

func getLinkedlist() *LinkedList {
	//linked_list_1 := &LinkedList{}
	//var linked_list_2 *LinkedList
	linked_list_3 := new(LinkedList)
	linked_list_3.head = new(Node)
	linked_list_3.tail = new(Node)
	linked_list_3.head.next = linked_list_3.tail
	linked_list_3.tail.pre = linked_list_3.head
	//return linked_list_1
	//return linked_list_2
	return linked_list_3
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) MapReduceHandler(args *RpcRequestArgs, reply *RpcReply) error {
	// 这个函数是master负责处理worker发过来的请求，包括：task请求、finish请求
	// 加锁
	m.mu.Lock()
	defer m.mu.Unlock() // defer表示延迟执行，即MapReduceHandler函数结束前最后一刻执行
	if args.RequestType == "request" {
		// worker想要一个task
		if !(m.MapTasks.head.next == m.MapTasks.tail && len(m.notFinishMap) == 0) {
			if m.MapTasks.head.next != m.MapTasks.tail {
				//log.Println("master give a mapTask")
				// mapList不为空
				var node = removeFirst(m.MapTasks)
				// 放到notFinishMap中
				m.notFinishMap[node.data.TaskId] = node
				reply.Task = node.data
				go m.checkTimeout("Map", node.data.TaskId, 10)
				return nil
			} else {
				// notFinishMap中还有task还在等待finish响应
				//log.Println("master give a wait")
				reply.Task.TaskType = "Wait"
				return nil
			}
		} else if !(m.ReduceTasks.head.next == m.ReduceTasks.tail && len(m.notFinishReduce) == 0) {
			if m.ReduceTasks.head.next != m.ReduceTasks.tail {
				//log.Println("master give a reduceTask")
				// reduceList不为空
				var node = removeFirst(m.ReduceTasks)
				// 放到notFinishReduce中
				m.notFinishReduce[node.data.TaskId] = node
				reply.Task = node.data
				go m.checkTimeout("Reduce", node.data.TaskId, 10)
				return nil
			} else {
				// notFinishReduce中还有task还在等待finish响应
				//log.Println("master give a wait")
				reply.Task.TaskType = "Wait"
				return nil
			}
		} else {
			return nil
		}
	} else if args.RequestType == "finish" {
		// 如果是task完成的finish包
		if args.Task.TaskType == "Map" {
			// 需要改这个包的标志位，这个包是在notFinishMap中，finish之后从
			// m.notFinishMap[args.Task.TaskId].data.TaskStatus = "Finished"

			// 不需要task的状态了，如果是finish就直接从map中移除就行了
			delete(m.notFinishMap, args.Task.TaskId)
		} else {
			delete(m.notFinishReduce, args.Task.TaskId)
		}
	} else {
		return nil
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if (m.MapTasks.head.next == m.MapTasks.tail && len(m.notFinishMap) == 0) && (m.ReduceTasks.head.next == m.ReduceTasks.tail && len(m.notFinishReduce) == 0) {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	mapList := getLinkedlist()
	reduceList := getLinkedlist()

	// 把任务保存到两个list中
	NumMap := len(files)
	NumReduce := nReduce

	for index, file := range files {
		var tempTask MapReduceTask
		tempTask.TaskId = index
		tempTask.TaskType = "Map"
		tempTask.MapFile = file
		tempTask.NumReduce = NumReduce
		tempTask.NumMap = NumMap
		//tempTask.TaskStatus = "Unassigned"

		// 加入到map链表
		var node = new(Node)
		node.data = tempTask
		addLast(mapList, node)
	}

	for i := 0; i < NumReduce; i++ {
		var tempTask MapReduceTask
		tempTask.TaskId = i
		tempTask.TaskType = "Reduce"
		for j := 0; j < NumMap; j++ {
			tempTask.ReduceFiles = append(tempTask.ReduceFiles, intermediateFilename(j, i))
		}
		//tempTask.TaskStatus = "Unassigned"

		// 加入到map链表
		var node = new(Node)
		node.data = tempTask
		addLast(reduceList, node)
	}
	m.MapTasks = mapList
	m.ReduceTasks = reduceList

	m.notFinishMap = make(map[int]*Node)
	m.notFinishReduce = make(map[int]*Node)

	m.server()
	return &m
}
