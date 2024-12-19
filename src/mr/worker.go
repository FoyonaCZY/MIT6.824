package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type KeyValue struct {
	Key   string
	Value string
}

func ihash(key string) int {
	h := fnv.New32a()
	_, err := h.Write([]byte(key))
	if err != nil {
		return 0
	}
	return int(h.Sum32() & 0x7fffffff)
}

// Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		reply := TaskReply{}
		ok := call("Coordinator.RequestTask", &RequestArgs{}, &reply)

		log.Printf("Received task: %+v, nMap: %d, nReduce: %d", reply.Task, reply.Task.NMap, reply.Task.NReduce)

		if !ok {
			break
		}
		task := reply.Task

		//检测任务类型
		if task.Type == "map" {
			//执行map任务
			doMap(task, reply.Task.NReduce, mapf)
		}
		if task.Type == "reduce" {
			//执行reduce任务
			doReduce(task, reply.Task.NMap, reducef)
		}
		if task.Type == "wait" {
			time.Sleep(1 * time.Second)
		}
		if task.Type == "exit" {
			break
		}

		//通知Master任务完成
		call("Coordinator.TaskDone", &RequestArgs{TaskID: task.ID, TaskType: task.Type}, &TaskReply{})
	}
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer func(c *rpc.Client) {
		err := c.Close()
		if err != nil {

		}
	}(c)

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// doMap 执行map任务
func doMap(task Task, nReduce int, mapf func(string, string) []KeyValue) {
	//读取文件内容
	filename := task.InputFile
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	err = file.Close()
	if err != nil {
		return
	}

	kva := mapf(filename, string(content))
	//将map结果写入中间文件
	intermediate := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		reduceTask := ihash(kv.Key) % nReduce
		intermediate[reduceTask] = append(intermediate[reduceTask], kv)
	}

	//将中间文件写入磁盘
	for i, kva := range intermediate {
		oname := fmt.Sprintf("mrx-%d-%d", task.ID, i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v", kv)
			}
		}
		err := os.Rename(oname, fmt.Sprintf("mr-%d-%d", task.ID, i))
		if err != nil {
			return
		}

		err = ofile.Close()
		if err != nil {
			return
		}
	}
}

// doReduce 执行reduce任务
func doReduce(task Task, nMap int, reducef func(string, []string) string) {
	//根据任务ID读取中间文件
	var intermediate []KeyValue
	for i := 0; i < nMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, task.ID)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		// 读取文件内容
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		err = file.Close()
		if err != nil {
			return
		}
	}

	// 按键排序（方便分组）
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	// 对每个键调用 Reduce 函数
	oname := fmt.Sprintf("mr-xout-%d", task.ID) //临时文件
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// 写入最终输出文件
		_, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			return
		}

		i = j
	}
	//重命名文件，保证原子性
	err := os.Rename(oname, fmt.Sprintf("mr-out-%d", task.ID))
	if err != nil {
		return
	}

	//关闭文件
	err = ofile.Close()
	if err != nil {
		return
	}
}
