package mr

import (
	// "bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// for sorting by key.
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
func MakeWorker(mapf func(string, string) []*KeyValue,
	reducef func(string, []string) string) {
	fmt.Println(fmt.Printf("%v worker created", os.Getpid()))
	// Your worker implementation here.
	cnt := 0
	for {
		serve(mapf, reducef)
		cnt += 1
		fmt.Printf("worker %v serve %v times\n", os.Getpid(), cnt)
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

func serve(mapf func(string, string) []*KeyValue,
	reducef func(string, []string) string) {
	pid := os.Getpid()
	tasks := CallRequestTask()
	if len(tasks) <= 0 {
		log.Println("didn't receive task")
		return
	}
	switch tasks[0].TaskType {
	case 0: // map
		fileNameMapByReduceID, originFileName := exec_Map_WC(mapf, tasks[0])
		cnt := 0
		for !report_MapTask(fileNameMapByReduceID, originFileName) {
			cnt += 1
			if cnt == 5 {
				log.Println("report map task error after 5 trials.")
				return
			}
			time.Sleep(time.Second * 2)
		}
	case 1: // reduce
		reduceID := exec_Reduce_WC(reducef, tasks)
		fmt.Printf("worker %v reduced %v\n", pid, reduceID)
		cnt := 0
		for !report_ReduceTask(reduceID) {
			cnt += 1
			if cnt == 5 {
				log.Println("report reduce task error after 5 trials.")
				return
			}
			time.Sleep(time.Second * 1)
		}
	case 2: // wait
		fmt.Printf("master told %v to wait\n", pid)
		time.Sleep(time.Second * 3)
	case 3: // exit
		fmt.Printf("master told %v to exit\n", pid)
		os.Exit(0)
	}
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
func CallRequestTask() []*Task {

	// declare an argument structure.fill in the argument(s).
	args := RequestTask_Request{}
	args.PID = os.Getpid()

	// declare a reply structure.
	reply := RequestTask_Reply{}

	// send the RPC request, wait for the reply.
	call("Master.RequestTask", &args, &reply)
	return reply.Tasks
}

func exec_Map_WC(mapf func(string, string) []*KeyValue, task *Task) (fileNameMapByReduceID map[int]string, originFileName string) {
	if task.NReduce == 0 {
		log.Fatalln("nReduce is 0, may divide by zero")
	}
	originFileName = task.Name

	file, err := os.Open(task.Location)
	if err != nil {
		log.Fatalf("cannot open %v", task.Name)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Name)
	}

	// write to buffer
	KVbuffer := make([][]KeyValue, task.NReduce)
	kva := mapf(task.Name, string(content))
	for _, v := range kva {
		reduceID := ihash(v.Key) % task.NReduce
		KVbuffer[reduceID] = append(KVbuffer[reduceID], *v)
	}
	// write buffer to file
	// all kv in buf have same reduceID
	fileNameMapByReduceID = make(map[int]string, len(KVbuffer))
	for reduceID, buf := range KVbuffer {
		filename := "mr-" + strconv.Itoa(os.Getpid()) +
			"-" + strconv.Itoa(reduceID)

		fileNameMapByReduceID[reduceID] = filename
		f, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
		if err != nil {
			log.Printf("open file error, %v", err)
		}
		defer f.Close()

		enc := json.NewEncoder(f)
		for _, kv := range buf {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("encode kv error, %v", err)
			}
		}
	}
	return fileNameMapByReduceID, originFileName
}

func report_MapTask(fileNameMapByReduceID map[int]string, originFileName string) bool {
	wd, _ := os.Getwd()
	taskList := []*Task{}
	for reduceID, filename := range fileNameMapByReduceID {
		task := Task{
			Name:      filename,
			Location:  wd + "/" + filename,
			ReduceID:  reduceID,
			TaskType:  1, // reduce
			State:     0,
			WorkerPID: -1,
		}
		taskList = append(taskList, &task)
	}

	args := ReportTask_Request{
		os.Getpid(),
		0,  // map
		-1, // reduceID
		taskList,
		originFileName,
	}
	reply := ReportTask_Reply{}
	return call("Master.ReportTask", &args, &reply)
}

func exec_Reduce_WC(reducef func(string, []string) string, taskList []*Task) (reduceID int) {
	intermediate := make([]KeyValue, 500)
	for _, task := range taskList {
		f, err := os.OpenFile(task.Location, os.O_RDONLY, 0777)
		if err != nil {
			log.Fatalln(err)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)

		}
	}

	sort.Sort(ByKey(intermediate))
	reduceID = taskList[0].ReduceID
	oname := "mr-out-" + strconv.Itoa(reduceID)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-reduceID.
	//
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
	return reduceID
}

func report_ReduceTask(reduceID int) bool {
	args := ReportTask_Request{
		PID:        os.Getpid(),
		ReportType: 1, // reduce
		ReduceID:   reduceID,
		Tasks:      make([]*Task, 0),
	}
	reply := ReportTask_Reply{}
	return call("Master.ReportTask", &args, &reply)
}
