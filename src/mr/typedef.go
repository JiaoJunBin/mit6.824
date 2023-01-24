package mr

import "sync"

var Dir = "/home/junbin/mit6.824/src/main/"

type Master struct {
	// Your definitions here.
	nReduce        int          // defined by mrmaster
	map_chan       chan *Task   // deliver map task
	reduce_chan    chan []*Task // deliver reduce task
	reduce_taskMap sync.Map     // to record reduce task(reducceID,tasklist)
	map_taskMap    sync.Map     // to record map task(filename,task)
	workerMap      sync.Map     // to record worker
}

type Worker struct {
	PID         int
	TaskName    []string
	State       int       // 0: "idle", 1: "in-progress", 2: "lose connect"
	WaitChannel chan bool // wait 10 seconds
}

type Task struct {
	Name      string
	Location  string
	NReduce   int // defined by mrmaster
	ReduceID  int // ihash(v.Key) % task.NReduce
	TaskType  int // 0:map, 1:reduce, 2:please wait, 3:please exit
	State     int // 0: "fresh", 1: "in-progress", 2: "completed"
	WorkerPID int // -1 if didn't assigned
}

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
