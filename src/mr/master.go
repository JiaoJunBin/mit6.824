package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	// "sync"
	"time"
)

// start a thread that listens for RPCs from worker.go
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

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	time.Sleep(time.Second * 10)
	// Your code here.
	return m.jobFinished()
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {

	m := Master{
		nReduce:     nReduce,
		map_chan:    make(chan *Task, 10),
		reduce_chan: make(chan []*Task, 30),
	}
	fmt.Println("master created")

	// Your code here.
	// get file location
	m.getTxtFile(Dir)
	// put map task into channel
	m.map_taskMap.Range(func(key, value interface{}) bool {
		m.map_chan <- value.(*Task)
		return true
	})
	m.server()
	return &m
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

// worker request task from master
// may receive
func (m *Master) RequestTask(args *RequestTask_Request, reply *RequestTask_Reply) error {
	// get a task, using map channel first
	taskList := make([]*Task, 1)
	reply.Tasks = make([]*Task, 1)
	if len(m.map_chan) > 0 { // has map task
		taskList[0] = <-m.map_chan
		reply.Tasks[0] = taskList[0]
	} else if len(m.reduce_chan) > 0 { // has reduce task && map job is done
		taskList = <-m.reduce_chan
		reply.Tasks = taskList
	} else { // if no task, wait or exit
		if worker, ok := m.workerMap.Load(args.PID); ok {
			worker := worker.(*Worker)
			// not reduceJobFinished, in case 1 file 2 process
			if m.jobFinished() {
				worker.State = 2
				reply.Tasks[0] = &Task{TaskType: 3} // please exit
			} else {
				// put reduce task into channel, should only be executed once

				fmt.Println("emiting reduce task")
				m.reduce_taskMap.Range(func(key, value interface{}) bool {
					m.reduce_chan <- value.([]*Task)
					return true
				})

				worker.State = 0
				reply.Tasks[0] = &Task{TaskType: 2} // please wait
			}
			m.workerMap.Store(args.PID, worker)
		}
		return nil
	}

	taskNameList := []string{}
	for _, task := range taskList {
		task.NReduce = m.nReduce
		task.State = 1
		task.WorkerPID = args.PID
		taskNameList = append(taskNameList, task.Name)
	}

	// record worker machine and assign task
	worker := Worker{
		args.PID,
		taskNameList,
		1,
		make(chan bool),
	}
	m.workerMap.Store(args.PID, &worker)

	// receive report, or timeout and reassign
	go func(taskList []*Task) {
		timer := time.NewTimer(10 * time.Second)

		select {
		case <-worker.WaitChannel:
			timer.Stop()
		case <-timer.C: // overtime, put task back to map chan
			log.Printf("%v timeout\n", args.PID)
			if taskList[0].TaskType == 0 { // map
				m.map_chan <- taskList[0]
			} else if taskList[0].TaskType == 1 { // reduce
				m.reduce_chan <- taskList
			} else {
				log.Println("reassign task error, TaskType not in 0 or 1")
			}

			if worker, ok := m.workerMap.Load(args.PID); ok {
				worker := worker.(*Worker)
				worker.State = 2
			}
		}
	}(taskList)

	return nil
}

// worker finished their job and report
func (m *Master) ReportTask(args *ReportTask_Request, reply *ReportTask_Reply) error {
	fmt.Printf("worker %v reporting %v#0:map, 1:reduce#\n", args.PID, args.ReportType)
	if args.ReportType == 0 { // map, collect reduce task
		go func() {
			if worker, ok := m.workerMap.Load(args.PID); ok {
				worker := worker.(*Worker)
				worker.State = 0
				worker.WaitChannel <- true
			}
			for _, task := range args.Tasks {

				newlist := []*Task{task}
				if list, ok := m.reduce_taskMap.LoadOrStore(task.ReduceID, newlist); ok {
					list := list.([]*Task)
					list = append(list, task)
				}
			}
			m.map_taskMap.Delete(args.OriginFileName)
			// if oldTask, ok := m.map_taskMap.Load(args.OriginFileName); ok {

			// 	oldTask := oldTask.(*Task)
			// 	oldTask.State = 2
			// }
		}()

	} else if args.ReportType == 1 { // reduce,delete task in map
		go func() {
			// if worker, ok := m.workerMap.Load(args.PID); ok {
			// 	worker := worker.(*Worker)
			// 	worker.State = 0
			// 	worker.WaitChannel <- true
			// }
			m.reduce_taskMap.Delete(args.ReduceID)

		}()
	} else {
		log.Println("reportType not 0 or 1")
		reply.State = false
		return nil
	}
	reply.State = true
	return nil
}

// get initial file from local disk
func (m *Master) getTxtFile(dir string) {
	outputDirRead, err := os.Open(dir)
	if err != nil {
		log.Fatal(err)
		return
	}
	outputDirFiles, err := outputDirRead.Readdir(0)
	if err != nil {
		log.Fatal(err)
		return
	}

	for outputIndex := range outputDirFiles {
		outputFileHere := outputDirFiles[outputIndex]
		// Get filename
		name := outputFileHere.Name()
		postfix := name[len(name)-4:]
		if postfix == ".txt" {
			task := Task{
				Name:      name,
				Location:  dir + name,
				TaskType:  0,
				State:     0,
				WorkerPID: -1,
			}
			m.map_taskMap.Store(name, &task)
		}
	}
}

// map & reduce job both finished
func (m *Master) jobFinished() bool {
	if m.mapJobFinished() && m.reduceJobFinished() {
		fmt.Println("all job finished")
		return true
	}
	return false
}

func (m *Master) mapJobFinished() bool {
	if len(m.map_chan) > 0 {
		return false
	}
	// flag := false
	// m.map_taskMap.Range(func(key, value any) bool {
	// 	if value.(*Task).State != 2 {
	// 		flag = false
	// 		return false
	// 	}
	// 	flag = true
	// 	return true
	// })
	// return flag
	cnt := 0
	m.map_taskMap.Range(func(key, value any) bool {
		fmt.Printf("kv in map_taskMap, (%v, %v)\n", key, value)
		cnt++
		return true
	})
	return cnt == 0
}

func (m *Master) reduceJobFinished() bool {
	if len(m.reduce_chan) > 0 {
		return false
	}
	// cnt := 0
	// m.reduce_taskMap.Range(func(key, value any) bool {
	// 	fmt.Printf("kv in reduce_taskMap, (%v, %v)\n", key, value)
	// 	cnt++
	// 	return true
	// })
	// return cnt == 0

	cnt := 0
	m.reduce_taskMap.Range(func(key, value any) bool {
		if value != nil {
			cnt += 1
			return false
		}
		return true
	})
	return cnt == 0
}
