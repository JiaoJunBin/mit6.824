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

type RequestTask_Request struct {
	PID int
}

type RequestTask_Reply struct {
	Tasks []*Task
}

type ReportTask_Request struct {
	PID            int     // worker PID
	ReportType     int     // 0:map, 1:reduce
	ReduceID       int     // if reduce task
	Tasks          []*Task // if map task
	OriginFileName string  // if map task
}

type ReportTask_Reply struct {
	State bool // true: received
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
