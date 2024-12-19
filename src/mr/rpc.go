package mr

type RequestArgs struct {
	TaskType string
	TaskID   int
}

type TaskReply struct {
	Task Task
}

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-0"
	return s
}
