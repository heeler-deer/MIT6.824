package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// @description: 协调map和reduce的文件
// param:
// return:
const MAX_TASK_TIME = 10

type Map_Task struct {
	Begin_Second int64
	FileId       string
	WorkerID     int
}

type Reduce_Task struct {
	Begin_Second int64
	FileId       string
	WorkerID     int
}

type Master struct {
	// Your definitions here.
	File_Names    []string
	Reduce_Cnt    int
	Cur_Worker_id int

	Unsolved_Map_Tasks *BlockQueue

	Solved_Map_Tasks *Mapset

	Solved_Map_Mutex sync.Mutex

	Unsolved_Reduce_Tasks *BlockQueue

	Solved_Reduce_Tasks *Mapset

	Solved_Reduce_Mutex sync.Mutex

	Map_Task    []Map_Task
	Reduce_Task []Reduce_Task

	mapDone bool
	allDone bool
}

// Your code here -- RPC handlers for the worker to call.
type MapTaskArgs struct {
	Workerid int
}

type MapTaskReply struct {
	Workerid int
	Filename string
	Fileid   int
	Nreduce  int
	Done     bool
}

func MapDoneProcess(reply *MapTaskReply) {
	log.Printf("all map tasks Done,do reduce tasks")
	reply.Fileid = -1
	reply.Done = true
}

func (m *Master) MapAssignTask(args *MapTaskArgs, reply *MapTaskReply) error {
	if (args.Workerid) == -1 {
		reply.Workerid = m.Cur_Worker_id
		m.Cur_Worker_id++
	} else {

		reply.Workerid = args.Workerid

	}
	log.Printf("worker %v asks for a map task\n", reply.Workerid)
	m.Solved_Map_Mutex.Lock()

	if m.mapDone {

		m.Solved_Map_Mutex.Unlock()
		MapDoneProcess(reply)
		return nil
	}
	if m.Solved_Map_Tasks.Size() == 0 && m.Unsolved_Map_Tasks.size() == 0 {
		m.Solved_Map_Mutex.Unlock()
		MapDoneProcess(reply)
		log.Printf("Prepare_Reduce_Tasks")
		m.PrepareReduceTasks()
		m.mapDone = true
		return nil
	}
	log.Printf("%v unissued map tasks AND %v issued map tasks at hand\n", m.Unsolved_Map_Tasks.size(), m.Solved_Map_Tasks.Size())
	m.Solved_Map_Mutex.Unlock()
	nowtime := getNowTimeSecond()
	ret, err := m.Unsolved_Map_Tasks.PopBack()
	var fileId int
	if err != nil {
		log.Printf("no map task yet,worker wait")
		fileId = -1
	} else {

		fileId = ret.(int)
		log.Printf("master_110 line fileId: %v", fileId)
		m.Solved_Map_Mutex.Lock()
		reply.Filename = m.File_Names[fileId]
		m.Map_Task[fileId].Begin_Second = nowtime
		m.Map_Task[fileId].WorkerID = reply.Workerid
		m.Solved_Map_Tasks.Insert(fileId)
		m.Solved_Map_Mutex.Unlock()
		log.Printf("giving map task %v on file %v at second %v\n", fileId, reply.Filename, nowtime)
	}
	reply.Fileid = fileId
	reply.Done = false
	reply.Nreduce = m.Reduce_Cnt
	return nil
}

type MapJoinArgs struct {
	Workerid int
	Fileid   int
}
type MapJoinReply struct {
	Accept bool
}

func (m *Master) MapJoinTask(args *MapJoinArgs, reply *MapJoinReply) error {
	m.Solved_Map_Mutex.Lock()
	nowtime := getNowTimeSecond()
	tasktime := m.Map_Task[args.Fileid].Begin_Second
	if !m.Solved_Map_Tasks.Exist(args.Fileid) {
		log.Printf("file not exist")
		m.Solved_Map_Mutex.Unlock()
		reply.Accept = false
		return nil
	}
	if m.Map_Task[args.Fileid].WorkerID != args.Workerid {
		log.Printf("tasks belong to worker %v not %v", m.Map_Task[args.Fileid].WorkerID, args.Workerid)
		m.Solved_Map_Mutex.Unlock()
		reply.Accept = false
		return nil
	}
	if nowtime-tasktime > MAX_TASK_TIME {
		log.Printf("task exceeds max_task_time")
		reply.Accept = false
		m.Unsolved_Map_Tasks.AddBefore(args.Fileid)

	} else {
		log.Printf("task is Accepting...")
		reply.Accept = true
		m.Solved_Map_Tasks.Delete(args.Fileid)
	}
	m.Solved_Map_Mutex.Unlock()
	return nil
}

func getNowTimeSecond() int64 {
	return time.Now().UnixNano() / int64(time.Second)
}

type ReduceTaskArgs struct {
	Workerid int
}
type ReduceTaskReply struct {
	Nreduce int
	Rindex  int
	Filecnt int
	Done    bool
}

func (m *Master) PrepareReduceTasks() {

	for i := 0; i < m.Reduce_Cnt; i++ {
		log.Printf("Adding %vth reduce task into channel", i)
		m.Unsolved_Reduce_Tasks.AddBefore(i)
	}
}

func (m *Master) GiveReduceTask(args *ReduceTaskArgs, reply *ReduceTaskReply) error {
	log.Printf("worker %v asking for a reduce task", args.Workerid)
	m.Solved_Reduce_Mutex.Lock()
	if m.Unsolved_Reduce_Tasks.size() == 0 && m.Solved_Reduce_Tasks.Size() == 0 {
		log.Printf("reduce task finished")
		m.Solved_Reduce_Mutex.Unlock()
		reply.Done = true
		reply.Rindex = -1
		m.allDone = true
		return nil
	}
	log.Printf("%v unissued reduce tasks %v issued reduce tasks at hand\n", m.Unsolved_Reduce_Tasks.size(), m.Solved_Reduce_Tasks.Size())
	m.Solved_Reduce_Mutex.Unlock() // release lock to allow unissued update
	curTime := getNowTimeSecond()
	ret, err := m.Unsolved_Reduce_Tasks.PopBack()
	var Rindex int
	if err != nil {
		log.Printf("no reduce task yet, let worker wait...")
		Rindex = -1
	} else {
		Rindex = ret.(int)
		m.Solved_Reduce_Mutex.Lock()
		m.Reduce_Task[Rindex].Begin_Second = curTime
		m.Reduce_Task[Rindex].WorkerID = args.Workerid
		m.Solved_Reduce_Tasks.Insert(Rindex)
		m.Solved_Reduce_Mutex.Unlock()
		log.Printf("giving reduce task %v at second %v\n", Rindex, curTime)
	}
	reply.Rindex = Rindex
	reply.Nreduce = m.Reduce_Cnt
	reply.Done = false
	log.Printf("filecnt: %v", len(m.File_Names))
	reply.Filecnt = len(m.File_Names)

	return nil
}

type ReduceTaskJoinArgs struct {
	Workerid int
	Rindex   int
}
type ReduceTaskJoinReply struct {
	Accept bool
}

func (m *Master) JoinReduceTask(args *ReduceTaskJoinArgs, reply *ReduceTaskJoinReply) error {
	log.Printf("got join request from worker %v on reduce task %v\n", args.Workerid, args.Rindex)
	m.Solved_Reduce_Mutex.Lock()
	curtime := getNowTimeSecond()
	tasktime := m.Reduce_Task[args.Rindex].Begin_Second
	if !m.Solved_Reduce_Tasks.Exist(args.Rindex) {
		log.Printf("task abandoned or does not exists, ignoring...")
		m.Solved_Reduce_Mutex.Unlock()
		return nil
	}

	if m.Reduce_Task[args.Rindex].WorkerID != args.Workerid {
		log.Printf("reduce task belongs to worker %v not this %v, ignoring...", m.Reduce_Task[args.Rindex].WorkerID, args.Workerid)
		m.Solved_Reduce_Mutex.Unlock()
		reply.Accept = false
		return nil
	}
	if curtime-tasktime > MAX_TASK_TIME {
		log.Printf("task exceeds max wait time, abadoning...")
		reply.Accept = false
		m.Unsolved_Reduce_Tasks.AddBefore(args.Rindex)
	} else {
		log.Printf("task within max wait time, Accepting...")
		reply.Accept = true
		m.Solved_Reduce_Tasks.Delete(args.Rindex)
	}
	m.Solved_Reduce_Mutex.Unlock()

	return nil
}
func (m *Mapset) removeTimeoutMapTasks(mapTasks []Map_Task, unIssuedMapTasks *BlockQueue) {
	for fileId, issued := range m.mapbool {
		now := getNowTimeSecond()
		if issued {
			if now-mapTasks[fileId.(int)].Begin_Second > MAX_TASK_TIME {
				log.Printf("worker %v on file %v abanDoned due to timeout\n", mapTasks[fileId.(int)].WorkerID, fileId)
				m.mapbool[fileId.(int)] = false
				m.count--
				unIssuedMapTasks.AddBefore(fileId.(int))
			}
		}
	}
}
func (m *Mapset) removeTimeoutReduceTasks(reduceTasks []Reduce_Task, unIssuedReduceTasks *BlockQueue) {
	for fileId, issued := range m.mapbool {
		now := getNowTimeSecond()
		if issued {
			if now-reduceTasks[fileId.(int)].Begin_Second > MAX_TASK_TIME {
				log.Printf("worker %v on file %v abanDoned due to timeout\n", reduceTasks[fileId.(int)].WorkerID, fileId)
				m.mapbool[fileId.(int)] = false
				m.count--
				unIssuedReduceTasks.AddBefore(fileId.(int))
			}
		}
	}
}
func (m *Master) removeTimeoutTasks() {
	log.Printf("removing timeout maptasks...")
	m.Solved_Map_Mutex.Lock()
	m.Solved_Map_Tasks.removeTimeoutMapTasks(m.Map_Task, m.Unsolved_Map_Tasks)
	m.Solved_Map_Mutex.Unlock()
	m.Solved_Reduce_Mutex.Lock()
	m.Solved_Reduce_Tasks.removeTimeoutReduceTasks(m.Reduce_Task, m.Unsolved_Reduce_Tasks)
	m.Solved_Reduce_Mutex.Unlock()
}
func (m *Master) loopRemoveTimeoutMapTasks() {
	for true {
		time.Sleep(2 * 1000 * time.Millisecond)
		m.removeTimeoutTasks()
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

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

func (m *Master) Done() bool {
	// ret := false

	// Your code here.

	// return ret

	if m.allDone {
		log.Printf("asked whether i am Done, replying yes...")
	} else {
		log.Printf("asked whether i am Done, replying no...")
	}

	return m.allDone
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	log.SetPrefix("coordinator: ")
	log.Printf("making coordinator")

	m.File_Names = files
	log.Printf("FileName:  %v", m.File_Names)
	m.Reduce_Cnt = nReduce
	m.Cur_Worker_id = 0
	m.Map_Task = make([]Map_Task, len(files))
	m.Reduce_Task = make([]Reduce_Task, nReduce)
	m.Unsolved_Map_Tasks = NewBlockQueue()
	m.Solved_Map_Tasks = NewMapSet()
	m.Unsolved_Reduce_Tasks = NewBlockQueue()
	m.Solved_Reduce_Tasks = NewMapSet()
	m.allDone = false
	m.mapDone = false

	m.server()
	log.Printf("listening started...")
	go m.loopRemoveTimeoutMapTasks()
	log.Printf("file count %d\n", len(files))
	for i := 0; i < len(files); i++ {
		log.Printf("sending %vth file map task to channel\n", i)
		m.Unsolved_Map_Tasks.AddBefore(i)
	}
	return &m
}
