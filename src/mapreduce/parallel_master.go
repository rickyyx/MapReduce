package mapreduce

import (
	"fmt"
	"net"
	"sync/atomic"
)

// A parallel master executes a MapReduce job on many workers in parallel.
type ParallelMaster struct {
	JobName        string
	InputFileNames []string
	NumReducers    uint
	MapF           MapFunction
	ReduceF        ReduceFunction

	freeWorkers  chan string  // Workers that have registered and are free.
	totalWorkers int32        // Total number of workers registered.
	rpcListener  net.Listener // The RPC listener.
	active       int32        // Whether this master is active or not.
	done         chan bool    // Used to signal that the RPC server is done.
}

// Constructs a new parallel master with the given inputs.
func NewParallelMaster(jobName string, inputFileNames []string,
	numReducers uint, mapF MapFunction, reduceF ReduceFunction) *ParallelMaster {
	return &ParallelMaster{
		JobName:        jobName,
		InputFileNames: inputFileNames,
		NumReducers:    numReducers,
		MapF:           mapF,
		ReduceF:        reduceF,
		active:         0,
		freeWorkers:    make(chan string),
		done:           make(chan bool),
	}
}

// Used by workers over RPC: registers the worker with `workerAddress` with the
// parallel master. After registration, the master begins giving work to the
// worker.
func (s *ParallelMaster) Register(workerAddress string) {
	atomic.AddInt32(&s.totalWorkers, 1)
	go func() {
		fmt.Printf("Worker at %s has registered.\n", workerAddress)
		s.freeWorkers <- workerAddress
	}()
}

// Starts the master. Spins up the RPC server, schedules tasks, and blocks until
// the job has completed.
func (m *ParallelMaster) Start() {
	atomic.StoreInt32(&m.active, 1)
	m.rpcListener = startMasterRPCServer(m)
	// Don't remove the code above here.

	// Each input as one job
	numMapJobs := len(m.InputFileNames)

	// schedule maps
	mapCh := make(chan TaskArgs, numMapJobs)
	// Populate channels

	for mN, job := range m.InputFileNames {
		mapCh <- TaskArgs(&DoMapArgs{job, uint(mN), m.NumReducers})
	}
	m.schedule(mapCh)

	// Map tasks completed
	rCh := make(chan TaskArgs, m.NumReducers)
	for rN := uint(0); rN < m.NumReducers; rN++ {
		// Atomic totalWorkers?
		rCh <- TaskArgs(&DoReduceArgs{rN, uint(numMapJobs)})
	}
	m.schedule(rCh)

	// Don't remove the code below here.
	m.Shutdown()
	<-m.done
}

// Dishes out work to all available workers until all the tasks are complete.
// Blocks until all the work with arguments in `tasks` has been completed.
func (m *ParallelMaster) schedule(tasks chan TaskArgs) {

	nJobDone, nAllJob := uint32(0), uint32(len(tasks))
	for {
		select {
		case task := <-tasks:
			go func() {
				worker := <-m.freeWorkers
				ok := callWorker(worker, task.TaskName(), task, new(interface{}))
				// Handle failures
				if !ok {
					tasks <- task
				} else {
					//Increment Jobs Done number
					atomic.AddUint32(&nJobDone, 1)
				}
				m.freeWorkers <- worker
			}()
		default:
			if atomic.LoadUint32(&nJobDone) == nAllJob {
				return
			}
		}
	}
}

// Merges the output of all reduce tasks into one file. Returns the filename for
// the merged output.
func (m *ParallelMaster) Merge() string {
	mergeReduceOutputs(m.JobName, m.NumReducers)
	return MergeOutputName(m.JobName)
}

// Shuts the master down by shutting down all workers and shutting off the RPC
// server.
func (m *ParallelMaster) Shutdown() {
	atomic.StoreInt32(&m.active, 0)

	for i := uint(0); i < uint(m.totalWorkers); i++ {
		worker := <-m.freeWorkers
		callWorker(worker, "Shutdown", new(interface{}), new(interface{}))
	}

	m.rpcListener.Close()
	close(m.freeWorkers)
}

// Returns whether this master is running a job at the moment.
func (m *ParallelMaster) IsActive() bool {
	return atomic.LoadInt32(&m.active) == 1
}
