package mapreduce

import (
	"fmt"
	"net"
	"sync/atomic"
	"time"
)

// A worker executes a user's map and reduce functions.
type Worker struct {
	jobName     string         // The name of the job.
	mapF        MapFunction    // The user's map function.
	reduceF     ReduceFunction // The user's reduce function.
	rpcListener net.Listener   // The RPC listener.
	active      int32          // Whether this worker is accepting work.
	done        chan bool      // Used to signal RPC server is done.
	address     string         // This worker's address for RPC.
	mapsDone    int32          // How many map ops this worker has completed.
	reducesDone int32          // How many reduce ops this worker has completed.
}

// Constructs a new worker with the given inputs.
func NewWorker(job string, mapF MapFunction, reduceF ReduceFunction) *Worker {
	return &Worker{
		jobName:     job,
		mapF:        mapF,
		reduceF:     reduceF,
		active:      0,
		done:        make(chan bool),
		address:     genWorkerAddress(),
		mapsDone:    0,
		reducesDone: 0,
	}
}

// Starts the worker by launching the RPC server and blocking until the worker
// receives a Shutdown call.
func (w *Worker) Start() {
	atomic.StoreInt32(&w.active, 1)
	w.rpcListener = startWorkerRPCServer(w)
	w.RegisterWithServer()
	<-w.done
}

// Runs the user's mapper function on the given inputs. The key to the mapper
// function will simply be the input filename, and the value will be the full
// contents of that file. The key and value pairs returned from the user's map
// function should be split into reduce tasks, serialized, and written out to
// `numReducers` output files. Each key can be mapped to a reducer using the
// `ihash` function modulo the number of reducers. The filename of the reducer
// output file for a given job, from a given mapper, for a given reducer can be
// determined using the `reduceInputName` function.
func (w *Worker) DoMap(inputFileName string, mapperNum, numReducers uint) {
	fmt.Printf("MAP[%s:%d]: Processing '%s' for %d reducers.\n", w.jobName,
		mapperNum, inputFileName, numReducers)

	// FIXME: Remove the line below and implement me when ready.
	w.mapF("", "") // FIXME: Don't forget to remove me!
}

// Run's the user's reduce function on the given inputs. It does this by reading
// in each mapper output intended for this reducer, deserializing the keys,
// grouping together all of the values for a given key, and then passing the key
// and list of values to the user's reduce function. The value output from the
// user's reduce function should be coupled with the key, serialized, and
// written out to the merger's input file, which can be obtained by calling the
// `ReduceOutputName` function with the proper values.
func (w *Worker) DoReduce(reducerNum, numMappers uint) {
	fmt.Printf("REDUCE[%s:%d]: Reducing from %d mappers.\n", w.jobName,
		reducerNum, numMappers)

	// FIXME: Remove the line below and implement me when ready.
	w.reduceF("", make([]string, 0)) // FIXME: Don't forget to remove me!
}

// Shuts the worker down by shutting down the RPC server.
func (w *Worker) Shutdown() {
	fmt.Println("SHUTDOWN")
	atomic.StoreInt32(&w.active, 0)
	w.rpcListener.Close()
}

// Returns whether this worker is accepting work.
func (w *Worker) IsActive() bool {
	return atomic.LoadInt32(&w.active) == 1
}

//
// RPC methods begin after this. Feel free to ignore them. Don't change them.
//

func (w *Worker) RegisterWithServer() {
	// Try to register for 10 seconds.
	ok := false
	for i := 0; i < 40 && !ok; i++ {
		ok = callMaster("Register", &RegisterArgs{w.address}, new(interface{}))
		if !ok {
			time.Sleep(250 * time.Millisecond)
			if (i % 10) == 0 {
				fmt.Println("Retrying registration attempt...")
			}
		}
	}

	if !ok {
		fmt.Println("Failed to register. Shutting down.")
		w.Shutdown()
	} else {
		fmt.Println("Registered successfully!")
	}
}

type RPCWorker Worker

type TaskArgs interface {
	TaskName() string
}

type DoMapArgs struct {
	InputFileName          string
	MapperNum, NumReducers uint
}

type DoReduceArgs struct {
	ReducerNum, NumMappers uint
}

func (w *RPCWorker) DoMap(args *DoMapArgs, reply *interface{}) error {
	(*Worker)(w).DoMap(args.InputFileName, args.MapperNum, args.NumReducers)
	atomic.AddInt32(&(*Worker)(w).mapsDone, 1)
	return nil
}

func (w *RPCWorker) DoReduce(args *DoReduceArgs, reply *interface{}) error {
	(*Worker)(w).DoReduce(args.ReducerNum, args.NumMappers)
	atomic.AddInt32(&(*Worker)(w).reducesDone, 1)
	return nil
}

func (w *RPCWorker) Shutdown(args *interface{}, reply *interface{}) error {
	(*Worker)(w).Shutdown()
	return nil
}

func (a *DoMapArgs) String() string {
	return fmt.Sprintf("DoMapArgs[mapper=%v]{%s}[=>%v]", a.MapperNum,
		a.InputFileName, a.NumReducers)
}

func (a *DoReduceArgs) String() string {
	return fmt.Sprintf("DoReduceArgs[reducer=%v][<=%v]", a.ReducerNum,
		a.NumMappers)
}

func (a *DoMapArgs) TaskName() string {
	return "DoMap"
}

func (a *DoReduceArgs) TaskName() string {
	return "DoReduce"
}
