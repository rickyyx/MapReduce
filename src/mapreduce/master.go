package mapreduce

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// The master interface. Any kind of MapReduce master implements this interface.
type Master interface {
	Register(worker string)
	Shutdown()
	IsActive() bool
	Start()
	Merge() string
}

// A function that can be used by any master: merges the results from
// `numReducers` for the job named `jobName` into the corresponding merger ouput
// file as governed by `MergeOutputName`.
func mergeReduceOutputs(jobName string, numReducers uint) {
	kvs := make(map[string]string)
	for i := uint(0); i < numReducers; i++ {
		p := ReduceOutputName(jobName, i)
		fmt.Printf("MERGE[%s:%d]: Reading %s.\n", jobName, i, p)

		file, err := os.Open(p)
		checkErr(err, "Opening reduce output file failed.")

		var kv KeyValue
		dec := json.NewDecoder(file)
		for err := dec.Decode(&kv); err == nil; err = dec.Decode(&kv) {
			kvs[kv.Key] = kv.Value
		}

		file.Close()
	}

	keys := make([]string, 0, len(kvs))
	for k := range kvs {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	file, err := os.Create(MergeOutputName(jobName))
	checkErr(err, "Merger failed to create ouput file.")

	w := bufio.NewWriter(file)
	for _, k := range keys {
		fmt.Fprintf(w, "%s: %s\n", k, kvs[k])
	}

	w.Flush()
	file.Close()
}

//
// RPC methods begin after this. Feel free to ignore them. Don't change them.
//

type RPCMaster struct {
	master Master
}

type RegisterArgs struct {
	WorkerAddress string
}

func (m *RPCMaster) Register(args *RegisterArgs, reply *interface{}) error {
	(m.master).Register(args.WorkerAddress)
	return nil
}
