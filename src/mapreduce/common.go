package mapreduce

import (
	"fmt"
	"hash/fnv"
	"strconv"
)

const (
	DataInputDir  = "data/input/"
	DataOutputDir = "data/output/"
	DataSocketDir = "data/"
)

// The type of the map function.
type MapFunction func(string, string) []KeyValue

// The type of the reduce function.
type ReduceFunction func(string, []string) string

// The type for keyvalue pairs. This could be a tuple, but it's nice to keep
// both fields named and in one structure.
type KeyValue struct {
	Key   string
	Value string
}

// A convenience function. Checks whether some error is nil. If it not, i.e.,
// there is an error, panics with the error along with the message `msg`.
func checkErr(err error, msg string) {
	if err != nil {
		panicMessage := fmt.Sprintf("Error: %s\n%v", msg, err)
		panic(panicMessage)
	}
}

// Returns a 32-bit unsigned hash of the input string.
func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// Returns a string for the filename of the reduce input file (also the mapper
// output file) for job with name `jobName` from mapper `mapperNum` for reducer
// `reducerNum`.
func reduceInputName(jobName string, mapperNum, reducerNum uint) string {
	return DataOutputDir + "mr." + jobName + "-" +
		strconv.Itoa(int(mapperNum)) + "-" + strconv.Itoa(int(reducerNum))
}

// Returns a string for the filename of the reduce output file (also the merger
// input file) for job with name `jobName` from from reducer `reducerNum`.
func ReduceOutputName(jobName string, reducerNum uint) string {
	return DataOutputDir + "mr." + jobName + "-res-" +
		strconv.Itoa(int(reducerNum))
}

// Returns the name of the merger's output file for a given job.
func MergeOutputName(jobName string) string {
	return DataOutputDir + "mr.result." + jobName
}
