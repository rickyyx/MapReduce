package mapreduce

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	NUM_TEST_NUMBERS = 10000
)

var (
	NUM_MAPS    int32 = 0
	NUM_REDUCES int32 = 0
	CWD               = flag.String("cwd", "", "set cwd")
)

// Parses the test flags to set the current working directory. This is rather
// important since output files are stored in the relative directory
// DataOutputDir.
func init() {
	flag.Parse()
	if *CWD != "" {
		if err := os.Chdir(*CWD); err != nil {
			fmt.Println("Chdir error:", err)
		}
	}
}

func inputFileName(num uint) string {
	return fmt.Sprintf(DataOutputDir+"mr-test.input.%d.txt", num)
}

// Make input files
func makeInputs(num uint) []string {
	chunkSize := NUM_TEST_NUMBERS / num
	names := make([]string, 0)
	for f := uint(0); f < num; f++ {
		names = append(names, inputFileName(f))
		file, err := os.Create(names[f])
		checkErr(err, "Couldn't create test input file.")

		w := bufio.NewWriter(file)
		for i := (f * chunkSize); i < (f*chunkSize)+chunkSize; i++ {
			fmt.Fprintf(w, "%d\n", i)
		}

		w.Flush()
		file.Close()
	}

	return names
}

// The test map and reduce functions take files with a number on each line and
// map them to the filename. The reducer, as such, will get the filename as the
// key and the list of numbers in that file as the value. The reducer simply
// joins the strings with a comma. We can then verify the values
// deterministically.
func testMapF(fileName string, contents string) (res []KeyValue) {
	words := strings.Fields(contents)
	for _, w := range words {
		kv := KeyValue{fileName, w}
		res = append(res, kv)
	}

	atomic.AddInt32(&NUM_MAPS, 1)
	return
}

func testReduceF(key string, values []string) string {
	atomic.AddInt32(&NUM_REDUCES, 1)
	return strings.Join(values, ",")
}

func verifyAndResetTaskCounts(t *testing.T, nInputs, nReducers uint) {
	countedMaps, countedReduces := NUM_MAPS, NUM_REDUCES
	NUM_MAPS, NUM_REDUCES = 0, 0
	if countedMaps != int32(nInputs) {
		t.Fatalf("Wrong number of maps were run! Expected %v, but found %v.",
			nInputs, countedMaps)
	} else if countedReduces != int32(nReducers) {
		t.Fatalf("Wrong number of reduces were run! Expected %v, but found %v.",
			nReducers, countedReduces)
	}
}

func verify(t *testing.T, nInputs uint, mergedFilename string) {
	contents, err := ioutil.ReadFile(mergedFilename)
	if err != nil {
		t.Fatalf("Couldn't open merged output file %s.", mergedFilename)
	}

	lines := strings.Split(string(contents), "\n")
	numLines := len(lines) - 1 // We disregard the empty last line.
	if uint(numLines) != nInputs {
		t.Fatalf("Not all files appear to have been read. Expected %v, but "+
			"found %v.", nInputs, numLines)
	}

	for i := 0; i < numLines; i++ {
		line := lines[i]
		components := strings.Split(line, ".")
		if len(components) != 4 {
			t.Fatalf("MapReduce output is malformed. Missing key?")
		}

		fileNum, err := strconv.Atoi(components[2])
		if err != nil {
			t.Fatalf("MapReduce output key appears to be malformed.")
		}

		numbersJoined := strings.TrimSpace(strings.Split(line, ":")[1])
		numbers := strings.Split(numbersJoined, ",")
		chunkSize := int(NUM_TEST_NUMBERS / nInputs)
		if len(numbers) != chunkSize {
			t.Fatalf("Wrong number of values in reducer output. Expected %v"+
				"but found %v.", chunkSize, len(numbers)-1)
		}

		min, max := fileNum*chunkSize, (fileNum+1)*chunkSize
		for j := 0; j < len(numbers); j++ {
			num, err := strconv.Atoi(numbers[j])
			if err != nil {
				t.Fatalf("Reducer output is malformed.")
			}

			if num >= max || num < min {
				t.Fatalf("Number is out of range. Is %v, but should be >= %v"+
					"and < %v.", num, min, max)
			}
		}
	}
}

func cleanup(job string, t *testing.T, nInputs, nReducers uint, merged bool) {
	fileRemoveAndCheck := func(fileName string) {
		err := os.Remove(fileName)
		if err != nil && t != nil {
			t.Fatalf("Failed to remove '%v'. The file must exist.\nError: %v",
				fileName, err)
		}
	}

	for i := uint(0); i < nInputs; i++ {
		fileRemoveAndCheck(inputFileName(i))
	}

	for i := uint(0); i < nInputs; i++ {
		for j := uint(0); j < nReducers; j++ {
			// These files may or may not exist, so only _try_ to remove them.
			os.Remove(reduceInputName(job, i, j))
		}
	}

	for i := uint(0); i < nReducers; i++ {
		fileRemoveAndCheck(ReduceOutputName(job, i))
	}

	if merged {
		fileRemoveAndCheck(MergeOutputName(job))
	}
}

func TestSequentialNoMerge(t *testing.T) {
	job, mN, rN := "testSeqNoMerge", uint(15), uint(15)
	m := NewSequentialMaster(job, makeInputs(mN), rN, testMapF, testReduceF)
	defer cleanup(job, nil, mN, rN, false)

	m.Start()
	verifyAndResetTaskCounts(t, mN, mN)
}

func testSequentialGeneric(t *testing.T, job string, mN, rN uint) {
	m := NewSequentialMaster(job, makeInputs(mN), rN, testMapF, testReduceF)
	defer cleanup(job, nil, mN, rN, true)

	m.Start()
	verifyAndResetTaskCounts(t, mN, mN)
	m.Merge()

	verify(t, mN, MergeOutputName(job))
	cleanup(job, t, mN, rN, true)
}

func TestFullSequentialSingle(t *testing.T) {
	testSequentialGeneric(t, "testSeqSingle", 1, 1)
}

func TestFullSequentialManyMappers(t *testing.T) {
	testSequentialGeneric(t, "testSeqManyMappers", 10, 1)
}

func TestFullSequentialManyReducers(t *testing.T) {
	testSequentialGeneric(t, "testSeqManyReducers", 1, 10)
}

func TestFullSequentialManyMappersAndReducers(t *testing.T) {
	testSequentialGeneric(t, "testSeqManyMappersReducers", 10, 10)
}

func TestParallelBasic(t *testing.T) {
	job, mN, rN, wN := "testPara", uint(300), uint(200), 5
	m := NewParallelMaster(job, makeInputs(mN), rN, testMapF, testReduceF)
	defer cleanup(job, nil, mN, rN, true)

	done := make(chan bool)
	go func() {
		m.Start()
		done <- true
	}()

	// Make sure the master (probably) sets up so workers can register quickly.
	runtime.Gosched()
	time.Sleep(100 * time.Millisecond)
	runtime.Gosched()

	workers := make([]*Worker, 0, wN)
	for i := 0; i < wN; i++ {
		worker := NewWorker(job, testMapF, testReduceF)
		workers = append(workers, worker)
		go worker.Start()
	}

	<-done
	for i := 0; i < wN; i++ {
		fmt.Printf("Worker %d: %v\n", i, workers[i])
		if workers[i].mapsDone < 1 {
			t.Error("A worker didn't receive any map tasks!")
		}

		if workers[i].reducesDone < 1 {
			t.Error("A worker didn't receive any reduce tasks!")
		}
	}

	m.Merge()
	verify(t, mN, MergeOutputName(job))
	cleanup(job, t, mN, rN, true)
}

func TestParallelWithFailures(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	job, mN, rN, wN := "testParaFailures", uint(100), uint(100), 15
	m := NewParallelMaster(job, makeInputs(mN), rN, testMapF, testReduceF)
	defer cleanup(job, nil, mN, rN, true)

	done := make(chan bool)
	go func() {
		m.Start()
		done <- true
	}()

	workers := make([]*Worker, 0, wN)
	for i := 0; i < wN; i++ {
		worker := NewWorker(job, testMapF, testReduceF)
		workers = append(workers, worker)
		go func(w *Worker) {
			go w.Start()
			neededMaps := 1 + int32(rand.Intn(4))
			for atomic.LoadInt32(&w.mapsDone) < neededMaps {
				runtime.Gosched() // Let other threads run.
			}

			// Kill with 33% probability after at least 1 map was done.
			if rand.Intn(3) == 0 {
				w.Shutdown()
				w.mapsDone = -1    // So we can tell this was killed.
				w.reducesDone = -1 // So we can tell this was killed.
				return
			}

			neededReduces := 1 + int32(rand.Intn(4))
			for atomic.LoadInt32(&w.reducesDone) < neededReduces {
				runtime.Gosched() // Let other threads run.
			}

			// Kill with 33% probability after at least 1 reduce was done.
			if rand.Intn(3) == 0 {
				w.Shutdown()
			}
		}(worker)
	}

	<-done
	m.Merge()
	verify(t, mN, MergeOutputName(job))
	cleanup(job, t, mN, rN, true)
}
