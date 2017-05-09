The output from MapReduce jobs will live in this directory. The file names are
determined by the Go code in src/mapreduce/common.go. In general, three kinds of
files will be created in the folder:

  1) Reduce Input Files "mr.{jobName}-{mapperNumber}-{reducerNumber}"

    These are produced by mapper number {mapperNumber} for reducer number
    {reducerNumber} for {jobName}.

  2) Reduce Output Files "mr.{jobName}-res-{reducerNumber}"

    These are the output of reducer number {reducerNumber} for {jobName}.

  3) Merger Output Files "mr.result.{jobName}"

    These are produced by the merger by merging all of reducer output files for
    {jobName}.

Additionally, the unit testing code generates files of the form:

  mr-test.input.{inputNum}.txt

These files are used as inputs to the MapReduce library and are automatically
deleted once a test completes.

NOTE:
  The Go code (semi) assumes that this directory exists. Please do not delete
  this directory.
