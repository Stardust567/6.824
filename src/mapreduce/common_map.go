package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	data, err := ioutil.ReadFile(inFile)
	if nil != err { log.Fatal(err) }
	// mapF() returns a slice containing the key/value pairs for reduce
	kvs := mapF(inFile, string(data))

	var outFiles []*os.File
	defer func() {
		for _, file := range outFiles { file.Close() }
	}()
	for i:=0; i<nReduce; i++ {
		//the filename as the intermediate file for reduce task r.
		name := reduceName(jobName, mapTask, i)
		file, err := os.Create(name)
		if nil != err { log.Fatal(err) }
		outFiles = append(outFiles, file)
	}

	for _, kv := range kvs {
		index := ihash(kv.Key) % nReduce
		//enc opens IO stream of outFiles[index]
		enc := json.NewEncoder(outFiles[index])
		//writes kv in stream
		enc.Encode(kv)
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
