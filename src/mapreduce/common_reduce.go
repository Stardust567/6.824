package mapreduce

import (
	"encoding/json"
	"log"
	"os"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//set of having the same key
	kvsMap := make(map[string]([]string))

	for i := 0; i < nMap; i++ {
		//yield the filename from map task m
		name := reduceName(jobName, i, reduceTask)
		file, err := os.Open(name)
		if nil != err { log.Fatal(err) }
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil { break }
			kvsMap[kv.Key] = append(kvsMap[kv.Key], kv.Value)
		}
		file.Close()
	}

	reduceFile, err := os.Create(outFile)
	if nil != err { log.Fatal(err) }
	enc := json.NewEncoder(reduceFile)
	for key, value := range kvsMap {
		//reduceF() returns the reduced value for that key
		data := reduceF(key, value)
		kv := KeyValue{key, data}
		enc.Encode(kv)
	}
	reduceFile.Close()
}
