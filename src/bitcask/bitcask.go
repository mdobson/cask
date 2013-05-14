package bitcask

import "time"
import "io/ioutil"
import "fmt"
import "os"

//Keydir value struct has necessary info for keydir lookups
type keydirValue struct {
	fileId string
	value string
	valueSz int
	valuePos int
	timestamp int32
}

type caskRecord struct {

}

//Main keydir object. This is what is primarily exported by the DB
type Keydir struct {
	dir map[string]keydirValue
	dataFileDirectory string
	dataFile *os.File
}

//Create a new Keydir initialize a new data file in the directory.
//Probably will run compaction here too
func New() *Keydir {
	
	var dataDirectory string = "data"

	dir := make(map[string]KeydirValue)

	files, err := ioutil.ReadDir(dataDirectory)

	if err != nil {
		panic(err)
	}

	var fileNumber int = len(files)

	var fileName string = fmt.Sprintf("%s/data.%d.dfile", dataDirectory, fileNumber)

	file, err := os.Create(fileName)

	if err != nil {
		panic(err)
	}

	k := &Keydir{dir:dir, dataFileDirectory:dataDirectory, dataFile:file}

	return k
}

//Simple set function for now. Eventually it will take cask records and make them lovely bytes.
func (k *Keydir) Set(key string, value string) {
	k.dir[key] = KeydirValue{fileId: k.dataFile.Name(), valueSz:len(value), valuePos:1, timestamp:int32(time.Now().Unix())}
	record := []byte(value)
	_, err := k.dataFile.Write(record)

	if err != nil {
		panic(err)
	}
}

