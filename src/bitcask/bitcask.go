package bitcask

import "time"
import "io/ioutil"
import "fmt"
import "os"
import "encoding/binary"
import "bytes"

const HEADER_SIZE = 4 + 4 + 4

//Keydir value struct has necessary info for keydir lookups
type keydirValue struct {
	fileId string
	value string
	valueSz int
	valuePos int
	timestamp int32
}

//Record we serialized to bytes and write to data file.
type caskRecord struct {
	crc int32
	timestamp int32
	ksz int
	valueSz int
	key string
	value string
}

//Main keydir object. This is what is primarily exported by the DB
type Keydir struct {
	dir map[string]keydirValue
	dataFileDirectory string
	dataFile *os.File
	currentDataOffset int
}

func (c *caskRecord) Buffer() *bytes.Buffer {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, c.crc)
	_ = binary.Write(buf, binary.LittleEndian, c.timestamp)
	_ = binary.Write(buf, binary.LittleEndian, c.ksz)
	_ = binary.Write(buf, binary.LittleEndian, c.valueSz)
	_ = binary.Write(buf, binary.LittleEndian, c.key)
	_ = binary.Write(buf, binary.LittleEndian, c.value)
	return buf

}

//Create a new Keydir initialize a new data file in the directory.
//Probably will run compaction here too
func New() *Keydir {
	
	var dataDirectory string = "data"

	dir := make(map[string]keydirValue)

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

	k := &Keydir{dir:dir, dataFileDirectory:dataDirectory, dataFile:file, currentDataOffset:0}

	return k
}

//Simple set function for now. Eventually it will take cask records and make them lovely bytes.
func (k *Keydir) Set(key string, value string) {
	offset := HEADER_SIZE + 4 + 4 + len(key) + k.currentDataOffset
	k.dir[key] = keydirValue{fileId: k.dataFile.Name(), valueSz:len(value), valuePos:offset, timestamp:int32(time.Now().Unix())}
	record := &caskRecord{crc:1, timestamp:int32(time.Now().Unix()), ksz:len(key), valueSz:len(value), key:key, value:value}
	
	//Create binary buffer of the caskRecord object write that to file
	buf := record.Buffer()
	
	print(len(buf.Bytes()))
	_, err := k.dataFile.Write(buf.Bytes())
	k.currentDataOffset = offset
	if err != nil {
		panic(err)
	}
}

func (k *Keydir) Get(key string) string {
	val := k.dir[key]
	offset := int64(val.valuePos)
	valSize := val.valueSz
	buf := make([]byte, valSize)
	_, err := k.dataFile.ReadAt(buf, offset)
	if err != nil {
		panic(err)
	}
	return string(buf)
}

