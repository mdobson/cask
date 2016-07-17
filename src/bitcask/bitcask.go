package bitcask

import "time"
import "io/ioutil"
import "io"
import "fmt"
import "os"
import "encoding/binary"
import "bytes"
import "strings"
import "path"

const HEADER_SIZE = 4 + 4 + 2 + 8
const HEADER_CRC_OFFSET = 0
const HEADER_TIMESTAMP_OFFSET = 4
const HEADER_KEYSIZE_OFFSET = 12
const HEADER_VALSIZE_OFFSET = 14
const TOMBSTONE = "CASK.ENTOMBED"
const DATA_DIRECTORY = "data"

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
	ksz int16
	valueSz int64
	key *bytes.Buffer
	value *bytes.Buffer
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
	keyErr := binary.Write(buf, binary.LittleEndian, c.key.Bytes())
	valueErr := binary.Write(buf, binary.LittleEndian, c.value.Bytes())
	if keyErr != nil {
		panic(keyErr)
	}

	if valueErr != nil {
		panic(valueErr)
	}
	return buf

}

//Create a new Keydir initialize a new data file in the directory.
//Probably will run compaction here too
func New() *Keydir {

	var dataDirectory string = DATA_DIRECTORY

	dir := make(map[string]keydirValue)

	files, err := ioutil.ReadDir(dataDirectory)

	if err != nil {
		os.Mkdir("./data", 0777)
	}

	var filesToRead []string = []string{}
	var fileNumber int = 0
	for _, file := range files {
		if strings.Contains(file.Name(), "data") {
			filesToRead = append(filesToRead, path.Join(dataDirectory, file.Name()))
			fileNumber+=1
		}
	}

	var fileName string = fmt.Sprintf("%s/data.%d.dfile", dataDirectory, fileNumber)

	file, err := os.Create(fileName)

	if err != nil {
		panic(err)
	}

	k := &Keydir{dir:dir, dataFileDirectory:dataDirectory, dataFile:file, currentDataOffset:0}
	for _, file := range filesToRead {
		k.Load(file)
	}
	return k
}

//Simple set function for now. Eventually it will take cask records and make them lovely bytes.
func (k *Keydir) Set(key string, value string) {
	record := &caskRecord{crc:1, timestamp:int32(time.Now().Unix()), ksz:int16(len(key)), valueSz:int64(len(value)), key:bytes.NewBufferString(key), value:bytes.NewBufferString(value)}
	offset := HEADER_SIZE + record.key.Len() + k.currentDataOffset
	k.dir[key] = keydirValue{fileId: k.dataFile.Name(), valueSz:len(value), valuePos:offset, timestamp:int32(time.Now().Unix())}
	//Create binary buffer of the caskRecord object write that to file
	buf := record.Buffer()
	_, err := k.dataFile.Write(buf.Bytes())
	k.currentDataOffset = offset + record.value.Len()
	if err != nil {
		panic(err)
	}

}

//If there is a data directory pre-populate our keydir with that data
func (k *Keydir) Load(p string) {
	fmt.Printf("Reading file: %s\n", p)
	var currentValueStartPos int = 0
	byteArray, err := ioutil.ReadFile(p)
	if err != nil {
		panic(err)
	}

	buf := bytes.NewReader(byteArray)
	for {
		crcBuf := make([]byte, 4)
		tsBuf := make([]byte, 4)
		kszBuf := make([]byte, 2)
		vszBuf := make([]byte, 4)



		n, err := buf.Read(crcBuf)
		if n != 4 {
			break
		}

		if err == io.EOF {
			break
		}
		//Read timestamp
		buf.Read(tsBuf)
		//Read key size
		buf.Read(kszBuf)
		//Read value size
		buf.Read(vszBuf)

		ksz := binary.LittleEndian.Uint16(kszBuf)
		vsz := binary.LittleEndian.Uint32(vszBuf)

		kBuf := make([]byte, ksz + 4)
		vBuf := make([]byte, vsz)


		buf.Read(kBuf)
		buf.Read(vBuf)
		crc := binary.LittleEndian.Uint32(crcBuf)
		ts := binary.LittleEndian.Uint32(tsBuf)
		currentValueStartPos += 18 + int(ksz)
		fmt.Printf("RECORD: CRC: %d TS: %d KSZ: %d VSZ: %d KEY: %s VAL: %s\n", crc, ts, ksz, vsz, string(kBuf), string(vBuf))

		k.dir[string(kBuf)] = keydirValue{fileId: p, valueSz:len(vBuf), valuePos:currentValueStartPos, timestamp:int32(time.Now().Unix())}

		currentValueStartPos += int(vsz)

	}
}

func (k *Keydir) Merge() {
	//Take total file count
	//Look at curent file being written to
	//Take all files before that
	//for each file
	//Read contents -> Get latest key value
	//Create new merged file with value
	//Create new hint file with value
	//Merge latest keys together
	//Spit out merged file and hint file
}

func (k *Keydir) Get(key string) (string, bool)  {
	val, ok := k.dir[key]
	if ok {
		offset := int64(val.valuePos)
		//println(offset)
		//var value *bytes.Buffer
		valSize := val.valueSz
		buf := make([]byte, valSize)
		_, err := k.dataFile.ReadAt(buf, offset)
		if err != nil {
			panic(err)
		}
		return string(buf), true
	} else {
		return "", false
	}
}

func (k *Keydir) Del(key string) {
	k.Set(key, TOMBSTONE)
	delete(k.dir, key)
}
