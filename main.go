package main

// import "bitcask"
import "io"
import "io/ioutil"
import "bytes"
import "encoding/binary"
import "path"
import "strings"
import "fmt"
import "os"

const CRC_LENGTH = 4
const TIMESTAMP_LENGTH = 4
const KEY_SIZE_LENGTH = 2
const VALUE_SIZE_LENGTH = 4
const OFFSET = 4
const TOTAL = CRC_LENGTH + TIMESTAMP_LENGTH + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH + OFFSET

func main() {
	// keyDir := bitcask.New()
	// keyDir.Set("Hello","World")
	// keyDir.Set("How","Are")
	// keyDir.Set("You","Doing")
	// val := keyDir.Get("Hello")
	// val2 := keyDir.Get("How")
	// val3 := keyDir.Get("You")
	// println(val)
	// println(val2)
	// println(val3)






	//var start int = 0

	ReadFilesInDataDir()

}

func ReadFilesInDataDir() {
	dir := "./data"
	files, err := ioutil.ReadDir(dir)
	fmt.Printf("Starting to read files...\n")
	if err != nil {
		panic(err)
	}

	for _, file := range files {
		filePath := path.Join(dir, file.Name())
		if strings.Contains(file.Name(), "data") {
			fmt.Printf("Beginning to read file: %s\n", filePath)
			hints := ReadFile(filePath)
			testByteArray, testErr := ioutil.ReadFile(filePath)
			if testErr != nil {
				panic(err)
			}
			testBuf := bytes.NewReader(testByteArray)
			for _, hintVal := range hints {
				vBuf := make([]byte, hintVal.valueSz)
				testBuf.ReadAt(vBuf, int64(hintVal.valuePos))
			}

			hf := hintFile {
				values: hints,
				fileId: "foo",
			}

			CreateHintFile(hf)
		}
	}

	postDataFiles, err := ioutil.ReadDir(dir)

	for _, file := range postDataFiles {
		filePath := path.Join(dir, file.Name())
		if strings.Contains(file.Name(), "hint") {
			ReadHintFile(filePath)
		}
	}
}

func ReadFile(p string) []hintFileValue{
	fmt.Printf("Reading file: %s\n", p)
	var currentValueStartPos int = 0
	var hints []hintFileValue
	byteArray, err := ioutil.ReadFile(p)
	if err != nil {
		panic(err)
	}

	buf := bytes.NewReader(byteArray)
	for {
		crcBuf := make([]byte, CRC_LENGTH)
		tsBuf := make([]byte, TIMESTAMP_LENGTH)
		kszBuf := make([]byte, KEY_SIZE_LENGTH)
		vszBuf := make([]byte, VALUE_SIZE_LENGTH)



		n, err := buf.Read(crcBuf)
		if n != CRC_LENGTH {
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

		kBuf := make([]byte, ksz + OFFSET)
		vBuf := make([]byte, vsz)


		buf.Read(kBuf)
		buf.Read(vBuf)
		crc := binary.LittleEndian.Uint32(crcBuf)
		ts := binary.LittleEndian.Uint32(tsBuf)
		currentValueStartPos += TOTAL + int(ksz)
		fmt.Printf("RECORD: CRC: %d TS: %d KSZ: %d VSZ: %d KEY: %s VAL: %s\n", crc, ts, ksz, vsz, string(kBuf), string(vBuf))


		hintFileVal := hintFileValue{
			timestamp: int32(ts),
			ksz: int16(ksz),
			valueSz: int64(vsz),
			valuePos: int32(currentValueStartPos),
			key: bytes.NewBufferString(string(kBuf)),
		}
		fmt.Printf("HINT: TS: %d KSZ: %d VSZ: %d VPOS: %d KEY: %s\n", hintFileVal.timestamp, hintFileVal.ksz, hintFileVal.valueSz, hintFileVal.valuePos, hintFileVal.key.String())

		hints = append(hints, hintFileVal)
		currentValueStartPos += int(vsz)

	}
	return hints
}

//Read data files
//for latest keys generate hint file entries
//Hint file format
//timestamp
//key size
//value size
//value position
//key

type hintFileValue struct {
	timestamp int32
	ksz int16
	valueSz int64
	valuePos int32
	key *bytes.Buffer
}

type hintFile struct {
	values []hintFileValue
	cache latestHintCache
	fileId string
}

type latestHintCache struct {
	hintTimes map[string]int32
}

func Buffer(h hintFileValue) *bytes.Buffer {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, h.timestamp)
	_ = binary.Write(buf, binary.LittleEndian, h.ksz)
	_ = binary.Write(buf, binary.LittleEndian, h.valueSz)
	_ = binary.Write(buf, binary.LittleEndian, h.valuePos)
	keyErr := binary.Write(buf, binary.LittleEndian, h.key.Bytes())
	if keyErr != nil {
		panic(keyErr)
	}

	return buf

}

func CreateHintFile(hf hintFile) {
	dataDirectory := "./data"
	files, _ := ioutil.ReadDir(dataDirectory)
	var numberOfHintFiles int = 0;
	for _, file := range files {
		if strings.Contains(file.Name(), "hint") {
			numberOfHintFiles+=1
		}
	}

	var fileName string = fmt.Sprintf("%s/hint.%d.hfile", dataDirectory, numberOfHintFiles)

	file, _ := os.Create(fileName)

	for _, hint := range hf.values {

		buf := Buffer(hint)
		file.Write(buf.Bytes())
	}
}

func ReadHintFile(p string) {
	fmt.Printf("Reading file: %s\n", p)
	byteArray, err := ioutil.ReadFile(p)
	if err != nil {
		panic(err)
	}

	buf := bytes.NewReader(byteArray)
	for {
		tsBuf := make([]byte, TIMESTAMP_LENGTH)
		kszBuf := make([]byte, KEY_SIZE_LENGTH)
		vszBuf := make([]byte, 8)
		valuePosBuf := make([]byte, TIMESTAMP_LENGTH)

		n, err := buf.Read(tsBuf)
		if n != TIMESTAMP_LENGTH {
			break
		}

		if err == io.EOF {
			break
		}

		buf.Read(kszBuf)
		amt, _ := buf.Read(vszBuf)
		buf.Read(valuePosBuf)

		ts := binary.LittleEndian.Uint32(tsBuf)
		ksz := binary.LittleEndian.Uint16(kszBuf)
		vsz := binary.LittleEndian.Uint64(vszBuf)
		valuePos := binary.LittleEndian.Uint32(valuePosBuf)
		fmt.Printf("Value size read result: %d\n", amt)
		kBuf := make([]byte, ksz + OFFSET)
		buf.Read(kBuf)
		fmt.Printf("RECORD: TS: %d KSZ: %d VSZ: %d VALPOS: %d KEY: %s\n", ts, ksz, vsz, valuePos, string(kBuf))
	}
}

func Compact() {

}
