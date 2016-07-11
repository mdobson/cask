package main

// import "bitcask"
import "io"
import "io/ioutil"
import "bytes"
import "encoding/binary"
import "path"

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

	if err != nil {
		panic(err)
	}
	var currentValueStartPos int = 0
	for _, file := range files {
		var hints []hintFileValue
		byteArray, err := ioutil.ReadFile(path.Join(dir, file.Name()))
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
			buf.Read(tsBuf)
			nThree, _ := buf.Read(kszBuf)
			nFour, _ := buf.Read(vszBuf)

			ksz := binary.LittleEndian.Uint16(kszBuf)
			vsz := binary.LittleEndian.Uint32(vszBuf)

			kBuf := make([]byte, ksz + OFFSET)
			println("--KBUFLEN--")
			println(len(kBuf))
			vBuf := make([]byte, vsz)


			nFive, _ := buf.Read(kBuf)
			nSix, _ := buf.Read(vBuf)
			//crc := binary.LittleEndian.Uint32(crcBuf)
			ts := binary.LittleEndian.Uint32(tsBuf)
			currentValueStartPos += TOTAL + int(ksz)
			// println("--CRC--")
			// println(n)
			// println(crc)
			// println("--TS--")
			// println(nTwo)
			// println(ts)
			println("--KSZ--")
			println(nThree)
			println(int64(ksz))
			println("--VSZ--")
			println(nFour)
			println(vsz)
			println("--KBUF--")
			println(nFive)
			println(string(kBuf))
			println("--VBUF--")
			println(nSix)
			println(string(vBuf))
			println("--DATA POSITION--")
			println(currentValueStartPos)
			

			hintFileVal := hintFileValue{
				timestamp: int32(ts),
				ksz: int16(ksz),
				valueSz: int64(vsz),
				valuePos: int32(currentValueStartPos),
				key: bytes.NewBufferString(string(kBuf)),
			}

			hints = append(hints, hintFileVal)
			currentValueStartPos += int(vsz)

		}
		testByteArray, testErr := ioutil.ReadFile(path.Join(dir, file.Name()))
		if testErr != nil {
			panic(err)
		}
		testBuf := bytes.NewReader(testByteArray)
		for _, hintVal := range hints {
			vBuf := make([]byte, hintVal.valueSz)
			testBuf.ReadAt(vBuf, int64(hintVal.valuePos))
			println("--SIZE--")
			println(hintVal.valueSz)
			println("--POS--")
			println(hintVal.valuePos)
			println("--HINTKEY--")
			println(hintVal.key.String())
			println("--HINTVAL--")
			println(string(vBuf))
		}
	}
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
	fileId string
}

func CreateHintFile() {

}

func Compact() {

}
