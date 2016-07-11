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
	for _, file := range files {
		byteArray, err := ioutil.ReadFile(path.Join(dir, file.Name()))
		if err != nil {
			panic(err)
		}

		buf := bytes.NewBuffer(byteArray)
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
			nTwo, _ := buf.Read(tsBuf)
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
			println("--CRC--")
			println(n)
			println(binary.LittleEndian.Uint32(crcBuf))
			println("--TS--")
			println(nTwo)
			println(binary.LittleEndian.Uint32(tsBuf))
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
		}
	}
}
