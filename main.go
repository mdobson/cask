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
}
