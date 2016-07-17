package main

import "bitcask"

func main() {
	keyDir := bitcask.New()
	keyDir.Set("Hello","World")
	keyDir.Set("How","Are")
	keyDir.Set("You","Doing")
	_, valReturn := keyDir.Get("Hello")
	println(valReturn)
	_, val2Return := keyDir.Get("How")
	println(val2Return)
	_, val3Return := keyDir.Get("You")
	println(val3Return)
	keyDir.Del("Hello")
	_, deletedValReturn := keyDir.Get("Hello")
	println(deletedValReturn)
}
