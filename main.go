package main

import "bitcask"

func main() {
	keyDir := bitcask.New()
	keyDir.Set("Hello","World")
	keyDir.Set("How","Are")
	keyDir.Set("You","Doing")
	val := keyDir.Get("Hello")
	val2 := keyDir.Get("How")
	val3 := keyDir.Get("You")
	println(val)
	println(val2)
	println(val3)
}
