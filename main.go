package main

import "bitcask"

func main() {
	keyDir := bitcask.New()
	keyDir.Set("Hello","World")
	//val := keyDir.Get("Hello")
	//print(val)
}