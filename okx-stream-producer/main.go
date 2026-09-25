package main

import (
	"log"
	"okx-stream-producer/app"
)

func main() {
	if err := app.NewServer(); err != nil {
		log.Printf("[NewServer] [Panic]: %s\n", err.Error())
		panic(err)
	}
}
