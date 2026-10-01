// Package main là entry point của dataops-backend-api.
//
// Khởi động HTTP server theo chuỗi:
//   Config → MySQL → Redis → Services → Gin Router → ListenAndServe
//
// Graceful shutdown được xử lý bên trong app.NewServer() thông qua
// signal.NotifyContext (SIGINT / SIGTERM → 5s shutdown timeout).
package main

import (
	"log"
	"dataops-backend-api/app"
)

// main là entry point duy nhất của service.
// Mọi lỗi khởi động đều được log và panic để container orchestrator (Docker/K8s)
// phát hiện và restart service theo restart policy.
func main() {
	if err := app.NewServer(); err != nil {
		log.Printf("[NewServer] [Panic]: %s\n", err.Error())
		panic(err)
	}
}
