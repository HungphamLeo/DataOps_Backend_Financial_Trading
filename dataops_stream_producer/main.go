// Package main là entry point của dataops-stream-producer.
//
// Service này chạy hai workers độc lập trong goroutine riêng:
//  1. BinanceProducer — WebSocket đến Binance, 1 goroutine per trading pair, publish → Kafka
//  2. TCBSPoller      — Cron poll REST TCBS iFlash trong giờ giao dịch, publish → Kafka
//
// Cả hai worker dùng chung Kafka producer và Redis client được khởi tạo trong app.NewServer().
// Graceful shutdown được xử lý qua signal.NotifyContext (SIGINT / SIGTERM).
package main

import (
	"dataops-stream-producer/app"
	"log"
)

// main là entry point của stream producer service.
// Panic khi khởi động thất bại để container orchestrator restart theo policy.
func main() {
	if err := app.NewServer(); err != nil {
		log.Printf("[NewServer] [Panic]: %s\n", err.Error())
		panic(err)
	}
}
