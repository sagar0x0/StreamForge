package main

import (
	"encoding/json"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sagar/streamforge/internal/client"
	"github.com/sagar/streamforge/pkg/log"
)

func main() {
	brokersStr := os.Getenv("BROKERS")
	mode := os.Getenv("MODE")

	if brokersStr == "" {
		brokersStr = "127.0.0.1:9092"
	}

	brokers := strings.Split(brokersStr, ",")
	log.Logger.Info("Starting load generator", "brokers", brokersStr, "mode", mode)

	numPartitions := 4
	prod := client.NewProducer(brokers, int32(numPartitions))
	defer prod.Close()

	cities := []string{"NYC", "LA", "CHI", "SF", "BOS", "ATL", "SEA", "DEN"}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)



	log.Logger.Info("Producing events...")

	// Spawn multiple workers for high authentic throughput
	numWorkers := 10
	for i := 0; i < numWorkers; i++ {
		go func() {
			for {
				select {
				case <-sigCh:
					return
				default:
					city := cities[rand.Intn(len(cities))]
					amount := 10.0 + rand.Float64()*990.0
					val, _ := json.Marshal(map[string]interface{}{
						"city":   city,
						"amount": amount,
						"ts":     time.Now().UnixNano(),
					})

					prod.Send("orders", []byte(city), val)
					// Small sleep to not absolutely kill the CPU, but small enough to get 20k+ ops/sec
					time.Sleep(100 * time.Microsecond)
				}
			}
		}()
	}

	<-sigCh
	log.Logger.Info("Shutting down loadgen...")
}
