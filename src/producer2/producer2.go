package producer2

import (
	// "os"
	"fmt"
	"net/http"
	"io/ioutil"
	"main/producer/config"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func Producer() {
    fmt.Println("🫧 Kafka Producer2")

	p := config.Kafka()
	defer p.Close()

	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("❗️Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("🌿Produced event to topic %s: key = %-10s value = %s\n",
					*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := "tasks"

	// using mockAPI
	res, err := http.Get("https://6458779a4eb3f674df75126b.mockapi.io/api/mock/tasks")
	if err != nil {
		fmt.Println("❗️error;", err)
		panic(err)
	}
	defer res.Body.Close()

	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println("❗️error;", err)
		panic(err)
	}

	if data != nil {
		p.Produce(&kafka.Message{
            TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			// Key:			[]byte(key)
            Value:          data,
        }, nil)
	}

	// Wait for message deliveries before shutting down
	// `.Flush()` that will block until all message deliveries are done or the provided timeout elapses.
	p.Flush(15*1000)
}