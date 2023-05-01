package Basic

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"producer/Config"
)

func Producer() {
	p := Config.Kafka()
	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("❗️Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivery message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := "myTopic"

	for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
		_ = p.Produce(&kafka.Message {
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value: []byte(word),
		}, nil)
	}

	// Wait for message deliveries before shutting down
	p.Flush(15*1000)
}