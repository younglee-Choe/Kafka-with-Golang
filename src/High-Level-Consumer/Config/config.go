package Config

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func Kafka() *kafka.Consumer {
	c, err := kafka.NewConsumer(&kafka.ConfigMap {
		"bootstrap.servers": "localhost:9092",
		"group.id":			 "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		// When a connection error occurs, a panic occurs and the system is shut down
		panic(err)
	}

	return c
}