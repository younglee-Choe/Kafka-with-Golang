package Config

import (
    "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func Kafka() *kafka.Consumer {
	c, err := kafka.NewConsumer(&kafka.ConfigMap {
		"bootstrap.servers": "203.247.240.235",
		"group.id":			 "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		// When a connection error occurs, a panic occurs and the system is shut down
		panic(err)
	}

	return c
}