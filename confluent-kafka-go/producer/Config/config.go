package Config

import (
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func Kafka() *kafka.Producer {
	p, err := kafka.NewProducer(&kafka.ConfigMap {
		"bootstrap.servers": "203.247.240.235",
	})
	if err != nil {
		// When a connection error occurs, a panic occurs and the system is shut down
		panic(err)
	}

	return p
}