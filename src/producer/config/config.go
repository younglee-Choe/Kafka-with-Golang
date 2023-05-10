package config

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func Kafka() *kafka.Producer {
	p, err := kafka.NewProducer(&kafka.ConfigMap {
		"bootstrap.servers": "203.247.240.235:9092",	// producer can find the Kakfa cluster
		// "client.id": socket.gethostname(),			// easily correlate requests on the broker with the client instance
    	"acks": "all",									// acks=all;
	})
	if err != nil {
		// When a connection error occurs, 
		// a panic occurs and the system is shut down
		fmt.Printf("❗️Failed to create producer: %s", err)
		panic(err)
	}

	return p
}