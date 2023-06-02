package config

import (
	"fmt"
	
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func KafkaProducer() *kafka.Producer {
	brokers := "203.247.240.235:9092, 203.247.240.235:9093, 203.247.240.235:9094"

	p, err := kafka.NewProducer(&kafka.ConfigMap {
		"bootstrap.servers": brokers,					// producer can find the Kakfa cluster
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

func KafkaConsumer() *kafka.Consumer {
	c, err := kafka.NewConsumer(&kafka.ConfigMap {
		"bootstrap.servers": "203.247.240.235:9092",
		"group.id":			 "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		// When a connection error occurs, a panic occurs and the system is shut down
		panic(err)
	}

	return c
}