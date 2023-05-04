package main

import (
	"os"
	"fmt"
	"io/ioutil"
	"producer/Config"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
    fmt.Println("🫧 Kafka Producer;")

	p := Config.Kafka()
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
					fmt.Printf("Produced event to topic %s: key = %-10s value = %s\n",
					*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := "purchases"

	jsonFile, err := os.Open("../json-data/customers.json")
	if err != nil {
		fmt.Println("Failed to open file;", err)
	} else {
		fmt.Println("Successfully Opened JSON file!")
	}
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	if byteValue != nil {
		p.Produce(&kafka.Message{
            TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			// Key:			[]byte(key)
            Value:          byteValue,
        }, nil)
	}

	// Wait for message deliveries before shutting down
	p.Flush(15*1000)
}