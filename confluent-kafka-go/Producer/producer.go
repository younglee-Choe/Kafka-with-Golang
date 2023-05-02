package main

import (
	"fmt"
	"math/rand"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"producer/Config"
)

func main() {
    fmt.Println("Kafka Producer Example")

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
					// fmt.Printf("Delivery message to %v\n", ev.TopicPartition)
					fmt.Printf("Produced event to topic %s: key = %-10s value = %s\n",
					*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := "purchases"

	users := [...]string{"eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther"}
    items := [...]string{"book", "alarm clock", "t-shirts", "gift card", "batteries"}

	for n := 0; n < 10; n++ {
        key := users[rand.Intn(len(users))]
        data := items[rand.Intn(len(items))]
        p.Produce(&kafka.Message{
            TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
            Key:            []byte(key),
            Value:          []byte(data),
        }, nil)
	}

	// Wait for message deliveries before shutting down
	p.Flush(15*1000)
}