package main

import (
	"fmt"
    "main/consumer/config"
)

func main() {
    fmt.Println("🫧 Kafka Consumer")

	c := config.Kafka()

	// subscribe topic or multiple tipics
	c.SubscribeTopics([]string{"purchases", "tasks"}, nil)
	defer c.Close()

	for {
		// ReadMessage polls the consumer for a message.
		msg, err := c.ReadMessage(-1)
		if err == nil {
			// msg.TopicPartition provides partition-specific information (such as topic, partition and offset).
			fmt.Printf("🌿Message on %s: key=%s, vlaue=%s\n", msg.TopicPartition, msg.Key, msg.Value)
		} else {
			fmt.Printf("❗️Consumer error: %v (%v)\n", err, msg)
		}
	}
}