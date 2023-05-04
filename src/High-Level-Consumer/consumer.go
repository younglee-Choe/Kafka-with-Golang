package main

import (
	"fmt"
	"encoding/json"
    "consumer/Config"
	consumerStruct "consumer/structure"
)

func main() {
    fmt.Println("🫧 Kafka Consumer")

	c := Config.Kafka()

	c.SubscribeTopics([]string{"purchases"}, nil)
	defer c.Close()

	var customers consumerStruct.Customers
	
	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			json.Unmarshal(msg.Value, &customers)
			fmt.Printf("Message on %s: key=%v, vlaue=%v\n", msg.TopicPartition, msg.Key, customers)
		} else {
			fmt.Printf("❗️Consumer error: %v (%v)\n", err, msg)
		}
	}
}