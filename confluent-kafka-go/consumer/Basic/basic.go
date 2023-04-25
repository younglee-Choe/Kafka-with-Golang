package Basic

import (
	"fmt"
    "consumer/Config"
)

func Consumer() {
	c := Config.Kafka()

	c.SubscribeTopics([]string{"myTopic"}, nil)
	defer c.Close()

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			fmt.Printf("❗️Consumer error: %v (%v)\n", err, msg)
		}
	}
}