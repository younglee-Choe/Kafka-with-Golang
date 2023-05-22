package main

import (
	"fmt"
	"encoding/json"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	
	"main/structures"
	"main/processing/config"
	pConfig "main/producer/config"
)

// Filter messages that match conditions
func filterSlice(slice []byte, condition string) []byte {
	var streets []structures.Street

	err := json.Unmarshal(slice, &streets)
	if err != nil {
		fmt.Println("❗️ Unmarshal error:", err)
	}
	
	filteredData := make([]structures.Street, 0)

	for _, item := range streets {
		if item.Name == condition {
			filteredData = append(filteredData, item)
		}
	}

	fmt.Printf("⚙️  Filtered Data: %s \n", filteredData)
	marshaledData, _ := json.Marshal(filteredData)

	return marshaledData
}

func processAndProduce(message *kafka.Message, p *kafka.Producer) {
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("❗️ Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("✨ Produced event to topic %s: key = %-10s value = %s\n",
					*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
				}
			}
		}
	}()
	
	key := message.Key
	processedData := filterSlice(message.Value, "Mohammad Mill")

	topic := "leele-topic"
	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:			[]byte(key),
		Value:          []byte(processedData),
	}, nil)
}

func main() {
	fmt.Println("🫧  Consumer and Producer for data processing")

	p := pConfig.Kafka()
	defer p.Close()

	c := config.Kafka()
	c.SubscribeTopics([]string{"topic0", "topic1", "topic2"}, nil)
	defer c.Close()

	for {
		// ReadMessage polls the consumer for a message
		msg, err := c.ReadMessage(-1)
		if err == nil {
			// json.Unmarshal(msg.Value, &customers)
			fmt.Printf("✅ Received message %s: \n", msg.TopicPartition)
			processAndProduce(msg, p)
		} else {
			fmt.Printf("❗️ Consumer error: %v (%v)\n", err, msg)
		}
	}
}