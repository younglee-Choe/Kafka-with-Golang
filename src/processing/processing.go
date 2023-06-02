package main

import (
	"fmt"
	"encoding/json"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	
	"main/structures"
	"main/processing/config"
)

// Filter messages that match conditions
func filterSlice(slice []byte, condition string) []byte {
	var person []structures.Person

	err := json.Unmarshal(slice, &person)
	if err != nil {
		fmt.Println("❗️ Unmarshal error:", err)
	}
	filteredData := make([]structures.Person, 0)

	for _, item := range person {
		if item.Name == condition {
			filteredData = append(filteredData, item)
		}
	}

	fmt.Printf("⚙️  Filtered Data: %s \n", filteredData)
	marshaledData, _ := json.Marshal(filteredData)

	return marshaledData
}

// Find someting different value
func findDifferentValue(slice []byte) []byte {
	var blocks []structures.Block

	err := json.Unmarshal(slice, &blocks)
	if err != nil {
		fmt.Println("❗️ Unmarshal error:", err)
	}

	filteredData := make([]structures.Block, 0)

	referenceValue := blocks[0].Value
	matchingData := []structures.Block{}

	for _, d := range blocks {
		if d.Value == referenceValue {
			matchingData = append(matchingData, d)
		}
	}

	for _, d := range matchingData {
		filteredData = append(filteredData, d)
	}
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

	processedData := filterSlice(message.Value, "Charlie")
	// processedData := findDifferentValue(message.Value)

	key := message.Key
	topic := "leele-last-topic"
	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:			[]byte(key),
		Value:          []byte(processedData),
	}, nil)
}

func main() {
	fmt.Println("🫧  Consumer and Producer for data processing")

	p := config.KafkaProducer()
	defer p.Close()

	c := config.KafkaConsumer()
	c.SubscribeTopics([]string{"leele-topic"}, nil)
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