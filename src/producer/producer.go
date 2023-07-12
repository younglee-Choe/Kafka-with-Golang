package producer

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"golang.org/x/time/rate"

	"main/src/fountain"
	"main/src/producer/config"
)

func Producer() {
	p := config.Kafka()
	defer p.Close()

	limiter := rate.NewLimiter(rate.Every(time.Second), 1000)

	deliveryChan := make(chan kafka.Event)
	var startTime time.Time

	var i int
	for {
		if err := limiter.Wait(context.TODO()); err != nil {
			log.Println("Rate Limit exceeded. Waiting...")
			time.Sleep(5 * time.Second)
			continue
		}

		// Go-routine to handle message delivery reports and
		// possibly other event types (errors, stats, etc)
		go func() {
			deliveryReport := <-deliveryChan
			m := deliveryReport.(*kafka.Message)

			if m.TopicPartition.Error != nil {
				fmt.Printf("❗️ Delivery failed: %v\n", m.TopicPartition.Error)
			} else if string(m.Value) == "Over rate limit" {
				fmt.Println("❗️ Over rate limit")
				time.Sleep(5 * time.Second)
			} else {
				// Check acks
				fmt.Printf("Delivered message to topic %s[%d] at offset %v with key '%s'\n",
					*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset, m.Key)
			}

			endTime := time.Now()
			latency := endTime.Sub(startTime)
			fmt.Printf("latency: %s\n", latency)
		}()

		// reqUrl := "https://6458779a4eb3f674df75126b.mockapi.io/api/mock/tasks"
		// message := generateMessage.GenerateMessage(reqUrl)

		filePath := "./dummy_data/small_dummy.json"
		message, err := ioutil.ReadFile(filePath)
		if err != nil {
			log.Fatal(err)
		}
		startTime = time.Now()

		topic := "leele-topic"
		key := "1producer"
		i++

		if string(message) == "Over rate limit" {
			time.Sleep(5 * time.Second)
		}

		kSize := 16
		encodedSymbolSize := 7
		message = fountain.FountainEncode([]byte(message), kSize, encodedSymbolSize, 8923483)

		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(key),
			Value:          message,
		}, deliveryChan)
		if err != nil {
			fmt.Printf("❗️ Failed to produce message: %s\n", err)
			panic(err)
		}

		// Wait for message deliveries before shutting down
		p.Flush(15 * 1000)
	}
}
