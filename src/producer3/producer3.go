package producer3

import (
	"fmt"
	"net/http"
	"io/ioutil"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"main/producer/config"
)

func Producer() {
	fmt.Println("🫧  Kafka Producer3")

	p := config.Kafka()
	defer p.Close()

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

	topic := "leele-topic"

	// using mockAPI
	res, err := http.Get("https://6458779a4eb3f674df75126b.mockapi.io/api/mock/tasks")
	if err != nil {
		fmt.Println("❗️ error;", err)
		panic(err)
	}
	defer res.Body.Close()

	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println("❗️ error;", err)
		panic(err)
	}

	key := "3producer"

	if data != nil {
		p.Produce(&kafka.Message{
            TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:			[]byte(key),
            Value:          data,
        }, nil)
	}

	p.Flush(15*1000)
}