package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"time"

	"main/src/fountain"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

var (
	brokers                  = []string{"203.247.240.235:9092", "203.247.240.235:9093", "203.247.240.235:9094"}
	topic        goka.Stream = "leele-topic"
	topicRekeyed goka.Stream = "leele-topic-rekeyed"
	groupEncoded goka.Group  = "leele-group-encoded"
	groupDecoded goka.Group  = "leele-group-decoded"

	tmc                         *goka.TopicManagerConfig
	producerSize                int = 8
	defaultPartitionChannelSize     = producerSize
)

type block struct {
	Counter     int
	EncodedData string
	DecodedData []byte
}

// This codec allows marshalling (encode) and unmarshalling (decode) the user to and from the group table
type blockCodec struct{}

func init() {
	tmc = goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
}

// Encodes a user into []byte
func (jc *blockCodec) Encode(value interface{}) ([]byte, error) {
	if _, isUser := value.(*block); !isUser {
		return nil, fmt.Errorf("Codec requires value *user, got %T", value)
	}
	return json.Marshal(value)
}

// Decodes a user from []byte to it's go representation
func (jc *blockCodec) Decode(data []byte) (interface{}, error) {
	var (
		c   block
		err error
	)
	err = json.Unmarshal(data, &c)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshaling user: %v", err)
	}
	return &c, nil
}

func runEmitter() {
	emitter, err := goka.NewEmitter(brokers, topic,
		new(codec.String))
	if err != nil {
		panic(err)
	}
	defer emitter.Finish()

	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()

	var i int
	for range t.C {
		key := fmt.Sprintf("user-%d", i%10)
		value := fmt.Sprintf("user%d", i%10) // Value: fountain encoded data
		fmt.Printf("key: %s, value: %s\n", key, value)
		emitter.EmitSync(key, value)
		i++
	}
}

// Rekeyed: blockID -> index(offset)
func process(ctx goka.Context, msg interface{}) {
	key := ctx.Offset()
	ctx.Loopback(strconv.Itoa(int(key)), msg)
}

// Update the state
func loopProcess(ctx goka.Context, msg interface{}) {
	var b *block
	if val := ctx.Value(); val != nil {
		b = val.(*block)
	} else {
		b = new(block)
	}

	if b.Counter < producerSize {
		b.Counter++

		// Stores the encoded data(source data) sent by the Producer by offset (size: the number of Producer)
		b.EncodedData += (msg.(string) + ", ")

		// source data가 다 모였으면, 데이터들 조합해서 디코딩 시도
		// combinationAlgorithm()
	} else {
		fmt.Println("❕Reset")
		b.Counter = 0
	}

	ctx.SetValue(b)
	fmt.Printf("Partition: %d, Offset: %d, Key: %s, Value: %s, msg: %v\n", ctx.Partition(), ctx.Offset(), ctx.Key(), ctx.Value(), msg)
}

// table에 디코딩된 데이터들 저장(stateful-based)
func secondProcess(ctx goka.Context, msg interface{}) {
	var b *block
	if val := ctx.Value(); val != nil {
		b = val.(*block)
	} else {
		b = new(block)
	}

	decodedData := fountainDecoding(ctx, msg)
	b.DecodedData = append(b.DecodedData, decodedData...)

	ctx.SetValue(b)

	// 성공적으로 디코딩된 결과만 새로운 topic에 저장
	ctx.Emit(topicRekeyed, ctx.Key(), "Successfully decoded data")
}

func combinationAlgorithm() {
	// Producer가 보낸 데이터들을 조합(nCn-1) 알고리즘으로 묶어서 디코딩 시도

	// 디코딩 결과를 Kafka에 저장(stateful-based)
	// decodedData := fountainDecoding(ctx, msg)
	// u.DecodedData = append(u.DecodedData, decodedData...)
}

func fountainDecoding(ctx goka.Context, msg interface{}) []byte {
	filePath := "../../dummy_data/small_dummy.json"
	filePath2 := "../../dummy_data/small_malicious_dummy.json"

	message, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Fatal(err)
	}

	malicious_message, err := ioutil.ReadFile(filePath2)
	if err != nil {
		log.Fatal(err)
	}

	serializedEncodedBlocks := make([][]byte, 3)

	serializedEncodedBlocks[0] = fountain.EncodeM(message, 8923483)
	serializedEncodedBlocks[1] = fountain.EncodeM(message, 8923486)
	serializedEncodedBlocks[2] = fountain.EncodeM(malicious_message, 8923487)

	out, err := fountain.Decode(serializedEncodedBlocks, len(message))

	if err != nil {
		log.Print(err.Error())
	}

	return out
}

func runProcessor(initialized chan struct{}) {
	g := goka.DefineGroup(groupEncoded,
		goka.Input(topic, new(codec.String), process),
		goka.Loop(new(codec.String), loopProcess),
		// goka.Output(topicRekeyed, new(codec.String)),
		goka.Persist(new(blockCodec)),
	)
	p, err := goka.NewProcessor(brokers,
		g,
		goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(tmc)),
		goka.WithConsumerGroupBuilder(goka.DefaultConsumerGroupBuilder),
	)
	if err != nil {
		panic(err)
	}

	close(initialized)

	if err = p.Run(context.Background()); err != nil {
		log.Printf("Error running processor: %v", err)
	}
}

func runProcessor2(initialized chan struct{}) {
	g := goka.DefineGroup(groupDecoded,
		goka.Input(topic, new(codec.String), secondProcess),
		goka.Output(topicRekeyed, new(codec.String)),
		goka.Persist(new(blockCodec)),
	)
	p, err := goka.NewProcessor(brokers,
		g,
		goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(tmc)),
		goka.WithConsumerGroupBuilder(goka.DefaultConsumerGroupBuilder),
	)
	if err != nil {
		panic(err)
	}

	close(initialized)

	if err = p.Run(context.Background()); err != nil {
		log.Printf("Error running processor2: %v", err)
	}
}

func runView(initialized chan struct{}) {
	<-initialized

	view, err := goka.NewView(brokers,
		goka.GroupTable(groupDecoded),
		new(blockCodec),
	)
	if err != nil {
		panic(err)
	}

	view.Run(context.Background())
}

func main() {
	tm, err := goka.NewTopicManager(brokers, goka.DefaultConfig(), tmc)
	if err != nil {
		log.Fatalf("Error creating topic manager: %v", err)
	}
	defer tm.Close()
	err = tm.EnsureStreamExists(string(topic), defaultPartitionChannelSize)
	if err != nil {
		log.Printf("Error creating kafka topic %s: %v", topic, err)
	}

	// When this example is run the first time, wait for creation of all internal topics (this is done
	// by goka.NewProcessor)
	initialized := make(chan struct{})

	go runEmitter()
	go runProcessor(initialized)
	go runProcessor2(initialized)
	runView(initialized)
}
