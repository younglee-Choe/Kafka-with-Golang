package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"main/src/fountain"
	"strconv"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"gonum.org/v1/gonum/stat/combin"
)

var (
	brokers                  = []string{"kafkahost:9092"}
	topic        goka.Stream = "leele-topic"
	topicRekeyed goka.Stream = "leele-topic-rekeyed"
	group        goka.Group  = "leele-group"

	tmc                         *goka.TopicManagerConfig
	producerSize                int = 4
	defaultPartitionChannelSize int = 4
)

type block struct {
	Counter     int
	EncodedData []byte
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

		// Append encoded data(source data) sent by producer
		b.EncodedData = append(b.EncodedData, msg.(string)+"standard"...)
	} else {
		fmt.Println("❕ reset")
		b.Counter = 0
	}

	ctx.SetValue(b)

	if b.Counter == producerSize {
		// Attempt to decode by combining data
		fountainDecoding(ctx, b.EncodedData, ctx.Offset())
	}
}

// Attempt decoding by grouping the data sent by the producer with a combination algorithm,
// and attempting decoding by combining n-1 out of n
func combinationAlgorithm(size int) [][]int {
	var indexArr [][]int
	n := size
	k := n - 1
	gen := combin.NewCombinationGenerator(n, k)
	for gen.Next() {
		indexArr = append(indexArr, gen.Combination(nil))
	}
	return indexArr
}

func fountainDecoding(ctx goka.Context, msg []byte, offset int64) {
	result := bytes.Split(msg, []byte("standard"))

	arr := make([][]byte, producerSize)
	for k, v := range result {
		if len(v) > 0 {
			arr[k] = v
		}
	}

	combin := combinationAlgorithm(producerSize)

	kSize := 16
	for _, combinIdx := range combin {
		serializedEncodedBlocks := make([][]byte, producerSize-1)
		i := 0
		for _, producerIdx := range combinIdx {
			serializedEncodedBlocks[i] = arr[producerIdx]
			i++
		}

		out, err := fountain.FountainDecode(serializedEncodedBlocks, kSize, 94)
		if err != nil {
			fmt.Println("Fountain decode error;", err.Error())
		}

		var jsonData []fountain.Data
		err = json.Unmarshal(out, &jsonData)
		if err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Printf("🦋 Successfully decoded data at offset[%d]: %v\n", offset, string(out))
			ctx.Emit(topicRekeyed, ctx.Key(), string(out))
		}
	}
}

func runProcessor(initialized chan struct{}) {
	g := goka.DefineGroup(group,
		goka.Input(topic, new(codec.String), process),
		goka.Loop(new(codec.String), loopProcess),
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
		log.Printf("Error running processor: %v", err)
	}
}

func runView(initialized chan struct{}) {
	<-initialized

	view, err := goka.NewView(brokers,
		goka.GroupTable(group),
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
	runView(initialized)
}
