package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
	"strconv"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/storage"
)

var (
	brokers             = []string{"kafkahost:9092"}
	topic   goka.Stream = "topic"
	topicRekeyed   goka.Stream = "topic-rekeyed"
	group   goka.Group  = "goka-group"
	st    storage.Storage

	tmc *goka.TopicManagerConfig
	producerSize int = 8
	defaultPartitionChannelSize = producerSize
)

type user struct {
	Counter int
	Data string
}

// This codec allows marshalling (encode) and unmarshalling (decode) the user to and from the group table
type userCodec struct{}

func init() {
	tmc = goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
}

// Encodes a user into []byte
func (jc *userCodec) Encode(value interface{}) ([]byte, error) {
	if _, isUser := value.(*user); !isUser {
		return nil, fmt.Errorf("Codec requires value *user, got %T", value)
	}
	return json.Marshal(value)
}

// Decodes a user from []byte to it's go representation
func (jc *userCodec) Decode(data []byte) (interface{}, error) {
	var (
		c   user
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
		key := fmt.Sprintf("user-%d", i%8)	
		value := fmt.Sprintf("user%d", i%8)		// Value: fountain encoded data
		emitter.EmitSync(key, value)
		i++
	}
}

func process(ctx goka.Context, msg interface{}) {
	key := ctx.Offset()
	ctx.Loopback(strconv.Itoa(int(key)), msg)
}

func loopProcess(ctx goka.Context, msg interface{}) {
	var u *user
	if val := ctx.Value(); val != nil {
		u = val.(*user)
	} else {
		u = new(user)
	}

	if u.Counter < producerSize {
		u.Counter++

		// previous state + current input
		u.Data += (msg.(string) + ", ")
	} else {
		fmt.Println("❕Reset")
		u.Counter = 0
	}

	ctx.SetValue(u)

	// Try decoding
	fountainDecoding(ctx)

	fmt.Printf("Partition: %d, Offset: %d, Key: %s, Value: %s, msg: %v\n", ctx.Partition(), ctx.Offset(), ctx.Key(), ctx.Value(), msg)
}

func fountainDecoding(ctx goka.Context) {
	// write decode part...
	test := "20"

	// write if-else; emit only decoded data
	if ctx.Key() == test {
		fmt.Println("‼️ Decoding Success")
		ctx.Emit(topicRekeyed, ctx.Key(), "decoded data")
	}
}

func runProcessor(initialized chan struct{}) {
	g := goka.DefineGroup(group,
		goka.Input(topic, new(codec.String), process),
		goka.Loop(new(codec.String), loopProcess),
		// goka.Output(topicRekeyed, new(userCodec)),
		goka.Output(topicRekeyed, new(codec.String)),
		goka.Persist(new(userCodec)),
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
		new(userCodec),
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
