package fountain

import (
	"encoding/json"
	"errors"
	"log"
	"math/rand"
)

type Data struct {
	Name    string `json:"name"`
	Account string `json:"account"`
}

func serializeCodeBlock(codeBlocks []LTBlock) []byte {
	bytes, err := json.Marshal(codeBlocks)
	if err != nil {
		log.Fatal(err)
	}
	return bytes
}

func deserializeCodeBlock(bytes []byte) []LTBlock {
	var newLTBlock []LTBlock
	err := json.Unmarshal(bytes, &newLTBlock)
	if err != nil {
		log.Fatal(err)
	}

	return newLTBlock
}

func FountainEncode(message []byte, kSize int, encodedSymbolSize int, seed int64) []byte {
	c := NewRaptorCodec(kSize, 4)
	ids := make([]int64, encodedSymbolSize)
	random := rand.New(rand.NewSource(seed))

	messageCopy := make([]byte, len(message))
	copy(messageCopy, message)

	for i := range ids {
		ids[i] = int64(random.Intn(60000))
	}

	encodedBlocks := EncodeLTBlocks(messageCopy, ids, c)
	serializedEncodedBlocks := serializeCodeBlock(encodedBlocks)

	return serializedEncodedBlocks
}

func FountainDecode(serializedEncodedBlocks [][]byte, kSize int, messageSize int) ([]byte, error) {
	c := NewRaptorCodec(kSize, 4)
	decoder := newRaptorDecoder(c.(*raptorCodec), messageSize)

	for i := 0; i < len(serializedEncodedBlocks); i++ {
		EncodedBlocks := deserializeCodeBlock(serializedEncodedBlocks[i])
		for j := 0; j < len(EncodedBlocks); j++ {
			if decoder.AddBlocks([]LTBlock{EncodedBlocks[j]}) {
				out := decoder.Decode()
				return out, nil
			}
		}
	}

	return nil, errors.New("Not Decoding!")
}
