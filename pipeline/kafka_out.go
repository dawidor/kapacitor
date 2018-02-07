package pipeline

// A KafkaOut will take the incoming data stream and publish to kafka instance.
type KafkaOutNode struct {
	// Include the generic node implementation.
	chainnode

	// topic name
	Topic string
	Url string
}


// Create a new KafkaOutNode that accepts any edge type.
func newKafkaOutNode(wants EdgeType, url string, topic string) *KafkaOutNode {



	return &KafkaOutNode{
		chainnode: newBasicChainNode("kafkaOut", wants, wants),
		Topic:  topic,
		Url: url,
	}
}
