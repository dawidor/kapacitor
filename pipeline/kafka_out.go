package pipeline

// A KafkaOut will take the incoming data stream and publish to kafka instance.
type KafkaOutNode struct {
	// Include the generic node implementation.
	node

	// URL for connecting to kafka instance
	Url string

	// topic name
	topic string
}

// Create a new KafkaOutNode that accepts any edge type.
func newKafkaOutNode(wants EdgeType) *KafkaOutNode {
	return &KafkaOutNode{
		node: node{
			desc: "kafkaOut",
			wants: wants,
			provides: NoEdge,
		},
	}
}