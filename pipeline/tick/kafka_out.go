package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// LogNode converts the LogNode pipeline node into the TICKScript AST
type KafkaOutNode struct {
	Function
}

// NewLog creates a LogNode function builder
func newKafkaOutNode(parents []ast.Node) *LogmeNode {
	return &LogmeNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a KafkaOutNode ast.Node
func (n *KafkaOutNode) Build(l *pipeline.KafkaOutNode) (ast.Node, error) {
	n.Pipe("kafkaOut").
		Dot("Topic", "kafkatopic").
		Dot("Url", "kafkaUrl")


	return n.prev, n.err
}
