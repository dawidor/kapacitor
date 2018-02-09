package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
	"fmt"
)

// LogNode converts the LogNode pipeline node into the TICKScript AST
type KafkaOutNode struct {
	Function
}

// NewLog creates a LogNode function builder
func newKafkaOutNode(parents []ast.Node) *KafkaOutNode {
	fmt.Println("-----------------------------test10")
	return &KafkaOutNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a KafkaOutNode ast.Node
func (n *KafkaOutNode) Build(l *pipeline.KafkaOutNode) (ast.Node, error) {
	fmt.Println("-----------------------------test4")
	n.Pipe("kafkaOut")
	//	Dot("Topic", "kafkatopic").
	//	Dot("Url", "kafkaUrl")


	return n.prev, n.err
}
