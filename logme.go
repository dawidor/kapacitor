package kapacitor

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/pipeline"
)

type LogmeNode struct {
	node

	key    string
	level  string
	prefix string
	buf    bytes.Buffer
	enc    *json.Encoder

	batchBuffer *edge.BatchBuffer
}

// Create a new LogmeNode which logs all data it receives
func newLogmeNode(et *ExecutingTask, n *pipeline.LogmeNode, d NodeDiagnostic) (*LogmeNode, error) {
	nn := &LogmeNode{
		node:        node{Node: n, et: et, diag: d},
		level:       strings.ToUpper(n.Level),
		prefix:      n.Prefix,
		batchBuffer: new(edge.BatchBuffer),
	}
	nn.enc = json.NewEncoder(&nn.buf)
	nn.node.runF = nn.runLogme
	return nn, nil
}

func (n *LogmeNode) runLogme([]byte) error {
	consumer := edge.NewConsumerWithReceiver(
		n.ins[0],
		edge.NewReceiverFromForwardReceiverWithStats(
			n.outs,
			edge.NewTimedForwardReceiver(n.timer, n),
		),
	)
	return consumer.Consume()

}

func (n *LogmeNode) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	return nil, n.batchBuffer.BeginBatch(begin)
}

func (n *LogmeNode) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	return nil, n.batchBuffer.BatchPoint(bp)
}

func (n *LogmeNode) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return n.BufferedBatch(n.batchBuffer.BufferedBatchMessage(end))
}

func (n *LogmeNode) BufferedBatch(batch edge.BufferedBatchMessage) (edge.Message, error) {
	n.diag.LogBatchData(n.level, n.prefix, batch)
	return batch, nil
}

func (n *LogmeNode) Point(p edge.PointMessage) (edge.Message, error) {
	n.diag.LogPointData(n.level, n.prefix, p)
	return p, nil
}

func (n *LogmeNode) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}
func (n *LogmeNode) DeleteGroup(d edge.DeleteGroupMessage) (edge.Message, error) {
	return d, nil
}
