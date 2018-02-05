package kapacitor

import (
	"github.com/influxdb/kapacitor/pipeline"
)

type KafkaOutNode struct {
	// Include the generic node implementation
	node
	// Keep a reference to the pipeline node
	h    *pipeline.KafkaOutNode
}