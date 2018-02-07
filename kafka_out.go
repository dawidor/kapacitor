package kapacitor

import (
	"fmt"
	"bytes"
	"github.com/influxdata/kapacitor/pipeline"
	//s"encoding/json"
	goavro "gopkg.in/linkedin/goavro.v1"
	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/models"
	"github.com/Shopify/sarama"
	"strconv"
	"time"
)

type KafkaOutNode struct {
	node

	topic string
	url string
}

var recordSchemaJSON = `
	{
	  "type": "record",
	  "name": "triggerevents",
	  "doc:": "A basic schema for event trigger",
	  "namespace": "com.avro.kapacitor.kafka",
	  "fields": [
		{
		  "doc": "Database name",
		  "type": "string",
		  "name": "database"
		},
		{
		  "doc": "retention name",
		  "type": "string",
		  "name": "retention",
   		  "default": "null"
		},
		{
		  "doc": "metric name",
		  "type": "string",
		  "name": "name"
		},
		{
		  "doc": "fields",
		  "type": "map",
		  "name": "fields",
          "values":"double"
		},
		{
		  "doc": "tags",
		  "type": "map",
		  "name": "tags",
		  "values":"string"
		},
		{
		  "doc": "Unix epoch time in milliseconds",
		  "type": "string",
		  "name": "timestamp"
		},
		{
		  "doc": "event type",
		  "type": "string",
		  "name": "type"
		}
	  ]
	}
`

// Create a new  NoOpNode which does nothing with the data and just passes it through.
func newKafkaOutNode(et *ExecutingTask, n *pipeline.KafkaOutNode) (*KafkaOutNode, error) {

	nn := &KafkaOutNode{
		node:        node{Node: n, et: et},
		topic: n.Topic,
		url: n.Url,
	}

	nn.node.runF = nn.runKafkaOut
	return nn, nil

}

func (n *KafkaOutNode) runKafkaOut(bb2 []byte) error {

	for m, ok := n.ins[0].Emit(); ok; m, ok = n.ins[0].Emit() {

		switch v := m.(type) {

		case edge.BufferedBatchMessage:
			fmt.Printf(v.Name())
			fmt.Printf(v.Type().String())
			//bbm := m.(edge.BufferedBatchMessage)
			//N/A

		case edge.PointMessage:
			pm := m.(edge.PointMessage)

			timestamp := strconv.FormatInt(pm.Time().UTC().UnixNano(), 10)

			var tagsMapNew map[string]interface{} = make(map[string]interface{})
			var tags models.Tags = pm.Tags()
			var tagsMap map[string]string = tags
			for key, value := range tagsMap {
				tagsMapNew[key] = value
			}

			var fieldsMap map[string]interface{} = pm.Fields()

			//j, err := json.Marshal(mainMap)
			//fmt.Printf(string(j), err)
			//fmt.Println("")

			someRecord, err := goavro.NewRecord(goavro.RecordSchema(recordSchemaJSON))
			if err != nil {
				panic(err)
			}

			someRecord.Set("database", v.Database())
			someRecord.Set("retention", v.RetentionPolicy())
			someRecord.Set("name", v.Name())
			someRecord.Set("fields", fieldsMap)
			someRecord.Set("tags", tagsMapNew)
			someRecord.Set("timestamp", timestamp)
			someRecord.Set("type", v.Type().String())

			codec, err := goavro.NewCodec(recordSchemaJSON)
			if err != nil {
				panic(err)
			}

			bb := new(bytes.Buffer)
			if err = codec.Encode(bb, someRecord); err != nil {
				panic(err)
			}

			actual := bb.Bytes()

			dataString := string(actual)

			config := sarama.NewConfig()
			config.Producer.Retry.Max = 5
			config.Producer.RequiredAcks = sarama.WaitForAll
			brokers := []string{n.url}
			producer, err := sarama.NewAsyncProducer(brokers, config)
			if err != nil {
				panic(err)
			}

			defer func() {
				if err := producer.Close(); err != nil {
					panic(err)
				}
			}()

			strTime := strconv.Itoa(int(time.Now().Unix()))

			msg := &sarama.ProducerMessage{
				Topic: n.topic,
				Key:   sarama.StringEncoder(strTime),
				Value: sarama.StringEncoder(dataString),
			}

			producer.Input() <- msg

			if err := edge.Forward(n.outs, m); err != nil {
				return err
			}


		case edge.BarrierMessage:
			fmt.Printf(v.Name())
			fmt.Printf(v.Type().String())
			//pm := m.(edge.BarrierMessage)
			//N/A

		}

	}
	return nil
}
