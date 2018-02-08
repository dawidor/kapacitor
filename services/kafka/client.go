package kafka

import (
	//"fmt"
	"time"
	//"bytes"
	//"strconv"
	//"encoding/json"
	//"github.com/influxdata/kapacitor/tlsconfig"

	"fmt"
	"github.com/Shopify/sarama"
	//"go/token"
	"strconv"
//	"bytes"
	"github.com/influxdata/kapacitor/alert"
	//goavro "gopkg.in/linkedin/goavro.v1"
	//"bytes"
)

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


// Client describes an immutable Kafka client
type Client interface {
	Connect() error
	Disconnect()
	Publish(topic string, retained bool, message string, data alert.EventData) error
}
/*

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



*/
// newClient produces a disconnected MQTT client
var newClient = func(c Config) (*KafkaClient, error) {

	fmt.Println("--------------------------------------------------1")

	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll

	if c.ClientID != "" {
		config.ClientID = c.ClientID
	} else {
		config.ClientID = c.Name
	}

	//opts.AddBroker(c.URL)
	//opts.SetUsername(c.Username)
	//opts.SetPassword(c.Password)

	//tlsConfig, err := tlsconfig.Create(c.SSLCA, c.SSLCert, c.SSLKey, c.InsecureSkipVerify)
	//if err != nil {
	//	return nil, err
	//}

	//opts.SetTLSConfig(tlsConfig)

	return &KafkaClient{
		config: config,
	}, nil

	return nil, nil
}

type KafkaClient struct {
	//opts   *pahomqtt.ClientOptions
	//client pahomqtt.Client
	config * sarama.Config
	producer sarama.AsyncProducer
}

// DefaultQuiesceTimeout is the duration the client will wait for outstanding
// messages to be published before forcing a disconnection
const DefaultQuiesceTimeout time.Duration = 250 * time.Millisecond

func (k *KafkaClient) Connect() error {
	brokers := []string{"godzilla-kafka-1.uat.iggroup.local:9092"}
	producer, err := sarama.NewAsyncProducer(brokers, k.config)
	k.producer = producer
	if err != nil {
		panic(err)
	}
	fmt.Println("--------------------------------------------------6s")

	return err
}

func (p *KafkaClient) Disconnect() {
	fmt.Println("--------------------------------------------------3")
	if err := p.producer.Close(); err != nil {
		panic(err)
	}
}

func (p *KafkaClient) Publish(topic string, retained bool, message string, data alert.EventData) error {
	fmt.Println("--------------------------------------------------4s")



	//j, err := json.Marshal(mainMap)
	//fmt.Printf(string(j), err)
	//fmt.Println("")

	//someRecord, err := goavro.NewRecord(goavro.RecordSchema(recordSchemaJSON))
	//if err != nil {
	//	panic(err)
	//}
	//
	//someRecord.Set("database", data.Database())
	//someRecord.Set("retention", v.RetentionPolicy())
	//someRecord.Set("name", v.Name())
	//someRecord.Set("fields", fieldsMap)
	//someRecord.Set("tags", tagsMapNew)
	//someRecord.Set("timestamp", timestamp)
	//someRecord.Set("type", v.Type().String())
	//
	//codec, err := goavro.NewCodec(recordSchemaJSON)
	//if err != nil {
	//	panic(err)
	//}
	//
	//bb := new(bytes.Buffer)
	//if err = codec.Encode(bb, someRecord); err != nil {
	//	panic(err)
	//}
	//
	////actual := bb.Bytes()
	//actual := message
	//
	//dataString := string(actual)


	strTime := strconv.Itoa(int(time.Now().Unix()))

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(strTime),
		Value: sarama.StringEncoder("adsdas"),
	}

	p.producer.Input() <- msg

	//TODO: fix it
	return nil

	//fmt.Println("=============================================")
	//return nil
}
