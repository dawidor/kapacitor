package kafka

import (
	//"fmt"
	//"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/influxdata/kapacitor/tlsconfig"
	"strconv"
	"time"

	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/influxdata/kapacitor/alert"
	goavro "gopkg.in/linkedin/goavro.v1"
	"io/ioutil"
	"log"
)

//
//{
//"doc": "fields",
//"type": "map",
//"name": "fields",
//"values":"double"
//},
//{
//"doc": "tags",
//"type": "map",
//"name": "tags",
//"values":"string"
//},

var recordSchemaJSON = `
	{
	  "type": "record",
	  "name": "triggerevents",
	  "doc:": "A basic schema for event trigger",
	  "namespace": "com.avro.kapacitor.kafka",
	  "fields": [
		{
		  "doc": "metric name",
		  "type": "string",
		  "name": "name"
		},
		{
		  "doc": "tick script name",
		  "type": "string",
		  "name": "taskname",
 		  "default": "null"
		},
		{
		  "doc": "Unix epoch time in milliseconds",
		  "type": "string",
		  "name": "timestamp"
		},
		{
		  "doc": "event level",
		  "type": "string",
		  "name": "level",
 		  "default": "null"
		},
		{
		  "doc": "event duration",
		  "type": "string",
		  "name": "duration",
 		  "default": "null"
		},
		{
		  "doc": "event id",
		  "type": "string",
		  "name": "id",
 		  "default": "null"
		},
		{
		  "doc": "tags",
		  "type": "map",
		  "name": "tags",
		  "values":"string"
		},
		{
  		  "doc": "fields",
		  "type": "map",
		  "name": "fields",
		  "values":"double"
		}
	  ]
	}
`

// Client describes an immutable Kafka client
type Client interface {
	Connect(Url string) error
	Disconnect()
	Publish(topic string, state alert.EventState, data alert.EventData) error
}

// newClient produces a disconnected Kafka client
var newClient = func(c Config) (*KafkaClient, error) {
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll

	if c.ClientID != "" {
		config.ClientID = c.ClientID
	} else {
		config.ClientID = c.Name
	}

	if c.Username != "" {
		config.Net.SASL.User = c.Username
	}
	if c.Password != "" {
		config.Net.SASL.Password = c.Password
	}

	tlsConfig, err := tlsconfig.Create(c.SSLCA, c.SSLCert, c.SSLKey, c.InsecureSkipVerify)
	if err != nil {
		return nil, err
	}

	if tlsConfig != nil {
		config.Net.TLS.Config = tlsConfig
	}

	return &KafkaClient{
		config: config,
	}, nil

	return nil, nil
}

func createTlsConfiguration(c Config) (t *tls.Config) {

	if c.SSLCert != "" && c.SSLKey != "" && c.SSLCA != "" {
		cert, err := tls.LoadX509KeyPair(c.SSLCert, c.SSLKey)
		if err != nil {
			log.Fatal(err)
		}

		caCert, err := ioutil.ReadFile(c.SSLCA)
		if err != nil {
			log.Fatal(err)
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		t = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			InsecureSkipVerify: c.InsecureSkipVerify,
		}
	}
	// will be nil by default if nothing is provided
	return t
}

type KafkaClient struct {
	config   *sarama.Config
	producer sarama.AsyncProducer
}

func (k *KafkaClient) Connect(Url string) error {

	brokers := []string{Url}
	producer, err := sarama.NewAsyncProducer(brokers, k.config)
	k.producer = producer
	if err != nil {
		panic(err)
	}

	return err
}

func (p *KafkaClient) Disconnect() {
	if err := p.producer.Close(); err != nil {
		panic(err)
	}
}

func (p *KafkaClient) Publish(topic string, state alert.EventState, data alert.EventData) error {

	//j, err := json.Marshal(mainMap)
	//fmt.Printf(string(j), err)
	//fmt.Println("")

	someRecord, err := goavro.NewRecord(goavro.RecordSchema(recordSchemaJSON))
	if err != nil {
		panic(err)
	}

	timestamp := strconv.FormatInt(state.Time.UTC().UnixNano(), 10)

	var tagsMapNew map[string]interface{} = make(map[string]interface{})
	var tagsMap map[string]string = data.Tags
	for key, value := range tagsMap {
		tagsMapNew[key] = value
	}

	var fieldsMap map[string]interface{} = data.Fields
	someRecord.Set("name", data.Name)
	someRecord.Set("taskname", data.TaskName)
	someRecord.Set("message", state.Message)
	someRecord.Set("timestamp", timestamp)
	someRecord.Set("level", state.Level.String())
	someRecord.Set("duration", state.Duration.String())
	someRecord.Set("id", state.ID)
	someRecord.Set("tags", tagsMapNew )
	someRecord.Set("fields", fieldsMap)

	codec, err := goavro.NewCodec(recordSchemaJSON)
	if err != nil {
		panic(err)
	}

	bb := new(bytes.Buffer)
	fmt.Println(bb)
	if err = codec.Encode(bb, someRecord); err != nil {
		panic(err)
	}

	actual := bb.Bytes()
	dataString := string(actual)

	strTime := strconv.Itoa(int(time.Now().Unix()))

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(strTime),
		Value: sarama.StringEncoder(dataString),
	}

	p.producer.Input() <- msg

	return err
}
