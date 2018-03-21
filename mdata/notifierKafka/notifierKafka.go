package notifierKafka

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	//"sync/atomic"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/kafka"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/util"
	"github.com/raintank/worldping-api/pkg/log"
)

type NotifierKafka struct {
	instance string
	in       chan mdata.SavedChunk
	buf      []mdata.SavedChunk
	wg       sync.WaitGroup
	bPool    *util.BufferPool
	idx      idx.MetricIndex
	metrics  mdata.Metrics
	consumer *kafka.Consumer
	producer *confluent.Producer
	StopChan chan int

	// signal to PartitionConsumers to shutdown
	stopConsuming chan struct{}
}

func New(instance string, metrics mdata.Metrics, idx idx.MetricIndex) *NotifierKafka {
	producer, err := confluent.NewProducer(kafka.GetConfig(brokerStr, "snappy", batchNumMessages, bufferMaxMs, channelBufferSize, fetchMin, netMaxOpenRequests, maxWaitMs, sessionTimeout))
	if err != nil {
		log.Fatal(2, "kafka-cluster failed to initialize producer: %s", err)
	}

	c := NotifierKafka{
		instance: instance,
		in:       make(chan mdata.SavedChunk),
		bPool:    util.NewBufferPool(),
		producer: producer,
		metrics:  metrics,
		idx:      idx,

		StopChan:      make(chan int),
		stopConsuming: make(chan struct{}),
	}

	conf := kafka.NewConfig()
	conf.GaugePrefix = "cluster.notifier.kafka.partition"
	conf.BatchNumMessages = batchNumMessages
	conf.BufferMaxMs = bufferMaxMs
	conf.ChannelBufferSize = channelBufferSize
	conf.FetchMin = fetchMin
	conf.NetMaxOpenRequests = netMaxOpenRequests
	conf.MaxWaitMs = maxWaitMs
	conf.SessionTimeout = sessionTimeout
	conf.MetadataRetries = metadataRetries
	conf.MetadataBackoffTime = metadataBackoffTime
	conf.MetadataTimeout = metadataTimeout
	conf.OffsetCommitInterval = offsetCommitInterval
	conf.Topics = []string{topic}
	conf.Broker = brokerStr
	conf.Partitions = partitionStr
	conf.StartAtOffset = offsetStr
	conf.Handler = c.handleMessage

	consumer, err := kafka.NewConsumer(conf)
	if err != nil {
		log.Fatal(2, "kafka-cluster failed to initialize consumer: %s", err)
	}

	c.consumer = consumer
	err = c.consumer.StartAndAwaitBacklog(backlogProcessTimeout)
	if err != nil {
		log.Fatal(4, "kafka-cluster: Failed to start consumer: %s", err)
	}

	go c.produce()

	return &c
}

func (c *NotifierKafka) handleMessage(data []byte, partition int32) {
	mdata.Handle(c.metrics, data, c.idx)
}

// Stop will initiate a graceful stop of the Consumer (permanent)
//
// NOTE: receive on StopChan to block until this process completes
func (c *NotifierKafka) Stop() {
	// closes notifications and messages channels, amongst others
	close(c.stopConsuming)
	c.producer.Close()

	go func() {
		c.wg.Wait()
		close(c.StopChan)
	}()
}

func (c *NotifierKafka) Send(sc mdata.SavedChunk) {
	c.in <- sc
}

func (c *NotifierKafka) produce() {
	ticker := time.NewTicker(time.Second)
	max := 5000
	for {
		select {
		case chunk := <-c.in:
			c.buf = append(c.buf, chunk)
			if len(c.buf) == max {
				c.flush()
			}
		case <-ticker.C:
			c.flush()
		}
	}
}

// flush makes sure the batch gets sent, asynchronously.
func (c *NotifierKafka) flush() {
	if len(c.buf) == 0 {
		return
	}

	hasher := fnv.New32a()

	// In order to correctly route the saveMessages to the correct partition,
	// we cant send them in batches anymore.
	payload := make([]*confluent.Message, 0, len(c.buf))
	var pMsg mdata.PersistMessageBatch
	for i, msg := range c.buf {
		def, ok := c.idx.Get(strings.SplitN(msg.Key, "_", 2)[0])
		if !ok {
			log.Error(3, "kafka-cluster: failed to lookup metricDef with id %s", msg.Key)
			continue
		}
		buf := bytes.NewBuffer(c.bPool.Get())
		binary.Write(buf, binary.LittleEndian, uint8(mdata.PersistMessageBatchV1))
		encoder := json.NewEncoder(buf)
		pMsg = mdata.PersistMessageBatch{Instance: c.instance, SavedChunks: c.buf[i : i+1]}
		err := encoder.Encode(&pMsg)
		if err != nil {
			log.Fatal(4, "kafka-cluster failed to marshal persistMessage to json.")
		}
		messagesSize.Value(buf.Len())
		key := c.bPool.Get()
		key, err = partitioner.GetPartitionKey(&def, key)
		if err != nil {
			log.Fatal(4, "Unable to get partitionKey for metricDef with id %s. %s", def.Id, err)
		}

		hasher.Reset()
		_, err = hasher.Write(key)
		partition := int32(hasher.Sum32()) % int32(len(partitions))
		if partition < 0 {
			partition = -partition
		}

		kafkaMsg := &confluent.Message{
			TopicPartition: confluent.TopicPartition{
				Topic: &topic, Partition: partition,
			},
			Value: []byte(buf.Bytes()),
			Key:   []byte(key),
		}
		payload = append(payload, kafkaMsg)
	}

	c.buf = nil

	go func() {
		log.Debug("kafka-cluster sending %d batch metricPersist messages", len(payload))
		producerCh := c.producer.ProduceChannel()
		for _, msg := range payload {
			producerCh <- msg
		}
		sent := 0

	EVENTS:
		for e := range c.producer.Events() {
			switch ev := e.(type) {
			case *confluent.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
					time.Sleep(time.Second)
					ev.TopicPartition.Error = nil
					producerCh <- ev
				} else {
					sent++
				}
				if sent == len(payload) {
					break EVENTS
					return
				}
			default:
				fmt.Printf("Ignored unexpected event: %s\n", ev)
			}
		}

		messagesPublished.Add(sent)

		// put our buffers back in the bufferPool
		for _, msg := range payload {
			c.bPool.Put(msg.Key)
			c.bPool.Put(msg.Value)
		}
	}()
}
