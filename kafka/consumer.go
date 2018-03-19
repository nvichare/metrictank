package kafka

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/twinj/uuid"
)

type Consumer struct {
	conf             ConsumerConf
	wg               sync.WaitGroup
	consumer         *confluent.Consumer
	partitions       []int32
	currentOffsets   map[int32]*int64
	bootTimeOffsets  map[int32]int64
	partitionOffset  map[int32]*stats.Gauge64
	partitionLogSize map[int32]*stats.Gauge64
	partitionLag     map[int32]*stats.Gauge64
	LagMonitor       *LagMonitor
	StopChan         chan int
	// signal consumers to shutdown
	stopConsuming chan struct{}
}

type ConsumerConf struct {
	Broker               string
	Partitions           string
	StartAtOffset        string
	GaugePrefix          string
	Topics               []string
	Handler              func([]byte, int32)
	BatchNumMessages     int
	BufferMaxMs          int
	ChannelBufferSize    int
	FetchMin             int
	NetMaxOpenRequests   int
	MaxWaitMs            int
	SessionTimeout       int
	MetadataRetries      int
	MetadataBackoffTime  int
	MetadataTimeout      int
	OffsetCommitInterval time.Duration
}

func NewConfig() *ConsumerConf {
	return &ConsumerConf{
		GaugePrefix:          "default.kafka.partition",
		BatchNumMessages:     10000,
		BufferMaxMs:          100,
		ChannelBufferSize:    1000000,
		FetchMin:             1,
		NetMaxOpenRequests:   100,
		MaxWaitMs:            100,
		SessionTimeout:       30000,
		MetadataRetries:      5,
		MetadataBackoffTime:  500,
		MetadataTimeout:      10000,
		OffsetCommitInterval: time.Second * 5,
	}
}

func NewConsumer(conf *ConsumerConf) (*Consumer, error) {
	if len(conf.Topics) < 1 {
		return nil, fmt.Errorf("kafka-consumer: Requiring at least 1 topic")
	}

	consumer, err := confluent.NewConsumer(&confluent.ConfigMap{
		"bootstrap.servers":                     conf.Broker,
		"compression.codec":                     "snappy",
		"group.id":                              uuid.NewV4().String(),
		"fetch.min.bytes":                       conf.FetchMin,
		"fetch.wait.max.ms":                     conf.MaxWaitMs,
		"max.in.flight.requests.per.connection": conf.NetMaxOpenRequests,
		"queue.buffering.max.messages":          conf.ChannelBufferSize,
		"retries":                               10,
		"session.timeout.ms":                    conf.SessionTimeout,
		"queue.buffering.max.ms":                conf.BufferMaxMs,
		"batch.num.messages":                    conf.BatchNumMessages,
		"enable.auto.offset.store":              false,
		"enable.auto.commit":                    false,
		"go.events.channel.enable":              true,
		"go.application.rebalance.enable":       true,
	})
	if err != nil {
		return nil, err
	}

	c := Consumer{
		conf:             *conf,
		consumer:         consumer,
		currentOffsets:   make(map[int32]*int64),
		bootTimeOffsets:  make(map[int32]int64),
		partitionOffset:  make(map[int32]*stats.Gauge64),
		partitionLogSize: make(map[int32]*stats.Gauge64),
		partitionLag:     make(map[int32]*stats.Gauge64),
	}

	availParts, err := GetPartitions(c.consumer, c.conf.Topics, c.conf.MetadataRetries, c.conf.MetadataBackoffTime, c.conf.MetadataTimeout)
	if err != nil {
		return nil, err
	}

	log.Info("kafka-mdm: available partitions %v", availParts)
	if c.conf.Partitions == "*" {
		c.partitions = availParts
	} else {
		parts := strings.Split(c.conf.Partitions, ",")
		for _, part := range parts {
			i, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("Could not parse partition %q. partitions must be '*' or a comma separated list of id's", part)
			}
			c.partitions = append(c.partitions, int32(i))
		}
		missing := DiffPartitions(c.partitions, availParts)
		if len(missing) > 0 {
			return nil, fmt.Errorf("Configured partitions not in list of available partitions. Missing %v", missing)
		}
	}

	for _, part := range c.partitions {
		_, offset, err := c.consumer.QueryWatermarkOffsets(c.conf.Topics[0], part, c.conf.MetadataTimeout)
		if err != nil {
			return nil, fmt.Errorf("Failed to get newest offset for topic %s part %d: %s", c.conf.Topics[0], part, err)
		}
		c.bootTimeOffsets[part] = offset
		c.partitionOffset[part] = stats.NewGauge64(fmt.Sprintf("%s.%d.offset", c.conf.GaugePrefix, part))
		c.partitionLogSize[part] = stats.NewGauge64(fmt.Sprintf("%s.%d.log_size", c.conf.GaugePrefix, part))
		c.partitionLag[part] = stats.NewGauge64(fmt.Sprintf("%s.%d.lag", c.conf.GaugePrefix, part))
	}

	return &c, nil
}

func (c *Consumer) InitLagMonitor(size int) {
	c.LagMonitor = NewLagMonitor(10, c.partitions)
}

func (c *Consumer) Start() error {
	err := c.startConsumer()
	if err != nil {
		return fmt.Errorf("Failed to start consumer: %s", err)
	}
	go c.monitorLag(nil)

	for _ = range c.partitions {
		go c.consume()
	}

	return nil
}

func (c *Consumer) StartAndAwaitBacklog(backlogProcessTimeout time.Duration) error {
	pre := time.Now()
	processBacklog := new(sync.WaitGroup)

	err := c.startConsumer()
	if err != nil {
		return fmt.Errorf("Failed to start consumer: %s", err)
	}
	processBacklog.Add(len(c.partitions))

	// wait for our backlog to be processed before returning.  This will block metrictank from consuming metrics until
	// we have processed old metricPersist messages. The end result is that we wont overwrite chunks in cassandra that
	// have already been previously written.
	// We don't wait more than backlogProcessTimeout for the backlog to be processed.
	log.Info("kafka-consumer: waiting for metricPersist backlog to be processed.")
	backlogProcessed := make(chan struct{}, 1)
	go func() {
		processBacklog.Wait()
		backlogProcessed <- struct{}{}
	}()

	go c.monitorLag(processBacklog)

	for _ = range c.partitions {
		go c.consume()
	}

	select {
	case <-time.After(backlogProcessTimeout):
		log.Warn("kafka-consumer: Processing metricPersist backlog has taken too long, giving up lock after %s.", backlogProcessTimeout)
	case <-backlogProcessed:
		log.Info("kafka-consumer: metricPersist backlog processed in %s.", time.Since(pre))
	}

	return nil
}

func (c *Consumer) consume() {
	c.wg.Add(1)
	defer c.wg.Done()

	var ok bool
	var offsetPtr *int64
	events := c.consumer.Events()
	for {
		select {
		case ev := <-events:
			switch e := ev.(type) {
			case confluent.AssignedPartitions:
				c.consumer.Assign(e.Partitions)
				log.Info("notifier-kafka: Assigned partitions: %+v", e)
			case confluent.RevokedPartitions:
				c.consumer.Unassign()
				log.Info("notifier-kafka: Revoked partitions: %+v", e)
			case confluent.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case *confluent.Message:
				tp := e.TopicPartition
				if mdata.LogLevel < 2 {
					log.Debug("notifier-kafka: received message: Topic %s, Partition: %d, Offset: %d, Key: %x", tp.Topic, tp.Partition, tp.Offset, e.Key)
				}

				if offsetPtr, ok = c.currentOffsets[tp.Partition]; !ok || offsetPtr == nil {
					log.Error(3, "kafka-mdm received message of unexpected partition: %s:%d", tp.Topic, tp.Partition)
					continue
				}

				c.conf.Handler(e.Value, tp.Partition)
				atomic.StoreInt64(offsetPtr, int64(tp.Offset))
			case *confluent.Error:
				log.Error(3, "notifier-kafka: kafka consumer error: %s", e.String())
				return
			}
		case <-c.stopConsuming:
			log.Info("notifier-kafka: consumer ended.")
			return
		}
	}
}

func (c *Consumer) monitorLag(processBacklog *sync.WaitGroup) {
	completed := make(map[int32]bool, len(c.partitions))
	for _, partition := range c.partitions {
		completed[partition] = false
	}

	storeOffsets := func(ts time.Time) {
		for partition := range c.currentOffsets {
			offset := atomic.LoadInt64(c.currentOffsets[partition])
			c.partitionOffset[partition].Set(int(offset))
			if c.LagMonitor != nil {
				c.LagMonitor.StoreOffset(partition, offset, ts)
			}
			if !completed[partition] && offset >= c.bootTimeOffsets[partition]-1 {
				if processBacklog != nil {
					processBacklog.Done()
				}
				completed[partition] = true
				delete(c.bootTimeOffsets, partition)
				if len(c.bootTimeOffsets) == 0 {
					c.bootTimeOffsets = nil
				}
			}

			_, newest, err := c.consumer.QueryWatermarkOffsets(c.conf.Topics[0], partition, c.conf.MetadataTimeout)
			if err != nil {
				log.Error(3, "kafka-consumer: %s", err)
			} else {
				c.partitionLogSize[partition].Set(int(newest))
			}

			c.partitionOffset[partition].Set(int(offset))
			if err == nil {
				lag := int(newest - offset)
				c.partitionLag[partition].Set(lag)
				if c.LagMonitor != nil {
					c.LagMonitor.StoreLag(partition, lag)
				}
			}
		}
	}

	ticker := time.NewTicker(c.conf.OffsetCommitInterval)
	for {
		select {
		case ts := <-ticker.C:
			storeOffsets(ts)
		case <-c.stopConsuming:
			storeOffsets(time.Now())
			return
		}
	}
}

func (c *Consumer) startConsumer() error {
	var offset confluent.Offset
	var err error
	var topicPartitions confluent.TopicPartitions
	c.currentOffsets = make(map[int32]*int64, len(c.partitions))

	for i, topic := range c.conf.Topics {
		for _, partition := range c.partitions {
			var currentOffset int64
			switch c.conf.StartAtOffset {
			case "oldest":
				currentOffset, _, err = c.tryGetOffset(topic, partition, int64(confluent.OffsetBeginning), 3, time.Second)
				if err != nil {
					return err
				}
			case "newest":
				_, currentOffset, err = c.tryGetOffset(topic, partition, int64(confluent.OffsetEnd), 3, time.Second)
				if err != nil {
					return err
				}
			default:
				offsetDuration, err := time.ParseDuration(c.conf.StartAtOffset)
				if err != nil {
					return fmt.Errorf("invalid offest format %s: %s", c.conf.StartAtOffset, err)
				}
				currentOffset = time.Now().Add(-1*offsetDuration).UnixNano() / int64(time.Millisecond)
				currentOffset, _, err = c.tryGetOffset(topic, partition, currentOffset, 3, time.Second)
				if err != nil {
					return err
				}
			}

			offset, err = confluent.NewOffset(currentOffset)
			if err != nil {
				return err
			}

			topicPartitions = append(topicPartitions, confluent.TopicPartition{
				Topic:     &topic,
				Partition: partition,
				Offset:    offset,
			})

			if i == 0 {
				c.currentOffsets[partition] = &currentOffset
			}
		}
	}

	return c.consumer.Assign(topicPartitions)
}

func (c *Consumer) tryGetOffset(topic string, partition int32, offsetI int64, attempts int, sleep time.Duration) (int64, int64, error) {
	offset, err := confluent.NewOffset(offsetI)
	if err != nil {
		return 0, 0, err
	}

	var val1, val2 int64

	attempt := 1
	for {
		if offset == confluent.OffsetBeginning || offset == confluent.OffsetEnd {
			val1, val2, err = c.consumer.QueryWatermarkOffsets(topic, partition, c.conf.MetadataTimeout)
		} else {
			times := []confluent.TopicPartition{{Topic: &topic, Partition: partition, Offset: offset}}
			times, err = c.consumer.OffsetsForTimes(times, c.conf.MetadataTimeout)
			if err == nil {
				if len(times) == 0 {
					err = fmt.Errorf("Got 0 topics returned from broker")
				} else {
					val1 = int64(times[0].Offset)
				}
			}
		}

		if err == nil {
			return val1, val2, err
		}

		if attempt >= attempts {
			break
		}

		log.Warn("kafka-mdm %s", err)
		attempt += 1
		time.Sleep(sleep)
	}

	return 0, 0, fmt.Errorf("failed to get offset %s of partition %s:%d. %s (attempt %d/%d)", offset.String(), topic, partition, err, attempt, attempts)
}
