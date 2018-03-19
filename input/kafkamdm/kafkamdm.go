package kafkamdm

import (
	"flag"
	//"fmt"
	//"strconv"
	"strings"
	"sync"
	//"sync/atomic"
	"time"

	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"

	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/input"
	"github.com/grafana/metrictank/kafka"
	"github.com/grafana/metrictank/stats"
	"gopkg.in/raintank/schema.v1"
)

// metric input.kafka-mdm.metrics_per_message is how many metrics per message were seen.
var metricsPerMessage = stats.NewMeter32("input.kafka-mdm.metrics_per_message", false)

// metric input.kafka-mdm.metrics_decode_err is a count of times an input message failed to parse
var metricsDecodeErr = stats.NewCounter32("input.kafka-mdm.metrics_decode_err")

type KafkaMdm struct {
	input.Handler
	consumer *kafka.Consumer
	wg       sync.WaitGroup

	// signal to PartitionConsumers to shutdown
	stopConsuming chan struct{}
	// signal to caller that it should shutdown
	fatal chan struct{}
}

func (k *KafkaMdm) Name() string {
	return "kafka-mdm"
}

var DataDir string
var Enabled bool
var LogLevel int
var batchNumMessages int
var brokerStr string
var bufferMaxMs int
var channelBufferSize int
var currentOffsets map[string]map[int32]*int64
var fetchMin int
var maxWaitMs int
var metadataBackoffTime int
var metadataRetries int
var metadataTimeout int
var netMaxOpenRequests int
var offsetCommitInterval time.Duration
var offsetDuration time.Duration
var offsetStr string
var partitionLag map[int32]*stats.Gauge64
var partitionLogSize map[int32]*stats.Gauge64
var partitionOffset map[int32]*stats.Gauge64
var partitionStr string
var partitions []int32
var sessionTimeout int
var topicStr string

//var topics []string

func ConfigSetup() {
	inKafkaMdm := flag.NewFlagSet("kafka-mdm-in", flag.ExitOnError)
	inKafkaMdm.BoolVar(&Enabled, "enabled", false, "")
	inKafkaMdm.DurationVar(&offsetCommitInterval, "offset-commit-interval", time.Second*5, "Interval at which offsets should be saved.")
	inKafkaMdm.IntVar(&batchNumMessages, "batch-num-messages", 10000, "Maximum number of messages batched in one MessageSet")
	inKafkaMdm.IntVar(&bufferMaxMs, "metrics-buffer-max-ms", 100, "Delay in milliseconds to wait for messages in the producer queue to accumulate before constructing message batches (MessageSets) to transmit to brokers")
	inKafkaMdm.IntVar(&channelBufferSize, "channel-buffer-size", 1000000, "Maximum number of messages allowed on the producer queue")
	inKafkaMdm.IntVar(&fetchMin, "consumer-fetch-min", 1, "Minimum number of bytes the broker responds with. If fetch.wait.max.ms expires the accumulated data will be sent to the client regardless of this setting")
	inKafkaMdm.IntVar(&maxWaitMs, "consumer-max-wait-ms", 100, "Maximum time the broker may wait to fill the response with fetch.min.bytes")
	inKafkaMdm.IntVar(&metadataBackoffTime, "metadata-backoff-time", 500, "Time to wait between attempts to fetch metadata in ms")
	inKafkaMdm.IntVar(&metadataRetries, "metadata-retries", 5, "Number of retries to fetch metadata in case of failure")
	inKafkaMdm.IntVar(&metadataTimeout, "consumer-metadata-timeout-ms", 10000, "Maximum time to wait for the broker to reply to metadata queries in ms")
	inKafkaMdm.IntVar(&netMaxOpenRequests, "net-max-open-requests", 100, "Maximum number of in-flight requests per broker connection. This is a generic property applied to all broker communication, however it is primarily relevant to produce requests.")
	inKafkaMdm.IntVar(&sessionTimeout, "consumer-session-timeout", 30000, "Client group session and failure detection timeout in ms")
	inKafkaMdm.StringVar(&DataDir, "data-dir", "", "Directory to store partition offsets index")
	inKafkaMdm.StringVar(&brokerStr, "brokers", "kafka:9092", "tcp address for kafka (may be be given multiple times as a comma-separated list)")
	inKafkaMdm.StringVar(&offsetStr, "offset", "oldest", "Set the offset to start consuming from. Can be one of newest, oldest or a time duration")
	inKafkaMdm.StringVar(&partitionStr, "partitions", "*", "kafka partitions to consume. use '*' or a comma separated list of id's")
	inKafkaMdm.StringVar(&topicStr, "topics", "mdm", "kafka topic (may be given multiple times as a comma-separated list)")
	globalconf.Register("kafka-mdm-in", inKafkaMdm)
}

func ConfigProcess(instance string) {
	if !Enabled {
		return
	}

	if offsetCommitInterval == 0 {
		log.Fatal(4, "kafkamdm: offset-commit-interval must be greater then 0")
	}

	if maxWaitMs == 0 {
		log.Fatal(4, "kafkamdm: consumer-max-wait-time must be greater then 0")
	}

	// record our partitions so others (MetricIdx) can use the partitioning information.
	// but only if the manager has been created (e.g. in metrictank), not when this input plugin is used in other contexts
	if cluster.Manager != nil {
		cluster.Manager.SetPartitions(partitions)
	}
}

func New() *KafkaMdm {
	log.Info("kafka-mdm consumer created without error")
	k := KafkaMdm{
		stopConsuming: make(chan struct{}),
	}

	conf := kafka.NewConfig()
	conf.GaugePrefix = "input.kafka-mdm.partition"
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
	conf.Topics = strings.Split(topicStr, ",")
	conf.Broker = brokerStr
	conf.Partitions = partitionStr
	conf.StartAtOffset = offsetStr
	conf.Handler = k.handleMsg

	consumer, err := kafka.NewConsumer(conf)
	if err != nil {
		log.Fatal(2, "kafka-cluster failed to initialize consumer: %s", err)
	}
	consumer.InitLagMonitor(10)

	k.consumer = consumer

	return &k
}

func (k *KafkaMdm) Start(handler input.Handler, fatal chan struct{}) error {
	k.Handler = handler
	k.fatal = fatal

	k.consumer.Start()

	return nil
}

func (k *KafkaMdm) handleMsg(data []byte, partition int32) {
	md := schema.MetricData{}
	_, err := md.UnmarshalMsg(data)
	if err != nil {
		metricsDecodeErr.Inc()
		log.Error(3, "kafka-mdm decode error, skipping message. %s", err)
		return
	}
	metricsPerMessage.ValueUint32(1)
	k.Handler.Process(&md, partition)
}

// Stop will initiate a graceful stop of the Consumer (permanent)
// and block until it stopped.
func (k *KafkaMdm) Stop() {
	log.Info("kafka-mdm: stopping kafka input")
	/*close(k.stopConsuming)
	k.wg.Wait()
	k.consumer.Close()*/
}

func (k *KafkaMdm) MaintainPriority() {
	go func() {
		ticker := time.NewTicker(time.Second * 10)
		for {
			select {
			case <-k.stopConsuming:
				return
			case <-ticker.C:
				cluster.Manager.SetPriority(k.consumer.LagMonitor.Metric())
			}
		}
	}()
}
