package notifierKafka

import (
	"flag"
	//"fmt"
	//"strconv"
	//"strings"
	"time"

	//confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	part "github.com/grafana/metrictank/cluster/partitioner"
	//"github.com/grafana/metrictank/kafka"
	"github.com/grafana/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
)

var Enabled bool
var backlogProcessTimeout time.Duration
var backlogProcessTimeoutStr string
var batchNumMessages int
var bootTimeOffsets map[int32]int64
var brokerStr string
var bufferMaxMs int
var channelBufferSize int
var dataDir string
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
var partitionScheme string
var partitionStr string
var partitioner *part.Kafka
var partitions []int32
var sessionTimeout int
var topic string

// metric cluster.notifier.kafka.messages-published is a counter of messages published to the kafka cluster notifier
var messagesPublished = stats.NewCounter32("cluster.notifier.kafka.messages-published")

// metric cluster.notifier.kafka.message_size is the sizes seen of messages through the kafka cluster notifier
var messagesSize = stats.NewMeter32("cluster.notifier.kafka.message_size", false)

func init() {
	fs := flag.NewFlagSet("kafka-cluster", flag.ExitOnError)
	fs.BoolVar(&Enabled, "enabled", false, "")
	fs.DurationVar(&offsetCommitInterval, "offset-commit-interval", time.Second*5, "Interval at which offsets should be saved.")
	fs.IntVar(&batchNumMessages, "batch-num-messages", 10000, "Maximum number of messages batched in one MessageSet")
	fs.IntVar(&bufferMaxMs, "metrics-buffer-max-ms", 100, "Delay in milliseconds to wait for messages in the producer queue to accumulate before constructing message batches (MessageSets) to transmit to brokers")
	fs.IntVar(&channelBufferSize, "channel-buffer-size", 1000000, "Maximum number of messages allowed on the producer queue")
	fs.IntVar(&fetchMin, "consumer-fetch-min", 1, "Minimum number of bytes the broker responds with. If fetch.wait.max.ms expires the accumulated data will be sent to the client regardless of this setting")
	fs.IntVar(&maxWaitMs, "consumer-max-wait-ms", 100, "Maximum time the broker may wait to fill the response with fetch.min.bytes")
	fs.IntVar(&metadataBackoffTime, "metadata-backoff-time", 500, "Time to wait between attempts to fetch metadata in ms")
	fs.IntVar(&metadataRetries, "metadata-retries", 5, "Number of retries to fetch metadata in case of failure")
	fs.IntVar(&metadataTimeout, "consumer-metadata-timeout-ms", 10000, "Maximum time to wait for the broker to send its metadata in ms")
	fs.IntVar(&netMaxOpenRequests, "net-max-open-requests", 100, "Maximum number of in-flight requests per broker connection. This is a generic property applied to all broker communication, however it is primarily relevant to produce requests.")
	fs.IntVar(&sessionTimeout, "consumer-session-timeout", 30000, "Client group session and failure detection timeout in ms")
	fs.StringVar(&backlogProcessTimeoutStr, "backlog-process-timeout", "60s", "Maximum time backlog processing can block during metrictank startup.")
	fs.StringVar(&brokerStr, "brokers", "kafka:9092", "tcp address for kafka (may be given multiple times as comma separated list)")
	fs.StringVar(&dataDir, "data-dir", "", "Directory to store partition offsets index")
	fs.StringVar(&offsetStr, "offset", "oldest", "Set the offset to start consuming from. Can be one of newest, oldest or a time duration")
	fs.StringVar(&partitionScheme, "partition-scheme", "bySeries", "method used for partitioning metrics. This should match the settings of tsdb-gw. (byOrg|bySeries)")
	fs.StringVar(&partitionStr, "partitions", "*", "kafka partitions to consume. use '*' or a comma separated list of id's. This should match the partitions used for kafka-mdm-in")
	fs.StringVar(&topic, "topic", "metricpersist", "kafka topic")
	globalconf.Register("kafka-cluster", fs)
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

	var err error
	backlogProcessTimeout, err = time.ParseDuration(backlogProcessTimeoutStr)
	if err != nil {
		log.Fatal(4, "kafka-cluster: unable to parse backlog-process-timeout. %s", err)
	}

	partitioner, err = part.NewKafka(partitionScheme)
	if err != nil {
		log.Fatal(4, "kafka-cluster: failed to initialize partitioner. %s", err)
	}
}
