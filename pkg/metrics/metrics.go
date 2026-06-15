package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sagar/streamforge/pkg/log"
)

var (
	// Broker Metrics
	BrokerMessagesProduced = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "streamforge_broker_messages_produced_total",
			Help: "Total number of messages produced to a broker partition",
		},
		[]string{"topic", "partition"},
	)

	BrokerProduceDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "streamforge_broker_produce_duration_seconds",
			Help:    "Histogram of produce request durations",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"topic", "partition"},
	)

	BrokerMessagesFetched = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "streamforge_broker_messages_fetched_total",
			Help: "Total number of messages fetched from a broker partition",
		},
		[]string{"topic", "partition"},
	)

	BrokerPartitionLatestOffset = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "streamforge_broker_partition_latest_offset",
			Help: "Latest offset available in a partition",
		},
		[]string{"topic", "partition"},
	)

	// Raft Metrics
	RaftElectionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "streamforge_raft_elections_total",
			Help: "Total number of raft elections started",
		},
		[]string{"node_id"},
	)

	RaftTermCurrent = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "streamforge_raft_term_current",
			Help: "Current raft term",
		},
		[]string{"node_id"},
	)

	RaftState = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "streamforge_raft_state",
			Help: "Current raft state (0=Follower, 1=Candidate, 2=Leader)",
		},
		[]string{"node_id", "state"},
	)

	RaftHeartbeatsSent = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "streamforge_raft_heartbeats_sent_total",
			Help: "Total number of raft heartbeats sent",
		},
		[]string{"node_id"},
	)

	// Processor Metrics
	ProcessorEventsProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "streamforge_processor_events_processed_total",
			Help: "Total number of events processed by a partition",
		},
		[]string{"partition"},
	)

	ProcessorPartitionLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "streamforge_processor_partition_lag",
			Help: "Approximate consumer lag for a partition",
		},
		[]string{"partition"},
	)

	ProcessorCheckpointDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "streamforge_processor_checkpoint_duration_seconds",
			Help:    "Histogram of checkpoint durations",
			Buckets: prometheus.DefBuckets,
		},
	)

	ProcessorCheckpointsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "streamforge_processor_checkpoints_total",
			Help: "Total number of checkpoints completed",
		},
	)

	// Speculative Metrics
	SpeculativeLaunchedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "streamforge_speculative_launched_total",
			Help: "Total number of speculative tasks launched",
		},
		[]string{"partition"},
	)

	SpeculativeWinsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "streamforge_speculative_wins_total",
			Help: "Total number of speculative tasks that won the race",
		},
		[]string{"partition"},
	)

	SpeculativeDiscardedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "streamforge_speculative_discarded_total",
			Help: "Total number of speculative tasks (or originals) discarded due to tombstone",
		},
		[]string{"partition"},
	)

	// Loadgen Metrics
	LoadgenEventsSent = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "streamforge_loadgen_events_sent_total",
			Help: "Total number of events sent by load generator",
		},
		[]string{"topic"},
	)
)

// StartMetricsServer starts an HTTP server exposing Prometheus metrics on the given address
func StartMetricsServer(addr string) {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.WithComponent("metrics").Info("Starting metrics server", "address", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.WithComponent("metrics").Error("Metrics server failed", "error", err)
		}
	}()
}
