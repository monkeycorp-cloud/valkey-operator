package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ValkeyClusterPhase represents the current lifecycle phase of a ValkeyCluster.
type ValkeyClusterPhase string

const (
	PhasePending      ValkeyClusterPhase = "Pending"
	PhaseInitializing ValkeyClusterPhase = "Initializing"
	PhaseRunning      ValkeyClusterPhase = "Running"
	PhaseDegraded     ValkeyClusterPhase = "Degraded"
	PhaseScaling      ValkeyClusterPhase = "Scaling"
	PhaseUpdating     ValkeyClusterPhase = "Updating"
)

// ValkeyClusterSpec defines the desired state of ValkeyCluster.
type ValkeyClusterSpec struct {
	// Shards is the number of primary shards. Each shard owns a portion of the
	// 16384 hash slots. Total pods = Shards * (1 + ReplicasPerShard).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=3
	Shards int32 `json:"shards"`

	// ReplicasPerShard is the number of replica pods per primary shard.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=1
	ReplicasPerShard int32 `json:"replicasPerShard"`

	// Image is the container image for Valkey.
	// Must be built from module/Dockerfile — the operator module is required.
	// +kubebuilder:default="ghcr.io/geoffrey/valkey-operator-server:9.0.3-trixie"
	// +kubebuilder:validation:Required
	Image string `json:"image"`

	// Resources defines the resource requirements for each Valkey pod.
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Storage defines the persistent volume configuration.
	Storage ValkeyStorageSpec `json:"storage,omitempty"`

	// Port is the port Valkey listens on.
	// +kubebuilder:default=6379
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port,omitempty"`

	// OperatorSecret references a Secret containing the password for the internal
	// operator account. This account has full privileges and is used exclusively
	// by the operator for cluster management (CLUSTER MEET, CLUSTER FAILOVER, ACL…).
	// The Secret must contain a key named "password".
	// +kubebuilder:validation:Required
	OperatorSecret corev1.SecretKeySelector `json:"operatorSecret"`

	// ACLUsers defines additional Valkey ACL accounts for application clients.
	// The default account is disabled — applications must use one of these named accounts.
	// +kubebuilder:validation:Optional
	ACLUsers []ValkeyACLUser `json:"aclUsers,omitempty"`

	// CustomConfig contains additional raw Valkey configuration lines
	// appended to the generated valkey.conf.
	CustomConfig string `json:"customConfig,omitempty"`

	// TerminationGracePeriodSeconds is the time allowed for a pod to shut down
	// gracefully. Must be long enough to cover pro-active cluster failover.
	// +kubebuilder:default=60
	// +kubebuilder:validation:Minimum=10
	TerminationGracePeriodSeconds int64 `json:"terminationGracePeriodSeconds,omitempty"`

	// ClusterNodeTimeout is the maximum time in milliseconds that a Valkey Cluster
	// node can be unavailable before it is considered failing.
	// +kubebuilder:default=2000
	// +kubebuilder:validation:Minimum=500
	ClusterNodeTimeout int32 `json:"clusterNodeTimeout,omitempty"`

	// Topology defines topology-aware placement and election rules.
	Topology *ValkeyTopologySpec `json:"topology,omitempty"`

	// Config defines structured Valkey configuration parameters.
	// These are merged with CustomConfig, with structured values taking precedence.
	// +kubebuilder:validation:Optional
	Config *ValkeyConfigSpec `json:"config,omitempty"`

	// Metrics configures the redis_exporter sidecar for Prometheus scraping.
	// +kubebuilder:validation:Optional
	Metrics *ValkeyMetricsSpec `json:"metrics,omitempty"`

	// Rebalance configures automatic slot rebalancing when shards become imbalanced.
	// +kubebuilder:validation:Optional
	Rebalance *ValkeyRebalanceSpec `json:"rebalance,omitempty"`
}

// ValkeyRebalanceSpec configures automatic slot rebalancing.
type ValkeyRebalanceSpec struct {
	// Enabled activates automatic rebalancing when ShardImbalanced condition is raised.
	// +kubebuilder:default=false
	Enabled bool `json:"enabled"`

	// Threshold is the maximum allowed memory deviation (%) between the most and
	// least loaded shard before ShardImbalanced is raised.
	// +kubebuilder:default=20
	// +kubebuilder:validation:Minimum=5
	// +kubebuilder:validation:Maximum=100
	Threshold int32 `json:"threshold,omitempty"`

	// MaxSlotsPerRound limits how many slots valkey-cli --cluster rebalance moves
	// per execution. Reduces rebalance impact on latency.
	// +kubebuilder:default=100
	// +kubebuilder:validation:Minimum=1
	MaxSlotsPerRound int32 `json:"maxSlotsPerRound,omitempty"`
}

// ValkeyConfigSpec defines structured Valkey configuration parameters.
// These values are injected into valkey.conf alongside CustomConfig.
type ValkeyConfigSpec struct {
	// MaxmemoryPolicy defines the eviction policy when maxmemory is reached.
	// Common values: allkeys-lru, volatile-lru, allkeys-lfu, noeviction.
	// +kubebuilder:default="allkeys-lru"
	MaxmemoryPolicy string `json:"maxmemoryPolicy,omitempty"`

	// MaxmemoryRatio is the percentage of the pod memory limit allocated to Valkey.
	// maxmemory = resources.limits.memory * MaxmemoryRatio / 100.
	// If no memory limit is set, maxmemory is not configured (Valkey default).
	// +kubebuilder:default=80
	// +kubebuilder:validation:Minimum=10
	// +kubebuilder:validation:Maximum=99
	MaxmemoryRatio int32 `json:"maxmemoryRatio,omitempty"`

	// Hz is the Valkey scheduler frequency (background tasks per second).
	// Higher values reduce latency but increase CPU usage.
	// +kubebuilder:default=10
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=500
	Hz int32 `json:"hz,omitempty"`

	// Lazyfree enables lazy (background thread) deletion for evictions,
	// expirations and key flushing — avoids latency spikes on large keys.
	// +kubebuilder:default=true
	Lazyfree bool `json:"lazyfree,omitempty"`

	// TCPKeepalive is the interval in seconds for TCP keepalive probes.
	// Important for PHP persistent connections.
	// +kubebuilder:default=300
	// +kubebuilder:validation:Minimum=0
	TCPKeepalive int32 `json:"tcpKeepalive,omitempty"`

	// IOThreads overrides the automatic io-threads calculation.
	// When not set, io-threads is derived from resources.limits.cpu:
	//   < 4 CPUs → 1 (disabled), 4 CPUs → 2, 8 CPUs → 4, 16+ CPUs → 8.
	// Set to 1 to disable io-threads explicitly.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=8
	// +kubebuilder:validation:Optional
	IOThreads *int32 `json:"ioThreads,omitempty"`
}

// ValkeyMetricsSpec configures the redis_exporter sidecar container.
type ValkeyMetricsSpec struct {
	// Enabled controls whether the redis_exporter sidecar is injected.
	// +kubebuilder:default=true
	Enabled bool `json:"enabled"`

	// Image is the redis_exporter container image.
	// +kubebuilder:default="oliver006/redis_exporter:latest"
	Image string `json:"image,omitempty"`

	// MetricsSecret references a Secret containing the password for the
	// dedicated metrics ACL user. The Secret must contain a key "password".
	// +kubebuilder:validation:Required
	MetricsSecret corev1.SecretKeySelector `json:"metricsSecret"`

	// ServiceMonitor configures the Prometheus Operator ServiceMonitor resource.
	// +kubebuilder:validation:Optional
	ServiceMonitor *ValkeyServiceMonitorSpec `json:"serviceMonitor,omitempty"`
}

// ValkeyServiceMonitorSpec configures the ServiceMonitor created for Prometheus scraping.
type ValkeyServiceMonitorSpec struct {
	// Enabled controls whether the ServiceMonitor is created.
	// +kubebuilder:default=true
	Enabled bool `json:"enabled"`

	// Labels are added to the ServiceMonitor metadata so that the Prometheus
	// Operator can discover it via its serviceMonitorSelector.
	// Example: {"release": "kube-prometheus-stack"}
	// +kubebuilder:validation:Optional
	Labels map[string]string `json:"labels,omitempty"`

	// Interval is the Prometheus scrape interval (e.g. "30s").
	// Defaults to the Prometheus global scrape interval if empty.
	// +kubebuilder:validation:Optional
	Interval string `json:"interval,omitempty"`

	// ScrapeTimeout is the Prometheus scrape timeout (e.g. "10s").
	// Must be less than or equal to Interval.
	// +kubebuilder:validation:Optional
	ScrapeTimeout string `json:"scrapeTimeout,omitempty"`
}

// SpreadPolicy controls how strictly pods are distributed across topology domains.
// +kubebuilder:validation:Enum=Hard;Soft;None
type SpreadPolicy string

const (
	// SpreadPolicyHard — DoNotSchedule: the scheduler blocks pod placement if
	// the constraint cannot be satisfied. Use for production clusters.
	SpreadPolicyHard SpreadPolicy = "Hard"

	// SpreadPolicyNone — no spread or anti-affinity rules are generated.
	// Use for single-node dev clusters with fewer nodes than replicas.
	SpreadPolicyNone SpreadPolicy = "None"

	// SpreadPolicySoft — ScheduleAnyway: the scheduler prefers spreading pods
	// but allows co-location when the constraint cannot be satisfied.
	// Use for staging clusters where availability is desired but not enforced.
	SpreadPolicySoft SpreadPolicy = "Soft"
)

// ValkeyTopologySpec defines topology-aware placement and election behaviour.
type ValkeyTopologySpec struct {
	// NodeTopologyKey is the node label key used to identify topology domains.
	// Common values:
	//   "topology.kubernetes.io/zone"    — availability zone
	//   "topology.kubernetes.io/region"  — cloud region
	//   "kubernetes.io/hostname"         — individual node
	// +kubebuilder:default="topology.kubernetes.io/zone"
	NodeTopologyKey string `json:"nodeTopologyKey,omitempty"`

	// NodeSpreadPolicy controls isolation at the node level (kubernetes.io/hostname).
	//   Hard — at most 1 pod per node; pods go Pending if nodes are insufficient.
	//   Soft — prefer 1 pod per node; co-location allowed when nodes are scarce.
	//   None — no node-level constraint; multiple pods may share a node.
	// +kubebuilder:default=Hard
	NodeSpreadPolicy SpreadPolicy `json:"nodeSpreadPolicy,omitempty"`

	// ZoneSpreadPolicy controls isolation at the zone level (nodeTopologyKey).
	//   Hard — at most 1 pod per zone; pods go Pending if zones are insufficient.
	//   Soft — prefer 1 pod per zone; co-location allowed when zones are scarce.
	//   None — no zone-level constraint; multiple pods may share a zone.
	// +kubebuilder:default=Hard
	ZoneSpreadPolicy SpreadPolicy `json:"zoneSpreadPolicy,omitempty"`

	// PodAssignments declares which topology domain value is preferred for each
	// pod ordinal.
	// +kubebuilder:validation:Optional
	PodAssignments []TopologyPodAssignment `json:"podAssignments,omitempty"`

	// AvoidSameZoneAsFailed instructs the election algorithm to deprioritise
	// replicas that are in the same topology domain as the failing primary.
	// +kubebuilder:default=true
	AvoidSameZoneAsFailed bool `json:"avoidSameZoneAsFailed,omitempty"`

	// ElectionTopologyWeight controls how much topology preference influences
	// replica selection during pro-active failover, relative to replication lag.
	// Range 0–100: 0=lag only, 100=topology only, 70=balanced.
	// +kubebuilder:default=70
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	ElectionTopologyWeight int32 `json:"electionTopologyWeight,omitempty"`
}

// TopologyPodAssignment declares the preferred topology domain for a given
// StatefulSet pod ordinal.
type TopologyPodAssignment struct {
	// PodIndex is the StatefulSet ordinal.
	// +kubebuilder:validation:Minimum=0
	PodIndex int32 `json:"podIndex"`

	// PreferredValues is an ordered list of topology domain values preferred for this pod.
	// +kubebuilder:validation:MinItems=1
	PreferredValues []string `json:"preferredValues"`
}

// ValkeyACLUser defines a named ACL account for application clients.
type ValkeyACLUser struct {
	// Name is the Valkey ACL username.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// PasswordSecret references the Secret key containing this user's password.
	PasswordSecret corev1.SecretKeySelector `json:"passwordSecret"`

	// KeyPatterns is a list of key patterns this user can access (e.g. "~*", "~app:*").
	// Defaults to "~*" (all keys).
	// +kubebuilder:default={"~*"}
	KeyPatterns []string `json:"keyPatterns,omitempty"`

	// Commands is the ACL command permission string (e.g. "+@read", "+@write -@dangerous").
	// Defaults to "+@all" (all commands).
	// +kubebuilder:default="+@all"
	Commands string `json:"commands,omitempty"`
}

// ValkeyStorageSpec defines the persistent storage configuration.
type ValkeyStorageSpec struct {
	// StorageClassName for PVCs. Empty means the cluster default.
	StorageClassName *string `json:"storageClassName,omitempty"`

	// Size is the capacity of each PVC.
	// +kubebuilder:default="10Gi"
	Size resource.Quantity `json:"size,omitempty"`
}

// ValkeyNodeStatus holds the observed state of a single Valkey cluster node.
type ValkeyNodeStatus struct {
	// NodeID is the unique Valkey cluster node identifier.
	NodeID string `json:"nodeId"`

	// PodName is the Kubernetes pod name hosting this node.
	PodName string `json:"podName"`

	// IP is the announced IP address of the node (from cluster-announce-ip).
	IP string `json:"ip"`

	// Role is the current role: "primary" or "replica".
	Role string `json:"role"`

	// MasterID is the node ID of the primary this replica replicates from.
	// Empty for primary nodes.
	MasterID string `json:"masterId,omitempty"`

	// Slots is the slot range owned by this node (e.g. "0-5460").
	// Empty for replica nodes.
	Slots string `json:"slots,omitempty"`
}

// ValkeyClusterStatus defines the observed state of ValkeyCluster.
type ValkeyClusterStatus struct {
	// Phase is the current lifecycle phase of the cluster.
	Phase ValkeyClusterPhase `json:"phase,omitempty"`

	// ReadyReplicas is the number of pods ready to serve requests.
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// ClusterState is the value of cluster_state from CLUSTER INFO ("ok" or "fail").
	ClusterState string `json:"clusterState,omitempty"`

	// SlotsOk is the number of hash slots in ok state (out of 16384).
	SlotsOk int32 `json:"slotsOk,omitempty"`

	// NodesOk is the count of nodes reachable and healthy in the cluster.
	NodesOk int32 `json:"nodesOk,omitempty"`

	// PodRoles maps pod name to its current role ("primary" or "replica").
	// Derived from CLUSTER NODES and kept in sync each reconcile loop.
	PodRoles map[string]string `json:"podRoles,omitempty"`

	// Nodes holds the observed topology of every cluster node, populated from
	// CLUSTER NODES on each reconcile. Used by the operator for scaling decisions.
	Nodes []ValkeyNodeStatus `json:"nodes,omitempty"`

	// PodTopology maps pod name to its observed topology domain value.
	PodTopology map[string]string `json:"podTopology,omitempty"`

	// PrimaryTopologyValue is the topology domain value of the primary of shard 0.
	PrimaryTopologyValue string `json:"primaryTopologyValue,omitempty"`

	// Conditions represents the latest available observations of the cluster's state.
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedGeneration reflects the generation of the most recently reconciled spec.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.status.clusterState`
// +kubebuilder:printcolumn:name="Slots",type=integer,JSONPath=`.status.slotsOk`
// +kubebuilder:printcolumn:name="Update",type=string,JSONPath=`.status.conditions[?(@.type=="RollingUpdate")].message`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:resource:shortName=vc;vk,categories=valkey

// ValkeyCluster is the Schema for the valkeyclusters API.
type ValkeyCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ValkeyClusterSpec   `json:"spec,omitempty"`
	Status ValkeyClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ValkeyClusterList contains a list of ValkeyCluster.
type ValkeyClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ValkeyCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ValkeyCluster{}, &ValkeyClusterList{})
}
