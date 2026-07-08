package models

// LogEntry represents a mutation log entry in the Write-Ahead Log
type LogEntry struct {
	Index     int    `json:"index"`
	Operation string `json:"operation"` // "put" or "delete"
	Key       string `json:"key"`
	Value     string `json:"value,omitempty"`
}

// PutArgs represents the arguments for a PUT RPC
type PutArgs struct {
	Key   string
	Value string
}

// PutReply represents the reply for a PUT RPC
type PutReply struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Message string `json:"message"`
}

// GetArgs represents the arguments for a GET RPC
type GetArgs struct {
	Key string
}

// GetReply represents the reply for a GET RPC
type GetReply struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Message string `json:"message"`
}

// DeleteArgs represents the arguments for a DELETE RPC
type DeleteArgs struct {
	Key string
}

// DeleteReply represents the reply for a DELETE RPC
type DeleteReply struct {
	Key     string  `json:"key"`
	Value   *string `json:"value"` // Using pointer to serialize to null/nil in JSON
	Message string  `json:"message"`
}

// HealthArgs represents the arguments for a Health RPC
type HealthArgs struct{}

// HealthReply represents the reply for a Health RPC/HTTP check
type HealthReply struct {
	NodeID string `json:"node_id"`
	Role   string `json:"role"`
	Status string `json:"status"`
}

// MetricsArgs represents the arguments for a Metrics RPC
type MetricsArgs struct{}

// MetricsReply represents the reply for a Metrics RPC/HTTP endpoint
type MetricsReply struct {
	NodeID                  string `json:"node_id"`
	Role                    string `json:"role"`
	TotalReads              int    `json:"total_reads"`
	TotalWrites             int    `json:"total_writes"`
	TotalDeletes            int    `json:"total_deletes"`
	ReplicationSuccessCount int    `json:"replication_success_count"`
	ReplicationFailureCount int    `json:"replication_failure_count"`
	LogIndex                int    `json:"log_index"`
}

// ClusterArgs represents the arguments for a Cluster RPC
type ClusterArgs struct{}

// PeerStatus represents the health and details of a peer node
type PeerStatus struct {
	Peer   string      `json:"peer"`
	Status string      `json:"status"` // "healthy" or "unreachable"
	Detail HealthReply `json:"detail,omitempty"`
	Error  string      `json:"error,omitempty"`
}

// ClusterReply represents the reply for a Cluster status RPC/HTTP check
type ClusterReply []PeerStatus

// ReplicateArgs represents the arguments for an internal replicate RPC
type ReplicateArgs struct {
	Entries []LogEntry
}

// ReplicateReply represents the reply for an internal replicate RPC
type ReplicateReply struct {
	Status   string `json:"status"`
	LogIndex int    `json:"log_index"`
}

// SyncArgs represents the arguments for an internal WAL sync RPC
type SyncArgs struct {
	AfterIndex int
}

// SyncReply represents the reply for an internal WAL sync RPC
type SyncReply struct {
	Entries        []LogEntry `json:"entries"`
	LeaderLogIndex int        `json:"leader_log_index"`
}
