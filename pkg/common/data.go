package common

// KVPair represents a key-value pair for batch operations
type KVPair struct {
	Key   string
	Value string
}

var CommittedTxnFile = "committed_txns.json"
