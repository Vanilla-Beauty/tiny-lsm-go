package skiplist

import (
	"tiny-lsm-go/pkg/iterator"
)

// Node represents a node in the skip list
// Node represents a node in the skip list
type Node struct {
	entry    iterator.Entry
	forward  []*Node // forward pointers for different levels
	backward *Node   // backward pointer to previous node
	level    int     // number of levels for this node
}

// NewNode creates a new skip list node
func NewNode(key, value string, txnID uint64, level int) *Node {
	return &Node{
		entry: iterator.Entry{
			Key:   key,
			Value: value,
			TxnID: txnID,
		},
		forward:  make([]*Node, level),
		backward: nil,
		level:    level,
	}
}

// Key returns the key of the node
func (n *Node) Key() string {
	return n.entry.Key
}

// Value returns the value of the node
func (n *Node) Value() string {
	return n.entry.Value
}

// TxnID returns the transaction ID of the node
func (n *Node) TxnID() uint64 {
	return n.entry.TxnID
}

// IsDeleted returns whether this node represents a delete marker
func (n *Node) IsDeleted() bool {
	return n.entry.Value == ""
}

// Entry returns the entry of the node
func (n *Node) Entry() iterator.Entry {
	return n.entry
}

// Level returns the level of the node
func (n *Node) Level() int {
	return n.level
}

// Next returns the next node at level 0
func (n *Node) Next() *Node {
	if len(n.forward) > 0 {
		return n.forward[0]
	}
	return nil
}

// ForwardAt returns the forward pointer at the specified level
func (n *Node) ForwardAt(level int) *Node {
	if level >= 0 && level < len(n.forward) {
		return n.forward[level]
	}
	return nil
}

// SetForwardAt sets the forward pointer at the specified level
func (n *Node) SetForwardAt(level int, node *Node) {
	if level >= 0 && level < len(n.forward) {
		n.forward[level] = node
	}
}

// CompareNode compares this node with another node
// Returns:
//
//	-1 if this node < other
//	 0 if this node == other
//	 1 if this node > other
func (n *Node) CompareNode(other *Node) int {
	return iterator.CompareEntries(n.entry, other.entry)
}

// CompareKey compares this node's key with the given key
func (n *Node) CompareKey(key string) int {
	return iterator.CompareKeys(n.entry.Key, key)
}
