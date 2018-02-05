package pipeline

import (
	"encoding/json"
	"fmt"
)

// A node that logs all data that passes through the node.
//
// Example:
//    stream.from()...
//      |window()
//          .period(10s)
//          .every(10s)
//      |log()
//      |count('value')
//
type LogmeNode struct {
	chainnode

	// The level at which to log the data.
	// One of: DEBUG, INFO, WARN, ERROR
	// Default: INFO
	Level string `json:"level"`
	// Optional prefix to add to all log messages
	Prefix string `json:"prefix"`
}

func newLogmeNode(wants EdgeType) *LogmeNode {
	return &LogmeNode{
		chainnode: newBasicChainNode("logme", wants, wants),
		Level:     "INFO",
	}
}

// MarshalJSON converts LogNode to JSON
// tick:ignore
func (n *LogmeNode) MarshalJSON() ([]byte, error) {
	type Alias LogmeNode
	var raw = &struct {
		TypeOf
		*Alias
	}{
		TypeOf: TypeOf{
			Type: "logme",
			ID:   n.ID(),
		},
		Alias: (*Alias)(n),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an LogNode
// tick:ignore
func (n *LogmeNode) UnmarshalJSON(data []byte) error {
	type Alias LogmeNode
	var raw = &struct {
		TypeOf
		*Alias
	}{
		Alias: (*Alias)(n),
	}
	err := json.Unmarshal(data, raw)
	if err != nil {
		return err
	}
	if raw.Type != "log" {
		return fmt.Errorf("error unmarshaling node %d of type %s as LogmeNode", raw.ID, raw.Type)
	}
	n.setID(raw.ID)
	return nil
}
