package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	node       *maelstrom.Node
	values     []float64
	neighbours map[string]interface{}

	valuesMu sync.RWMutex
}

func main() {
	n := maelstrom.NewNode()
	s := &server{node: n}

	n.Handle("broadcast", s.broadcast)
	n.Handle("read", s.read)
	n.Handle("topology", s.topology)

	if err := n.Run(); err != nil {
		log.Printf("Error is %s", err)
		log.Fatal(err)
	}

}

func (s *server) broadcast(msg maelstrom.Message) error {
	// Unmarshal the message body as an loosely-typed map.
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// Store the value
	s.valuesMu.Lock()
	s.values = append(s.values, body["message"].(float64))
	defer s.valuesMu.Unlock()

	// Update the message type to return back.
	body = map[string]any{
		"type": "broadcast_ok",
	}

	// Echo the original message back with the updated message type.
	return s.node.Reply(msg, body)
}

func (s *server) read(msg maelstrom.Message) error {
	// Unmarshal the message body as an loosely-typed map.
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// Update the message type to return back.
	s.valuesMu.RLock()
	body = map[string]any{
		"type":     "read_ok",
		"messages": s.values,
	}
	defer s.valuesMu.RUnlock()

	// Echo the original message back with the updated message type.
	return s.node.Reply(msg, body)
}

func (s *server) topology(msg maelstrom.Message) error {
	// Unmarshal the message body as an loosely-typed map.
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// Store the neighbours
	s.neighbours = body["topology"].(map[string]interface{})

	// Update the message type to return back.
	body = map[string]any{
		"type": "topology_ok",
	}

	// Echo the original message back with the updated message type.
	return s.node.Reply(msg, body)
}
