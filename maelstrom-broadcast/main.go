package main

import (
	"encoding/json"
	"log"
	"maps"
	"slices"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	node       *maelstrom.Node
	values     map[int]bool
	neighbours topology

	valuesMu sync.RWMutex
	topoMu   sync.RWMutex
}

type topology struct {
	topo []string
}

func main() {
	n := maelstrom.NewNode()
	s := &server{node: n, values: make(map[int]bool)}

	n.Handle("broadcast", s.broadcast)
	n.Handle("read", s.read)
	n.Handle("topology", s.topology)
	n.Handle("broadcast_ok", s.recvResp)

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
	var val = int(body["message"].(float64))
	if !s.values[val] {
		s.sendToNeighbour(val, msg.Src)
	}
	s.values[val] = true
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
		"messages": slices.Collect(maps.Keys(s.values)),
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
	s.topoMu.Lock()
	t := body["topology"].(map[string]interface{})
	for _, val := range t[msg.Dest].([]interface{}) {
		s.neighbours.topo = append(s.neighbours.topo, val.(string))
	}
	defer s.topoMu.Unlock()

	// Update the message type to return back.
	body = map[string]any{
		"type": "topology_ok",
	}

	// Echo the original message back with the updated message type.
	return s.node.Reply(msg, body)
}

func (s *server) sendToNeighbour(val int, sender string) {
	s.topoMu.RLock()
	nbor := s.neighbours.topo
	log.Printf("Neighbours are %s", nbor)
	defer s.topoMu.RUnlock()

	for _, n := range nbor {
		if sender == n {
			continue
		}
		err := s.node.Send(
			n,
			map[string]any{
				"type":    "broadcast",
				"message": float64(val),
			},
		)
		if err != nil {
			log.Printf("Error is %s", err)
			log.Fatal(err)
		}
	}
}

func (s *server) recvResp(msg maelstrom.Message) error {
	return nil
}
