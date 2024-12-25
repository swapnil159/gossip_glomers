package main

// Note: This solution works for challenge 3e as well.

import (
	"encoding/json"
	"log"
	"maps"
	"slices"
	"sync"
	"time"

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

const retries = 100

func main() {
	n := maelstrom.NewNode()
	s := &server{node: n, values: make(map[int]bool)}

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
	var val = int(body["message"].(float64))
	s.valuesMu.Lock()
	s.values[val] = true
	s.valuesMu.Unlock()

	// Update the message type to return back.
	body = map[string]any{
		"type": "broadcast_ok",
	}

	// Echo the original message back with the updated message type.
	err := s.node.Reply(msg, body)
	if err != nil {
		log.Printf("Error is %s", err)
	}
	return s.sendToNeighbours(val, msg.Src)
}

func (s *server) read(msg maelstrom.Message) error {
	// Unmarshal the message body as an loosely-typed map.
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// Update the message type to return back.
	s.valuesMu.RLock()
	vals := slices.Collect(maps.Keys(s.values))
	s.valuesMu.RUnlock()
	body = map[string]any{
		"type":     "read_ok",
		"messages": vals,
	}

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
	defer s.topoMu.Unlock()
	for _, val := range s.node.NodeIDs() {
		if val == s.node.ID() {
			continue
		}
		s.neighbours.topo = append(s.neighbours.topo, val)
	}

	// Update the message type to return back.
	body = map[string]any{
		"type": "topology_ok",
	}

	// Echo the original message back with the updated message type.
	return s.node.Reply(msg, body)
}

func (s *server) sendToNeighbours(val int, sender string) error {
	s.topoMu.RLock()
	nbor := s.neighbours.topo
	s.topoMu.RUnlock()

	if sender[0] == 'n' {
		return nil
	}

	for _, n := range nbor {
		go func() {
			for i := 0; i < retries; i++ {
				err := s.node.RPC(
					n,
					map[string]any{
						"type":    "broadcast",
						"message": float64(val),
					},
					s.recvResp)
				if err == nil {
					return
				}
				log.Printf("Error is %s", err)
				time.Sleep(time.Duration(i) * 100 * time.Millisecond)
			}
		}()
	}

	return nil
}

func (s *server) recvResp(msg maelstrom.Message) error {
	return nil
}
