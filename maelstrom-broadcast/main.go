package main

import (
	"encoding/json"
	"log"
	"maps"
	"slices"
	"sync"

	gocron "github.com/jasonlvhit/gocron"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	node       *maelstrom.Node
	values     map[int]bool
	neighbours topology
	buffer     map[task]int

	valuesMu sync.RWMutex
	topoMu   sync.RWMutex
	buffMu   sync.RWMutex
}

type topology struct {
	topo []string
}

type task struct {
	receiver string
	id       int
}

func main() {
	n := maelstrom.NewNode()
	s := &server{node: n, values: make(map[int]bool), buffer: make(map[task]int)}

	n.Handle("broadcast", s.broadcast)
	n.Handle("read", s.read)
	n.Handle("topology", s.topology)
	n.Handle("broadcast_ok", s.recvResp)

	gocron.Every(10).Second().Do(s.sendToNeighbour)

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
		s.updateBuffer(val, msg.Src, int(body["msg_id"].(float64)))
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

func (s *server) updateBuffer(val int, sender string, msgId int) {
	s.topoMu.RLock()
	nbor := s.neighbours.topo
	defer s.topoMu.RUnlock()

	for _, n := range nbor {
		if sender == n {
			continue
		}
		s.buffMu.Lock()
		t := task{receiver: n, id: msgId}
		s.buffer[t] = val
		defer s.buffMu.Unlock()
	}
}

func (s *server) recvResp(msg maelstrom.Message) error {
	// Unmarshal the message body as an loosely-typed map.
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	var val = int(body["message"].(float64))
	t := task{receiver: msg.Src, id: val}

	s.buffMu.Lock()
	delete(s.buffer, t)
	defer s.buffMu.Unlock()
	return nil
}

func (s *server) sendToNeighbour() {
	s.buffMu.RLock()
	for key, val := range s.buffer {
		s.node.Send(key.receiver, map[string]any{
			"type":    "broadcast",
			"message": float64(val),
			"msg_id":  key.id,
		})
	}
	defer s.buffMu.RUnlock()
}
