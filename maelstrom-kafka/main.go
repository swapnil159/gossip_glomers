package main

import (
	"encoding/json"
	"log"
	"math"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	node *maelstrom.Node

	logs    map[string][]int
	commits map[string]int

	logsMu sync.RWMutex
	comMu  sync.RWMutex
}

func main() {
	n := maelstrom.NewNode()
	s := &server{node: n}

	s.logs = make(map[string][]int)
	s.commits = make(map[string]int)

	n.Handle("send", s.send)
	n.Handle("poll", s.poll)
	n.Handle("commit_offsets", s.commit)
	n.Handle("list_committed_offsets", s.listCommits)

	if err := n.Run(); err != nil {
		log.Printf("Error is %s", err)
		log.Fatal(err)
	}
}

func (s *server) send(msg maelstrom.Message) error {
	// Unmarshal the message body as an loosely-typed map.
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	var key = body["key"].(string)
	var val = int(body["msg"].(float64))

	s.logsMu.Lock()
	defer s.logsMu.Unlock()
	s.logs[key] = append(s.logs[key], val)
	var size = len(s.logs[key])

	// Update the message type to return back.
	body = map[string]any{
		"type":   "send_ok",
		"offset": size - 1,
	}

	// Return the original message back with the updated message type.
	return s.node.Reply(msg, body)
}

func (s *server) poll(msg maelstrom.Message) error {
	// Unmarshal the message body as an loosely-typed map.
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	var offsets = body["offsets"].(map[string]interface{})
	var msgResp = make(map[string][][]int)

	s.logsMu.RLock()
	defer s.logsMu.RUnlock()
	for key, offset := range offsets {
		var curr []int = s.logs[key]
		if len(curr) == 0 || len(curr) <= int(offset.(float64)) {
			continue
		}
		var resp [][]int
		for i := int(offset.(float64)); i < len(curr); i++ {
			var slc []int = make([]int, 0)
			slc = append(slc, i)
			slc = append(slc, s.logs[key][i])
			resp = append(resp, slc)
		}
		msgResp[key] = resp
	}

	// Update the message type to return back.
	body = map[string]any{
		"type": "poll_ok",
		"msgs": msgResp,
	}

	// Echo the original message back with the updated message type.
	return s.node.Reply(msg, body)
}

func (s *server) commit(msg maelstrom.Message) error {
	// Unmarshal the message body as an loosely-typed map.
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	var offsets = body["offsets"].(map[string]interface{})

	s.comMu.Lock()
	defer s.comMu.Unlock()
	for key, offset := range offsets {
		var offs = int(math.Min(offset.(float64), float64(s.commits[key])))
		s.commits[key] = offs
	}

	// Update the message type to return back.
	body = map[string]any{
		"type": "commit_offsets_ok",
	}

	// Echo the original message back with the updated message type.
	return s.node.Reply(msg, body)
}

func (s *server) listCommits(msg maelstrom.Message) error {
	// Unmarshal the message body as an loosely-typed map.
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	var keys = body["keys"].([]interface{})
	var resp map[string]int = make(map[string]int)

	s.comMu.RLock()
	defer s.comMu.RUnlock()
	for _, key := range keys {
		resp[key.(string)] = s.commits[key.(string)]
	}

	// Update the message type to return back.
	body = map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": resp,
	}

	// Echo the original message back with the updated message type.
	return s.node.Reply(msg, body)
}
