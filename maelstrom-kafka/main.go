package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const retries = 100

type ctxKey string

type server struct {
	node  *maelstrom.Node
	linKv *maelstrom.KV

	commits map[string]int

	logsMu sync.RWMutex
	comMu  sync.RWMutex
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	s := &server{node: n, linKv: kv}

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
	var msgId = int(body["msg_id"].(float64))

	size, err := s.updateLogs(key, val, msgId)

	if err != nil {
		return errors.New("could not update value of key in storage")
	}

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
	var msgId = int(body["msg_id"].(float64))
	var msgResp = make(map[string][][]int)

	for key, offset := range offsets {
		curr, err := s.getCurrLogs(key, msgId)
		if err != nil || len(curr) == 0 || len(curr) <= int(offset.(float64)) {
			continue
		}
		var resp [][]int
		for i := int(offset.(float64)); i < len(curr); i++ {
			var slc []int = make([]int, 0)
			slc = append(slc, i)
			slc = append(slc, curr[i])
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

func (s *server) getCurrLogs(key string, msgId int) ([]int, error) {
	s.logsMu.RLock()
	defer s.logsMu.RUnlock()
	return s.readLogsFromKv(key, msgId)
}

func (s *server) readLogsFromKv(key string, msgId int) ([]int, error) {
	var uuid = uuid.New().String()
	var ctx = context.WithValue(context.Background(), ctxKey(uuid), msgId)
	var currVal, err = s.linKv.Read(ctx, key)

	if err != nil && strings.Contains(err.Error(), "key does not exist") {
		return make([]int, 0), nil
	}

	if err != nil {
		return make([]int, 0), err
	}

	var data []int

	// Iterate through the []interface{}
	for _, item := range currVal.([]interface{}) {
		if val, ok := item.(float64); ok {
			data = append(data, int(val))
		} else {
			// Handle cases where the item is not an int
			log.Printf("Warning: Skipping non-integer value: %v (type %T)\n", item, item)
		}
	}

	return data, nil
}

func (s *server) updateLogs(key string, val int, msgId int) (int, error) {
	s.logsMu.Lock()
	defer s.logsMu.Unlock()
	for i := 0; i < retries; i++ {
		currVal, err := s.readLogsFromKv(key, msgId)
		if err != nil {
			log.Printf("Error is %s", err)
			continue
		}
		updVal := append(currVal, val)
		var uuid = uuid.New().String()
		var ctx = context.WithValue(context.Background(), ctxKey(uuid), msgId)
		err = s.linKv.CompareAndSwap(ctx, key, currVal, updVal, true /* createIfNotExists */)
		if err == nil {
			return len(updVal), nil
		}
		log.Printf("Error is %s", err)
		time.Sleep(time.Duration(i) * 10 * time.Millisecond)
	}
	return -1, errors.New("could not write to key value store")
}
