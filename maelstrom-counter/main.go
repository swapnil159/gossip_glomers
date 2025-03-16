package main

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const typ = "node"
const retries = 100

type ctxKey string

type server struct {
	node *maelstrom.Node
	kv   *maelstrom.KV
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	s := &server{node: n, kv: kv}

	n.Handle("read", s.read)
	n.Handle("add", s.add)

	if err := n.Run(); err != nil {
		log.Printf("Error is %s", err)
		log.Fatal(err)
	}
}

func (s *server) add(msg maelstrom.Message) error {
	// Unmarshal the message body as an loosely-typed map.
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	var delta = int(body["delta"].(float64))
	var msgId = int(body["msg_id"].(float64))

	s.updateVal(msgId, delta)

	body = map[string]any{
		"type": "add_ok",
	}

	return s.node.Reply(msg, body)
}

func (s *server) read(msg maelstrom.Message) error {
	// Unmarshal the message body as an loosely-typed map.
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	var msgId = int(body["msg_id"].(float64))
	var currVal, err = s.getCurrVal(msgId)

	if err != nil {
		return err
	}

	body = map[string]any{
		"type":  "read_ok",
		"value": currVal,
	}

	return s.node.Reply(msg, body)
}

func (s *server) getCurrVal(msgId int) (int, error) {
	var uuid = uuid.New().String()
	var ctx = context.WithValue(context.Background(), ctxKey(uuid), msgId)
	var currVal, err = s.kv.ReadInt(ctx, typ)

	if err != nil && strings.Contains(err.Error(), "key does not exist") {
		return 0, nil
	}

	if err != nil {
		return 0, err
	}

	return currVal, nil
}

func (s *server) updateVal(msgId int, delta int) {
	go func() {
		for i := 0; i < retries; i++ {
			currVal, err := s.getCurrVal(msgId)
			if err != nil {
				log.Printf("Error is %s", err)
				continue
			}
			updVal := delta + int(currVal)
			var uuid = uuid.New().String()
			var ctx = context.WithValue(context.Background(), ctxKey(uuid), msgId)
			err = s.kv.CompareAndSwap(ctx, typ, currVal, updVal, true /* createIfNotExists */)
			if err == nil {
				return
			}
			log.Printf("Error is %s", err)
			time.Sleep(time.Duration(i) * 100 * time.Millisecond)
		}
	}()
}
