package main

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	node       *maelstrom.Node
	neighbours topology

	kv map[int]int

	kvMu   sync.RWMutex
	topoMu sync.RWMutex
}

type topology struct {
	topo []string
}

const retries = 100

func main() {
	n := maelstrom.NewNode()
	s := &server{node: n}

	s.kv = make(map[int]int)
	err := s.initTopology()
	if err != nil {
		log.Fatal(err)
		os.Exit(0)
	}

	n.Handle("txn", s.txn)

	if err := n.Run(); err != nil {
		log.Printf("Error is %s", err)
		log.Fatal(err)
	}
}

func (s *server) txn(msg maelstrom.Message) error {
	// Unmarshal the message body as an loosely-typed map.
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	var t = body["txn"].([]interface{})
	data := make([]interface{}, 0)
	for _, instruction := range t {
		op := instruction.([]interface{})
		switch op[0] {
		case "r":
			val := s.read(int(op[1].(float64)))
			if val != nil {
				op[2] = *val
			}

		case "w":
			err := s.write(int(op[1].(float64)), int(op[2].(float64)))
			if err != nil {
				return errors.New("could not write to storage")
			}
		}
		data = append(data, op)
	}

	// Update the message type to return back.
	body = map[string]any{
		"type": "txn_ok",
		"txn":  data,
	}
	err := s.node.Reply(msg, body)
	if err != nil {
		return err
	}

	return s.sendToNeighbours(msg)
}

func (s *server) read(key int) *int {
	s.kvMu.RLock()
	defer s.kvMu.RUnlock()

	val, ok := s.kv[key]
	if ok {
		return &val
	}

	return nil
}

func (s *server) write(key int, val int) error {
	s.kvMu.Lock()
	defer s.kvMu.Unlock()

	s.kv[key] = val

	return nil
}

func (s *server) initTopology() error {
	// Store the neighbours
	s.topoMu.Lock()
	defer s.topoMu.Unlock()
	for _, val := range s.node.NodeIDs() {
		if val == s.node.ID() {
			continue
		}
		s.neighbours.topo = append(s.neighbours.topo, val)
	}

	return nil
}

func (s *server) sendToNeighbours(msg maelstrom.Message) error {
	s.topoMu.RLock()
	nbor := s.neighbours.topo
	s.topoMu.RUnlock()

	if msg.Src[0] == 'n' {
		return nil
	}

	for _, n := range nbor {
		node := n
		go func() {
			for i := 0; i < retries; i++ {
				err := s.node.RPC(
					node,
					msg.Body,
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
