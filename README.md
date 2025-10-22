# Go gossip protocols exercises

These are solutions to Fly.io's distributed systems challenges: https://fly.io/dist-sys/

## Prerequisites

1. Install [Maelstrom](https://github.com/jepsen-io/maelstrom)
2. Install the [go toolchain](https://go.dev/doc/install)
3. Set **MAELSTROM_LOC** to where your Maelstrom binary is located

## To run a specific challenge implementation

1. run `go get github.com/jepsen-io/maelstrom/demo/go` in the specific challenge subdirectory
2. Run `go install .` in the specific challenge subdirectory
3. Run the corresponding challenge script file

## Challenges

- [x] [Echo](https://github.com/swapnil159/gossip_glomers/blob/master/maelstrom-echo/main.go)
- [x] [Unique ID](https://github.com/swapnil159/gossip_glomers/blob/master/maelstrom-unique-ids/main.go)
- [x] [Single-Node Broadcast](https://github.com/swapnil159/gossip_glomers/blob/master/maelstrom-broadcast/main.go)
- [x] [Multi-Node Broadcast](https://github.com/swapnil159/gossip_glomers/blob/master/maelstrom-broadcast/main.go)
- [x] [Fault Tolerant Broadcast](https://github.com/swapnil159/gossip_glomers/blob/master/maelstrom-broadcast/main.go)
- [x] [Efficient Broadcast](https://github.com/swapnil159/gossip_glomers/blob/master/maelstrom-broadcast-3d/main.go)
- [x] [Grow-Only Counter](https://github.com/swapnil159/gossip_glomers/blob/master/maelstrom-counter/main.go)
- [x] [Single-Node Kafka-Style Log](https://github.com/swapnil159/gossip_glomers/blob/master/maelstrom-kafka/main.go)
- [x] [Multi-Node Kafka-Style Log](https://github.com/swapnil159/gossip_glomers/blob/master/maelstrom-kafka/main.go)
- [x] [Efficient Kafka-Style Log](https://github.com/swapnil159/gossip_glomers/blob/master/maelstrom-kafka/main.go)
- [x] [Single-Node, Totally-Available Transactions](https://github.com/swapnil159/gossip_glomers/blob/master/maelstrom-txn/main.go)
- [x] [Totally-Available, Read Uncommitted Transactions](https://github.com/swapnil159/gossip_glomers/blob/master/maelstrom-txn/main.go)
- [x] [Totally-Available, Read Committed Transactions](https://github.com/swapnil159/gossip_glomers/blob/master/maelstrom-txn/main.go)

### Note
I do not think Maelstrom checks for dirty reads correctly. My algorithm currently assumes that a transaction always gets committed. There are following scenarios possible-

-   We have to abort the transaction mid-way
-   The thread executing txn-1 gets interrupted by another thread executing txn-2 and resumes at a later point in time

Right now, I am locking the keys before doing an operation (read or write) in a transaction. This does not guarantee atomicity when committing the transaction and hence there is room for dirty reads here. A simple solution for (2) is to hold the lock at DB level when executing the transaction. However for (1), we should probably go to Read committed using MVCC (Multi versioned concurrency control)?