# Ydb

A Y.js document server with pluggable storage and broadcast backends.

Ydb is a WebSocket server for [Yjs](https://yjs.dev) collaborative documents. It persists documents as append-only operation logs and relays updates between connected clients in real-time. It speaks the [y-protocols](https://github.com/yjs/y-protocols) wire format (sync messages with sub-types 0/1/2).

## Architecture

```
┌─────────────┐     WebSocket      ┌──────────┐
│  Yjs Client ├───────────────────►│          │
└─────────────┘                    │          │     ┌───────────┐
                                   │   Ydb    ├────►│   Store   │
┌─────────────┐     WebSocket      │          │     └───────────┘
│  Yjs Client ├───────────────────►│          │
└─────────────┘                    │          │     ┌─────────────┐
                                   │          ├────►│ Broadcaster │
                                   └──────────┘     └─────────────┘
```

### Pluggable interfaces

**Store** — append-only persistence for document operation logs:

```go
type Store interface {
    Append(room YjsRoomName, data []byte) (newOffset uint32, err error)
    ReadFrom(room YjsRoomName, offset uint32) ([]byte, uint32, error)
    Size(room YjsRoomName) (uint32, error)
    SetInitialContent(room YjsRoomName, data []byte) error
}
```

Built-in: `DiskStore` (file-per-room on local disk). Implement your own for Redis, S3, DynamoDB, etc.

**Broadcaster** — fan-out of updates to subscribers:

```go
type Broadcaster interface {
    Publish(room YjsRoomName, senderSessionID uint64, data []byte)
    Subscribe(room YjsRoomName, sessionID uint64) (<-chan []byte, error)
    Unsubscribe(room YjsRoomName, sessionID uint64)
}
```

Built-in: `LocalBroadcaster` (in-process). Implement your own for Redis Pub/Sub, NATS, etc.

### Config

```go
cfg := Config{
    SendBufferSize:   256,           // per-connection write channel buffer
    MaxMessageSize:   10 * 1024 * 1024,  // max WebSocket message size (10MB)
    MaxRoomSize:      50 * 1024 * 1024,  // max stored data per room (50MB)
    BroadcastBuffer:  64,            // per-subscriber broadcast channel buffer
    RoomIdleTimeout:  5 * time.Minute,   // idle room cleanup threshold
    RoomReapInterval: 1 * time.Minute,   // how often the reaper runs
}
```

### Room lifecycle

1. Room created on first client connection (lazy)
2. Session created per WebSocket connection, bound to one room
3. New sessions catch up from Store, then receive live broadcasts
4. When all sessions disconnect, room becomes idle
5. Room reaper removes idle rooms after `RoomIdleTimeout`

## Usage

### As a standalone server

```bash
go build -o ydb ./cli.go
./ydb start --dir /path/to/data
```

Listens on `:8899`. Clients connect to `ws://host:8899/ws/<roomname>`.

### As a library

```go
store := ydb.NewDiskStore("/data",
    ydb.WithMaxRoomSize(50*1024*1024),
    ydb.WithInitialContentProvider(func(room string) []byte {
        return loadTemplate(room)
    }),
)
broadcaster := ydb.NewLocalBroadcaster(64)
cfg := ydb.DefaultConfig()

server := ydb.InitYdb(store, broadcaster, cfg)
defer server.Close()

http.HandleFunc("/ws/", ydb.YdbWsConnectionHandler(server))
http.ListenAndServe(":8080", nil)
```

## Testing

The project includes a comprehensive test suite (38 tests + 4 benchmarks):

```bash
# Unit + integration tests with race detector
go test -race -count=1 -timeout 120s ./...

# Stress tests only
go test -run TestStress -count=1 -timeout 300s ./...

# Benchmarks
go test -bench=. -benchmem ./...
```

Test categories:
- **DiskStore** (9 tests) — append, read, offsets, size limits, concurrency, initial content
- **LocalBroadcaster** (8 tests) — pub/sub, sender exclusion, fanout, slow subscriber drop, cross-room isolation
- **Ydb core** (7 tests) — concurrent room creation, session lifecycle, room reaper, store persistence, catch-up
- **Integration** (6 tests) — full WebSocket stack: single/multi client sync, late joiner catch-up, disconnect cleanup, room isolation, reconnect
- **Stress** (4 tests) — 100 clients x 1 room, 50 rooms x 5 clients, rapid connect/disconnect, 1MB payloads
- **Benchmarks** (4) — DiskStore append (sequential + parallel), broadcaster fanout with 100 subscribers, room lookup throughput

## Wire protocol

Ydb speaks the [y-protocols](https://github.com/yjs/y-protocols) sync protocol:

| Message | Format |
|---------|--------|
| SyncStep1 | `[0][0][len][stateVector]` |
| SyncStep2 | `[0][1][len][diff]` |
| Update | `[0][2][len][update]` |
| Awareness | `[1][clientId][clock][...][json]` |

All integers are unsigned varints. Payloads are length-prefixed byte arrays.
