package ydb

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 50 * time.Second // TODO: make this configurable

	// Time allowed to read the next pong message from the peer.
	pongWait = 50 * time.Second // TODO: make this configurable

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 10000000
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // TODO: implement origin checking
	},
}

type wsServer struct {
}

type wsConn struct {
	conn           *websocket.Conn
	session        *session
	send           chan *websocket.PreparedMessage
	ydb            *Ydb
	closeWritePump chan struct{}
}

func newWsConn(session *session, conn *websocket.Conn, ydbi *Ydb) *wsConn {
	return &wsConn{
		conn:           conn,
		session:        session,
		ydb:            ydbi,
		send:           make(chan *websocket.PreparedMessage, 5),
		closeWritePump: make(chan struct{}, 0),
	}
}

func (wsConn *wsConn) WriteMessage(m []byte, pm *websocket.PreparedMessage) {
	defer func() {
		recover() // recover if channel is already closed
	}()
	debugMessageType("sending message to client..", m)
	wsConn.send <- pm
}

func (wsConn *wsConn) readPump() {
	wsConn.conn.SetReadLimit(maxMessageSize)
	wsConn.conn.SetReadDeadline(time.Now().Add(pongWait))
	wsConn.conn.SetPongHandler(func(string) error {
		wsConn.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	for {
		_, message, err := wsConn.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("ydb error: %v", err)
			}
			break
		}
		mbuffer := bytes.NewBuffer(message)
		for {
			err := wsConn.ydb.readMessage(mbuffer, wsConn.session, nil)
			if err != nil {
				break
			}
		}
	}
	close(wsConn.closeWritePump)
	wsConn.conn.Close()
	debug("ending read pump for ws conn")
	// TODO: unregister conn from ydb
}

var SessionIdMap = make(map[string]uint64)

func (wsConn *wsConn) writePump() {
	conn := wsConn.conn
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		debug("ending write pump for ws conn")
		ticker.Stop()
		wsConn.session.removeConn(wsConn, wsConn.ydb)
		close(wsConn.send)
		conn.Close()
	}()
	for {
		select {
		case <-wsConn.closeWritePump:
			return
		case message, ok := <-wsConn.send:
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// The hub closed the channel.
				conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			err := conn.WritePreparedMessage(message)
			if err != nil {
				fmt.Println("server error when writing prepared message to conn", err)
				return
			}
		case <-ticker.C:
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := wsConn.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func YdbWsConnectionHandler(ydbInstance *Ydb) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("new client..")

		roomnameInterface := r.Context().Value("roomname")

		urlParts := strings.Split(r.URL.Path, "/")
		roomname := urlParts[len(urlParts)-1]

		if roomnameInterface != nil {
			roomname = roomnameInterface.(string)
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			fmt.Printf("error: error upgrading client %s", err.Error())
			return
		}
		var sessionid uint64               // TODO: get the sessionid from http headers
		sessionid = SessionIdMap[roomname] // ydbInstance.sessionIdFetcher.GetSessionId(r, roomname)
		var session *session
		if sessionid == 0 {
			session = ydbInstance.createSession(roomname, nil)
			SessionIdMap[roomname] = session.sessionid
		} else {
			session = ydbInstance.getSession(sessionid)
		}
		wsConn := newWsConn(session, conn, ydbInstance)
		session.add(wsConn)

		go ydbInstance.subscribeRoom(session, uint32(0), nil)

		go wsConn.readPump()
		go wsConn.writePump()
	}
}

func setupWebsocketsListener(addr string, ydbInstance *Ydb) {
	// TODO: only set this if in testing mode!
	//http.HandleFunc("/clearAll", func(w http.ResponseWriter, r *http.Request) {
	//	ydbInstance.UnsafeClearAllYdbContent()
	//	w.WriteHeader(200)
	//	fmt.Fprintf(w, "OK")
	//})
	http.HandleFunc("/ws", YdbWsConnectionHandler(ydbInstance))
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		exitBecause(err.Error())
	}
}
