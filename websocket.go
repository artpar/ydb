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
	writeWait  = 50 * time.Second
	pongWait   = 50 * time.Second
	pingPeriod = (pongWait * 9) / 10
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type wsConn struct {
	conn           *websocket.Conn
	session        *session
	send           chan *websocket.PreparedMessage
	ydb            *Ydb
	closeWritePump chan struct{}
}

func newWsConn(session *session, conn *websocket.Conn, ydb *Ydb) *wsConn {
	return &wsConn{
		conn:           conn,
		session:        session,
		ydb:            ydb,
		send:           make(chan *websocket.PreparedMessage, ydb.cfg.SendBufferSize),
		closeWritePump: make(chan struct{}),
	}
}

func (wsConn *wsConn) WriteMessage(m []byte, pm *websocket.PreparedMessage) {
	defer func() {
		recover()
	}()
	debugMessageType("sending message to client..", m)
	wsConn.send <- pm
}

func (wsConn *wsConn) readPump() {
	wsConn.conn.SetReadLimit(wsConn.ydb.cfg.MaxMessageSize)
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
			err := wsConn.ydb.readMessage(mbuffer, wsConn.session)
			if err != nil {
				break
			}
		}
	}
	close(wsConn.closeWritePump)
	wsConn.conn.Close()
	debug("ending read pump for ws conn")
}

func (wsConn *wsConn) writePump() {
	conn := wsConn.conn
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		debug("ending write pump for ws conn")
		ticker.Stop()
		wsConn.session.removeConn(wsConn.ydb)
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

		session := ydbInstance.createSession(roomname)
		wsConn := newWsConn(session, conn, ydbInstance)
		session.setConn(wsConn)

		go ydbInstance.subscribeRoom(session, 0)

		go wsConn.readPump()
		go wsConn.writePump()
	}
}

func setupWebsocketsListener(addr string, ydbInstance *Ydb) {
	http.HandleFunc("/ws", YdbWsConnectionHandler(ydbInstance))
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		exitBecause(err.Error())
	}
}
