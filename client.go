package ydb

import (
	"bytes"
	"encoding/binary"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type roomstate struct {
	offset int // TODO: this should reflect the offset stored by the server
	data   []byte
}

type client struct {
	conn     *websocket.Conn
	closedWG sync.WaitGroup
	send     chan []byte
	// outgoing messages that were not confirmed by the server
	unconfirmed              map[uint64][]byte
	nextExpectedConfirmation uint64
	nextConfirmationNumber   uint64
	rooms                    map[YjsRoomName]roomstate
	currentRoom              YjsRoomName
	closeOnce                sync.Once
}

func newClient() *client {
	return &client{
		send:        make(chan []byte, 10),
		unconfirmed: make(map[uint64][]byte),
		rooms:       make(map[YjsRoomName]roomstate),
	}
}

func (client *client) readMessage(message []byte) error {
	buf := bytes.NewBuffer(message)
	var err error
	switch messageType, err := buf.ReadByte(); messageType {
	case messageSync:
		if err != nil {
			return err
		}
		_, err = binary.ReadUvarint(buf) // syncSubType
		if err != nil {
			return err
		}
		payload, err := readPayload(buf)
		if err != nil {
			return err
		}
		room := client.rooms[client.currentRoom]
		room.data = append(room.data, payload...)
		client.rooms[client.currentRoom] = room
	case messageConfirmation:
		if err != nil {
			return err
		}
		conf, err := binary.ReadUvarint(buf)
		if err != nil {
			return err
		}
		for conf >= client.nextExpectedConfirmation {
			delete(client.unconfirmed, client.nextExpectedConfirmation)
			client.nextExpectedConfirmation++
		}
	}
	return err
}

func (client *client) WaitForConfs() {
	for len(client.unconfirmed) != 0 {
		time.Sleep(time.Millisecond * 10)
	}
}

func (client *client) Connect(url string) (err error) {
	if client.conn == nil {
		client.closeOnce = sync.Once{}
		client.closedWG = sync.WaitGroup{}
		client.closedWG.Add(2)
		client.conn, _, err = websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			return err
		}
		doneReading := make(chan struct{})
		// read pump
		go func() {
			defer func() {
				client.closedWG.Done()
			}()
			for {
				_, message, err := client.conn.ReadMessage()
				if err != nil {
					client.closeOnce.Do(func() { close(client.send) })
					close(doneReading)
					return
				}
				if rerr := client.readMessage(message); rerr != nil {
					log.Printf("failed to read message from client: %v - %v", rerr, message)
				}
			}
		}()
		// write pump
		go func() {
			defer func() {
				client.conn.Close()
				client.closedWG.Done()
			}()
			for m := range client.send {
				client.conn.WriteMessage(websocket.BinaryMessage, m)
			}
			client.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			select {
			case <-doneReading:
			case <-time.After(time.Second):
			}
		}()
	}
	return
}

func (client *client) Disconnect() {
	if client.conn != nil {
		client.closeOnce.Do(func() { close(client.send) })
		client.closedWG.Wait()
		client.conn.Close()
		client.conn = nil
	}
}

func (client *client) Subscribe(subs ...subDefinition) {
	if len(subs) > 0 {
		client.currentRoom = subs[0].roomname
	}
	conf := client.nextConfirmationNumber
	m := createMessageSubscribe(conf, subs...)
	client.unconfirmed[conf] = m
	client.nextConfirmationNumber++
	client.send <- m
}

func (client *client) UpdateRoom(roomname YjsRoomName, data []byte) {
	m := createMessageUpdate(roomname, 0, data)
	roomstate := client.rooms[roomname]
	roomstate.data = append(roomstate.data, data...)
	client.rooms[roomname] = roomstate
	client.send <- m
}
