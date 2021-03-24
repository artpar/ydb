package ydb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// message type constants
// make sure to update message.js in ydb-client when updating these values..
const (
	messageUpdate                  = 0
	messageAwareness               = 1
	messageConfirmation            = 2
	messageSubConf                 = 3
	messageHostUnconfirmedByClient = 4
	messageConfirmedByHost         = 5
)

// a message is structured as [length of payload, payload], where payload is [messageType, typePayload]
type message interface {
	ReadByte() (byte, error)
	Read(p []byte) (int, error)
}

func (ydb *Ydb) readMessage(m message, session *session) (err error) {
	messageType, err := binary.ReadUvarint(m)
	if err != nil {
		return err
	}
	switch messageType {
	case messageAwareness:
		debug("reading sub message")
		err = ydb.readSubMessage(m, session)
	case messageUpdate:
		debug("reading update message")
		err = ydb.readUpdateMessage(m, session)
	case messageConfirmation:
		debug("reading conf message")
		err = readConfirmationMessage(m, session)
	default:
		debug(fmt.Sprintf("received unknown message type %d", messageType))
	}
	return err
}

func (ydb *Ydb) readSubMessage(m message, session *session) error {
	subConfBuf := &bytes.Buffer{}
	writeUvarint(subConfBuf, messageAwareness)
	clientId, _ := binary.ReadUvarint(m)
	clock, _ := binary.ReadUvarint(m)
	var1, _ := binary.ReadUvarint(m)
	var2, _ := binary.ReadUvarint(m)
	writeUvarint(subConfBuf, clientId)
	writeUvarint(subConfBuf, clock)
	writeUvarint(subConfBuf, var1)
	writeUvarint(subConfBuf, var2)

	jsonString, _ := readString(m)
	var clientState map[string]interface{}
	json.Unmarshal([]byte(jsonString), &clientState)
	fmt.Printf("var 1 : %v, var2: %v: ", var1, var2)
	println("subs message: " + jsonString)

	//
	//room := ydb.GetYjsRoom(session.roomname)
	writeString(subConfBuf, jsonString)

	session.send(subConfBuf.Bytes())
	return nil
}

func readConfirmationMessage(m message, session *session) (err error) {
	conf, err := binary.ReadUvarint(m)
	session.serverConfirmation.clientConfirmed(conf)
	return
}

type subDefinition struct {
	roomname YjsRoomName
	offset   uint64
	rsid     uint64
}

func createMessageSubscribe(conf uint64, subs ...subDefinition) []byte {
	buf := &bytes.Buffer{}
	writeUvarint(buf, messageAwareness)
	writeUvarint(buf, conf)
	writeUvarint(buf, uint64(len(subs)))
	for _, sub := range subs {
		writeRoomname(buf, sub.roomname)
		writeUvarint(buf, sub.offset)
		writeUvarint(buf, sub.rsid)
	}
	return buf.Bytes()
}

//
func createMessageUpdate(roomname YjsRoomName, offsetOrConf uint64, data []byte) []byte {
	buf := &bytes.Buffer{}
	writeUvarint(buf, messageUpdate)
	writeUvarint(buf, offsetOrConf)
	writeRoomname(buf, roomname)
	writePayload(buf, data)
	return buf.Bytes()
}

func createMessageHostUnconfirmedByClient(clientConf uint64, offset uint64) []byte {
	buf := &bytes.Buffer{}
	writeUvarint(buf, messageHostUnconfirmedByClient)
	writeUvarint(buf, clientConf)
	writeUvarint(buf, offset)
	return buf.Bytes()
}

func createMessageConfirmedByHost(roomname YjsRoomName, offset uint64) []byte {
	buf := &bytes.Buffer{}
	writeUvarint(buf, messageConfirmedByHost)
	writeRoomname(buf, roomname)
	writeUvarint(buf, offset)
	return buf.Bytes()
}

func createMessageConfirmation(conf uint64) []byte {
	buf := &bytes.Buffer{}
	writeUvarint(buf, messageConfirmation)
	writeUvarint(buf, conf)
	return buf.Bytes()
}

func (ydb *Ydb) readUpdateMessage(m message, session *session) error {
	confirmation, _ := binary.ReadUvarint(m)
	roomname, _ := readRoomname(m)
	bs, _ := readPayload(m)
	// send the rest of message
	ydb.updateRoom(roomname, session, confirmation, bs)
	return nil
}

func readString(m message) (string, error) {
	bs, err := readPayload(m)
	return string(bs), err
}

func readRoomname(m message) (YjsRoomName, error) {
	name, err := readString(m)
	return YjsRoomName(name), err
}

func readPayload(m message) ([]byte, error) {
	len, _ := binary.ReadUvarint(m)
	bs := make([]byte, len)
	m.Read(bs)
	return bs, nil
}

func writeUvarint(buf io.Writer, n uint64) error {
	bs := make([]byte, binary.MaxVarintLen64)
	len := binary.PutUvarint(bs, n)
	buf.Write(bs[:len])
	return nil
}

func writeString(buf io.Writer, str string) error {
	return writePayload(buf, []byte(str))
}

func writeRoomname(buf io.Writer, roomname YjsRoomName) error {
	return writeString(buf, string(roomname))
}

func writePayload(buf io.Writer, payload []byte) error {
	writeUvarint(buf, uint64(len(payload)))
	buf.Write(payload)
	return nil
}
