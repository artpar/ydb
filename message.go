package ydb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	messageSync                    = 0
	messageAwareness               = 1
	messageConfirmation            = 2
	messageSubConf                 = 3
	messageHostUnconfirmedByClient = 4
	messageConfirmedByHost         = 5
)

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
	case messageSync:
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

func createMessageUpdate(roomname YjsRoomName, offsetOrConf uint64, data []byte) []byte {
	buf := &bytes.Buffer{}
	writeUvarint(buf, messageSync)
	writeUvarint(buf, messageYjsUpdate)
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
	writeUvarint(buf, offset)
	return buf.Bytes()
}

func createMessageConfirmation(conf uint64) []byte {
	buf := &bytes.Buffer{}
	writeUvarint(buf, messageConfirmation)
	writeUvarint(buf, conf)
	return buf.Bytes()
}

const messageYjsSyncStep1 = 0
const messageYjsSyncStep2 = 1
const messageYjsUpdate = 2

func readStateVector(m message) []byte {
	ssLength, _ := binary.ReadUvarint(m)
	encoder := &bytes.Buffer{}
	for i := uint64(0); i < ssLength; i++ {
		client, _ := binary.ReadUvarint(m)
		clock, _ := binary.ReadUvarint(m)
		writeUvarint(encoder, client)
		writeUvarint(encoder, clock)
	}
	return encoder.Bytes()
}

func (ydb *Ydb) readUpdateMessage(m message, session *session) error {
	messageType, _ := binary.ReadUvarint(m)

	write := &bytes.Buffer{}

	var err error
	err = writeUvarint(write, messageSync)
	if err != nil {
		return err
	}

	switch messageType {
	case messageYjsSyncStep1:
		payload, _ := readPayload(m)
		err = writeUvarint(write, messageYjsSyncStep1)
		err = writePayload(write, payload)
	case messageYjsSyncStep2:
		payload, _ := readPayload(m)
		err = writeUvarint(write, messageYjsSyncStep2)
		err = writePayload(write, payload)
	case messageYjsUpdate:
		payload, _ := readPayload(m)
		err = writeUvarint(write, messageYjsUpdate)
		err = writePayload(write, payload)
	}
	if err != nil {
		return err
	}

	ydb.updateRoom(session.roomname, session, write.Bytes())
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
	len1, err := binary.ReadUvarint(m)
	if len1 > 1024*1024*1024 {
		return nil, errors.New("message payload too long")
	}
	if err != nil {
		return []byte{}, err
	}
	bs := make([]byte, len1)
	_, err = m.Read(bs)
	return bs, err
}

func writeUvarint(buf io.Writer, n uint64) error {
	bs := make([]byte, binary.MaxVarintLen64)
	len1 := binary.PutUvarint(bs, n)
	_, err := buf.Write(bs[:len1])
	return err
}

func writeString(buf io.Writer, str string) error {
	return writePayload(buf, []byte(str))
}

func writeRoomname(buf io.Writer, roomname YjsRoomName) error {
	return writeString(buf, string(roomname))
}

func writePayload(buf io.Writer, payload []byte) error {
	err := writeUvarint(buf, uint64(len(payload)))
	if err != nil {
		return err
	}
	_, err = buf.Write(payload)
	return err
}
