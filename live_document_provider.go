package ydb

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
)

type DocumentProvider interface {
	GetDocument(documentName YjsRoomName) *Document
	RegisterRoomUpdate(r *room, roomname YjsRoomName)
	ReadRoomSize(name YjsRoomName) uint32
}

type Document struct {
	contents  []byte
	name      YjsRoomName
	writepath string
}

func (d Document) SetInitialContent(initialContents []byte) {

	f, err := os.OpenFile(d.writepath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, stdPerms)
	if err != nil {
		panic(err)
	}
	f.Truncate(0)
	f.Seek(0, 0)
	debug("fswriter: opened file")
	if _, err = f.Write(initialContents); err != nil {
		panic(err)
	}
	debug("fswriter: writing file")
	f.Close()
}

func (d Document) GetInitialContentBytes() []byte {

	buf := bytes.Buffer{}
	f, err := os.OpenFile(d.writepath, os.O_RDONLY, stdPerms)
	if err != nil {
		panic(err)
	}
	io.Copy(&buf, f)
	f.Close()
	return buf.Bytes()
}

type DiskDocumentProvider struct {
	tempDir                   string
	fsAccessQueueLen          uint
	documentMap               map[YjsRoomName]Document
	documentAccessLock        sync.Mutex
	getDocumentInitialContent func(string) []byte
	fswriter                  *fswriter
	dl                        DocumentListener
}

func (ddp *DiskDocumentProvider) RegisterRoomUpdate(r *room, roomname YjsRoomName) {
	ddp.GetDocument(roomname)
	ddp.fswriter.queue <- roomUpdate{r, roomname}
}

func (ddp *DiskDocumentProvider) GetDocumentInitialContent(s string) []byte {
	return ddp.dl.GetDocumentInitialContent(s)
}

func (ddp *DiskDocumentProvider) ReadRoomSize(name YjsRoomName) uint32 {
	ddp.GetDocument(name)
	return ddp.fswriter.readRoomSize(fmt.Sprintf("%v%v%v", ddp.tempDir, string(os.PathSeparator), name))
}

type DocumentListener struct {
	GetDocumentInitialContent func(string) []byte
	SetDocumentInitialContent func(string, []byte)
}

func NewDiskDocumentProvider(tempDir string, fsAccessQueueLen uint, documentListener DocumentListener) DocumentProvider {
	fswriter := &fswriter{}

	fswriter.queue = make(chan roomUpdate, fsAccessQueueLen)
	// TODO: start several write tasks
	/*
		for i := 0; i < writeConcurrency; i++ {wsConn
			go fswriter.startWriteTask()
		}
	*/
	go fswriter.startWriteTask(tempDir)

	provider := DiskDocumentProvider{
		documentMap:        make(map[YjsRoomName]Document),
		tempDir:            tempDir,
		documentAccessLock: sync.Mutex{},
		dl:                 documentListener,
		fswriter:           fswriter,
	}

	return &provider

}

func (ddp *DiskDocumentProvider) newDocument(name YjsRoomName) Document {

	writeFilepath := fmt.Sprintf("%v%v%v", ddp.tempDir, string(os.PathSeparator), name)

	document := Document{
		writepath: writeFilepath,
		name:      name,
	}

	initialContent := ddp.GetDocumentInitialContent(string(name))
	if len(initialContent) > 0 {
		document.SetInitialContent(initialContent)
	}

	return document
}

func (ddp *DiskDocumentProvider) GetDocument(documentName YjsRoomName) *Document {

	ddp.documentAccessLock.Lock()
	defer ddp.documentAccessLock.Unlock()
	document, ok := ddp.documentMap[documentName]
	if !ok {
		document = ddp.newDocument(documentName)
		ddp.documentMap[documentName] = document
	}

	return &document
}
