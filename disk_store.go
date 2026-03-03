package ydb

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type DiskStoreOption func(*DiskStore)

func WithMaxRoomSize(size uint32) DiskStoreOption {
	return func(ds *DiskStore) {
		ds.maxRoomSize = size
	}
}

func WithInitialContentProvider(fn func(roomName string) []byte) DiskStoreOption {
	return func(ds *DiskStore) {
		ds.initialContentProvider = fn
	}
}

type DiskStore struct {
	dir                    string
	maxRoomSize            uint32
	locks                  sync.Map
	initialContentProvider func(string) []byte
	initialized            sync.Map
}

func NewDiskStore(dir string, opts ...DiskStoreOption) Store {
	ds := &DiskStore{
		dir: dir,
	}
	for _, opt := range opts {
		opt(ds)
	}
	return ds
}

func (ds *DiskStore) roomMutex(room YjsRoomName) *sync.Mutex {
	v, _ := ds.locks.LoadOrStore(room, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func (ds *DiskStore) roomPath(room YjsRoomName) string {
	return filepath.Join(ds.dir, string(room))
}

func (ds *DiskStore) ensureInitialContent(room YjsRoomName) {
	if ds.initialContentProvider == nil {
		return
	}
	if _, loaded := ds.initialized.LoadOrStore(room, struct{}{}); loaded {
		return
	}
	path := ds.roomPath(room)
	if _, err := os.Stat(path); err == nil {
		return
	}
	content := ds.initialContentProvider(string(room))
	if len(content) > 0 {
		os.WriteFile(path, content, 0600)
	}
}

func (ds *DiskStore) Append(room YjsRoomName, data []byte) (uint32, error) {
	mu := ds.roomMutex(room)
	mu.Lock()
	defer mu.Unlock()

	ds.ensureInitialContent(room)
	path := ds.roomPath(room)

	if ds.maxRoomSize > 0 {
		fi, err := os.Stat(path)
		if err == nil {
			currentSize := uint32(fi.Size())
			if currentSize+uint32(len(data)) > ds.maxRoomSize {
				return currentSize, fmt.Errorf("room %s exceeds max size %d", room, ds.maxRoomSize)
			}
		}
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	_, err = f.Write(data)
	if err != nil {
		return 0, err
	}

	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return uint32(fi.Size()), nil
}

func (ds *DiskStore) ReadFrom(room YjsRoomName, offset uint32) ([]byte, uint32, error) {
	mu := ds.roomMutex(room)
	mu.Lock()
	defer mu.Unlock()

	ds.ensureInitialContent(room)
	path := ds.roomPath(room)

	f, err := os.OpenFile(path, os.O_RDONLY, 0600)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	size := uint32(fi.Size())
	if offset >= size {
		return nil, size, nil
	}

	if offset > 0 {
		_, err = f.Seek(int64(offset), 0)
		if err != nil {
			return nil, 0, err
		}
	}

	data := make([]byte, size-offset)
	_, err = f.Read(data)
	if err != nil {
		return nil, 0, err
	}
	return data, size, nil
}

func (ds *DiskStore) Size(room YjsRoomName) (uint32, error) {
	mu := ds.roomMutex(room)
	mu.Lock()
	defer mu.Unlock()

	ds.ensureInitialContent(room)
	path := ds.roomPath(room)

	fi, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	return uint32(fi.Size()), nil
}

func (ds *DiskStore) SetInitialContent(room YjsRoomName, data []byte) error {
	mu := ds.roomMutex(room)
	mu.Lock()
	defer mu.Unlock()

	path := ds.roomPath(room)
	return os.WriteFile(path, data, 0600)
}
