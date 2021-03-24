package ydb

import (
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

func createYdbTest(f func(ydbInstance *Ydb)) {
	dir := "_test"
	os.RemoveAll(dir)
	ydbInstance := InitYdb(dir)
	go setupWebsocketsListener(":9999", ydbInstance)
	time.Sleep(time.Second)
	f(ydbInstance)
	os.RemoveAll(dir)
}

// testGetRoom test if GetYjsRoom is safe for parallel access
func TestGetRoom(t *testing.T) {
	createYdbTest(func(ydbInstance *Ydb) {
		runTest := func(seed int, wg *sync.WaitGroup) {
			src := rand.NewSource(int64(seed))
			r := rand.New(src)
			var numOfTests uint64 = 10000
			var i uint64
			for ; i < numOfTests; i++ {
				roomname := YjsRoomName(strconv.FormatUint(r.Uint64()%numOfTests, 10))
				ydbInstance.GetYjsRoom(roomname)
			}
			wg.Done()
		}
		p := 100
		wg := new(sync.WaitGroup)
		wg.Add(p)
		for i := 0; i < p; i++ {
			go runTest(i, wg)
		}
		wg.Wait()
	})
}
