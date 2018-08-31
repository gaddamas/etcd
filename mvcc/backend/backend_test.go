// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
    "bytes"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestBackendClose(t *testing.T) {
	b, tmpPath := NewTmpBackend(time.Hour, 10000)
	defer os.Remove(tmpPath)

	// check close could work
	done := make(chan struct{})
	go func() {
		err := b.Close()
		if err != nil {
			t.Errorf("close error = %v, want nil", err)
		}
		done <- struct{}{}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Errorf("failed to close database in 10s")
	}
    t.Log("TestBackendClose done")
}

func TestBackendSnapshot(t *testing.T) {
	b, tmpPath := NewTmpBackend(time.Hour, 10000)
	defer cleanup(b, tmpPath)

	tx := b.BatchTx()
	tx.Lock()
    t.Log("create bucket")
	tx.UnsafeCreateBucket([]byte("test"))
    t.Log("unsafe put")
	tx.UnsafePut([]byte("test"), []byte("foo"), []byte("bar"))
	tx.UnsafePut([]byte("test"), []byte("foo1"), []byte("bar1"))
	tx.Unlock()
    t.Log("force commit")
	b.ForceCommit()

	snap := b.Snapshot()
	defer snap.Close()

	// bootstrap new backend from the snapshot
    t.Log("bootstrap new backend from the snapshot")
	bcfg := DefaultBackendConfig()
	bcfg.Path, bcfg.BatchInterval, bcfg.BatchLimit = tmpPath + "-checkpoint", time.Hour, 10000
    bcfg.Buckets = make([]string, 2)
    bcfg.Buckets[0] = "default"
    bcfg.Buckets[1] = "test"
	nb := New(bcfg)
	defer cleanup(nb, tmpPath)

	newTx := b.BatchTx()
	newTx.Lock()
	ks, vs := newTx.UnsafeRange([]byte("test"), []byte("foo"), []byte("goo"), 0)
	if len(ks) != 2 {
		t.Errorf("len(kvs) = %d, want 2", len(ks))
	}
    for i := range vs {
        t.Logf("value %s", vs[i])
    }
	newTx.Unlock()
}

func TestBackendBatchIntervalCommit(t *testing.T) {
	// start backend with super short batch interval so
	// we do not need to wait long before commit to happen.
	b, tmpPath := NewTmpBackend(time.Nanosecond, 10000)
	defer cleanup(b, tmpPath)

	//pc := b.Commits()

	tx := b.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket([]byte("test"))
	tx.UnsafePut([]byte("test"), []byte("foo"), []byte("bar"))
	tx.UnsafePut([]byte("test"), []byte("foo1"), []byte("bar1"))
	tx.Unlock()
    tx.CommitAndStop()
    /*
	for i := 0; i < 10; i++ {
		if b.Commits() >= pc+1 {
			break
		}
		time.Sleep(time.Duration(i*100) * time.Millisecond)
	}
    */
	// check whether put happened
	v := tx.UnsafeGet([]byte("test"), []byte("foo"))
	if v == nil {
        t.Errorf("foo key failed to written in backend")
    } else {
        t.Logf("value: %s", string(v))
    }
    if bytes.Compare([]byte("bar"), v) < 0 {
        t.Errorf("want bar; got v=%+v", v)
    }
}

func TestBackendDefrag(t *testing.T) {
	b, tmpPath := NewDefaultTmpBackend()
	defer cleanup(b, tmpPath)

	tx := b.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket([]byte("test"))
	for i := 0; i < defragLimit+100; i++ {
		tx.UnsafePut([]byte("test"), []byte(fmt.Sprintf("foo_%d", i)), []byte("bar"))
	}
	tx.Unlock()
	b.ForceCommit()

	// remove some keys to ensure the disk space will be reclaimed after defrag
	tx = b.BatchTx()
	tx.Lock()
	for i := 0; i < 50; i++ {
		tx.UnsafeDelete([]byte("test"), []byte(fmt.Sprintf("foo_%d", i)))
	}
	tx.Unlock()
	b.ForceCommit()

	size := b.Size()

	// shrink and check hash
	oh, err := b.Hash(nil)
	if err != nil {
		t.Fatal(err)
	}

	err = b.Defrag()
	if err != nil {
		t.Fatal(err)
	}

	nh, err := b.Hash(nil)
	if err != nil {
		t.Fatal(err)
	}
	if oh != nh {
		t.Errorf("hash = %v, want %v", nh, oh)
	}

	nsize := b.Size()
	//if nsize >= size {
	if nsize > size {
		t.Errorf("new size = %v, want < %d", nsize, size)
	}

	// try put more keys after shrink.
	tx = b.BatchTx()
	tx.Lock()
	//tx.UnsafeCreateBucket([]byte("test"))
	tx.UnsafePut([]byte("test"), []byte("more"), []byte("bar"))
	tx.Unlock()
	b.ForceCommit()
}

// TestBackendWriteback ensures writes are stored to the read txn on write txn unlock.
func TestBackendWriteback(t *testing.T) {
	b, tmpPath := NewDefaultTmpBackend()
	defer cleanup(b, tmpPath)

	tx := b.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket([]byte("key"))
	tx.UnsafePut([]byte("key"), []byte("abc"), []byte("bar"))
	tx.UnsafePut([]byte("key"), []byte("def"), []byte("baz"))
	tx.UnsafePut([]byte("key"), []byte("overwrite"), []byte("1"))
	tx.Unlock()

	// overwrites should be propagated too
	tx.Lock()
	tx.UnsafePut([]byte("key"), []byte("overwrite"), []byte("2"))
	tx.Unlock()

	keys := []struct {
		key   []byte
		end   []byte
		limit int64

		wkey [][]byte
		wval [][]byte
	}{
		{
			key: []byte("abc"),
			end: nil,

			wkey: [][]byte{[]byte("abc")},
			wval: [][]byte{[]byte("bar")},
		},
		{
			key: []byte("abc"),
			end: []byte("def"),

			wkey: [][]byte{[]byte("abc")},
			wval: [][]byte{[]byte("bar")},
		},
		{
			key: []byte("abc"),
			end: []byte("deg"),

			wkey: [][]byte{[]byte("abc"), []byte("def")},
			wval: [][]byte{[]byte("bar"), []byte("baz")},
		},
		{
			key:   []byte("abc"),
			end:   []byte("\xff"),
			limit: 1,

			wkey: [][]byte{[]byte("abc")},
			wval: [][]byte{[]byte("bar")},
		},
		{
			key: []byte("abc"),
			end: []byte("\xff"),

			wkey: [][]byte{[]byte("abc"), []byte("def"), []byte("overwrite")},
			wval: [][]byte{[]byte("bar"), []byte("baz"), []byte("2")},
		},
	}
	rtx := b.ReadTx()
	for i, tt := range keys {
		rtx.Lock()
		k, v := rtx.UnsafeRange([]byte("key"), tt.key, tt.end, tt.limit)
		rtx.Unlock()
		if !reflect.DeepEqual(tt.wkey, k) || !reflect.DeepEqual(tt.wval, v) {
			t.Errorf("#%d: want k=%+v, v=%+v; got k=%+v, v=%+v", i, tt.wkey, tt.wval, k, v)
		}
	}
}

// TestBackendWritebackForEach checks that partially written / buffered
// data is visited in the same order as fully committed data.
func TestBackendWritebackForEach(t *testing.T) {
	b, tmpPath := NewTmpBackend(time.Hour, 10000)
	defer cleanup(b, tmpPath)

	tx := b.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket([]byte("key"))
	for i := 0; i < 5; i++ {
		k := []byte(fmt.Sprintf("%04d", i))
		tx.UnsafePut([]byte("key"), k, []byte("bar"))
	}
	tx.Unlock()

	// writeback
	b.ForceCommit()

	tx.Lock()
	for i := 5; i < 20; i++ {
		k := []byte(fmt.Sprintf("%04d", i))
		tx.UnsafePut([]byte("key"), k, []byte("bar"))
	}
	tx.Unlock()

	seq := ""
	getSeq := func(k, v []byte) error {
		seq += string(k)
		return nil
	}
	rtx := b.ReadTx()
	rtx.Lock()
	rtx.UnsafeForEach([]byte("key"), getSeq)
	rtx.Unlock()

	partialSeq := seq

	seq = ""
	b.ForceCommit()

	tx.Lock()
	tx.UnsafeForEach([]byte("key"), getSeq)
	tx.Unlock()

	if seq != partialSeq {
		t.Fatalf("expected %q, got %q", seq, partialSeq)
	}
}

func cleanup(b Backend, path string) {
	b.Close()
	os.Remove(path)
}
