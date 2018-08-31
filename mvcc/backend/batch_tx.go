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
	"math"
	"sync"
	"sync/atomic"
	"time"

    "github.com/tecbot/gorocksdb"
	"go.uber.org/zap"
)

type BatchTx interface {
	ReadTx
	UnsafeCreateBucket(name []byte)
	UnsafePut(bucketName []byte, key []byte, value []byte)
	UnsafeSeqPut(bucketName []byte, key []byte, value []byte)
	UnsafeDelete(bucketName []byte, key []byte)
	// Commit commits a previous tx and begins a new writable one.
	Commit()
	// CommitAndStop commits the previous tx and does not create a new one.
	CommitAndStop()
}

type batchTx struct {
	sync.Mutex
	tx      *gorocksdb.Transaction
	backend *backend

	pending int
    buckets map[string]*gorocksdb.ColumnFamilyHandle
}

func (t *batchTx) UnsafeCreateBucket(name []byte) {
    opts := gorocksdb.NewDefaultOptions()
    opts.SetCreateIfMissingColumnFamilies(true)
    opts.SetCreateIfMissing(true)
    plog.Printf("creating bucket %s", name)
    if t.backend == nil {
        plog.Error("batch tx has null pointer to backend")
    } else if t.backend.db == nil {
        plog.Error("backend has null pointer for db")
    }
    cf, err := t.backend.db.CreateColumnFamily(opts, string(name))
    plog.Printf("created bucket %s", name)
    if err != nil { 
	    plog.Fatalf("cannot create bucket %s (%v)", name, err)
    }
    if cf != nil {
        // cache the handle
        plog.Printf("caching handle for bucket %s", name)
        t.buckets[string(name)] = cf
        t.backend.buckets[string(name)] = cf
    }
    t.pending++
}

func (t *batchTx) UnsafeGet(bucketName []byte, key []byte) (val []byte) {
    bn := string(bucketName)
    bucket, ok := t.buckets[bn]
    if !ok {
        bucket = t.buckets[string(bucketName)]
        t.buckets[bn] = bucket
    }

    // ignore missing bucket since may have been created in this batch
    if bucket == nil {
        return nil
    }

    ro := gorocksdb.NewDefaultReadOptions()
    defer ro.Destroy()
    val, err := t.backend.db.GetCF(ro, bucket, key)
    if err != nil {
        plog.Printf("failed to get key %s, error %s", key, err)
    }

    return val
}

// UnsafePut must be called holding the lock on the tx.
func (t *batchTx) UnsafePut(bucketName []byte, key []byte, value []byte) {
	t.unsafePut(bucketName, key, value, false)
}

// UnsafeSeqPut must be called holding the lock on the tx.
func (t *batchTx) UnsafeSeqPut(bucketName []byte, key []byte, value []byte) {
	t.unsafePut(bucketName, key, value, true)
}

func (t *batchTx) unsafePut(bucketName []byte, key []byte, value []byte, seq bool) {
	bucket := t.buckets[string(bucketName)]
	if bucket == nil {
		if t.backend.lg != nil {
			t.backend.lg.Fatal(
				"failed to find a bucket",
				zap.String("bucket-name", string(bucketName)),
			)
		} else {
			plog.Fatalf("bucket %s does not exist", bucketName)
		}
	}
    wo := gorocksdb.NewDefaultWriteOptions()
    defer wo.Destroy()
    //plog.Infof("put key %s into column family %s", key, bucketName)
	if err := t.backend.db.PutCF(wo, bucket, key, value); err != nil {
		if t.backend.lg != nil {
			t.backend.lg.Fatal(
				"failed to write to a bucket",
				zap.String("bucket-name", string(bucketName)),
				zap.Error(err),
			)
		} else {
			plog.Fatalf("cannot put key into bucket (%v)", err)
		}
	}
    //plog.Infof("put key %s into column family %s succeeded", key, bucketName)
	t.pending++
}

// UnsafeRange must be called holding the lock on the tx.
func (t *batchTx) UnsafeRange(bucketName, key, endKey []byte, limit int64) (keys [][]byte, vs [][]byte) {
	bucket := t.buckets[string(bucketName)]
	if bucket == nil {
		if t.backend.lg != nil {
			t.backend.lg.Fatal(
				"failed to find a bucket",
				zap.String("bucket-name", string(bucketName)),
			)
		} else {
			plog.Fatalf("bucket %s does not exist", bucketName)
		}
	}

	if limit <= 0 {
		limit = math.MaxInt64
	}
	var isMatch func(b []byte) bool
	if len(endKey) > 0 {
		isMatch = func(b []byte) bool { return bytes.Compare(b, endKey) < 0 }
	} else {
		isMatch = func(b []byte) bool { return bytes.Equal(b, key) }
		limit = 1
	}

    ro := gorocksdb.NewDefaultReadOptions()
    defer ro.Destroy()
    iter := t.backend.db.NewIteratorCF(ro, bucket)
    defer iter.Close()
    for iter.Seek(key); iter.Valid() && isMatch(iter.Key().Data()); iter.Next() {
        vs = append(vs, iter.Value().Data())
		keys = append(keys, iter.Key().Data())
		if limit == int64(len(keys)) {
			break
		}
	}
	return keys, vs
}

// UnsafeDelete must be called holding the lock on the tx.
func (t *batchTx) UnsafeDelete(bucketName []byte, key []byte) {
	bucket := t.buckets[string(bucketName)]
	if bucket == nil {
		if t.backend.lg != nil {
			t.backend.lg.Fatal(
				"failed to find a bucket",
				zap.String("bucket-name", string(bucketName)),
			)
		} else {
			plog.Fatalf("bucket %s does not exist", bucketName)
		}
	}
    wo := gorocksdb.NewDefaultWriteOptions()
    defer wo.Destroy()
	err := t.backend.db.DeleteCF(wo, bucket, key)
	if err != nil {
		if t.backend.lg != nil {
			t.backend.lg.Fatal(
				"failed to delete a key",
				zap.String("bucket-name", string(bucketName)),
				zap.Error(err),
			)
		} else {
			plog.Fatalf("cannot delete key from bucket (%v)", err)
		}
	}
	t.pending++
}

// UnsafeForEach must be called holding the lock on the tx.
func (t *batchTx) UnsafeForEach(bucketName []byte, visitor func(k, v []byte) error) error {
	bucket := t.buckets[string(bucketName)]
	if bucket == nil {
		if t.backend.lg != nil {
			t.backend.lg.Fatal(
				"failed to find a bucket",
				zap.String("bucket-name", string(bucketName)),
			)
		} else {
			plog.Fatalf("bucket %s does not exist", bucketName)
		}
	}
    ro := gorocksdb.NewDefaultReadOptions()
    defer ro.Destroy()
    iter := t.backend.db.NewIteratorCF(ro, bucket)
    defer iter.Close()
    for iter.SeekToFirst(); iter.Valid(); iter.Next() {
        if err := visitor(iter.Key().Data(), iter.Value().Data()); err != nil {
            return err
        }
	}
	return nil
}

// Commit commits a previous tx and begins a new writable one.
func (t *batchTx) Commit() {
	t.Lock()
	t.commit(false)
	t.Unlock()
}

// CommitAndStop commits the previous tx and does not create a new one.
func (t *batchTx) CommitAndStop() {
	t.Lock()
	t.commit(true)
	t.Unlock()
}

func (t *batchTx) Lock() {
    plog.Print("geting lock to mutex")
	t.Mutex.Lock()
}

func (t *batchTx) Unlock() {
	if t.pending >= t.backend.batchLimit {
		t.commit(false)
	}
	t.Mutex.Unlock()
}

func (t *batchTx) safePending() int {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	return t.pending
}

func (t *batchTx) commit(stop bool) {
	// commit the last tx
	if t.tx != nil {
		if t.pending == 0 && !stop {
            plog.Printf("no pending transactions to commit")
			return
		}

		start := time.Now()
   
        plog.Printf("commit transaction")
		err := t.tx.Commit()

		//rebalanceSec.Observe(t.tx.Stats().RebalanceTime.Seconds())
		//spillSec.Observe(t.tx.Stats().SpillTime.Seconds())
		//writeSec.Observe(t.tx.Stats().WriteTime.Seconds())
		commitSec.Observe(time.Since(start).Seconds())
		atomic.AddInt64(&t.backend.commits, 1)

		t.pending = 0
		if err != nil {
			if t.backend.lg != nil {
				t.backend.lg.Fatal("failed to commit tx", zap.Error(err))
			} else {
				plog.Fatalf("cannot commit tx (%s)", err)
			}
		}
	}
	if !stop {
        plog.Printf("beginning transaction") 
		t.tx = t.backend.begin(true)
	}
}

