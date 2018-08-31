// Copyright 2017 The etcd Authors
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

    "github.com/tecbot/gorocksdb"
)

// Note: this code is adapted from bbolt backend to etcd
// ColumnFamily in rocksdb is treated as bucket in boltdb

// safeRangeBucket is a hack to avoid inadvertently reading duplicate keys;
// overwrites on a bucket should only fetch with limit=1, but safeRangeBucket
// is known to never overwrite any key so range is safe.
var safeRangeBucket = []byte("key")

type ReadTx interface {
	Lock()
	Unlock()

	UnsafeRange(bucketName []byte, key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte)
	UnsafeForEach(bucketName []byte, visitor func(k, v []byte) error) error
	UnsafeGet(bucketName []byte, key []byte) (val []byte)
}

type readTx struct {
	mu  sync.RWMutex
    buf txReadBuffer
    backend *backend

	// txmu protects accesses to buckets and tx on Range requests.
	txmu    sync.RWMutex
	tx      *gorocksdb.Transaction
	buckets map[string]*gorocksdb.ColumnFamilyHandle
}

func (rt *readTx) Lock()   { rt.mu.RLock() }
func (rt *readTx) Unlock() { rt.mu.RUnlock() }

func (rt *readTx) UnsafeGet(bucketName []byte, key []byte) (val []byte) {
	bn := string(bucketName)
	rt.txmu.RLock()
	bucket, ok := rt.buckets[bn]
	rt.txmu.RUnlock()
	if !ok {
		rt.txmu.Lock()
		bucket = rt.backend.buckets[bn]
		rt.buckets[bn] = bucket
		rt.txmu.Unlock()
	}

	// ignore missing bucket since may have been created in this batch
	if bucket == nil {
		return nil
	}

    ro := gorocksdb.NewDefaultReadOptions()
    defer ro.Destroy()
    val, err := rt.backend.db.GetCF(ro, bucket, key)
    if err != nil {
        plog.Fatalf("failed to get key %s from bucket %s", key, bucket) 
    }

    return val 
}

func (rt *readTx) UnsafeRange(bucketName, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	if endKey == nil {
		// forbid duplicates for single keys
		limit = 1
	}
	if limit <= 0 {
		limit = math.MaxInt64
	}
	if limit > 1 && !bytes.Equal(bucketName, safeRangeBucket) {
		panic("do not use unsafeRange on non-keys bucket")
	}
	keys, vals := rt.buf.Range(bucketName, key, endKey, limit)
	if int64(len(keys)) == limit {
		return keys, vals
	}

	// find/cache bucket
	bn := string(bucketName)
	rt.txmu.RLock()
	bucket, ok := rt.buckets[bn]
	rt.txmu.RUnlock()
	if !ok {
		rt.txmu.Lock()
		bucket = rt.backend.buckets[bn]
		rt.buckets[bn] = bucket
		rt.txmu.Unlock()
	}

	// ignore missing bucket since may have been created in this batch
	if bucket == nil {
        plog.Errorf("failed to find bucket %s", bn)
		return keys, vals
	}

    ro := gorocksdb.NewDefaultReadOptions()
    defer ro.Destroy()
    iter := rt.backend.db.NewIteratorCF(ro, bucket)
    defer iter.Close()
    for iter.Seek(key); iter.Valid(); iter.Next() {
        vals = append(vals, iter.Value().Data())
        keys = append(keys, iter.Key().Data())
        if limit == int64(len(keys)) {
            break
        }
    }

    return keys, vals
}

func (rt *readTx) UnsafeForEach(bucketName []byte, visitor func(k, v []byte) error) error {
	dups := make(map[string]struct{})
	getDups := func(k, v []byte) error {
		dups[string(k)] = struct{}{}
		return nil
	}
    /*
	visitNoDup := func(k, v []byte) error {
		if _, ok := dups[string(k)]; ok {
			return nil
		}
		return visitor(k, v)
	}
    */
	if err := rt.buf.ForEach(bucketName, getDups); err != nil {
		return err
	}
	// find/cache bucket
	bn := string(bucketName)
	rt.txmu.RLock()
	bucket, ok := rt.buckets[bn]
	rt.txmu.RUnlock()
	if !ok {
		rt.txmu.Lock()
		bucket = rt.backend.buckets[bn]
		rt.buckets[bn] = bucket
		rt.txmu.Unlock()
	}

	// ignore missing bucket since may have been created in this batch
	if bucket == nil {
        plog.Errorf("failed to find bucket %s", bn)
		return nil
	}

	rt.txmu.Lock()
    ro := gorocksdb.NewDefaultReadOptions()
    defer ro.Destroy()
    iter := rt.backend.db.NewIteratorCF(ro, bucket)
    defer iter.Close()
	rt.txmu.Unlock()
    for iter.SeekToFirst(); iter.Valid(); iter.Next() {
        if err := visitor(iter.Key().Data(), iter.Value().Data()); err != nil {
            return err
        }
    }
	return rt.buf.ForEach(bucketName, visitor)
}

func (rt *readTx) reset() {
	rt.buf.reset()
	rt.buckets = make(map[string]*gorocksdb.ColumnFamilyHandle)
	rt.tx = nil
}
