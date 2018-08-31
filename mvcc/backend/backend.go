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
    "math"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

    "github.com/tecbot/gorocksdb"

	"github.com/coreos/pkg/capnslog"
	//humanize "github.com/dustin/go-humanize"
	"go.uber.org/zap"
)

var (
	defaultBatchLimit    = 10000
	defaultBatchInterval = 100 * time.Millisecond

	defragLimit = 10000

	// initialMmapSize is the initial size of the mmapped region. Setting this larger than
	// the potential max db size can prevent writer from blocking reader.
	// This only works for linux.
	initialMmapSize = uint64(10 * 1024 * 1024 * 1024)

	plog = capnslog.NewPackageLogger("github.com/coreos/etcd", "mvcc/backend")

	// minSnapshotWarningTimeout is the minimum threshold to trigger a long running snapshot warning.
	minSnapshotWarningTimeout = 30 * time.Second
)

type Backend interface {
	ReadTx() ReadTx
	BatchTx() BatchTx

	Snapshot() Snapshot
	Hash(ignores map[IgnoreKey]struct{}) (uint32, error)
	// Size returns the current size of the backend physically allocated.
	// The backend can hold DB space that is not utilized at the moment,
	// since it can conduct pre-allocation or spare unused space for recycling.
	// Use SizeInUse() instead for the actual DB size.
	Size() int64
	// SizeInUse returns the current size of the backend logically in use.
	// Since the backend can manage free space in a non-byte unit such as
	// number of pages, the returned value can be not exactly accurate in bytes.
	SizeInUse() int64
	Defrag() error
	ForceCommit()
	Close() error
}

type Snapshot interface {
	// Size gets the size of the snapshot.
	Size() int64
	// WriteTo writes the snapshot into the given writer.
	WriteTo(w io.Writer) (n int64, err error)
	// Close closes the snapshot.
	Close() error
}

type backend struct {
    config *BackendConfig    
	// size and commits are used with atomic operations so they must be
	// 64-bit aligned, otherwise 32-bit tests will crash

	// size is the number of bytes allocated in the backend
	size int64
	// sizeInUse is the number of bytes actually used in the backend
	sizeInUse int64
	// commits counts number of commits since start
	commits int64

	mu sync.RWMutex
	db *gorocksdb.TransactionDB

	batchInterval time.Duration
	batchLimit    int
	batchTx       *batchTx
	readTx        *readTx
    buckets map[string]*gorocksdb.ColumnFamilyHandle

	lg *zap.Logger
}

type BackendConfig struct {
	// Path is the file path to the backend file.
	Path string
    // bucket names
    Buckets []string
	// BatchInterval is the maximum time before flushing the BatchTx.
	BatchInterval time.Duration
	// BatchLimit is the maximum puts before flushing the BatchTx.
	BatchLimit int
	// MmapSize is the number of bytes to mmap for the backend.
	MmapSize uint64
	// Logger logs backend-side operations.
	Logger *zap.Logger
}

func DefaultBackendConfig() BackendConfig {
	return BackendConfig{
		BatchInterval: defaultBatchInterval,
		BatchLimit:    defaultBatchLimit,
		MmapSize:      initialMmapSize,
	}
}

func New(bcfg BackendConfig) Backend {
	return newBackend(bcfg)
}

func NewDefaultBackend(path string) Backend {
    bcfg := DefaultBackendConfig()
	bcfg.Path = path
	return newBackend(bcfg)
}

func newBackend(bcfg BackendConfig) *backend {
    bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
    bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
    opts := gorocksdb.NewDefaultOptions()
    opts.SetBlockBasedTableFactory(bbto)
    opts.SetCreateIfMissing(true)
    opts.SetCreateIfMissingColumnFamilies(true)
    transactionDBOpts := gorocksdb.NewDefaultTransactionDBOptions()

    numColumnFamilies := len(bcfg.Buckets)
    cfOpts := make([]*gorocksdb.Options, numColumnFamilies) 
    var txndb *gorocksdb.TransactionDB
    var cfHandles []*gorocksdb.ColumnFamilyHandle
    var err error
    if numColumnFamilies > 0 {
        for i := 0; i< numColumnFamilies; i++ {
            cfOpts[i] = opts
        }
        txndb, cfHandles, err = gorocksdb.OpenTransactionDbColumnFamilies(opts, transactionDBOpts, bcfg.Path,
                                                        bcfg.Buckets, cfOpts)
    } else {
        txndb, err = gorocksdb.OpenTransactionDb(opts, transactionDBOpts, bcfg.Path)
    }
	if err != nil {
		if bcfg.Logger != nil {
			bcfg.Logger.Panic("failed to open database", zap.String("path", bcfg.Path), zap.Error(err))
		} else {
			plog.Panicf("cannot open database at %s (%v)", bcfg.Path, err)
		}
	}

    b := &backend{
        config : &bcfg,
        db: txndb,

        batchInterval: bcfg.BatchInterval,
        batchLimit:    bcfg.BatchLimit,

        readTx: &readTx{
            buf: txReadBuffer{
                txBuffer: txBuffer{make(map[string]*bucketBuffer)},
            },
            buckets: make(map[string]*gorocksdb.ColumnFamilyHandle),
        },
        batchTx: &batchTx{
            buckets: make(map[string]*gorocksdb.ColumnFamilyHandle),
            pending: 0,
        },

        buckets: make(map[string]*gorocksdb.ColumnFamilyHandle),

        lg: bcfg.Logger,
    }

    if cfHandles != nil {
        for i := range cfHandles {
            b.readTx.buckets[bcfg.Buckets[i]] = cfHandles[i]
            b.batchTx.buckets[bcfg.Buckets[i]] = cfHandles[i]
            b.buckets[bcfg.Buckets[i]] = cfHandles[i]
        }
    }
    plog.Print("created new backend") 
	return b
}

// BatchTx returns the current batch tx in coalescer. The tx can be used for read and
// write operations. The write result can be retrieved within the same tx immediately.
// The write result is isolated with other txs until the current one get committed.
func (b *backend) BatchTx() BatchTx {
    wo := gorocksdb.NewDefaultWriteOptions()
    defer wo.Destroy()
    to := gorocksdb.NewDefaultTransactionOptions()
    defer to.Destroy()
    b.batchTx.backend = b
    b.batchTx.tx = b.db.TransactionBegin(wo, to, nil)
	return b.batchTx
}

func (b *backend) ReadTx() ReadTx {
    wo := gorocksdb.NewDefaultWriteOptions()
    defer wo.Destroy()
    to := gorocksdb.NewDefaultTransactionOptions()
    defer to.Destroy()
    b.readTx.backend = b
    b.readTx.tx = b.db.TransactionBegin(wo, to, nil)
    return b.readTx 
}

// ForceCommit forces the current batching tx to commit.
func (b *backend) ForceCommit() {
	b.batchTx.Commit()
}

func (b *backend) Snapshot() Snapshot {
    checkpoint, err := b.db.NewCheckpoint()
    defer checkpoint.Destroy()
    if checkpoint == nil {
        plog.Print("create checkpoint")
    }
    dir := b.config.Path + "-checkpoint"

    err = checkpoint.CreateCheckpoint(dir, 0)
    if err != nil {
        plog.Print("failed to create snapshot")
    }
	return &snapshot{b.db, checkpoint }
}

type IgnoreKey struct {
	Bucket string
	Key    string
}

// return GetLatestSequenceNumber 
func (b *backend) Hash(ignores map[IgnoreKey]struct{}) (uint32, error) {
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	return h.Sum32(), nil
}

func (b *backend) Size() int64 {
	return atomic.LoadInt64(&b.size)
}

func (b *backend) SizeInUse() int64 {
	return atomic.LoadInt64(&b.sizeInUse)
}

func (b *backend) Close() error {
	b.db.Close()
    return nil
}

// Commits returns total number of commits since start
func (b *backend) Commits() int64 {
	return atomic.LoadInt64(&b.commits)
}

func (b *backend) Defrag() error {
	return b.defrag()
}

func (b *backend) defrag() error {
	return nil
}

// NewTmpBackend creates a backend implementation for testing.
func NewTmpBackend(batchInterval time.Duration, batchLimit int) (*backend, string) {
	dir, err := ioutil.TempDir(os.TempDir(), "etcd_backend_test")
	if err != nil {
		panic(err)
	}
	tmpPath := filepath.Join(dir, "database")
	bcfg := DefaultBackendConfig()
	bcfg.Path, bcfg.BatchInterval, bcfg.BatchLimit = tmpPath, batchInterval, batchLimit
	return newBackend(bcfg), tmpPath
}

func NewDefaultTmpBackend() (*backend, string) {
	return NewTmpBackend(defaultBatchInterval, defaultBatchLimit)
}

type snapshot struct {
	db *gorocksdb.TransactionDB
	Tx *gorocksdb.Checkpoint
}

func (s *snapshot) Close() error {
    return nil
}

func (s *snapshot) Size() int64 {
	return math.MaxInt64
}

func (s *snapshot) WriteTo(w io.Writer) (n int64, err error) {
    return math.MaxInt64, nil
}

func (b *backend) begin(write bool) *gorocksdb.Transaction {
    var tx *gorocksdb.Transaction
    wo := gorocksdb.NewDefaultWriteOptions()
    defer wo.Destroy()
    to := gorocksdb.NewDefaultTransactionOptions()
    defer to.Destroy()
    b.mu.RLock()
    tx = b.db.TransactionBegin(wo, to, nil)
    b.mu.RUnlock()

    //size := tx.Size()
    //db := tx.DB()
    //atomic.StoreInt64(&b.size, size)
    //atomic.StoreInt64(&b.sizeInUse, size-(int64(db.Stats().FreePageN)*int64(db.Info().PageSize)))

    return tx
}

func (b *backend) unsafeBegin(write bool) *gorocksdb.Transaction {
    var tx *gorocksdb.Transaction
    wo := gorocksdb.NewDefaultWriteOptions()
    defer wo.Destroy()
    to := gorocksdb.NewDefaultTransactionOptions()
    defer to.Destroy()
    tx = b.db.TransactionBegin(wo, to, nil)

    return tx
}


