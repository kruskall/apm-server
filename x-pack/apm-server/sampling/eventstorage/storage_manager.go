// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package eventstorage

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"golang.org/x/sync/errgroup"

	"github.com/elastic/apm-data/model/modelpb"
	"github.com/elastic/apm-server/internal/logs"
	"github.com/elastic/elastic-agent-libs/logp"
)

const (
	// subscriberPositionFile holds the file name used for persisting
	// the subscriber position across server restarts.
	subscriberPositionFile = "subscriber_position.json"
)

type wrappedDB struct {
	sm *StorageManager
	db *pebble.DB
}

func (w *wrappedDB) Get(key []byte) ([]byte, io.Closer, error) {
	return w.db.Get(key)
}

func (w *wrappedDB) Set(key, value []byte, opts *pebble.WriteOptions) error {
	return w.db.Set(key, value, opts)
}

func (w *wrappedDB) Delete(key []byte, opts *pebble.WriteOptions) error {
	return w.db.Delete(key, opts)
}

func (w *wrappedDB) NewIter(o *pebble.IterOptions) (*pebble.Iterator, error) {
	return w.db.NewIter(o)
}

func (w *wrappedDB) PartitionID() int32 {
	return w.sm.partitionID.Load()
}

func (w *wrappedDB) PartitionCount() int32 {
	return w.sm.partitionCount
}

type StorageManagerOptions func(*StorageManager)

func WithCodec(codec Codec) StorageManagerOptions {
	return func(sm *StorageManager) {
		sm.codec = codec
	}
}

// StorageManager encapsulates pebble.DB.
// It is to provide file system access, simplify synchronization and enable underlying db swaps.
// It assumes exclusive access to pebble DB at storageDir.
type StorageManager struct {
	storageDir string
	logger     *logp.Logger

	db              *pebble.DB
	decisionDB      *pebble.DB
	eventStorage    *Storage
	decisionStorage *Storage
	rw              *ShardedReadWriter

	partitionID    atomic.Int32
	partitionCount int32

	codec Codec

	// subscriberPosMu protects the subscriber file from concurrent RW.
	subscriberPosMu sync.Mutex

	// runCh acts as a mutex to ensure only 1 Run is actively running per StorageManager.
	// as it is possible that 2 separate Run are created by 2 TBS processors during a hot reload.
	runCh chan struct{}
}

// NewStorageManager returns a new StorageManager with pebble DB at storageDir.
func NewStorageManager(storageDir string, opts ...StorageManagerOptions) (*StorageManager, error) {
	sm := &StorageManager{
		storageDir:     storageDir,
		runCh:          make(chan struct{}, 1),
		logger:         logp.NewLogger(logs.Sampling),
		codec:          ProtobufCodec{},
		partitionCount: 3,
	}
	for _, opt := range opts {
		opt(sm)
	}
	err := sm.reset()
	if err != nil {
		return nil, err
	}
	return sm, nil
}

// reset initializes db, storage, and rw.
func (s *StorageManager) reset() error {
	db, err := OpenPebble(s.storageDir)
	if err != nil {
		return err
	}
	s.db = db
	s.eventStorage = New(&wrappedDB{sm: s, db: s.db}, s.codec)

	decisionDB, err := OpenSamplingDecisionPebble(s.storageDir)
	if err != nil {
		return err
	}
	s.decisionDB = decisionDB
	s.decisionStorage = New(&wrappedDB{sm: s, db: s.decisionDB}, s.codec)

	return nil
}

func (s *StorageManager) Size() (lsm, vlog int64) {
	return int64(s.db.Metrics().DiskSpaceUsage() + s.decisionDB.Metrics().DiskSpaceUsage()), 0
}

func (s *StorageManager) Close() error {
	return s.close()
}

func (s *StorageManager) close() error {
	return errors.Join(s.db.Close(), s.decisionDB.Close())
}

// Reload flushes out pending disk writes to disk by reloading the database.
// It does not flush uncommitted writes.
// For testing only.
func (s *StorageManager) Reload() error {
	if err := s.close(); err != nil {
		return err
	}
	return s.reset()
}

// Run has the same lifecycle as the TBS processor as opposed to StorageManager to facilitate EA hot reload.
func (s *StorageManager) Run(stopping <-chan struct{}, gcInterval time.Duration, ttl time.Duration, storageLimit uint64, storageLimitThreshold float64) error {
	select {
	case <-stopping:
		return nil
	case s.runCh <- struct{}{}:
	}
	defer func() {
		<-s.runCh
	}()

	g := errgroup.Group{}
	g.Go(func() error {
		return s.runTTLLoop(stopping, gcInterval)
	})
	return g.Wait()
}

func (s *StorageManager) runTTLLoop(stopping <-chan struct{}, ttl time.Duration) error {
	ticker := time.NewTicker(ttl)
	defer ticker.Stop()
	for {
		select {
		case <-stopping:
			return nil
		case <-ticker.C:
			if err := s.IncrementPartition(); err != nil {
				s.logger.With(logp.Error(err)).Error("failed to increment partition")
			}
		}
	}
}

func (s *StorageManager) IncrementPartition() error {
	oldPID := s.partitionID.Load()
	s.partitionID.Store((oldPID + 1) % s.partitionCount)

	pidToDelete := (oldPID + s.partitionCount - 1) % s.partitionCount
	lbPrefix := byte(pidToDelete)
	ubPrefix := lbPrefix + 1 // Do not use % here as it MUST BE greater than lb
	return errors.Join(
		s.db.DeleteRange([]byte{lbPrefix}, []byte{ubPrefix}, pebble.NoSync),
		s.decisionDB.DeleteRange([]byte{lbPrefix}, []byte{ubPrefix}, pebble.NoSync),
		s.db.Compact([]byte{lbPrefix}, []byte{ubPrefix}, false),
		s.decisionDB.Compact([]byte{lbPrefix}, []byte{ubPrefix}, false),
	)
}

func (s *StorageManager) ReadSubscriberPosition() ([]byte, error) {
	s.subscriberPosMu.Lock()
	defer s.subscriberPosMu.Unlock()
	return os.ReadFile(filepath.Join(s.storageDir, subscriberPositionFile))
}

func (s *StorageManager) WriteSubscriberPosition(data []byte) error {
	s.subscriberPosMu.Lock()
	defer s.subscriberPosMu.Unlock()
	return os.WriteFile(filepath.Join(s.storageDir, subscriberPositionFile), data, 0644)
}

func (s *StorageManager) NewReadWriter() SplitReadWriter {
	return SplitReadWriter{
		eventRW:    s.eventStorage.NewShardedReadWriter(),
		decisionRW: s.decisionStorage.NewShardedReadWriter(),
	}
}

// NewBypassReadWriter returns a SplitReadWriter directly reading and writing to the database,
// bypassing any wrapper e.g. ShardedReadWriter.
// This should be used for testing only, useful to check if data is actually persisted to the DB.
func (s *StorageManager) NewBypassReadWriter() SplitReadWriter {
	return SplitReadWriter{
		eventRW:    s.eventStorage.NewReadWriter(),
		decisionRW: s.decisionStorage.NewReadWriter(),
	}
}

type SplitReadWriter struct {
	eventRW, decisionRW RW
}

func (s SplitReadWriter) ReadTraceEvents(traceID string, out *modelpb.Batch) error {
	return s.eventRW.ReadTraceEvents(traceID, out)
}

func (s SplitReadWriter) WriteTraceEvent(traceID, id string, event *modelpb.APMEvent, opts WriterOpts) error {
	return s.eventRW.WriteTraceEvent(traceID, id, event, opts)
}

func (s SplitReadWriter) WriteTraceSampled(traceID string, sampled bool, opts WriterOpts) error {
	return s.decisionRW.WriteTraceSampled(traceID, sampled, opts)
}

func (s SplitReadWriter) IsTraceSampled(traceID string) (bool, error) {
	return s.decisionRW.IsTraceSampled(traceID)
}

func (s SplitReadWriter) DeleteTraceEvent(traceID, id string) error {
	return s.eventRW.DeleteTraceEvent(traceID, id)
}

func (s SplitReadWriter) Flush() error {
	return nil
}

func (s SplitReadWriter) Close() error {
	return nil
}
