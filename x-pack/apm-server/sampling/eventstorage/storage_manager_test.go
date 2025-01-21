package eventstorage_test

import (
	"testing"

	"github.com/gofrs/uuid/v5"
	"github.com/stretchr/testify/assert"

	"github.com/elastic/apm-data/model/modelpb"
	"github.com/elastic/apm-server/x-pack/apm-server/sampling/eventstorage"
)

func TestStorageManager_samplingDecisionTTL(t *testing.T) {
	sm := newStorageManager(t)
	rw := sm.NewBypassReadWriter()
	traceID := uuid.Must(uuid.NewV4()).String()
	err := rw.WriteTraceSampled(traceID, true, eventstorage.WriterOpts{})
	assert.NoError(t, err)
	sampled, err := rw.IsTraceSampled(traceID)
	assert.NoError(t, err)
	assert.True(t, sampled)

	// after 1 TTL
	err = sm.IncrementPartition()
	assert.NoError(t, err)

	sampled, err = rw.IsTraceSampled(traceID)
	assert.NoError(t, err)
	assert.True(t, sampled)

	// after 2 TTL
	err = sm.IncrementPartition()
	assert.NoError(t, err)

	_, err = rw.IsTraceSampled(traceID)
	assert.ErrorIs(t, err, eventstorage.ErrNotFound)

	// after 3 TTL
	err = sm.IncrementPartition()
	assert.NoError(t, err)

	_, err = rw.IsTraceSampled(traceID)
	assert.ErrorIs(t, err, eventstorage.ErrNotFound)
}

func TestStorageManager_eventTTL(t *testing.T) {
	sm := newStorageManager(t)
	rw := sm.NewBypassReadWriter()
	traceID := uuid.Must(uuid.NewV4()).String()
	txnID := uuid.Must(uuid.NewV4()).String()
	transaction := makeTransaction(txnID, traceID)
	err := rw.WriteTraceEvent(traceID, txnID, transaction, eventstorage.WriterOpts{})
	assert.NoError(t, err)

	var out modelpb.Batch
	err = rw.ReadTraceEvents(traceID, &out)
	assert.NoError(t, err)
	assert.Len(t, out, 1)

	// after 1 TTL
	err = sm.IncrementPartition()
	assert.NoError(t, err)

	out = nil
	err = rw.ReadTraceEvents(traceID, &out)
	assert.NoError(t, err)
	assert.Len(t, out, 1)

	// after 2 TTL
	err = sm.IncrementPartition()
	assert.NoError(t, err)

	out = nil
	err = rw.ReadTraceEvents(traceID, &out)
	assert.NoError(t, err)
	assert.Len(t, out, 0)

	// after 3 TTL
	err = sm.IncrementPartition()
	assert.NoError(t, err)

	out = nil
	err = rw.ReadTraceEvents(traceID, &out)
	assert.NoError(t, err)
	assert.Len(t, out, 0)
}
