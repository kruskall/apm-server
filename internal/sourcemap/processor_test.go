// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package sourcemap

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/apm-data/model/modelpb"
	"github.com/elastic/elastic-agent-libs/logp"

	"github.com/elastic/apm-server/internal/elasticsearch"
	"github.com/elastic/apm-server/internal/logs"
)

func TestBatchProcessor(t *testing.T) {
	t.Skip("TODO FIX")
	ch := make(chan []identifier)
	close(ch)

	client := newMockElasticsearchClient(t, http.StatusOK, sourcemapESResponseBody(true, validSourcemap))
	esFetcher := NewElasticsearchFetcher(client, "index")
	fetcher, err := NewBodyCachingFetcher(esFetcher, 100, ch)
	require.NoError(t, err)

	originalLinenoWithFilename := uint32(1)
	originalColnoWithFilename := uint32(7)
	originalLinenoWithoutFilename := uint32(1)
	originalColnoWithoutFilename := uint32(23)
	originalLinenoWithFunction := uint32(1)
	originalColnoWithFunction := uint32(67)

	nonMatchingFrame := modelpb.StacktraceFrame{
		AbsPath:  "bundle.js",
		Lineno:   newInt(0),
		Colno:    newInt(0),
		Function: "original function",
	}
	mappedFrameWithFilename := modelpb.StacktraceFrame{
		AbsPath:     "bundle.js",
		Function:    "<anonymous>",
		Filename:    "webpack:///bundle.js",
		Lineno:      newInt(1),
		Colno:       newInt(9),
		ContextLine: "/******/ (function(modules) { // webpackBootstrap",
		PostContext: []string{
			"/******/ \t// The module cache",
			"/******/ \tvar installedModules = {};",
			"/******/",
			"/******/ \t// The require function",
			"/******/ \tfunction __webpack_require__(moduleId) {",
		},
		Original: &modelpb.Original{
			AbsPath:  "bundle.js",
			Lineno:   &originalLinenoWithFilename,
			Colno:    &originalColnoWithFilename,
			Function: "original function",
		},
		SourcemapUpdated: true,
	}

	mappedFrameWithoutFilename := cloneFrame(&mappedFrameWithFilename)
	mappedFrameWithoutFilename.Original.Lineno = &originalLinenoWithoutFilename
	mappedFrameWithoutFilename.Original.Colno = &originalColnoWithoutFilename
	mappedFrameWithoutFilename.Lineno = newInt(5)
	mappedFrameWithoutFilename.Colno = newInt(0)
	mappedFrameWithoutFilename.Filename = ""
	mappedFrameWithoutFilename.ContextLine = " \tfunction __webpack_require__(moduleId) {"
	mappedFrameWithoutFilename.PreContext = []string{
		" \t// The module cache",
		" \tvar installedModules = {};",
		"",
		" \t// The require function",
	}
	mappedFrameWithoutFilename.PostContext = []string{
		"",
		" \t\t// Check if module is in cache",
		" \t\tif(installedModules[moduleId])",
		" \t\t\treturn installedModules[moduleId].exports;",
		"",
	}

	mappedFrameWithFunction := cloneFrame(mappedFrameWithoutFilename)
	mappedFrameWithFunction.Original.Lineno = &originalLinenoWithFunction
	mappedFrameWithFunction.Original.Colno = &originalColnoWithFunction
	mappedFrameWithFunction.Lineno = newInt(13)
	mappedFrameWithFunction.Colno = newInt(0)
	mappedFrameWithFunction.ContextLine = " \t\t\texports: {},"
	mappedFrameWithFunction.PreContext = []string{
		" \t\tif(installedModules[moduleId])",
		" \t\t\treturn installedModules[moduleId].exports;",
		"",
		" \t\t// Create a new module (and put it into the cache)",
		" \t\tvar module = installedModules[moduleId] = {",
	}
	mappedFrameWithFunction.PostContext = []string{
		" \t\t\tid: moduleId,",
		" \t\t\tloaded: false",
		" \t\t};",
		"",
		" \t\t// Execute the module function",
	}
	mappedFrameWithFunction2 := cloneFrame(mappedFrameWithFunction)
	mappedFrameWithFunction2.Function = "exports"

	service := modelpb.Service{
		Name:    "service_name",
		Version: "service_version",
	}

	// Service intentionally left blank
	transaction := modelpb.APMEvent{Transaction: &modelpb.Transaction{}}
	span1 := modelpb.APMEvent{Span: &modelpb.Span{}}

	error1 := modelpb.APMEvent{
		Service: &service,
		Error:   &modelpb.Error{},
	}
	span2 := modelpb.APMEvent{
		Service: &service,
		Span: &modelpb.Span{
			Stacktrace: []*modelpb.StacktraceFrame{cloneFrame(&nonMatchingFrame), {
				AbsPath:  "bundle.js",
				Lineno:   newInt(originalLinenoWithFilename),
				Colno:    newInt(originalColnoWithFilename),
				Function: "original function",
			}},
		},
	}
	error2 := modelpb.APMEvent{
		Service: &service,
		Error: &modelpb.Error{
			Log: &modelpb.ErrorLog{
				Stacktrace: []*modelpb.StacktraceFrame{{
					AbsPath:  "bundle.js",
					Lineno:   newInt(originalLinenoWithoutFilename),
					Colno:    newInt(originalColnoWithoutFilename),
					Function: "original function",
				}},
			},
		},
	}
	error3 := modelpb.APMEvent{
		Service: &service,
		Error: &modelpb.Error{
			Exception: &modelpb.Exception{
				Stacktrace: []*modelpb.StacktraceFrame{{
					AbsPath:  "bundle.js",
					Lineno:   newInt(originalLinenoWithFunction),
					Colno:    newInt(originalColnoWithFunction),
					Function: "original function",
				}},
				Cause: []*modelpb.Exception{{
					Stacktrace: []*modelpb.StacktraceFrame{{
						AbsPath:  "bundle.js",
						Lineno:   newInt(originalLinenoWithFunction),
						Colno:    newInt(originalColnoWithFunction),
						Function: "original function",
					}, {
						AbsPath:  "bundle.js",
						Lineno:   newInt(originalLinenoWithFunction),
						Colno:    newInt(originalColnoWithFunction),
						Function: "original function",
					}},
				}},
			},
		},
	}

	processor := BatchProcessor{
		Fetcher: fetcher,
		Logger:  logp.NewLogger(logs.Stacktrace),
	}
	err = processor.ProcessBatch(context.Background(), &modelpb.Batch{&transaction, &span1, &span2, &error1, &error2, &error3})
	assert.NoError(t, err)

	assert.Equal(t, &modelpb.Span{}, span1.Span)
	assert.Equal(t, &modelpb.Error{}, error1.Error)
	assert.Equal(t, &modelpb.Span{
		Stacktrace: []*modelpb.StacktraceFrame{
			cloneFrame(&nonMatchingFrame),
			cloneFrame(&mappedFrameWithFilename),
		},
	}, span2.Span)
	assert.Equal(t, &modelpb.Error{
		Log: &modelpb.ErrorLog{
			Stacktrace: []*modelpb.StacktraceFrame{
				cloneFrame(mappedFrameWithoutFilename),
			},
		},
	}, error2.Error)
	assert.Equal(t, &modelpb.Error{
		Exception: &modelpb.Exception{
			Stacktrace: []*modelpb.StacktraceFrame{
				cloneFrame(mappedFrameWithFunction),
			},
			Cause: []*modelpb.Exception{{
				Stacktrace: []*modelpb.StacktraceFrame{
					cloneFrame(mappedFrameWithFunction2),
					cloneFrame(mappedFrameWithFunction),
				},
			}},
		},
	}, error3.Error)
}

func TestBatchProcessorElasticsearchUnavailable(t *testing.T) {
	client := newUnavailableElasticsearchClient(t)
	fetcher := NewElasticsearchFetcher(client, "index")

	nonMatchingFrame := modelpb.StacktraceFrame{
		AbsPath:  "bundle.js",
		Lineno:   newInt(0),
		Colno:    newInt(0),
		Function: "original function",
	}

	span := modelpb.APMEvent{
		Service: &modelpb.Service{
			Name:    "service_name",
			Version: "service_version",
		},
		Span: &modelpb.Span{
			Stacktrace: []*modelpb.StacktraceFrame{cloneFrame(&nonMatchingFrame), cloneFrame(&nonMatchingFrame)},
		},
	}

	err := logp.DevelopmentSetup(logp.ToObserverOutput())
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		processor := BatchProcessor{
			Fetcher: fetcher,
			Logger:  logp.NewLogger(logs.Stacktrace),
		}
		err := processor.ProcessBatch(context.Background(), &modelpb.Batch{&span, &span})
		assert.NoError(t, err)
	}

	// SourcemapError should have been set, but the frames should otherwise be unmodified.
	expectedFrame := cloneFrame(&nonMatchingFrame)
	expectedFrame.SourcemapError = "failure querying ES: client error"
	assert.Equal(t, []*modelpb.StacktraceFrame{expectedFrame, expectedFrame}, span.Span.Stacktrace)

	// we should have 8 log messages (2 * 2 * 2)
	// we are running the processor twice for a batch of two spans with 2 stacktraceframe each
	entries := logp.ObserverLogs().TakeAll()
	require.Len(t, entries, 8)
	assert.Equal(t, "failed to fetch sourcemap with path (bundle.js): failure querying ES: client error", entries[0].Message)
}

func TestBatchProcessorTimeout(t *testing.T) {
	var transport roundTripperFunc = func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	}

	cfg := elasticsearch.DefaultConfig()
	cfg.Hosts = []string{""}
	client, err := elasticsearch.NewClientParams(elasticsearch.ClientParams{
		Config:    cfg,
		Transport: transport,
	})
	require.NoError(t, err)
	fetcher := NewElasticsearchFetcher(client, "index")

	frame := modelpb.StacktraceFrame{
		AbsPath:  "bundle.js",
		Lineno:   newInt(0),
		Colno:    newInt(0),
		Function: "original function",
	}
	span := modelpb.APMEvent{
		Service: &modelpb.Service{
			Name:    "service_name",
			Version: "service_version",
		},
		Span: &modelpb.Span{
			Stacktrace: []*modelpb.StacktraceFrame{cloneFrame(&frame)},
		},
	}

	before := time.Now()
	processor := BatchProcessor{
		Fetcher: fetcher,
		Timeout: 100 * time.Millisecond,
		Logger:  logp.NewLogger(logs.Stacktrace),
	}
	err = processor.ProcessBatch(context.Background(), &modelpb.Batch{&span})
	assert.NoError(t, err)
	taken := time.Since(before)
	assert.Less(t, taken, time.Second)
}

func cloneFrame(frame *modelpb.StacktraceFrame) *modelpb.StacktraceFrame {
	return &modelpb.StacktraceFrame{
		Vars:                frame.Vars,
		Lineno:              frame.Lineno,
		Colno:               frame.Colno,
		Filename:            frame.Filename,
		Classname:           frame.Classname,
		ContextLine:         frame.ContextLine,
		Module:              frame.Module,
		Function:            frame.Function,
		AbsPath:             frame.AbsPath,
		SourcemapError:      frame.SourcemapError,
		Original:            frame.Original,
		PreContext:          frame.PreContext,
		PostContext:         frame.PostContext,
		LibraryFrame:        frame.LibraryFrame,
		SourcemapUpdated:    frame.SourcemapUpdated,
		ExcludeFromGrouping: frame.ExcludeFromGrouping,
	}
}

func newInt(v uint32) *uint32 {
	return &v
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
