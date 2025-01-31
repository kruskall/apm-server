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

package jaeger

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	jaegermodel "github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/proto-gen/api_v2"
	jaegertranslator "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/elastic/apm-data/input"
	"github.com/elastic/apm-data/input/otlp"
	"github.com/elastic/apm-data/model/modelpb"
	"github.com/elastic/apm-server/internal/agentcfg"
	"github.com/elastic/apm-server/internal/beater/auth"
	"github.com/elastic/apm-server/internal/beater/config"
)

const (
	// elasticAuthTag is the name of the agent tag that will be used for auth.
	// The tag value should be "Bearer <secret token" or "ApiKey <api key>".
	elasticAuthTag = "elastic-apm-auth"

	deprecationNotice = "deprecation notice: support for Jaeger will be removed in an upcoming version"
)

// RegisterGRPCServices registers Jaeger gRPC services with srv.
func RegisterGRPCServices(
	srv *grpc.Server,
	logger *zap.Logger,
	processor modelpb.BatchProcessor,
	fetcher agentcfg.Fetcher,
	semaphore input.Semaphore,
	mp metric.MeterProvider,
) {
	traceConsumer := otlp.NewConsumer(otlp.ConsumerConfig{
		Processor: processor,
		Logger:    logger,
		Semaphore: semaphore,
	})

	logger.Info(deprecationNotice)

	api_v2.RegisterCollectorServiceServer(srv, &grpcCollector{sync.Once{}, logger, traceConsumer})
	api_v2.RegisterSamplingManagerServer(srv, &grpcSampler{sync.Once{}, logger, fetcher})
}

// grpcCollector implements Jaeger api_v2 protocol for receiving tracing data
type grpcCollector struct {
	// Use an atomic counter to ensure concurrent safety.
	once     sync.Once
	logger   *zap.Logger
	consumer consumer.Traces
}

// AuthenticateUnaryCall authenticates CollectorService calls.
func (c *grpcCollector) AuthenticateUnaryCall(
	ctx context.Context,
	req interface{},
	fullMethodName string,
	authenticator *auth.Authenticator,
) (auth.AuthenticationDetails, auth.Authorizer, error) {
	postSpansRequest, ok := req.(*api_v2.PostSpansRequest)
	if !ok {
		return auth.AuthenticationDetails{}, nil, status.Errorf(
			codes.Unauthenticated, "unhandled method %q", fullMethodName,
		)
	}
	batch := &postSpansRequest.Batch
	var kind, token string
	for i, kv := range batch.Process.GetTags() {
		if kv.Key != elasticAuthTag {
			continue
		}
		// Remove the auth tag.
		batch.Process.Tags = append(batch.Process.Tags[:i], batch.Process.Tags[i+1:]...)
		kind, token = auth.ParseAuthorizationHeader(kv.VStr)
		break
	}
	return authenticator.Authenticate(ctx, kind, token)
}

// PostSpans implements the api_v2/collector.proto. It converts spans received via Jaeger Proto batch to open-telemetry
// TraceData and passes them on to the internal Consumer taking care of converting into Elastic APM format.
// The implementation of the protobuf contract is based on the open-telemetry implementation at
// https://github.com/open-telemetry/opentelemetry-collector/tree/master/receiver/jaegerreceiver
func (c *grpcCollector) PostSpans(ctx context.Context, r *api_v2.PostSpansRequest) (*api_v2.PostSpansResponse, error) {

	c.once.Do(func() {
		c.logger.Warn(deprecationNotice)
	})

	if err := c.postSpans(ctx, r.Batch); err != nil {
		return nil, err
	}
	return &api_v2.PostSpansResponse{}, nil
}

func (c *grpcCollector) postSpans(ctx context.Context, batch jaegermodel.Batch) error {
	traces, err := jaegertranslator.ProtoToTraces([]*jaegermodel.Batch{&batch})
	if err != nil {
		return err
	}
	return c.consumer.ConsumeTraces(ctx, traces)
}

var (
	jaegerAgentPrefixes = []string{otlp.AgentNameJaeger}
)

type grpcSampler struct {
	// Use an atomic counter to ensure concurrent safety.
	once    sync.Once
	logger  *zap.Logger
	fetcher agentcfg.Fetcher
}

// GetSamplingStrategy implements the api_v2/sampling.proto.
// Only probabilistic sampling is supported.
// It fetches the sampling rate from the central configuration management and returns the sampling strategy.
func (s *grpcSampler) GetSamplingStrategy(
	ctx context.Context,
	params *api_v2.SamplingStrategyParameters) (*api_v2.SamplingStrategyResponse, error) {

	s.once.Do(func() {
		s.logger.Warn(deprecationNotice)
	})

	samplingRate, err := s.fetchSamplingRate(ctx, params.ServiceName)
	if err != nil {
		// do not return full error details since this is part of an unprotected endpoint response
		s.logger.Error("no valid sampling rate fetched from Kibana", zap.Error(err))
		return nil, errors.New("no sampling rate available, check server logs for more details")
	}

	return &api_v2.SamplingStrategyResponse{
		StrategyType:          api_v2.SamplingStrategyType_PROBABILISTIC,
		ProbabilisticSampling: &api_v2.ProbabilisticSamplingStrategy{SamplingRate: samplingRate},
	}, nil
}

func (s *grpcSampler) fetchSamplingRate(ctx context.Context, service string) (float64, error) {
	// Only service, and not agent, is known for config queries.
	// For anonymous/untrusted agents, we filter the results using
	// query.InsecureAgents below.
	authResource := auth.Resource{ServiceName: service}
	if err := auth.Authorize(ctx, auth.ActionAgentConfig, authResource); err != nil {
		return 0, err
	}

	query := agentcfg.Query{
		Service:              agentcfg.Service{Name: service},
		InsecureAgents:       jaegerAgentPrefixes,
		MarkAsAppliedByAgent: true,
	}
	result, err := s.fetcher.Fetch(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("fetching sampling rate failed: %w", err)
	}

	if sr, ok := result.Source.Settings[agentcfg.TransactionSamplingRateKey]; ok {
		srFloat64, err := strconv.ParseFloat(sr, 64)
		if err != nil {
			return 0, fmt.Errorf("parsing error for sampling rate `%v`: %w", sr, err)
		}
		return srFloat64, nil
	}
	return 0, fmt.Errorf("no sampling rate found for %v", service)
}

var anonymousAuthenticator *auth.Authenticator

func init() {
	// TODO(axw) introduce a function in the auth package for returning
	// anonymous details/authorizer, obviating the need for a separate
	// anonymous Authenticator.
	var err error
	anonymousAuthenticator, err = auth.NewAuthenticator(config.AgentAuth{
		Anonymous: config.AnonymousAgentAuth{Enabled: true},
	})
	if err != nil {
		panic(err)
	}
}

// AuthenticateUnaryCall authenticates SamplingManager calls.
//
// Sampling strategy queries are always unauthenticated. We still consult
// the authenticator in case auth isn't required, in which case we should
// not rate limit the request.
func (s *grpcSampler) AuthenticateUnaryCall(
	ctx context.Context,
	req interface{},
	fullMethodName string,
	authenticator *auth.Authenticator,
) (auth.AuthenticationDetails, auth.Authorizer, error) {
	details, authz, err := authenticator.Authenticate(ctx, "", "")
	if !errors.Is(err, auth.ErrAuthFailed) {
		return details, authz, err
	}
	return anonymousAuthenticator.Authenticate(ctx, "", "")
}
