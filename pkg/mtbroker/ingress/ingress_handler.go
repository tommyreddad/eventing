/*
 * Copyright 2019 The Knative Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ingress

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/client"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"

	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	eventinglisters "knative.dev/eventing/pkg/client/listers/eventing/v1"
	"knative.dev/eventing/pkg/health"
	"knative.dev/eventing/pkg/kncloudevents"
	broker "knative.dev/eventing/pkg/mtbroker"
	"knative.dev/eventing/pkg/tracing"
	"knative.dev/eventing/pkg/utils"
)

const (
	// noDuration signals that the dispatch step hasn't started
	noDuration = -1

	// TODO make these constants configurable (either as env variables, config map, or part of broker spec).
	//  Issue: https://github.com/knative/eventing/issues/1777
	// Constants for the underlying HTTP Client transport. These would enable better connection reuse.
	// Purposely set them to be equal, as the ingress only connects to its channel.
	// These are magic numbers, partly set based on empirical evidence running performance workloads, and partly
	// based on what serving is doing. See https://github.com/knative/serving/blob/master/pkg/network/transports.go.
	defaultMaxIdleConnections        = 1000
	defaultMaxIdleConnectionsPerHost = 1000
)

// Handler parses Cloud Events, determines if they pass a filter, and sends them to a subscriber.
type Handler struct {
	// receiver receives incoming HTTP requests
	receiver *kncloudevents.HttpMessageReceiver
	// sender sends requests to the broker
	sender *kncloudevents.HttpMessageSender
	// defaulter sets default values to incoming events
	defaulter client.EventDefaulter
	// reporter reports stats of status code and dispatch time
	reporter StatsReporter
	// brokerLister gets broker objects
	brokerLister eventinglisters.BrokerLister

	logger *zap.Logger
}

// NewHandler creates a new Handler and its associated MessageReceiver. The caller is responsible for
// Start()ing the returned Handler.
func NewHandler(logger *zap.Logger, brokerLister eventinglisters.BrokerLister, reporter StatsReporter, defaulter client.EventDefaulter, port int) (*Handler, error) {

	connectionArgs := kncloudevents.ConnectionArgs{
		MaxIdleConns:        defaultMaxIdleConnections,
		MaxIdleConnsPerHost: defaultMaxIdleConnectionsPerHost,
	}

	sender, err := kncloudevents.NewHttpMessageSender(&connectionArgs, "")
	if err != nil {
		return nil, fmt.Errorf("failed to create message sender: %w", err)
	}

	return &Handler{
		receiver:     kncloudevents.NewHttpMessageReceiver(port),
		sender:       sender,
		defaulter:    defaulter,
		reporter:     reporter,
		logger:       logger,
		brokerLister: brokerLister,
	}, nil
}

func (h *Handler) getBroker(name, namespace string) (*eventingv1.Broker, error) {
	broker, err := h.brokerLister.Brokers(namespace).Get(name)
	if err != nil {
		h.logger.Warn("Broker getter failed")
		return nil, err
	}
	return broker, nil
}

func guessChannelAddress(name, namespace, domain string) string {
	url := url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s-kne-trigger-kn-channel.%s.svc.%s", name, namespace, domain),
		Path:   "/",
	}
	return url.String()
}

func (h *Handler) getChannelAddress(name, namespace string) (string, error) {
	broker, err := h.getBroker(name, namespace)
	if err != nil {
		return "", err
	}
	if broker.Status.Annotations == nil {
		return "", fmt.Errorf("Broker status annotations uninitialized")
	}
	address, present := broker.Status.Annotations["channelAddress"]
	if !present {
		return "", fmt.Errorf("Channel address not found in broker status annotations")
	}
	return address, nil
}

// Start begins to receive messages for the handler.
//
// HTTP POST requests to the root path (/) are accepted.
//
// This method will block until ctx is done.
func (h *Handler) Start(ctx context.Context) error {
	return h.receiver.StartListen(ctx, health.WithLivenessCheck(h))
}

// 1. validate request
// 2. extract event from request
// 3. get broker from its broker reference extracted from the request URI
// 4. send event to channel address in broker annotation
// 5. write the response
func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	// validate request method
	if request.Method != http.MethodPost {
		h.logger.Warn("unexpected request method", zap.String("method", request.Method))
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// validate request URI
	if request.RequestURI == "/" {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	nsBrokerName := strings.Split(request.RequestURI, "/")
	if len(nsBrokerName) != 3 {
		h.logger.Info("Malformed uri", zap.String("URI", request.RequestURI))
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	ctx := request.Context()

	message := cehttp.NewMessageFromHttpRequest(request)
	defer message.Finish(nil)

	event, err := binding.ToEvent(ctx, message)
	if err != nil {
		h.logger.Warn("failed to extract event from request", zap.Error(err))
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	brokerNamespace := nsBrokerName[1]
	brokerName := nsBrokerName[2]
	brokerNamespacedName := types.NamespacedName{
		Name:      brokerName,
		Namespace: brokerNamespace,
	}

	ctx, span := trace.StartSpan(ctx, tracing.BrokerMessagingDestination(brokerNamespacedName))
	defer span.End()

	if span.IsRecordingEvents() {
		span.AddAttributes(
			tracing.MessagingSystemAttribute,
			tracing.MessagingProtocolHTTP,
			tracing.BrokerMessagingDestinationAttribute(brokerNamespacedName),
			tracing.MessagingMessageIDAttribute(event.ID()),
		)
		span.AddAttributes(client.EventTraceAttributes(event)...)
	}

	reporterArgs := &ReportArgs{
		ns:        brokerNamespace,
		broker:    brokerName,
		eventType: event.Type(),
	}

	statusCode, dispatchTime := h.receive(ctx, request.Header, event, brokerNamespace, brokerName)
	if dispatchTime > noDuration {
		_ = h.reporter.ReportEventDispatchTime(reporterArgs, statusCode, dispatchTime)
	}
	_ = h.reporter.ReportEventCount(reporterArgs, statusCode)

	writer.WriteHeader(statusCode)
}

func (h *Handler) receive(ctx context.Context, headers http.Header, event *cloudevents.Event, brokerNamespace, brokerName string) (int, time.Duration) {

	// Setting the extension as a string as the CloudEvents sdk does not support non-string extensions.
	event.SetExtension(broker.EventArrivalTime, cloudevents.Timestamp{Time: time.Now()})
	if h.defaulter != nil {
		newEvent := h.defaulter(ctx, *event)
		event = &newEvent
	}

	if ttl, err := broker.GetTTL(event.Context); err != nil || ttl <= 0 {
		h.logger.Debug("dropping event based on TTL status.", zap.Int32("TTL", ttl), zap.String("event.id", event.ID()), zap.Error(err))
		return http.StatusBadRequest, noDuration
	}

	channelAddress, err := h.getChannelAddress(brokerName, brokerNamespace)
	if err != nil {
		h.logger.Warn("Failed to get channel address, falling back on guess", zap.Error(err))
		channelAddress = guessChannelAddress(brokerName, brokerNamespace, utils.GetClusterDomainName())
	}

	return h.send(ctx, headers, event, channelAddress)
}

func (h *Handler) send(ctx context.Context, headers http.Header, event *cloudevents.Event, target string) (int, time.Duration) {

	request, err := h.sender.NewCloudEventRequestWithTarget(ctx, target)
	if err != nil {
		return http.StatusInternalServerError, noDuration
	}

	message := binding.ToMessage(event)
	defer message.Finish(nil)

	additionalHeaders := utils.PassThroughHeaders(headers)
	err = kncloudevents.WriteHttpRequestWithAdditionalHeaders(ctx, message, request, additionalHeaders)
	if err != nil {
		return http.StatusInternalServerError, noDuration
	}

	resp, dispatchTime, err := h.sendAndRecordDispatchTime(request)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return http.StatusInternalServerError, dispatchTime
	}

	return resp.StatusCode, dispatchTime
}

func (h *Handler) sendAndRecordDispatchTime(request *http.Request) (*http.Response, time.Duration, error) {
	start := time.Now()
	resp, err := h.sender.Send(request)
	dispatchTime := time.Since(start)
	return resp, dispatchTime, err
}
