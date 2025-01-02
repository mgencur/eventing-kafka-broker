//go:build e2e
// +build e2e

/*
 * Copyright 2021 The Knative Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package e2e_new

import (
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	cloudeventstest "github.com/cloudevents/sdk-go/v2/test"
	// Uncomment the following line to load the gcp plugin (only required to authenticate against GKE clusters).
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"knative.dev/pkg/system"
	_ "knative.dev/pkg/system/testing"

	brokerconfigmap "knative.dev/eventing-kafka-broker/test/rekt/resources/configmap/broker"
	"knative.dev/eventing/pkg/apis/eventing"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/eventing/test/rekt/features/broker"
	b "knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/eventing/test/rekt/resources/trigger"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/feature"

	eventassert "knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"
	"knative.dev/reconciler-test/pkg/resources/service"
	"knative.dev/reconciler-test/pkg/state"
)

const (
	defaultEventType   = "dev.knative.test.event"
	defaultEventSource = "source"
)

func TestBrokerConformance(t *testing.T) {
	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	opts := b.WithEnvConfig()

	if b.EnvCfg.BrokerClass == eventing.MTChannelBrokerClassValue {
		configName := feature.MakeRandomK8sName("kafka-broker-config")
		opts = append(opts, b.WithConfig(configName))
		env.Prerequisite(ctx, t, BrokerCreateConfigMap(configName))
	}

	brokerName := feature.MakeRandomK8sName("broker")

	env.Prerequisite(ctx, t, broker.GoesReady(brokerName, opts...))

	env.TestSet(ctx, t, broker.ControlPlaneConformance(brokerName, opts...))
	env.TestSet(ctx, t, broker.DataPlaneConformance(brokerName))
	env.TestSet(ctx, t, DataPlaneConformance(brokerName))
}

func BrokerCreateConfigMap(configName string) *feature.Feature {
	f := feature.NewFeature()

	f.Setup("create broker config", brokerconfigmap.Install(configName,
		brokerconfigmap.WithKafkaChannelMTBroker(),
	))

	return f
}

func DataPlaneConformance(brokerName string) *feature.FeatureSet {
	fs := &feature.FeatureSet{
		Name: "Knative Kafka Broker Specification - Data Plane",
		Features: []*feature.Feature{
			DataPlaneIngress(brokerName),
		},
	}

	return fs
}

func DataPlaneIngress(brokerName string) *feature.Feature {
	f := feature.NewFeatureNamed("Ingress")

	//loggerName := "logger-pod"
	//secondLoggerName := "second-logger-pod"
	sink1 := feature.MakeRandomK8sName("sink-1")
	sink2 := feature.MakeRandomK8sName("sink-2")
	sinkTransformer := feature.MakeRandomK8sName("sink-transformer")
	trigger1 := feature.MakeRandomK8sName("trigger-1")
	trigger2 := feature.MakeRandomK8sName("trigger-2")
	triggerTransformer := feature.MakeRandomK8sName("trigger-transformer")
	triggerReply := feature.MakeRandomK8sName("trigger-reply")
	replySource := feature.MakeRandomK8sName("origin-for-reply")

	// Construct cloudevent message after transformation
	transformedEventType := "reply-check-type"
	transformedEventSource := "reply-check-source"
	transformedBody := `{"msg":"Transformed!"}`

	//baseEvent := cloudevents.NewEvent()
	//baseEvent.SetID(eventID)
	//baseEvent.SetType(testlib.DefaultEventType)
	//baseEvent.SetSource(baseSource)
	//baseEvent.SetSpecVersion("1.0")
	//body := fmt.Sprintf(`{"msg":%q}`, eventID)
	//baseEvent.SetData(cloudevents.ApplicationJSON, []byte(body))

	f.Setup("Set names", func(ctx context.Context, t feature.T) {
		state.SetOrFail(ctx, t, "brokerName", brokerName)
		state.SetOrFail(ctx, t, "sink1", sink1)
		state.SetOrFail(ctx, t, "sink2", sink2)
		state.SetOrFail(ctx, t, "sinkTransformer", sinkTransformer)
	})

	f.Setup("install sink-1", eventshub.Install(sink1, eventshub.StartReceiver))
	f.Setup("install sink-2", eventshub.Install(sink2, eventshub.StartReceiver))
	f.Setup("install sink-transformer", eventshub.Install(sinkTransformer,
		eventshub.ReplyWithTransformedEvent(transformedEventType, transformedEventSource, transformedBody),
		eventshub.StartReceiver),
	)

	filter1 := eventingv1.TriggerFilterAttributes{
		"type": eventingv1.TriggerAnyFilter,
	}

	f.Setup("install trigger1", trigger.Install(
		trigger1,
		trigger.WithBrokerName(brokerName),
		trigger.WithFilter(filter1),
		trigger.WithSubscriber(service.AsKReference(sink1), ""),
	))
	f.Setup("trigger1 goes ready", trigger.IsReady(trigger1))

	filter2 := eventingv1.TriggerFilterAttributes{
		"source": "filtered-event", // Reuse name of "source"?
	}

	f.Setup("install trigger2", trigger.Install(
		trigger2,
		trigger.WithBrokerName(brokerName),
		trigger.WithFilter(filter2),
		trigger.WithSubscriber(service.AsKReference(sink2), ""),
	))
	f.Setup("trigger2 goes ready", trigger.IsReady(trigger2))

	filterTransformer := eventingv1.TriggerFilterAttributes{
		"source": replySource,
		"type":   defaultEventType,
	}

	f.Setup("install trigger transformer", trigger.Install(
		triggerTransformer,
		trigger.WithBrokerName(brokerName),
		trigger.WithFilter(filterTransformer),
		trigger.WithSubscriber(service.AsKReference(sinkTransformer), ""),
	))
	f.Setup("trigger transformer goes ready", trigger.IsReady(triggerTransformer))

	filterReply := eventingv1.TriggerFilterAttributes{
		"source": transformedEventSource,
		"type":   transformedEventType,
	}

	f.Setup("install trigger reply", trigger.Install(
		triggerReply,
		trigger.WithBrokerName(brokerName),
		trigger.WithFilter(filterReply),
		trigger.WithSubscriber(service.AsKReference(sink1), ""),
	))
	f.Setup("trigger transformer goes ready", trigger.IsReady(triggerTransformer))

	f.Stable("Conformance").
		ShouldNot("The Broker SHOULD NOT perform an upgrade of the produced event's CloudEvents version.",
			brokerEventVersionNotUpgraded)
	return f
}

func brokerEventVersionNotUpgraded(ctx context.Context, t feature.T) {
	brokerName := state.GetStringOrFail(ctx, t, "brokerName")
	sink1 := state.GetStringOrFail(ctx, t, "sink1")

	event := cloudevents.NewEvent()
	event.SetID("no-upgrade")
	event.SetType(defaultEventType)
	event.SetSource(defaultEventSource)
	event.SetSpecVersion("1.0")
	body := fmt.Sprintf(`{"msg":%q}`, eventID)
	event.SetData(cloudevents.ApplicationJSON, []byte(body))
	event.Context = event.Context.AsV03()

	eventshub.Install(
		source,
		eventshub.StartSenderToResource(broker.GVR(), brokerName),
		eventshub.InputEvent(event),
	)(ctx, t)

	eventMatcher := eventassert.MatchEvent(
		cloudeventstest.HasId("no-upgrade"),
		cloudeventstest.HasSpecVersion("0.3"),
	)
	_ = eventshub.StoreFromContext(ctx, sink1).AssertExact(ctx, t, 1, eventMatcher)
}
