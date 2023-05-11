/*
 * Copyright 2023 The Knative Authors
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

package features

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	. "github.com/cloudevents/sdk-go/v2/test"
	cetest "github.com/cloudevents/sdk-go/v2/test"
	cetypes "github.com/cloudevents/sdk-go/v2/types"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	sourcesv1beta1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"
	testpkg "knative.dev/eventing-kafka-broker/test/pkg"
	"knative.dev/eventing-kafka-broker/test/rekt/features/featuressteps"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkasink"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/knative"
	"knative.dev/reconciler-test/pkg/manifest"
	"knative.dev/reconciler-test/pkg/resources/service"

	internalscg "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"
	kafkaclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/client"
	sourcesclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/client"
	consumergroupclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkasource"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkatopic"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	testingpkg "knative.dev/eventing-kafka-broker/test/pkg"
)

const (
	saslSecretName = "strimzi-sasl-secret"
	tlsSecretName  = "strimzi-tls-secret"
	SASLMech       = "sasl"
	TLSMech        = "tls"
	PlainMech      = "plain"
)

var (
	EmptyExtensions = map[string]string{}
)

type MatcherGenerator func(cloudEventsSourceName, cloudEventsEventType string) cetest.EventMatcher

func SetupAndCleanupKafkaSources(prefix string, n int) *feature.Feature {
	f := SetupKafkaSources(prefix, n)
	f.Teardown("cleanup resources", f.DeleteResources)
	return f
}

func SetupKafkaSources(prefix string, n int) *feature.Feature {
	sink := "sink"
	f := feature.NewFeatureNamed("KafkaSources")

	f.Setup("install a sink", service.Install(sink,
		service.WithSelectors(map[string]string{"app": "rekt"})))

	for i := 0; i < n; i++ {
		topicName := feature.MakeRandomK8sName("topic") // A k8s name is also a valid topic name.
		name := fmt.Sprintf("%s%d", prefix, i)

		f.Setup("install kafka topic", kafkatopic.Install(topicName))
		f.Setup(fmt.Sprintf("install kafkasource %s", name), kafkasource.Install(
			name,
			kafkasource.WithBootstrapServers(testingpkg.BootstrapServersPlaintextArr),
			kafkasource.WithTopics([]string{topicName}),
			kafkasource.WithSink(&duckv1.KReference{Kind: "Service", Name: sink, APIVersion: "v1"}, ""),
		))

		f.Assert(fmt.Sprintf("kafkasource %s is ready", name), kafkasource.IsReady(name))
	}

	return f
}

func KafkaSourcesAreNotPresentInContractConfigMaps(prefix string) *feature.Feature {
	f := feature.NewFeatureNamed("KafkaSources are not present in Contract configmaps")

	f.Assert("deleted KafkaSources are not present in Contract CMs", deletedKafkaSourcesAreNotPresentInContractConfigMaps(prefix))

	return f
}

func deletedKafkaSourcesAreNotPresentInContractConfigMaps(prefix string) feature.StepFn {
	return func(ctx context.Context, t feature.T) {

		namespace := environment.FromContext(ctx).Namespace()

		kss, err := kafkaclient.Get(ctx).SourcesV1beta1().
			KafkaSources(namespace).
			List(ctx, metav1.ListOptions{
				Limit: 2000,
			})
		if err != nil {
			t.Fatal("Failed to list KafkaSources")
		}

		systemNamespaceCMs, err := kubeclient.Get(ctx).CoreV1().
			ConfigMaps(knative.KnativeNamespaceFromContext(ctx)).
			List(ctx, metav1.ListOptions{
				Limit: 100,
			})
		if err != nil {
			t.Fatal(err)
		}
		for _, cm := range systemNamespaceCMs.Items {
			if !strings.HasPrefix(cm.Name, "kafka-source-dispatcher") {
				continue
			}

			ct, err := base.GetDataPlaneConfigMapData(zap.NewNop(), &cm, base.Json)
			if err != nil {
				t.Fatal(err)
			}

			for _, r := range ct.Resources {
				if r.Reference.Namespace != namespace {
					continue
				}
				if !strings.HasPrefix(r.Reference.Name, prefix) {
					continue
				}

				found := false
				for _, s := range kss.Items {
					if r.Reference.Namespace == s.Namespace && r.Reference.Name == s.Name {
						found = true
					}
				}
				if !found {
					t.Errorf("%v", ResourceError{
						Resource: r,
						Sources:  kss,
					}.Error())
				}
			}
		}
	}
}

type ResourceError struct {
	Resource *contract.Resource       `json:"resource"`
	Sources  *sources.KafkaSourceList `json:"sources"`
}

func (r ResourceError) Error() string {
	bytes, _ := json.Marshal(r)
	return string(bytes)
}

func ScaleKafkaSource() *feature.Feature {
	f := feature.NewFeatureNamed("scale KafkaSource")

	replicas := int32(3)
	source := feature.MakeRandomK8sName("kafkasource")
	topicName := feature.MakeRandomK8sName("scale-topic")
	sink := feature.MakeRandomK8sName("sink")

	f.Setup("install a sink", service.Install(sink,
		service.WithSelectors(map[string]string{"app": "rekt"})))
	f.Setup("install kafka topic", kafkatopic.Install(topicName))
	f.Setup("scale kafkasource", kafkasource.Install(source,
		kafkasource.WithBootstrapServers(testingpkg.BootstrapServersPlaintextArr),
		kafkasource.WithTopics([]string{topicName}),
		kafkasource.WithSink(service.AsKReference(sink), ""),
		kafkasource.WithAnnotations(map[string]string{
			// Disable autoscaling for this KafkaSource since we want to have the expected replicas
			// in the status reflected without the autoscaler intervention.
			"autoscaling.eventing.knative.dev/class": "disabled",
		}),
		kafkasource.WithConsumers(replicas),
	))

	f.Requirement("kafkasource is ready", kafkasource.IsReady(source))

	f.Assert("kafkasource is scaled", kafkasource.VerifyScale(source, replicas))

	return f
}

func KafkaSourceInitialOffsetEarliest(count int, topic string) *feature.Feature {

	f := feature.NewFeatureNamed("KafkaSource initial offset earliest")

	sink := feature.MakeRandomK8sName("sink")
	source := feature.MakeRandomK8sName("source")

	f.Setup("install sink", eventshub.Install(sink, eventshub.StartReceiver))

	f.Setup("install kafkasource", kafkasource.Install(
		source,
		kafkasource.WithBootstrapServers(testingpkg.BootstrapServersPlaintextArr),
		kafkasource.WithTopics([]string{topic}),
		kafkasource.WithInitialOffset(sources.OffsetEarliest),
		kafkasource.WithSink(service.AsKReference(sink), ""),
	))
	f.Setup("KafkaSource is ready", kafkasource.IsReady(source))

	f.Requirement("consumergroup has earliest offset set", compareConsumerGroup(
		source,
		func(group *internalscg.ConsumerGroup) error {
			got := group.Spec.Template.Spec.Delivery.InitialOffset
			if got != sources.OffsetEarliest {
				return fmt.Errorf("expected consumergroup to have offset %s set, got %s", sources.OffsetEarliest, got)
			}
			return nil
		},
	))

	for i := 1; i <= count; i++ {
		f.Assert(fmt.Sprintf("received event with sequence %d", i),
			assert.OnStore(sink).
				MatchEvent(cetest.HasExtension("sequence", fmt.Sprintf("%d", i))).
				AtLeast(1),
		)
	}

	return f
}

func compareConsumerGroup(source string, cmp func(*internalscg.ConsumerGroup) error) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		ns := environment.FromContext(ctx).Namespace()

		ks, err := sourcesclient.Get(ctx).
			SourcesV1beta1().
			KafkaSources(ns).
			Get(ctx, source, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}

		InternalsClient := consumergroupclient.Get(ctx)
		cg, err := InternalsClient.InternalV1alpha1().
			ConsumerGroups(ns).
			Get(ctx, string(ks.UID), metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}

		if err := cmp(cg); err != nil {
			t.Error(err)
		}
	}
}

func TestKafkaSourceAuth(auth string, extensions map[string]string, senderOpts []eventshub.EventsHubOption, matcherGen MatcherGenerator) *feature.Feature {
	f := feature.NewFeatureNamed("KafkaSourceWithAuth")

	topic := feature.MakeRandomK8sName("topic")
	kafkaSource := feature.MakeRandomK8sName("kafkaSource")
	eventshubSink := feature.MakeRandomK8sName("eventshubSink")
	ksink := feature.MakeRandomK8sName("ksink")
	eventshubSource := feature.MakeRandomK8sName("kafkaSource-to-ksink")

	f.Setup("install kafka topic", kafkatopic.Install(topic))
	f.Setup("topic is ready", kafkatopic.IsReady(topic))
	f.Setup("install kafkasink", kafkasink.Install(ksink, topic, testpkg.BootstrapServersPlaintextArr))
	f.Setup("KafkaSink is ready", kafkasink.IsReady(ksink))

	f.Setup("install eventshubSink", eventshub.Install(eventshubSink, eventshub.StartReceiver))

	opts := []manifest.CfgFn{
		kafkasource.WithSink(service.AsKReference(eventshubSink), ""),
		kafkasource.WithTopics([]string{topic}),
	}
	if len(extensions) != 0 {
		opts = append(opts, kafkasource.WithExtensions(extensions))
	}

	switch auth {
	case TLSMech:
		f.Setup("Create TLS secret", featuressteps.CopySecretInTestNamespace(system.Namespace(), tlsSecretName))
		opts = append(opts, kafkasource.WithBootstrapServers(testingpkg.BootstrapServersSslArr),
			kafkasource.WithTLSCACert(tlsSecretName, "ca.crt"),
			kafkasource.WithTLSCert(tlsSecretName, "user.crt"),
			kafkasource.WithTLSKey(tlsSecretName, "user.key"),
			kafkasource.WithTLSEnabled(),
			kafkasource.WithTLSCACert(tlsSecretName, "ca.crt"),
		)
	case SASLMech:
		f.Setup("Create SASL secret", featuressteps.CopySecretInTestNamespace(system.Namespace(), saslSecretName))
		opts = append(opts, kafkasource.WithBootstrapServers(testingpkg.BootstrapServersSslSaslScramArr),
			kafkasource.WithSASLEnabled(),
			kafkasource.WithSASLUser(saslSecretName, "user"),
			kafkasource.WithSASLPassword(saslSecretName, "password"),
			kafkasource.WithSASLType(saslSecretName, "saslType"),
			kafkasource.WithTLSEnabled(),
			kafkasource.WithTLSCACert(saslSecretName, "ca.crt"),
		)
	default:
		opts = append(opts, kafkasource.WithBootstrapServers(testingpkg.BootstrapServersPlaintextArr))
	}

	f.Setup("install KafkaSource", kafkasource.Install(kafkaSource, opts...))
	f.Setup("KafkaSource is ready", kafkasource.IsReady(kafkaSource))

	options := []eventshub.EventsHubOption{
		eventshub.StartSenderToResource(kafkasink.GVR(), ksink),
		eventshub.AddSequence,
		eventshub.SendMultipleEvents(1, time.Millisecond),
	}
	options = append(options, senderOpts...)
	f.Requirement("install sender for ksink", eventshub.Install(eventshubSource, options...))

	f.Assert("sink receives event", matchEvent(eventshubSink, kafkaSource, topic, matcherGen))

	return f
}

func matchEvent(sink, source, topic string, matcherGen MatcherGenerator) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		cloudEventsSourceName := sourcesv1beta1.KafkaEventSource(environment.FromContext(ctx).Namespace(), source, topic)
		cloudEventsEventType := sourcesv1beta1.KafkaEventType

		assert.OnStore(sink).MatchEvent(matcherGen(cloudEventsSourceName, cloudEventsEventType)).Exact(1)(ctx, t)
	}
}

func KafkaSourceBinaryEvent() *feature.Feature {
	senderOptions := []eventshub.EventsHubOption{
		eventshub.InputHeader("ce-specversion", "1.0"),
		eventshub.InputHeader("ce-type", "com.github.pull.create"),
		eventshub.InputHeader("ce-source", "github.com/cloudevents/spec/pull"),
		eventshub.InputHeader("ce-subject", "123"),
		eventshub.InputHeader("ce-id", "A234-1234-1234"),
		eventshub.InputHeader("content-type", "application/json"),
		eventshub.InputHeader("ce-comexampleextension1", "value"),
		eventshub.InputHeader("ce-comexampleothervalue", "5"),
		eventshub.InputBody(marshalJSON(map[string]string{
			"hello": "Francesco",
		})),
		eventshub.InputMethod("POST"),
	}
	matcherGen := func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
		return AllOf(
			HasSpecVersion(cloudevents.VersionV1),
			HasType("com.github.pull.create"),
			HasSource("github.com/cloudevents/spec/pull"),
			HasSubject("123"),
			HasId("A234-1234-1234"),
			HasDataContentType("application/json"),
			HasData([]byte(`{"hello":"Francesco"}`)),
			HasExtension("comexampleextension1", "value"),
			HasExtension("comexampleothervalue", "5"),
		)
	}

	return TestKafkaSourceAuth(PlainMech, EmptyExtensions, senderOptions, matcherGen)
}

func KafkaSourceStructuredEvent() *feature.Feature {
	eventTime, _ := cetypes.ParseTime("2018-04-05T17:31:00Z")
	senderOptions := []eventshub.EventsHubOption{
		eventshub.InputHeader("content-type", "application/cloudevents+json"),
		eventshub.InputBody(marshalJSON(map[string]interface{}{
			"specversion":     "1.0",
			"type":            "com.github.pull.create",
			"source":          "https://github.com/cloudevents/spec/pull",
			"subject":         "123",
			"id":              "A234-1234-1234",
			"time":            "2018-04-05T17:31:00Z",
			"datacontenttype": "application/json",
			"data": map[string]string{
				"hello": "Francesco",
			},
			"comexampleextension1": "value",
			"comexampleothervalue": 5,
		})),
		eventshub.InputMethod("POST"),
	}
	matcherGen := func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
		return AllOf(
			HasSpecVersion(cloudevents.VersionV1),
			HasType("com.github.pull.create"),
			HasSource("https://github.com/cloudevents/spec/pull"),
			HasSubject("123"),
			HasId("A234-1234-1234"),
			HasTime(eventTime),
			HasDataContentType("application/json"),
			HasData([]byte(`{"hello":"Francesco"}`)),
			HasExtension("comexampleextension1", "value"),
			HasExtension("comexampleothervalue", "5"),
		)
	}

	return TestKafkaSourceAuth(PlainMech, EmptyExtensions, senderOptions, matcherGen)
}

func KafkaSourceWithExtensions() *feature.Feature {
	senderOptions := []eventshub.EventsHubOption{
		eventshub.InputHeader("content-type", "application/cloudevents+json"),
		eventshub.InputBody(marshalJSON(map[string]interface{}{
			"specversion": "1.0",
			"type":        "com.github.pull.create",
			"source":      "https://github.com/cloudevents/spec/pull",
			"id":          "A234-1234-1234",
			"data": map[string]string{
				"hello": "Francesco",
			},
		})),
		eventshub.InputMethod("POST"),
	}
	matcherGen := func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
		return AllOf(
			HasSpecVersion(cloudevents.VersionV1),
			HasData([]byte(`{"hello":"Francesco"}`)),
			HasType("com.github.pull.create"),
			HasSource("https://github.com/cloudevents/spec/pull"),
			HasExtension("comexampleextension1", "value"),
			HasExtension("comexampleothervalue", "5"),
		)
	}
	extensions := map[string]string{
		"comexampleextension1": "value",
		"comexampleothervalue": "5",
	}

	return TestKafkaSourceAuth(PlainMech, extensions, senderOptions, matcherGen)
}

func KafkaSourceTLS() *feature.Feature {
	e := cetest.FullEvent()
	senderOptions := []eventshub.EventsHubOption{
		eventshub.InputEvent(e),
	}
	matcherGen := func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
		return HasData(e.Data())
	}

	return TestKafkaSourceAuth(TLSMech, EmptyExtensions, senderOptions, matcherGen)
}

func KafkaSourceSASL() *feature.Feature {
	e := cetest.FullEvent()
	senderOptions := []eventshub.EventsHubOption{
		eventshub.InputEvent(e),
	}
	matcherGen := func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
		return HasData(e.Data())
	}

	return TestKafkaSourceAuth(SASLMech, EmptyExtensions, senderOptions, matcherGen)
}

func marshalJSON(val interface{}) string {
	data, _ := json.Marshal(val)
	return string(data)
}
