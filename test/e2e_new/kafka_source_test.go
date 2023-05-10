//go:build e2e
// +build e2e

/*
 * Copyright 2022 The Knative Authors
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

	. "github.com/cloudevents/sdk-go/v2/test"
	"knative.dev/eventing-kafka-broker/test/rekt/features"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"
)

func TestKafkaSourceCreateSecretsAfterKafkaSource(t *testing.T) {

	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.Managed(t),
	)

	env.Test(ctx, t, features.CreateSecretsAfterKafkaSource())
}

func TestKafkaSourceDeletedFromContractConfigMaps(t *testing.T) {

	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.Managed(t),
	)
	t.Cleanup(env.Finish)

	env.Test(ctx, t, features.SetupKafkaSources("permanent-kafka-source-", 21))
	env.Test(ctx, t, features.SetupAndCleanupKafkaSources("x-kafka-source-", 42))
	env.Test(ctx, t, features.KafkaSourcesAreNotPresentInContractConfigMaps("x-kafka-source-"))
}

func TestKafkaSourceScale(t *testing.T) {

	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.Managed(t),
	)
	t.Cleanup(env.Finish)

	env.Test(ctx, t, features.ScaleKafkaSource())
}

func TestKafkaSourceInitialOffsetEarliest(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.Managed(t),
	)

	topic := feature.MakeRandomK8sName("kafka-topic-earliest")

	env.Test(ctx, t, features.SetupKafkaTopicWithEvents(2, topic, eventshub.InputEvent(FullEvent())))
	env.Test(ctx, t, features.KafkaSourceInitialOffsetEarliest(2, topic))
}

func TestKafkaSourceBinaryEvent(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.Managed(t),
	)

	env.TestSet(ctx, t, features.KafkaSourceBinaryEvent())
}

func TestKafkaSourceStructuredEvent(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.Managed(t),
	)

	env.TestSet(ctx, t, features.KafkaSourceStructuredEvent())
}

/* KafkaSource dispatcher throws this:
Picked up JAVA_TOOL_OPTIONS: -XX:+CrashOnOutOfMemoryError
May 10, 2023 1:04:30 PM io.vertx.core.impl.ContextBase
SEVERE: Unhandled exception
java.lang.NullPointerException: Cannot invoke "io.cloudevents.CloudEvent.getSpecVersion()" because "event" is null
at io.cloudevents.core.builder.CloudEventBuilder.from(CloudEventBuilder.java:314)
at dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.CloudEventOverridesMutator.apply(CloudEventOverridesMutator.java:40)
at dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.CloudEventOverridesMutator.apply(CloudEventOverridesMutator.java:27)
at dev.knative.eventing.kafka.broker.dispatcher.impl.RecordDispatcherMutatorChain.dispatch(RecordDispatcherMutatorChain.java:53)
at dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.OrderedConsumerVerticle.dispatch(OrderedConsumerVerticle.java:192)
at dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.OrderedConsumerVerticle.lambda$recordsHandler$4(OrderedConsumerVerticle.java:183)
at dev.knative.eventing.kafka.broker.core.OrderedAsyncExecutor.consume(OrderedAsyncExecutor.java:103)
at dev.knative.eventing.kafka.broker.core.OrderedAsyncExecutor.offer(OrderedAsyncExecutor.java:92)
at dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.OrderedConsumerVerticle.recordsHandler(OrderedConsumerVerticle.java:183)
at io.vertx.core.impl.future.FutureImpl$1.onSuccess(FutureImpl.java:91)
at io.vertx.core.impl.future.FutureImpl$ListenerArray.onSuccess(FutureImpl.java:262)
at io.vertx.core.impl.future.FutureBase.emitSuccess(FutureBase.java:60)
at io.vertx.core.impl.future.FutureImpl.tryComplete(FutureImpl.java:211)
at io.vertx.core.impl.future.PromiseImpl.tryComplete(PromiseImpl.java:23)
at io.vertx.core.impl.future.PromiseImpl.onSuccess(PromiseImpl.java:49)
at io.vertx.core.impl.future.PromiseImpl.handle(PromiseImpl.java:41)
at io.vertx.core.impl.future.PromiseImpl.handle(PromiseImpl.java:23)
at io.vertx.kafka.client.consumer.impl.KafkaConsumerImpl.lambda$poll$16(KafkaConsumerImpl.java:685)
at io.vertx.kafka.client.consumer.impl.KafkaReadStreamImpl.lambda$null$41(KafkaReadStreamImpl.java:878)
at io.vertx.core.impl.ContextInternal.dispatch(ContextInternal.java:264)
at io.vertx.core.impl.WorkerContext.lambda$run$3(WorkerContext.java:106)
at io.vertx.core.impl.WorkerContext.lambda$null$1(WorkerContext.java:92)
at io.vertx.core.impl.TaskQueue.run(TaskQueue.java:76)
at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
at io.netty.util.concurrent.FastThreadLocalRunnable.run(FastThreadLocalRunnable.java:30)
at java.base/java.lang.Thread.run(Thread.java:833)
*/
func TestKafkaSourceWithExtensions(t *testing.T) {
	t.Skip("Doesn't work.")

	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.Managed(t),
	)

	env.TestSet(ctx, t, features.KafkaSourceWithExtensions())
}

func TestKafkaSourceTLS(t *testing.T) {

	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.Managed(t),
	)

	env.TestSet(ctx, t, features.KafkaSourceTLS())
}

func TestKafkaSourceSASL(t *testing.T) {

	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.Managed(t),
	)

	env.TestSet(ctx, t, features.KafkaSourceSASL())
}
