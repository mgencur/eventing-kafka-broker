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
	ceevent "github.com/cloudevents/sdk-go/v2/event"
	cetest "github.com/cloudevents/sdk-go/v2/test"
	"github.com/google/uuid"
	featuresjobsink "knative.dev/eventing/test/rekt/features/jobsink"
	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/eventing/test/rekt/resources/jobsink"
	"knative.dev/eventing/test/rekt/resources/trigger"
	"knative.dev/pkg/apis"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
)

func BrokerWithJobSinkSetup() (*feature.Feature, string, string) {
	f := feature.NewFeature()

	sink := feature.MakeRandomK8sName("sink")
	jobSink := feature.MakeRandomK8sName("jobsink")

	sinkURL := &apis.URL{Scheme: "http", Host: sink}

	f.Setup("install forwarder sink", eventshub.Install(sink, eventshub.StartReceiver))
	f.Setup("install job sink", jobsink.Install(jobSink, jobsink.WithForwarderJob(sinkURL.String())))

	f.Setup("jobsink is addressable", jobsink.IsAddressable(jobSink))
	f.Setup("jobsink is ready", jobsink.IsAddressable(jobSink))

	return f, jobSink, sink
}

func BrokerWithJobSink(env environment.Environment, jobSink string) (*feature.Feature, ceevent.Event) {
	f := feature.NewFeature()

	source := feature.MakeRandomK8sName("source")
	brokerName := feature.MakeRandomK8sName("broker")
	triggerName := feature.MakeRandomK8sName("trigger")

	event := cetest.FullEvent()
	event.SetID(uuid.NewString())

	f.Setup("Install broker", broker.Install(brokerName, broker.WithEnvConfig()...))
	f.Setup("Broker is ready", broker.IsReady(brokerName))

	f.Setup("create trigger", trigger.Install(triggerName, trigger.WithBrokerName(brokerName),
		trigger.WithSubscriber(jobsink.AsKReferenceN(jobSink, env.Namespace()), ""),
	))
	f.Setup("trigger is ready", trigger.IsReady(triggerName))

	f.Requirement("install source", eventshub.Install(source,
		eventshub.StartSenderToResource(broker.GVR(), brokerName),
		eventshub.InputEvent(event)))

	f.Assert("source sent the event", assert.OnStore(source).
		Match(assert.MatchKind(eventshub.EventResponse)).
		Match(assert.MatchStatusCode(202)).
		AtLeast(1),
	)

	return f, event
}

func BrokerWithJobSinkVerify(jobSink, sink string, event ceevent.Event) *feature.Feature {
	f := feature.NewFeature()

	f.Assert("event delivered", assert.OnStore(sink).
		MatchReceivedEvent(cetest.HasId(event.ID())).
		AtLeast(1),
	)

	f.Assert("at least one Job is complete", featuresjobsink.AtLeastOneJobIsComplete(jobSink, ""))

	return f
}
