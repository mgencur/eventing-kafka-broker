/*
Copyright 2020 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1beta1

import (
	"context"
	"fmt"

	"knative.dev/eventing/pkg/apis/eventing"
	"knative.dev/pkg/apis"
)

func (kc *KafkaChannel) Validate(ctx context.Context) *apis.FieldError {
	errs := kc.Spec.Validate(ctx).ViaField("spec")

	// Validate annotations
	if kc.Annotations != nil {
		if scope, ok := kc.Annotations[eventing.ScopeAnnotationKey]; ok {
			if scope != "namespace" && scope != "cluster" {
				iv := apis.ErrInvalidValue(scope, "")
				iv.Details = "expected either 'cluster' or 'namespace'"
				errs = errs.Also(iv.ViaFieldKey("annotations", eventing.ScopeAnnotationKey).ViaField("metadata"))
			}
		}
	}

	return errs
}

func (kcs *KafkaChannelSpec) Validate(ctx context.Context) *apis.FieldError {
	var errs *apis.FieldError

	if kcs.NumPartitions <= 0 {
		fe := apis.ErrInvalidValue(kcs.NumPartitions, "numPartitions")
		errs = errs.Also(fe)
	}

	if kcs.ReplicationFactor <= 0 {
		fe := apis.ErrInvalidValue(kcs.ReplicationFactor, "replicationFactor")
		errs = errs.Also(fe)
	}

	retentionDuration, err := kcs.ParseRetentionDuration()
	if retentionDuration < 0 || err != nil {
		fe := apis.ErrInvalidValue(kcs.RetentionDuration, "retentionDuration")
		errs = errs.Also(fe)
	}

	for i, subscriber := range kcs.SubscribableSpec.Subscribers {
		if subscriber.ReplyURI == nil && subscriber.SubscriberURI == nil {
			fe := apis.ErrMissingField("replyURI", "subscriberURI")
			fe.Details = "expected at least one of, got none"
			errs = errs.Also(fe.ViaField(fmt.Sprintf("subscriber[%d]", i)).ViaField("subscribable"))
		}
	}
	return errs
}
