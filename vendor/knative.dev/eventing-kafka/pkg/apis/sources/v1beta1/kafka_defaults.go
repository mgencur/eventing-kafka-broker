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

	"github.com/google/uuid"
	"k8s.io/utils/pointer"
)

const (
	uuidPrefix = "knative-kafka-source-"
)

// SetDefaults ensures KafkaSource reflects the default values.
func (k *KafkaSource) SetDefaults(ctx context.Context) {
	if k.Spec.ConsumerGroup == "" {
		k.Spec.ConsumerGroup = uuidPrefix + uuid.New().String()
	}

	if k.Spec.Consumers == nil {
		k.Spec.Consumers = pointer.Int32Ptr(1)
	}

	if k.Spec.InitialOffset == "" {
		k.Spec.InitialOffset = OffsetLatest
	}
}
