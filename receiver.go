package k8sobjectreceiver

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
)

type k8sobjectreceiver struct {
	setting         component.ReceiverCreateSettings
	objects         []*K8sObjectsConfig
	stopperChanList []chan struct{}
	client          dynamic.Interface
	consumer        consumer.Logs
	startTime       time.Time
}

func newReceiver(params component.ReceiverCreateSettings, config *Config, consumer consumer.Logs) (component.LogsReceiver, error) {
	client, err := config.getDynamicClient()
	if err != nil {
		return nil, err
	}

	return &k8sobjectreceiver{
		client:    client,
		setting:   params,
		consumer:  consumer,
		objects:   config.Objects,
		startTime: time.Now(),
	}, nil
}

func (kr *k8sobjectreceiver) Start(ctx context.Context, host component.Host) error {
	kr.setting.Logger.Info("Object Receiver started")

	for _, object := range kr.objects {
		kr.start(ctx, object)
	}
	return nil
}

func (kr *k8sobjectreceiver) Shutdown(context.Context) error {
	kr.setting.Logger.Info("Object Receiver stopped")
	for _, stopperChan := range kr.stopperChanList {
		close(stopperChan)
	}
	return nil
}

func (kr *k8sobjectreceiver) start(ctx context.Context, object *K8sObjectsConfig) {
	resource := kr.client.Resource(*object.gvr)

	switch object.Mode {
	case PullMode:
		if len(object.Namespaces) == 0 {
			go kr.startPull(ctx, object, resource)
		} else {
			for _, ns := range object.Namespaces {
				go kr.startPull(ctx, object, resource.Namespace(ns))
			}
		}

	case WatchMode:
		if len(object.Namespaces) == 0 {
			go kr.startWatch(ctx, object, resource)
		} else {
			for _, ns := range object.Namespaces {
				go kr.startWatch(ctx, object, resource.Namespace(ns))
			}
		}
	}
}

func (kr *k8sobjectreceiver) startPull(ctx context.Context, config *K8sObjectsConfig, resource dynamic.ResourceInterface) {
	stopperChan := make(chan struct{})
	kr.stopperChanList = append(kr.stopperChanList, stopperChan)
	ticker := NewTicker(config.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			objects, err := resource.List(ctx, metav1.ListOptions{
				FieldSelector: config.FieldSelector,
				LabelSelector: config.LabelSelector,
			})
			if err != nil {
				kr.setting.Logger.Error("error in pulling object", zap.String("resource", config.gvr.String()), zap.Error(err))
			} else if len(objects.Items) > 0 {
				logs := unstructuredListToLogData(objects)
				kr.consumer.ConsumeLogs(ctx, logs)
			}
		case <-stopperChan:
			return
		}

	}

}

func (kr *k8sobjectreceiver) startWatch(ctx context.Context, config *K8sObjectsConfig, resource dynamic.ResourceInterface) {

	stopperChan := make(chan struct{})
	kr.stopperChanList = append(kr.stopperChanList, stopperChan)

	watch, err := resource.Watch(ctx, metav1.ListOptions{
		FieldSelector: config.FieldSelector,
		LabelSelector: config.LabelSelector,
	})
	if err != nil {
		kr.setting.Logger.Error("error in watching object", zap.String("resource", config.gvr.String()), zap.Error(err))
		return
	}

	res := watch.ResultChan()
	for {
		select {
		case data := <-res:
			logs := watchEventToLogData(data)
			kr.consumer.ConsumeLogs(ctx, logs)
		case <-stopperChan:
			watch.Stop()
			return
		}
	}

}

// Start ticking immediately.
// Ref: https://stackoverflow.com/questions/32705582/how-to-get-time-tick-to-tick-immediately
func NewTicker(repeat time.Duration) *time.Ticker {
	ticker := time.NewTicker(repeat)
	oc := ticker.C
	nc := make(chan time.Time, 1)
	go func() {
		nc <- time.Now()
		for tm := range oc {
			nc <- tm
		}
	}()
	ticker.C = nc
	return ticker
}
