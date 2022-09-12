package k8sobjectreceiver

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
)

type k8sobjectreceiver struct {
	setting   component.ReceiverCreateSettings
	objects   []*K8sObjectsConfig
	client    dynamic.Interface
	consumer  consumer.Logs
	startTime time.Time
	ctx       context.Context
	cancel    context.CancelFunc
}

func newReceiver(params component.ReceiverCreateSettings, config *Config, consumer consumer.Logs) (component.LogsReceiver, error) {
	client, err := config.getClient()
	if err != nil {
		return nil, err
	}

	objects := make([]*K8sObjectsConfig, 0)
	for _, objs := range config.Objects {
		objects = append(objects, objs...)
	}

	return k8sobjectreceiver{
		client:    client,
		setting:   params,
		consumer:  consumer,
		objects:   objects,
		startTime: time.Now(),
	}, nil
}

func (kr k8sobjectreceiver) Start(ctx context.Context, host component.Host) error {
	kr.setting.Logger.Info("Object Receiver started")
	kr.ctx, kr.cancel = context.WithCancel(ctx)

	for _, object := range kr.objects {
		kr.start(object)
	}
	return nil
}

func (kr k8sobjectreceiver) Shutdown(context.Context) error {
	kr.setting.Logger.Info("Object Receiver stopped")
	kr.cancel()
	return nil
}

func (kr *k8sobjectreceiver) start(object *K8sObjectsConfig) {
	resource := kr.client.Resource(*object.gvr)

	switch object.Mode {
	case PullMode:
		if len(object.Namespaces) == 0 {
			go kr.startPull(object, resource)
		} else {
			for _, ns := range object.Namespaces {
				go kr.startPull(object, resource.Namespace(ns))
			}
		}

	case WatchMode:
		if len(object.Namespaces) == 0 {
			go kr.startWatch(object, resource)
		} else {
			for _, ns := range object.Namespaces {
				go kr.startWatch(object, resource.Namespace(ns))
			}
		}
	}
}

func (kr *k8sobjectreceiver) startPull(config *K8sObjectsConfig, resource dynamic.ResourceInterface) {
	ticker := NewTicker(config.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			objects, err := resource.List(kr.ctx, metav1.ListOptions{
				FieldSelector: config.FieldSelector,
				LabelSelector: config.LabelSelector,
			})
			if err != nil {
				kr.setting.Logger.Error("error in pulling object", zap.String("resource", config.gvr.String()), zap.Error(err))
			} else {
				logs := unstructuredListToLogData(objects)
				kr.consumer.ConsumeLogs(kr.ctx, logs)
			}
		case <-kr.ctx.Done():
			return
		}

	}

}

func (kr *k8sobjectreceiver) startWatch(config *K8sObjectsConfig, resource dynamic.ResourceInterface) {

	watch, err := resource.Watch(kr.ctx, metav1.ListOptions{
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
			udata := data.Object.(*unstructured.Unstructured)
			kr.setting.Logger.Info(udata.GetName())
			logs := unstructuredToLogData(udata)
			kr.consumer.ConsumeLogs(kr.ctx, logs)
		case <-kr.ctx.Done():
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
