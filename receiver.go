package k8sobjectreceiver

import (
	"context"
	"encoding/json"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"

	"go.uber.org/zap"
)

type k8sobjectreceiver struct {
	setting  component.ReceiverCreateSettings
	config   *Config
	client   kubernetes.Interface
	consumer consumer.Logs
}

func newReceiver(params component.ReceiverCreateSettings, config *Config, consumer consumer.Logs) (component.LogsReceiver, error) {
	client, err := config.getClient()
	if err != nil {
		return nil, err
	}
	return k8sobjectreceiver{
		config:   config,
		client:   client,
		setting:  params,
		consumer: consumer,
	}, nil
}

func (kr k8sobjectreceiver) Start(ctx context.Context, host component.Host) error {
	kr.setting.Logger.Info("Object Receiver started")
	// result, err := kr.client.CoreV1().Pods(v1.NamespaceAll).List(ctx, v1.ListOptions{})
	// if err != nil {
	// 	kr.setting.Logger.Error(err.Error())
	// }
	dc, err := kr.config.getDynamicClient()
	if err != nil {
		kr.setting.Logger.Error(err.Error())
		return nil
	}
	resource := dc.Resource(schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "pods",
	})

	ul, err := resource.List(ctx, v1.ListOptions{})
	if err != nil {
		kr.setting.Logger.Error(err.Error())
		return nil
	}
	logData := kr.toLogData(ul)
	if err = kr.consumer.ConsumeLogs(ctx, logData); err != nil {
		kr.setting.Logger.Error(err.Error())
	}
	// unstructuredList, err := resource.List(ctx, v1.ListOptions{})
	// watch, err := resource.Watch(ctx, v1.ListOptions{})
	// if err != nil {
	// 	kr.setting.Logger.Error(err.Error())
	// 	return nil
	// }
	// res := watch.ResultChan()

	// for {
	// 	select {
	// 	case data := <-res:
	// 		kr.setting.Logger.Info(fmt.Sprint(data.Object))
	// 	case <-ctx.Done():
	// 		watch.Stop()
	// 		return nil
	// 	}
	// }

	// kr.setting.Logger.Info(fmt.Sprint(unstructuredList.Object))
	return nil
}

func (kr k8sobjectreceiver) Shutdown(context.Context) error {
	kr.setting.Logger.Info("Object Receiver stopped")
	return nil
}

func (kr k8sobjectreceiver) toLogData(event *unstructured.UnstructuredList) plog.Logs {
	out := plog.NewLogs()
	rls := out.ResourceLogs().AppendEmpty()
	logSlice := rls.ScopeLogs().AppendEmpty().LogRecords()
	logSlice.EnsureCapacity(len(event.Items))
	for _, e := range event.Items {
		bytes, err := json.Marshal(e.Object)
		if err != nil {
			kr.setting.Logger.Error("Error in unmarshalling", zap.Error(err))
			continue
		}
		record := logSlice.AppendEmpty()
		record.Body().SetStringVal(string(bytes))
	}
	if logSlice.Len() == 0 {
		kr.setting.Logger.Info(fmt.Sprint("Empty log record generated", event))
	}
	return out
}
