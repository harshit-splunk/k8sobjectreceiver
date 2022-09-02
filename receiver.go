package k8sobjectreceiver

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
)

type k8sobjectreceiver struct {
	setting  component.ReceiverCreateSettings
	objects  []*K8sObjectsConfig
	client   dynamic.Interface
	consumer consumer.Logs
	// stopperChanList []chan struct{}
	startTime     time.Time
	ctx           context.Context
	cancel        context.CancelFunc
	factory       dynamicinformer.DynamicSharedInformerFactory
	currentObject *K8sObjectsConfig
	count         int
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

	// dynamicinformer.NewDynamicSharedInformerFactory(client,)
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

	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(kr.client, time.Minute, metav1.NamespaceAll, dynamicinformer.TweakListOptionsFunc(func(options *metav1.ListOptions) {
		kr.count += 1
		if kr.currentObject == nil {
			return
		}
		options.LabelSelector = kr.currentObject.LabelSelector
		options.FieldSelector = kr.currentObject.FieldSelector
	}))
	kr.factory = factory
	factory.Start(kr.ctx.Done())

	kr.prepareInformers()

	// factory.WaitForCacheSync(kr.ctx.Done())

	for _, object := range kr.objects {
		kr.start(object)
	}
	// result, err := kr.client.CoreV1().Pods(v1.NamespaceAll).List(ctx, v1.ListOptions{})
	// if err != nil {
	// 	kr.setting.Logger.Error(err.Error())
	// }
	// if kr.client != nil {
	// 	return nil
	// }
	// resource := kr.client.Resource(schema.GroupVersionResource{
	// 	Group:    "",
	// 	Version:  "v1",
	// 	Resource: "pods",
	// })

	// ul, err := resource.List(ctx, v1.ListOptions{
	// 	LabelSelector: "",
	// 	FieldSelector: "",
	// })
	// if err != nil {
	// 	kr.setting.Logger.Error(err.Error())
	// 	return nil
	// }
	// logData := kr.toLogData(ul)
	// if err = kr.consumer.ConsumeLogs(ctx, logData); err != nil {
	// 	kr.setting.Logger.Error(err.Error())
	// }
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
	kr.cancel()
	return nil
}
func (kr *k8sobjectreceiver) start(object *K8sObjectsConfig) {
	switch object.Mode {
	case PullMode:
		if len(object.Namespaces) == 0 {
			kr.startPull(object, object.listers[0])
		} else {
			for i := range object.Namespaces {
				kr.startPull(object, object.listers[i])
			}
		}

	case WatchMode:
		if len(object.Namespaces) == 0 {
			kr.startWatch(object, metav1.NamespaceAll)
		} else {
			for _, ns := range object.Namespaces {
				kr.startWatch(object, ns)
			}
		}

	}
}

func (kr *k8sobjectreceiver) startPull(config *K8sObjectsConfig, lister cache.GenericNamespaceLister) {

	objects, err := lister.List(labels.Everything())
	if err != nil {
		kr.setting.Logger.Error("error in pulling object", zap.String("resource", config.gvr.String()), zap.Error(err))
	} else {
		// feed info
		fmt.Println(len(objects), "objects received.", "hit count is ", kr.count)
	}
	// ticker := NewTicker(config.Interval)
	// for {
	// 	select {
	// 	case <-ticker.C:
	// 		// pull object
	// 		objects, err := lister.List(labels.Everything())
	// 		if err != nil {
	// 			kr.setting.Logger.Error("error in pulling object", zap.String("resource", config.gvr.String()), zap.Error(err))
	// 			return
	// 		} else {
	// 			// feed info
	// 			fmt.Println(objects)
	// 		}
	// 	case <-kr.ctx.Done():
	// 		return
	// 	}

	// }
	// dynamicinformer.
	// lister := factory.ForResource(*object.gvr).Lister()
	// lister.List()

}

func (kr *k8sobjectreceiver) startWatch(config *K8sObjectsConfig, namespace string) {
	client := kr.client

	res, _ := client.Resource(*config.gvr).Watch(kr.ctx, metav1.ListOptions{})
	defer res.Stop()
	data := <-res.ResultChan()
	udata := data.Object.(*unstructured.Unstructured)
	// res.Items
	fmt.Println(udata)

}

func (kr *k8sobjectreceiver) prepareInformers() {
	for _, object := range kr.objects {
		kr.currentObject = object
		object.generateListers(kr.factory)
		kr.factory.Start(kr.ctx.Done())
		kr.factory.WaitForCacheSync(kr.ctx.Done())
	}
	kr.currentObject = nil
}

// func (kr *k8sobjectreceiver) toLogData(event *unstructured.UnstructuredList) plog.Logs {
// 	out := plog.NewLogs()
// 	rls := out.ResourceLogs().AppendEmpty()
// 	logSlice := rls.ScopeLogs().AppendEmpty().LogRecords()
// 	logSlice.EnsureCapacity(len(event.Items))
// 	for _, e := range event.Items {
// 		bytes, err := json.Marshal(e.Object)
// 		if err != nil {
// 			kr.setting.Logger.Error("Error in unmarshalling", zap.Error(err))
// 			continue
// 		}
// 		record := logSlice.AppendEmpty()
// 		record.Body().SetStringVal(string(bytes))
// 	}
// 	if logSlice.Len() == 0 {
// 		kr.setting.Logger.Info(fmt.Sprint("Empty log record generated", event))
// 	}
// 	return out
// }

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
