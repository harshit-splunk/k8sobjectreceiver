package k8sobjectreceiver

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/errors"
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
			kr.startPull(object, resource)
		} else {
			for _, ns := range object.Namespaces {
				kr.startPull(object, resource.Namespace(ns))
			}
		}

	case WatchMode:
		if len(object.Namespaces) == 0 {
			kr.startWatch(object, resource)
		} else {
			for _, ns := range object.Namespaces {
				kr.startWatch(object, resource.Namespace(ns))
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
			// pull object
			objects, err := resource.List(kr.ctx, metav1.ListOptions{
				FieldSelector: config.FieldSelector,
				LabelSelector: config.LabelSelector,
			})
			if err != nil {
				kr.setting.Logger.Error("error in pulling object", zap.String("resource", config.gvr.String()), zap.Error(err))
				// return
			} else {
				// feed info
				fmt.Println(objects)
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
	ss, ok := err.(errors.APIStatus)
	if ok {
		kr.setting.Logger.Error("error in watching object", zap.String("resource", config.gvr.String()), zap.Any("status", ss.Status().Reason))
		return
	} else if err != nil {
		kr.setting.Logger.Error("error in watching object", zap.String("resource", config.gvr.String()), zap.Error(err))
		return
	}

	res := watch.ResultChan()
	for {
		select {
		case data := <-res:
			udata := data.Object.(*unstructured.Unstructured)
			udata.GetResourceVersion()
			fmt.Println(udata)
			// kr.setting.Logger.Info(fmt.Sprint(data.Object))
		case <-kr.ctx.Done():
			watch.Stop()
			return
		}
	}
	// client := kr.client

	// res, _ := client.Resource(*config.gvr).Watch(kr.ctx, metav1.ListOptions{})
	// defer res.Stop()
	// data := <-res.ResultChan()
	// udata := data.Object.(*unstructured.Unstructured)
	// // res.Items
	// fmt.Println(udata)

}

func (kr *k8sobjectreceiver) toLogData(event *unstructured.UnstructuredList) plog.Logs {
	out := plog.NewLogs()
	rls := out.ResourceLogs().AppendEmpty()
	// resource := rls.Resource()
	// resource.Attributes().Insert()
	sl := rls.ScopeLogs().AppendEmpty()
	logSlice := sl.LogRecords()
	logSlice.EnsureCapacity(len(event.Items))
	for _, e := range event.Items {
		// bytes, err := json.Marshal(e.Object)
		// if err != nil {
		// 	kr.setting.Logger.Error("Error in unmarshalling", zap.Error(err))
		// 	continue
		// }
		record := logSlice.AppendEmpty()
		dest := record.Body()
		// record.Body().SetStringVal(string(bytes))
		toAttributeMap(e.Object).CopyTo(dest)
	}
	if logSlice.Len() == 0 {
		kr.setting.Logger.Info(fmt.Sprint("Empty log record generated", event))
	}
	return out
}

func toAttributeMap(obsMap map[string]interface{}) pcommon.Value {
	attVal := pcommon.NewValueMap()
	attMap := attVal.MapVal()
	insertToAttributeMap(obsMap, attMap)
	return attVal
}

func insertToAttributeMap(obsMap map[string]interface{}, dest pcommon.Map) {
	dest.EnsureCapacity(len(obsMap))
	for k, v := range obsMap {
		switch t := v.(type) {
		case bool:
			dest.InsertBool(k, t)
		case string:
			dest.InsertString(k, t)
		case []string:
			arr := toStringArray(t)
			dest.Insert(k, arr)
		case []byte:
			dest.InsertBytes(k, pcommon.NewImmutableByteSlice(t))
		case int64:
			dest.InsertInt(k, t)
		case int32:
			dest.InsertInt(k, int64(t))
		case int16:
			dest.InsertInt(k, int64(t))
		case int8:
			dest.InsertInt(k, int64(t))
		case int:
			dest.InsertInt(k, int64(t))
		case uint64:
			dest.InsertInt(k, int64(t))
		case uint32:
			dest.InsertInt(k, int64(t))
		case uint16:
			dest.InsertInt(k, int64(t))
		case uint8:
			dest.InsertInt(k, int64(t))
		case uint:
			dest.InsertInt(k, int64(t))
		case float64:
			dest.InsertDouble(k, t)
		case float32:
			dest.InsertDouble(k, float64(t))
		case map[string]interface{}:
			subMap := toAttributeMap(t)
			dest.Insert(k, subMap)
		case []interface{}:
			arr := toAttributeArray(t)
			dest.Insert(k, arr)
		default:
			dest.InsertString(k, fmt.Sprintf("%v", t))
		}
	}
}

func toAttributeArray(obsArr []interface{}) pcommon.Value {
	arrVal := pcommon.NewValueSlice()
	arr := arrVal.SliceVal()
	arr.EnsureCapacity(len(obsArr))
	for _, v := range obsArr {
		insertToAttributeVal(v, arr.AppendEmpty())
	}
	return arrVal
}

func insertToAttributeVal(value interface{}, dest pcommon.Value) {
	switch t := value.(type) {
	case bool:
		dest.SetBoolVal(t)
	case string:
		dest.SetStringVal(t)
	case []string:
		toStringArray(t).CopyTo(dest)
	case []byte:
		dest.SetBytesVal(pcommon.NewImmutableByteSlice(t))
	case int64:
		dest.SetIntVal(t)
	case int32:
		dest.SetIntVal(int64(t))
	case int16:
		dest.SetIntVal(int64(t))
	case int8:
		dest.SetIntVal(int64(t))
	case int:
		dest.SetIntVal(int64(t))
	case uint64:
		dest.SetIntVal(int64(t))
	case uint32:
		dest.SetIntVal(int64(t))
	case uint16:
		dest.SetIntVal(int64(t))
	case uint8:
		dest.SetIntVal(int64(t))
	case uint:
		dest.SetIntVal(int64(t))
	case float64:
		dest.SetDoubleVal(t)
	case float32:
		dest.SetDoubleVal(float64(t))
	case map[string]interface{}:
		toAttributeMap(t).CopyTo(dest)
	case []interface{}:
		toAttributeArray(t).CopyTo(dest)
	default:
		dest.SetStringVal(fmt.Sprintf("%v", t))
	}
}

func toStringArray(strArr []string) pcommon.Value {
	arrVal := pcommon.NewValueSlice()
	arr := arrVal.SliceVal()
	arr.EnsureCapacity(len(strArr))
	for _, v := range strArr {
		insertToAttributeVal(v, arr.AppendEmpty())
	}
	return arrVal
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
