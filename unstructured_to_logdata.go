package k8sobjectreceiver

import (
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/collector/semconv/v1.9.0"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	// Number of log attributes to add to the plog.LogRecordSlice.
	totalLogAttributes = 2

	// Number of resource attributes to add to the plog.ResourceLogs.
	totalResourceAttributes = 3
)

func unstructuredToLogData(event *unstructured.Unstructured) plog.Logs {
	out := plog.NewLogs()
	rl := out.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	dest := lr.Body()

	resourceAttrs := rl.Resource().Attributes()
	resourceAttrs.EnsureCapacity(totalResourceAttributes)

	resourceAttrs.UpsertString("k8s.object.kind", event.GetKind())
	resourceAttrs.UpsertString("k8s.object.api_version", event.GetAPIVersion())
	if namespace := event.GetNamespace(); namespace != "" {
		resourceAttrs.UpsertString(semconv.AttributeK8SNamespaceName, namespace)
	}

	toAttributeMap(event.Object).CopyTo(dest)

	attrs := lr.Attributes()
	attrs.EnsureCapacity(totalLogAttributes)

	attrs.UpsertString("k8s.object.name", event.GetName())
	attrs.UpsertString("k8s.object.resource_version", event.GetResourceVersion())
	return out

}

func unstructuredListToLogData(event *unstructured.UnstructuredList) plog.Logs {
	out := plog.NewLogs()
	rls := out.ResourceLogs().AppendEmpty()
	sl := rls.ScopeLogs().AppendEmpty()
	logSlice := sl.LogRecords()
	logSlice.EnsureCapacity(len(event.Items))
	for _, e := range event.Items {
		record := logSlice.AppendEmpty()
		dest := record.Body()
		toAttributeMap(e.Object).CopyTo(dest)
	}
	return out
}

// Copied from https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/pkg/stanza/adapter/converter.go
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
