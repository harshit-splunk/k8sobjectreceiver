package k8sobjectreceiver

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/collector/semconv/v1.9.0"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

const (
	// Number of log attributes to add to the plog.LogRecordSlice.
	totalLogAttributes = 3

	// Number of resource attributes to add to the plog.ResourceLogs.
	totalResourceAttributes = 2
)

func watchEventToLogData(event watch.Event) plog.Logs {
	udata := event.Object.(*unstructured.Unstructured)
	out := plog.NewLogs()
	rl := out.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	dest := lr.Body()

	resourceAttrs := rl.Resource().Attributes()
	resourceAttrs.EnsureCapacity(totalResourceAttributes)

	resourceAttrs.UpsertString("k8s.object.kind", udata.GetKind())
	resourceAttrs.UpsertString("k8s.object.api_version", udata.GetAPIVersion())

	destMap := dest.SetEmptyMapVal()
	obj := map[string]interface{}{
		"type":   string(event.Type),
		"object": udata.Object,
	}
	toMap(obj).CopyTo(destMap)

	attrs := lr.Attributes()
	attrs.EnsureCapacity(totalLogAttributes)

	attrs.UpsertString("k8s.object.name", udata.GetName())
	attrs.UpsertString("k8s.object.resource_version", udata.GetResourceVersion())
	if namespace := udata.GetNamespace(); namespace != "" {
		attrs.UpsertString(semconv.AttributeK8SNamespaceName, namespace)
	}
	return out

}

func unstructuredListToLogData(event *unstructured.UnstructuredList) plog.Logs {
	out := plog.NewLogs()
	rl := out.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()

	resourceAttrs := rl.Resource().Attributes()
	resourceAttrs.EnsureCapacity(totalResourceAttributes)

	resourceAttrs.UpsertString("k8s.object.kind", event.Items[0].GetKind())
	resourceAttrs.UpsertString("k8s.object.api_version", event.GetAPIVersion())

	logSlice := sl.LogRecords()
	logSlice.EnsureCapacity(len(event.Items))
	for _, e := range event.Items {
		record := logSlice.AppendEmpty()
		attrs := record.Attributes()
		attrs.EnsureCapacity(totalLogAttributes)

		attrs.UpsertString("k8s.object.name", e.GetName())
		attrs.UpsertString("k8s.object.resource_version", event.GetResourceVersion())
		if namespace := e.GetNamespace(); namespace != "" {
			attrs.UpsertString(semconv.AttributeK8SNamespaceName, namespace)
		}
		dest := record.Body()
		destMap := dest.SetEmptyMapVal()
		toMap(e.Object).CopyTo(destMap)
	}
	return out
}

func toMap(objects map[string]interface{}) pcommon.Map {
	val := pcommon.NewMapFromRaw(objects)
	return val

}
