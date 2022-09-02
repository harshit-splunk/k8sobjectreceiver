package k8sobjectreceiver

import (
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/config"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

type Mode string

const (
	PullMode  Mode = "pull"
	WatchMode Mode = "watch"
)

var modeMap = map[Mode]bool{
	PullMode:  true,
	WatchMode: true,
}

type K8sObjectsConfig struct {
	Name          string        `mapstructure:"name"`
	Namespaces    []string      `mapstructure:"namespaces"`
	Mode          Mode          `mapstructure:"mode"`
	LabelSelector string        `mapstructure:"label_selector"`
	FieldSelector string        `mapstructure:"field_selector"`
	Interval      time.Duration `mapstructure:"interval"`
	gvr           *schema.GroupVersionResource
	listers       []cache.GenericNamespaceLister
}

type Config struct {
	config.ReceiverSettings `mapstructure:",squash"`
	Objects                 map[string][]*K8sObjectsConfig `mapstructure:"objects"`

	// For mocking purposes only.
	makeClient func() (dynamic.Interface, error)
}

func (c *Config) Validate() error {
	for apiGroup, apiGroupConf := range c.Objects {
		split := strings.Split(apiGroup, "/")
		if len(split) != 2 {
			return fmt.Errorf("invalid group/version: %v", apiGroup)
		}
		if split[0] == "core" {
			split[0] = ""
		}
		for _, obj := range apiGroupConf {
			if _, ok := modeMap[obj.Mode]; !ok {
				return fmt.Errorf("invalid mode: %v", obj.Mode)
			}
			obj.gvr = &schema.GroupVersionResource{
				Group:    split[0],
				Version:  split[1],
				Resource: obj.Name,
			}
		}
	}
	return c.ReceiverSettings.Validate()
}

func (c *Config) getClient() (dynamic.Interface, error) {
	if c.makeClient != nil {
		return c.makeClient()
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return dynamic.NewForConfig(config)
}

func (object *K8sObjectsConfig) generateListers(factory dynamicinformer.DynamicSharedInformerFactory) {
	if len(object.listers) == 0 {
		lister := factory.ForResource(*object.gvr).Lister()
		if length := len(object.Namespaces); length > 0 {
			object.listers = make([]cache.GenericNamespaceLister, len(object.Namespaces))
			for i, ns := range object.Namespaces {
				object.listers[i] = lister.ByNamespace(ns)
			}
		} else {
			object.listers = []cache.GenericNamespaceLister{lister}
		}
	}
}
