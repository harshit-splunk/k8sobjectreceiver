package k8sobjectreceiver

import (
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/config"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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
}

type Config struct {
	config.ReceiverSettings `mapstructure:",squash"`
	Objects                 map[string][]*K8sObjectsConfig `mapstructure:"objects"`

	// For mocking purposes only.
	makeDiscoveryClient func() (discovery.ServerResourcesInterface, error)
	makeDynamicClient   func() (dynamic.Interface, error)
}

func (c *Config) Validate() error {

	validObjects, err := c.getValidObjects()
	if err != nil {
		return err
	}
	for apiGroup, apiGroupConf := range c.Objects {
		validResources, ok := validObjects[apiGroup]
		if !ok {
			return fmt.Errorf("api group %v not found", apiGroup)
		}

		split := strings.Split(apiGroup, "/")
		if len(split) == 1 && apiGroup == "v1" {
			split = []string{"", apiGroup}
		} else if len(split) != 2 {
			return fmt.Errorf("invalid group/version: %v", apiGroup)
		}
		for _, obj := range apiGroupConf {

			if obj.Mode == "" {
				obj.Mode = PullMode
			} else if _, ok := modeMap[obj.Mode]; !ok {
				return fmt.Errorf("invalid mode: %v", obj.Mode)
			}

			if _, ok := validResources[obj.Name]; !ok {
				return fmt.Errorf("api resource %v not found in api group %v", obj.Name, apiGroup)
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

func (c *Config) getDiscoveryClient() (discovery.ServerResourcesInterface, error) {
	if c.makeDiscoveryClient != nil {
		return c.makeDiscoveryClient()
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return client.Discovery(), nil
}

func (c *Config) getDynamicClient() (dynamic.Interface, error) {
	if c.makeDynamicClient != nil {
		return c.makeDynamicClient()
	}
	config, err := rest.InClusterConfig()

	if err != nil {
		return nil, err
	}
	return dynamic.NewForConfig(config)
}

func (c *Config) getValidObjects() (map[string]map[string]struct{}, error) {
	dc, err := c.getDiscoveryClient()
	dc.ServerPreferredResources()
	if err != nil {
		return nil, err
	}

	res, err := dc.ServerPreferredResources()
	if err != nil {
		return nil, err
	}

	validObjects := make(map[string]map[string]struct{}, len(res))

	for _, group := range res {
		name := group.GroupVersion
		validObjects[name] = make(map[string]struct{}, len(group.APIResources))
		for _, resource := range group.APIResources {
			validObjects[name][resource.Name] = struct{}{}
		}
	}
	return validObjects, nil
}
