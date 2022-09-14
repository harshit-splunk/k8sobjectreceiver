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
	Objects                 []*K8sObjectsConfig `mapstructure:"objects"`

	// For mocking purposes only.
	makeDiscoveryClient func() (discovery.ServerResourcesInterface, error)
	makeDynamicClient   func() (dynamic.Interface, error)
}

func (c *Config) Validate() error {

	validObjects, err := c.getValidObjects()
	if err != nil {
		return err
	}
	for _, object := range c.Objects {
		gvr, ok := validObjects[object.Name]
		if !ok {
			return fmt.Errorf("resource %v not found", object.Name)
		}

		if object.Mode == "" {
			object.Mode = PullMode
		} else if _, ok := modeMap[object.Mode]; !ok {
			return fmt.Errorf("invalid mode: %v", object.Mode)
		}

		object.gvr = gvr
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

func (c *Config) getValidObjects() (map[string]*schema.GroupVersionResource, error) {
	dc, err := c.getDiscoveryClient()
	dc.ServerPreferredResources()
	if err != nil {
		return nil, err
	}

	res, err := dc.ServerPreferredResources()
	if err != nil {
		return nil, err
	}

	validObjects := make(map[string]*schema.GroupVersionResource)

	for _, group := range res {
		split := strings.Split(group.GroupVersion, "/")
		if len(split) == 1 && group.GroupVersion == "v1" {
			split = []string{"", "v1"}
		}
		for _, resource := range group.APIResources {
			validObjects[resource.Name] = &schema.GroupVersionResource{
				Group:    split[0],
				Version:  split[1],
				Resource: resource.Name,
			}
		}

	}
	return validObjects, nil
}
