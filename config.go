package k8sobjectreceiver

import (
	"fmt"

	"go.opentelemetry.io/collector/config"
	"k8s.io/client-go/dynamic"
	k8s "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Config struct {
	config.ReceiverSettings `mapstructure:",squash"`
}

func (c *Config) Validate() error {
	fmt.Println("Validate receiver")
	return c.ReceiverSettings.Validate()
}

func (c *Config) getClient() (k8s.Interface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return k8s.NewForConfig(config)
}

func (c *Config) getDynamicClient() (dynamic.Interface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return dynamic.NewForConfig(config)
}
