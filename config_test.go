package k8sobjectreceiver

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/service/servicetest"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	fakeDiscovery "k8s.io/client-go/discovery/fake"
)

func TestLoadConfig(t *testing.T) {
	factories, err := componenttest.NopFactories()
	require.NoError(t, err)

	factory := NewFactory()
	factories.Receivers[config.Type(typeStr)] = factory
	cfg, err := servicetest.LoadConfig(filepath.Join("testdata", "config.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	require.Equal(t, len(cfg.Receivers), 1)

	r1 := cfg.Receivers[config.NewComponentID(typeStr)].(*Config)
	r1.makeDiscoveryClient = getMockDiscoveryClient

	expected := map[string][]*K8sObjectsConfig{
		"v1": {
			{
				Name:          "pods",
				Mode:          PullMode,
				Interval:      time.Second * 30,
				FieldSelector: "status.phase=Running",
				LabelSelector: "environment in (production),tier in (frontend)",
			},
			{
				Name:       "events",
				Mode:       WatchMode,
				Namespaces: []string{"default"},
			},
		},
	}

	eq := reflect.DeepEqual(expected, r1.Objects)
	fmt.Println(eq)
	assert.EqualValues(t, expected, r1.Objects)

	err = cfg.Validate()
	require.NoError(t, err)

}

func TestValidateConfigs(t *testing.T) {
	factories, err := componenttest.NopFactories()
	require.NoError(t, err)

	factory := NewFactory()
	factories.Receivers[config.Type(typeStr)] = factory
	cfg, err := servicetest.LoadConfig(filepath.Join("testdata", "invalid_config.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	invalid_api_config := cfg.Receivers[config.NewComponentIDWithName(typeStr, "invalid_api")].(*Config)
	invalid_resource_config := cfg.Receivers[config.NewComponentIDWithName(typeStr, "invalid_resource")].(*Config)

	invalid_api_config.makeDiscoveryClient = getMockDiscoveryClient
	invalid_resource_config.makeDiscoveryClient = getMockDiscoveryClient

	err = invalid_api_config.Validate()
	assert.ErrorContains(t, err, "api group fakev1 not found")

	err = invalid_resource_config.Validate()
	assert.ErrorContains(t, err, "api resource fake_resource not found in api group v1")

}

type MockDiscovery struct {
	fakeDiscovery.FakeDiscovery
}

func (c *MockDiscovery) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return []*metav1.APIResourceList{
		{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{
					Name: "pods",
					Kind: "Pods",
				},
				{
					Name: "events",
					Kind: "Events",
				},
			},
		},
	}, nil
}

func getMockDiscoveryClient() (discovery.ServerResourcesInterface, error) {
	return &MockDiscovery{}, nil
}
