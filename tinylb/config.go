package tinylb

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

type Config struct {
	// TargetGroups is a list of configs for all if this load balancer's target groups.
	TargetGroups []TargetGroupConfig `json:"target_groups"`
	// ControlPlane is config for the load balancer's control plane server.
	ControlPlane ControlPlaneConfig `json:"control_plane"`
}

type ControlPlaneConfig struct {
	// Port is the port on which the control plane HTTP server should listen.
	Port uint
}

type TargetGroupConfig struct {
	// Name is a unique name for this target group.
	Name string
	// Port is the port on which this target group should listen for new connections.
	Port uint
	// Timeout is how long inactive connections should live.
	Timeout uint
	// Targets is a list of configs for all if this target group's targets.
	Targets []TargetConfig
}

type TargetConfig struct {
	// The host name of the target's backend server.
	Host string
	// The port that the target's backend server expects connections on.
	Port uint
}

// LoadConfig tries to decode a Config struct from the given reader.
func LoadConfig(reader io.Reader) (*Config, error) {
	var config Config
	err := json.NewDecoder(reader).Decode(&config)
	if err != nil {
		return nil, err
	}

	// Ensure config includes control plane config
	if config.ControlPlane.Port == 0 {
		return nil, errors.New("control plane config must be defined")
	}

	// Ensure config has at least one target group
	if len(config.TargetGroups) < 1 {
		return nil, errors.New("load balancer must have at least one target group")
	}

	// Ensure every target group has at least one target
	for _, targetGroup := range config.TargetGroups {
		if len(targetGroup.Targets) < 1 {
			return nil, fmt.Errorf("target group %s must have at least one target", targetGroup.Name)
		}
	}

	return &config, nil
}
