package tinylb

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadConfig(t *testing.T) {
	// A config with JSON syntax errors should error.
	config, err := LoadConfig(strings.NewReader("{]"))
	assert.Equal(t, err.Error(), "invalid character ']' looking for beginning of object key string")
	assert.Empty(t, config)

	// A config with no target group config should error.
	config, err = LoadConfig(strings.NewReader("{}"))
	assert.Equal(t, err.Error(), "load balancer must have at least one target group")
	assert.Empty(t, config)

	// A config with a target group with no targets should error.
	config, err = LoadConfig(strings.NewReader(`
	{
		"target_groups": [
			{"name": "target-1", "port": 8080, "timeout": 30}
		]
	}
	`))
	assert.Equal(t, err.Error(), "target group target-1 must have at least one target")
	assert.Empty(t, config)

	// A valid config should load without errors.
	config, err = LoadConfig(strings.NewReader(`
	{
		"target_groups": [
			{
				"name": "target-1",
				"port": 8080,
				"timeout": 30,
				"targets": [
					{ "host": "localhost", "port": 8081 },
					{ "host": "localhost", "port": 8082 }
				]
			},
			{
				"name": "target-2",
				"port": 7080,
				"timeout": 60,
				"targets": [
					{ "host": "1.2.3.4", "port": 7081 }
				]
			}
		]
	}
	`))
	assert.Empty(t, err)
	assert.Equal(t, *config, Config{
		TargetGroups: []TargetGroupConfig{
			{
				Name:    "target-1",
				Port:    8080,
				Timeout: 30,
				Targets: []TargetConfig{
					{Host: "localhost", Port: 8081},
					{Host: "localhost", Port: 8082},
				},
			},
			{
				Name:    "target-2",
				Port:    7080,
				Timeout: 60,
				Targets: []TargetConfig{
					{Host: "1.2.3.4", Port: 7081},
				},
			},
		}})
}
