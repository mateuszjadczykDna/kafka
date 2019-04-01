package common

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAgentNodes(t *testing.T) {
	expectedNodes := []string{
		"cc-trogdor-service-agent-0",
		"cc-trogdor-service-agent-1",
		"cc-trogdor-service-agent-2",
	}
	clientNodes := TrogdorAgentPodNames(3)
	assert.Equal(t, clientNodes, expectedNodes)
}
