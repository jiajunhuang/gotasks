package gotasks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUseBadRedisBroker(t *testing.T) {
	assert.Panics(t, func() { UseRedisBroker("abcd", WithRedisTaskTTL(1)) })
	assert.NotPanics(t, func() { UseRedisBroker(testRedisURL) })
}
