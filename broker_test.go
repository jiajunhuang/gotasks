package gotasks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUseBadRedisBroker(t *testing.T) {
	assert.Panics(t, func() { UseRedisBroker("abcd", 1) })
	assert.NotPanics(t, func() { UseRedisBroker(testRedisURL, 1) })
}
