package pool

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGoPool(t *testing.T) {
	gopool := NewGoPool(WithMaxLimit(3))
	for i := 0; i < 10; i++ {
		go gopool.Submit(func() { time.Sleep(time.Hour * time.Duration(1)) })
	}

	assert.Equal(t, 0, gopool.size())
}

func TestGoPoolWithPanicFn(t *testing.T) {
	gopool := NewGoPool(WithMaxLimit(1))
	for i := 0; i < 10; i++ {
		go gopool.Submit(func() { panic("whoops") })
	}

	assert.Equal(t, 0, gopool.size())
}
