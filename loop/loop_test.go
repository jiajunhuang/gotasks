package loop

import (
	"context"
	"testing"
	"time"
)

func TestLoop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(100)*time.Microsecond)
	Execute(ctx, func() { time.Sleep(time.Microsecond * time.Duration(10)) })
	cancel()
}
