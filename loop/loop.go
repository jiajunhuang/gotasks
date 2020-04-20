package loop

import (
	"context"
	"log"
)

// Execute loop execute fn until ctx.Done() is received
func Execute(ctx context.Context, fn func()) error {
	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			log.Printf("ctx is done for %s", err)
			return err
		default:
			fn()
		}
	}
}
