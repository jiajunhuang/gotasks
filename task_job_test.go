package gotasks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestArgsMapToStruct(t *testing.T) {
	type Case struct {
		Name string `json:"name"`
		This int    `json:"this"`
	}

	s := Case{"hello", 1}

	am := StructToArgsMap(s)

	s2 := Case{}
	err := ArgsMapToStruct(am, &s2)
	if assert.NoError(t, err) {
		assert.Equal(t, s, s2)
	}
}
