package headercodec

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeRequiresStringMap(t *testing.T) {
	headers, err := Decode(`{"source":"test"}`)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"source": "test"}, headers)

	for _, raw := range []string{"null", `[]`, `{"attempt":1}`, "{"} {
		_, err := Decode(raw)
		require.Error(t, err, raw)
	}
}
