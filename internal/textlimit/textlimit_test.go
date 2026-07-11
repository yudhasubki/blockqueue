package textlimit

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestUTF8LeavesBoundedTextUnchanged(t *testing.T) {
	require.Equal(t, "temporary failure", UTF8("temporary failure", 64))
}

func TestUTF8TruncatesAtRuneBoundaryWithSuffix(t *testing.T) {
	limited := UTF8(strings.Repeat("😀", 100), 64)
	require.LessOrEqual(t, len([]byte(limited)), 64)
	require.True(t, utf8.ValidString(limited))
	require.True(t, strings.HasSuffix(limited, truncatedSuffix))
}

func TestUTF8NormalizesInvalidInput(t *testing.T) {
	limited := UTF8("valid\xfftext", 64)
	require.True(t, utf8.ValidString(limited))
	require.Equal(t, "valid\uFFFDtext", limited)
}
