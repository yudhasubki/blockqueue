// Package textlimit provides UTF-8-safe bounds for diagnostic text stored by
// BlockQueue. It is internal so persistence and worker boundaries share one
// implementation without expanding the public API.
package textlimit

import (
	"strings"
	"unicode/utf8"
)

const truncatedSuffix = "…[truncated]"

// UTF8 normalizes value to valid UTF-8 and limits it to maxBytes. Truncated
// values retain a visible suffix when the limit is large enough to contain it.
func UTF8(value string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	value = strings.ToValidUTF8(value, "\uFFFD")
	if len(value) <= maxBytes {
		return value
	}
	if maxBytes < len(truncatedSuffix) {
		return prefix(value, maxBytes)
	}
	return prefix(value, maxBytes-len(truncatedSuffix)) + truncatedSuffix
}

func prefix(value string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(value) <= maxBytes {
		return value
	}
	end := maxBytes
	for end > 0 && !utf8.ValidString(value[:end]) {
		end--
	}
	return value[:end]
}
