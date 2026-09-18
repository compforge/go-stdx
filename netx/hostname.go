// Package netx extends the standard net package with network address helpers.
package netx

import (
	"net"
	"strings"
)

// IsDNSHostname reports whether host is an ASCII DNS hostname rather than an
// IP literal. A single trailing dot and case-insensitive letters are accepted.
// Local names such as localhost are valid here; callers decide whether to trust them.
func IsDNSHostname(host string) bool {
	host = strings.TrimSuffix(host, ".")
	if host == "" || len(host) > 253 || net.ParseIP(host) != nil {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' {
				continue
			}
			return false
		}
	}
	return true
}
