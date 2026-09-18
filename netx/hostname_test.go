package netx

import (
	"strings"
	"testing"
)

func TestIsDNSHostname(t *testing.T) {
	for _, tc := range []struct {
		host string
		want bool
	}{
		{"minio.minio", true},
		{"Storage.Example.", true},
		{"localhost", true},
		{"internal.localhost", true},
		{"xn--bcher-kva.example", true},
		{strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 61), true},
		{"10.255.144.82", false},
		{"::1", false},
		{"", false},
		{"storage..example", false},
		{"-storage.example", false},
		{"storage-.example", false},
		{"storage_example", false},
		{"bücher.example", false},
		{"storage.example..", false},
		{strings.Repeat("a", 64) + ".example", false},
		{strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 63), false},
	} {
		t.Run(tc.host, func(t *testing.T) {
			if got := IsDNSHostname(tc.host); got != tc.want {
				t.Fatalf("IsDNSHostname(%q) = %t, want %t", tc.host, got, tc.want)
			}
		})
	}
}
