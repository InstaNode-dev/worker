package main

import (
	"bytes"
	"strings"
	"testing"

	"instant.dev/common/buildinfo"
)

// TestRender pins the smoke-buildinfo output shape: a single line carrying
// the three linked-in buildinfo fields. The format is what `make
// smoke-buildinfo` greps to confirm the -ldflags -X override landed.
func TestRender(t *testing.T) {
	var buf bytes.Buffer
	render(&buf)

	out := buf.String()
	if !strings.HasPrefix(out, "GitSHA=") {
		t.Fatalf("output must start with GitSHA=; got %q", out)
	}
	if !strings.HasSuffix(out, "\n") {
		t.Errorf("output must end with newline; got %q", out)
	}
	for _, want := range []string{
		"GitSHA=" + buildinfo.GitSHA,
		"BuildTime=" + buildinfo.BuildTime,
		"Version=" + buildinfo.Version,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output %q missing %q", out, want)
		}
	}
}
