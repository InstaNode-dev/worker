// Command smoke-buildinfo prints the linked-in buildinfo values to stdout.
//
// Used by `make smoke-buildinfo` to verify the -ldflags -X path actually
// flows through to instant.dev/common/buildinfo at link time. The CI
// signal is "did the override land?" — not how the values are formatted.
package main

import (
	"fmt"
	"io"
	"os"

	"instant.dev/common/buildinfo"
)

// render writes the buildinfo smoke line to w. Extracted from main() so the
// output shape is unit-testable without spawning the binary.
func render(w io.Writer) {
	fmt.Fprintf(w, "GitSHA=%s BuildTime=%s Version=%s\n",
		buildinfo.GitSHA, buildinfo.BuildTime, buildinfo.Version)
}

func main() {
	render(os.Stdout)
}
