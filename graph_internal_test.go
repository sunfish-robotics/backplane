package backplane

import "testing"

func TestEscapeMermaid(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{`plain`, `plain`},
		{`say "hi"`, `say #quot;hi#quot;`},
		{`a\b`, `a#92;b`},
		{"line\nbreak", "line break"},
		// '#' must be escaped first so replacement codes survive intact.
		{`#quot;`, `#35;quot;`},
	}
	for _, tt := range tests {
		if got := escapeMermaid(tt.in); got != tt.want {
			t.Errorf("escapeMermaid(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
