// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cmdline

import "testing"

func TestGodocHeader(t *testing.T) {
	tests := []struct {
		Path, Short, Want string
	}{
		{"", "", ""},
		{"path", "", "Path"},
		{"", "short", "Short"},
		{"path", "short", "Path - short"},
		{"path a b c", "short x y z", "Path a b c - short x y z"},
		// Try some cases where the short string contains characters that godoc
		// disallows, so we fall back on just the path.
		{"path", "bad.", "Path"},
		{"path", "bad;", "Path"},
		{"path", "bad,", "Path"},
		{"path", "(bad)", "Path"},
	}
	for _, test := range tests {
		if got, want := godocHeader(test.Path, test.Short), test.Want; got != want {
			t.Errorf("(%q, %q) got %q, want %q", test.Path, test.Short, got, want)
		}
	}
}
