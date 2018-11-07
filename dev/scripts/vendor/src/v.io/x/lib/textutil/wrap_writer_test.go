// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package textutil

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

type lp struct {
	line, para string
}

var (
	allIndents  = [][]int{nil, {}, {1}, {2}, {1, 2}, {2, 1}}
	allIndents1 = [][]int{{1}, {2}, {1, 2}, {2, 1}}
)

func TestWrapWriter(t *testing.T) {
	tests := []struct {
		Width   int
		Indents [][]int
		In      string // See xlateIn for details on the format
		Want    string // See xlateWant for details on the format
	}{
		// Completely blank input yields empty output.
		{4, allIndents, "", ""},
		{4, allIndents, " ", ""},
		{4, allIndents, "  ", ""},
		{4, allIndents, "   ", ""},
		{4, allIndents, "    ", ""},
		{4, allIndents, "     ", ""},
		{4, allIndents, "      ", ""},
		{4, allIndents, "F N  R   V    L     P      ", ""},
		// Single words never get word-wrapped, even if they're long.
		{4, allIndents, "a", "0a."},
		{4, allIndents, "ab", "0ab."},
		{4, allIndents, "abc", "0abc."},
		{4, allIndents, "abcd", "0abcd."},
		{4, allIndents, "abcde", "0abcde."},
		{4, allIndents, "abcdef", "0abcdef."},
		// Word-wrapping boundary conditions.
		{4, allIndents, "abc ", "0abc."},
		{4, allIndents, "abc  ", "0abc."},
		{4, allIndents, "abcN", "0abc."},
		{4, allIndents, "abcN ", "0abc."},
		{4, allIndents, "abcd ", "0abcd."},
		{4, allIndents, "abcd  ", "0abcd."},
		{4, allIndents, "abcdN", "0abcd."},
		{4, allIndents, "abcdN ", "0abcd."},
		{4, [][]int{nil}, "a cd", "0a cd."},
		{4, [][]int{nil}, "a cd ", "0a cd."},
		{4, [][]int{nil}, "a cdN", "0a cd."},
		{4, allIndents1, "a cd", "0a.1cd."},
		{4, allIndents1, "a cd ", "0a.1cd."},
		{4, allIndents1, "a cdN", "0a.1cd."},
		{4, allIndents, "a cde", "0a.1cde."},
		{4, allIndents, "a cde ", "0a.1cde."},
		{4, allIndents, "a cdeN", "0a.1cde."},
		{4, [][]int{nil}, "a  d", "0a  d."},
		{4, [][]int{nil}, "a  d ", "0a  d."},
		{4, [][]int{nil}, "a  dN", "0a  d."},
		{4, allIndents1, "a  d", "0a.1d."},
		{4, allIndents1, "a  d ", "0a.1d."},
		{4, allIndents1, "a  dN", "0a.1d."},
		{4, allIndents, "a  de", "0a.1de."},
		{4, allIndents, "a  de ", "0a.1de."},
		{4, allIndents, "a  deN", "0a.1de."},
		// Multi-line word-wrapping boundary conditions.
		{4, allIndents, "abc e", "0abc.1e."},
		{4, allIndents, "abc.e", "0abc.1e."},
		{4, allIndents, "abc efgh", "0abc.1efgh."},
		{4, allIndents, "abc.efgh", "0abc.1efgh."},
		{4, allIndents, "abc efghi", "0abc.1efghi."},
		{4, allIndents, "abc.efghi", "0abc.1efghi."},
		{4, [][]int{nil}, "abc e gh", "0abc.1e gh."},
		{4, [][]int{nil}, "abc.e.gh", "0abc.1e gh."},
		{4, allIndents1, "abc e gh", "0abc.1e.2gh."},
		{4, allIndents1, "abc.e.gh", "0abc.1e.2gh."},
		{4, allIndents, "abc e ghijk", "0abc.1e.2ghijk."},
		{4, allIndents, "abc.e.ghijk", "0abc.1e.2ghijk."},
		// Verbatim lines.
		{4, allIndents, " b", "0 b."},
		{4, allIndents, "  bc", "0  bc."},
		{4, allIndents, "   bcd", "0   bcd."},
		{4, allIndents, "    bcde", "0    bcde."},
		{4, allIndents, "     bcdef", "0     bcdef."},
		{4, allIndents, "      bcdefg", "0      bcdefg."},
		{4, allIndents, " b de ghijk", "0 b de ghijk."},
		// Verbatim lines before word-wrapped lines.
		{4, allIndents, " b.vw yz", "0 b.1vw.2yz."},
		{4, allIndents, "  bc.vw yz", "0  bc.1vw.2yz."},
		{4, allIndents, "   bcd.vw yz", "0   bcd.1vw.2yz."},
		{4, allIndents, "    bcde.vw yz", "0    bcde.1vw.2yz."},
		{4, allIndents, "     bcdef.vw yz", "0     bcdef.1vw.2yz."},
		{4, allIndents, "      bcdefg.vw yz", "0      bcdefg.1vw.2yz."},
		{4, allIndents, " b de ghijk.vw yz", "0 b de ghijk.1vw.2yz."},
		// Verbatim lines after word-wrapped lines.
		{4, allIndents, "vw yz. b", "0vw.1yz.2 b."},
		{4, allIndents, "vw yz.  bc", "0vw.1yz.2  bc."},
		{4, allIndents, "vw yz.   bcd", "0vw.1yz.2   bcd."},
		{4, allIndents, "vw yz.    bcde", "0vw.1yz.2    bcde."},
		{4, allIndents, "vw yz.     bcdef", "0vw.1yz.2     bcdef."},
		{4, allIndents, "vw yz.      bcdefg", "0vw.1yz.2      bcdefg."},
		{4, allIndents, "vw yz. b de ghijk", "0vw.1yz.2 b de ghijk."},
		// Verbatim lines between word-wrapped lines.
		{4, allIndents, "vw yz. b.mn pq", "0vw.1yz.2 b.2mn.2pq."},
		{4, allIndents, "vw yz.  bc.mn pq", "0vw.1yz.2  bc.2mn.2pq."},
		{4, allIndents, "vw yz.   bcd.mn pq", "0vw.1yz.2   bcd.2mn.2pq."},
		{4, allIndents, "vw yz.    bcde.mn pq", "0vw.1yz.2    bcde.2mn.2pq."},
		{4, allIndents, "vw yz.     bcdef.mn pq", "0vw.1yz.2     bcdef.2mn.2pq."},
		{4, allIndents, "vw yz.      bcdefg.mn pq", "0vw.1yz.2      bcdefg.2mn.2pq."},
		{4, allIndents, "vw yz. b de ghijk.mn pq", "0vw.1yz.2 b de ghijk.2mn.2pq."},
		// Multi-paragraphs via explicit U+2029, and multi-newline.
		{4, allIndents, "ab de ghPij lm op", "0ab.1de.2gh.:0ij.1lm.2op."},
		{4, allIndents, "ab.de.ghPij.lm.op", "0ab.1de.2gh.:0ij.1lm.2op."},
		{4, allIndents, "ab de gh Pij lm op", "0ab.1de.2gh.:0ij.1lm.2op."},
		{4, allIndents, "ab.de.gh Pij.lm.op", "0ab.1de.2gh.:0ij.1lm.2op."},
		{4, allIndents, "ab de ghNNij lm op", "0ab.1de.2gh.:0ij.1lm.2op."},
		{4, allIndents, "ab.de.ghNNij.lm.op", "0ab.1de.2gh.:0ij.1lm.2op."},
		{4, allIndents, "ab de ghNNNij lm op", "0ab.1de.2gh.:0ij.1lm.2op."},
		{4, allIndents, "ab.de.ghNNNij.lm.op", "0ab.1de.2gh.:0ij.1lm.2op."},
		{4, allIndents, "ab de gh N Nij lm op", "0ab.1de.2gh.:0ij.1lm.2op."},
		{4, allIndents, "ab.de.gh N Nij.lm.op", "0ab.1de.2gh.:0ij.1lm.2op."},
		{4, allIndents, "ab de gh N N Nij lm op", "0ab.1de.2gh.:0ij.1lm.2op."},
		{4, allIndents, "ab.de.gh N N Nij.lm.op", "0ab.1de.2gh.:0ij.1lm.2op."},
		// Special-case /r/n is a single EOL, but may be combined.
		{4, allIndents, "ab de ghRNij lm op", "0ab.1de.2gh.2ij.2lm.2op."},
		{4, allIndents, "ab.de.ghRNij.lm.op", "0ab.1de.2gh.2ij.2lm.2op."},
		{4, allIndents, "ab de gh RNij lm op", "0ab.1de.2gh.2ij.2lm.2op."},
		{4, allIndents, "ab.de.gh RNij.lm.op", "0ab.1de.2gh.2ij.2lm.2op."},
		{4, allIndents, "ab de ghRNRNij lm op", "0ab.1de.2gh.:0ij.1lm.2op."},
		{4, allIndents, "ab.de.ghRNRNij.lm.op", "0ab.1de.2gh.:0ij.1lm.2op."},
		{4, allIndents, "ab de gh RN RNij lm op", "0ab.1de.2gh.:0ij.1lm.2op."},
		{4, allIndents, "ab.de.gh RN RNij.lm.op", "0ab.1de.2gh.:0ij.1lm.2op."},
		{4, allIndents, "ab de ghR Nij lm op", "0ab.1de.2gh.:0ij.1lm.2op."},
		{4, allIndents, "ab.de.ghR Nij.lm.op", "0ab.1de.2gh.:0ij.1lm.2op."},
		// Line separator via explicit U+2028 ends lines, but not paragraphs.
		{4, allIndents, "aLcd", "0a.1cd."},
		{4, allIndents, "a Lcd", "0a.1cd."},
		{4, allIndents, "aLLcd", "0a.1cd."},
		{4, allIndents, "a LLcd", "0a.1cd."},
		// 0 width ends up with one word per line, except verbatim lines.
		{0, allIndents, "a c e", "0a.1c.2e."},
		{0, allIndents, "a cd fghij", "0a.1cd.2fghij."},
		{0, allIndents, "a. cd fghij.l n", "0a.1 cd fghij.2l.2n."},
		// -1 width ends up with all words on same line, except verbatim lines.
		{-1, allIndents, "a c e", "0a c e."},
		{-1, allIndents, "a cd fghij", "0a cd fghij."},
		{-1, allIndents, "a. cd fghij.l n", "0a.1 cd fghij.2l n."},
	}
	for _, test := range tests {
		// Run with a variety of chunk sizes.
		for _, sizes := range [][]int{nil, {1}, {2}, {1, 2}, {2, 1}} {
			// Run with a variety of line terminators and paragraph separators.
			for _, lp := range []lp{{}, {"\n", "\n"}, {"L", "P"}, {"LLL", "PPP"}} {
				// Run with a variety of indents.
				if len(test.Indents) == 0 {
					t.Errorf("%d %q %q has no indents, use [][]int{nil} rather than nil", test.Width, test.In, test.Want)
				}
				for _, indents := range test.Indents {
					var buf bytes.Buffer
					w := newUTF8WrapWriter(t, &buf, test.Width, lp, indents)
					wrapWriterWriteFlush(t, w, xlateIn(test.In), sizes)
					if got, want := buf.String(), xlateWant(test.Want, lp, indents); got != want {
						t.Errorf("%q sizes:%v lp:%q indents:%v got %q, want %q", test.In, sizes, lp, indents, got, want)
					}
				}
			}
		}
	}
}

func TestWrapWriterForceVerbatim(t *testing.T) {
	tests := []struct {
		In   string // See xlateIn for details on the format
		Want string // See xlateIn for details on the format
	}{
		{"", ""},
		{"a", "a."},
		{"a.", "a."},
		{"ab", "ab."},
		{"ab.", "ab."},
		{"abc", "abc."},
		{"abc.", "abc."},
		{"a c", "a c."},
		{"a c.", "a c."},
		{"a cde", "a cde."},
		{"a cde.", "a cde."},
		{"a c e", "a c e."},
		{"a c e.", "a c e."},
		{"a c ef", "a c ef."},
		{"a c ef.", "a c ef."},
		{"a c  f", "a c  f."},
		{"a c  f.", "a c  f."},
		{"a    f", "a    f."},
		{"a    f.", "a    f."},
		{"a c e.g i k", "a c e.g i k."},
		{"a c e.g i k.", "a c e.g i k."},
	}
	for _, test := range tests {
		// Run with a variety of chunk sizes.
		for _, sizes := range [][]int{nil, {1}, {2}, {1, 2}, {2, 1}} {
			var buf bytes.Buffer
			w := newUTF8WrapWriter(t, &buf, 1, lp{}, nil)
			w.ForceVerbatim(true)
			wrapWriterWriteFlush(t, w, xlateIn(test.In), sizes)
			if got, want := buf.String(), xlateIn(test.Want); got != want {
				t.Errorf("%q sizes:%v got %q, want %q", test.In, sizes, got, want)
			}
		}
	}
}

// xlateIn translates our test.In pattern into an actual input string to feed
// into the writer.  The point is to make it easy to specify the various control
// sequences in a single character, so it's easier to understand.
func xlateIn(text string) string {
	text = strings.Replace(text, "F", "\f", -1)
	text = strings.Replace(text, "N", "\n", -1)
	text = strings.Replace(text, ".", "\n", -1) // Also allow . for easier reading
	text = strings.Replace(text, "R", "\r", -1)
	text = strings.Replace(text, "V", "\v", -1)
	text = strings.Replace(text, "L", "\u2028", -1)
	text = strings.Replace(text, "P", "\u2029", -1)
	return text
}

// xlateWant translates our test.Want pattern into an actual expected string to
// compare against the output.  The point is to make it easy to read and write
// the expected patterns, and to make it easy to test various indents.
func xlateWant(text string, lp lp, indents []int) string {
	// Dot "." and colon ":" in the want string indicate line terminators and
	// paragraph separators, respectively.
	line := lp.line
	if line == "" {
		line = "\n"
	}
	text = strings.Replace(text, ".", line, -1)
	para := lp.para
	if para == "" {
		para = "\n"
	}
	text = strings.Replace(text, ":", para, -1)
	// The numbers in the want string indicate paragraph line numbers, to make it
	// easier to automatically replace for various indent configurations.
	switch len(indents) {
	case 0:
		text = strings.Replace(text, "0", "", -1)
		text = strings.Replace(text, "1", "", -1)
		text = strings.Replace(text, "2", "", -1)
	case 1:
		text = strings.Replace(text, "0", spaces(indents[0]), -1)
		text = strings.Replace(text, "1", spaces(indents[0]), -1)
		text = strings.Replace(text, "2", spaces(indents[0]), -1)
	case 2:
		text = strings.Replace(text, "0", spaces(indents[0]), -1)
		text = strings.Replace(text, "1", spaces(indents[1]), -1)
		text = strings.Replace(text, "2", spaces(indents[1]), -1)
	case 3:
		text = strings.Replace(text, "0", spaces(indents[0]), -1)
		text = strings.Replace(text, "1", spaces(indents[1]), -1)
		text = strings.Replace(text, "2", spaces(indents[2]), -1)
	}
	return text
}

func spaces(count int) string {
	return strings.Repeat(" ", count)
}

func newUTF8WrapWriter(t testing.TB, buf io.Writer, width int, lp lp, indents []int) *WrapWriter {
	w := NewUTF8WrapWriter(buf, width)
	if lp.line != "" || lp.para != "" {
		if err := w.SetLineTerminator(lp.line); err != nil {
			t.Errorf("SetLineTerminator(%q) got %v, want nil", lp.line, err)
		}
		if err := w.SetParagraphSeparator(lp.para); err != nil {
			t.Errorf("SetParagraphSeparator(%q) got %v, want nil", lp.para, err)
		}
	}
	if indents != nil {
		indentStrs := make([]string, len(indents))
		for ix, indent := range indents {
			indentStrs[ix] = spaces(indent)
		}
		if err := w.SetIndents(indentStrs...); err != nil {
			t.Errorf("SetIndents(%v) got %v, want nil", indentStrs, err)
		}
	}
	return w
}

func wrapWriterWriteFlush(t testing.TB, w *WrapWriter, text string, sizes []int) {
	// Write chunks of different sizes until we've exhausted the input.
	remain := []byte(text)
	for ix := 0; len(remain) > 0; ix++ {
		var chunk []byte
		chunk, remain = nextChunk(remain, sizes, ix)
		got, err := w.Write(chunk)
		if want := len(chunk); got != want || err != nil {
			t.Errorf("%q Write(%q) got (%d,%v), want (%d,nil)", text, chunk, got, err, want)
		}
	}
	// Flush the writer.
	if err := w.Flush(); err != nil {
		t.Errorf("%q Flush() got %v, want nil", text, err)
	}
}

func benchUTF8WrapWriter(b *testing.B, width int, sizes []int) {
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		w := newUTF8WrapWriter(b, &buf, width, lp{}, nil)
		wrapWriterWriteFlush(b, w, benchText, sizes)
	}
}

func BenchmarkUTF8WrapWriter_Sizes_0_Width_0(b *testing.B) {
	benchUTF8WrapWriter(b, 0, nil)
}
func BenchmarkUTF8WrapWriter_Sizes_0_Width_10(b *testing.B) {
	benchUTF8WrapWriter(b, 10, nil)
}
func BenchmarkUTF8WrapWriter_Sizes_0_Width_Inf(b *testing.B) {
	benchUTF8WrapWriter(b, -1, nil)
}

func BenchmarkUTF8WrapWriter_Sizes_1_Width_0(b *testing.B) {
	benchUTF8WrapWriter(b, 0, []int{1})
}
func BenchmarkUTF8WrapWriter_Sizes_1_Width_10(b *testing.B) {
	benchUTF8WrapWriter(b, 10, []int{1})
}
func BenchmarkUTF8WrapWriter_Sizes_1_Width_Inf(b *testing.B) {
	benchUTF8WrapWriter(b, -1, []int{1})
}

func BenchmarkUTF8WrapWriter_Sizes_1_2_3_Width_0(b *testing.B) {
	benchUTF8WrapWriter(b, 0, []int{1, 2, 3})
}
func BenchmarkUTF8WrapWriter_Sizes_1_2_3_Width_10(b *testing.B) {
	benchUTF8WrapWriter(b, 10, []int{1, 2, 3})
}
func BenchmarkUTF8WrapWriter_Sizes_1_2_3_Width_Inf(b *testing.B) {
	benchUTF8WrapWriter(b, -1, []int{1, 2, 3})
}
