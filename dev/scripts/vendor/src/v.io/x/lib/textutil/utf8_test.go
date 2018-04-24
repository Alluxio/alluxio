// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package textutil

import (
	"reflect"
	"strings"
	"testing"
)

func TestUTF8ChunkDecoder(t *testing.T) {
	r2 := "Δ"
	r3 := "王"
	r4 := "\U0001F680"
	tests := []struct {
		Text  string
		Write []rune
		Flush []rune
	}{
		{"", nil, nil},
		{"a", []rune{'a'}, nil},
		{"abc", []rune{'a', 'b', 'c'}, nil},
		{"abc def ghi", []rune{'a', 'b', 'c', ' ', 'd', 'e', 'f', ' ', 'g', 'h', 'i'}, nil},
		// 2-byte runes.
		{"ΔΘΠΣΦ", []rune{'Δ', 'Θ', 'Π', 'Σ', 'Φ'}, nil},
		// 3-byte runes.
		{"王普澤世界", []rune{'王', '普', '澤', '世', '界'}, nil},
		// 4-byte runes.
		{"\U0001F680\U0001F681\U0001F682\U0001F683", []rune{'\U0001F680', '\U0001F681', '\U0001F682', '\U0001F683'}, nil},
		// Mixed-bytes.
		{"aΔ王\U0001F680普Θb", []rune{'a', 'Δ', '王', '\U0001F680', '普', 'Θ', 'b'}, nil},
		// Error runes translated to U+FFFD.
		{"\uFFFD", []rune{'\uFFFD'}, nil},
		{"a\uFFFDb", []rune{'a', '\uFFFD', 'b'}, nil},
		{"\x80", []rune{'\uFFFD'}, nil},
		{"\xFF", []rune{'\uFFFD'}, nil},
		{"a\x80b", []rune{'a', '\uFFFD', 'b'}, nil},
		{"a\xFFb", []rune{'a', '\uFFFD', 'b'}, nil},
		// Multi-byte full runes.
		{r2, []rune{[]rune(r2)[0]}, nil},
		{r3, []rune{[]rune(r3)[0]}, nil},
		{r4, []rune{[]rune(r4)[0]}, nil},
		// Partial runes translated to one or more U+FFFD.  Since each case is a
		// multi-byte encoding that's missing one or more bytes, the FFFD bytes are
		// all returned in Flush rather than Write.
		{r2[:1], nil, []rune{'\uFFFD'}},
		{r3[:1], nil, []rune{'\uFFFD'}},
		{r3[:2], nil, []rune{'\uFFFD', '\uFFFD'}},
		{r4[:1], nil, []rune{'\uFFFD'}},
		{r4[:2], nil, []rune{'\uFFFD', '\uFFFD'}},
		{r4[:3], nil, []rune{'\uFFFD', '\uFFFD', '\uFFFD'}},
		// Trailing partial runes translated to U+FFFD.  Similar to above, the FFFD
		// bytes are all returned in Flush rather than Write
		{"a" + r2[:1], []rune{'a'}, []rune{'\uFFFD'}},
		{"a" + r3[:1], []rune{'a'}, []rune{'\uFFFD'}},
		{"a" + r3[:2], []rune{'a'}, []rune{'\uFFFD', '\uFFFD'}},
		{"a" + r4[:1], []rune{'a'}, []rune{'\uFFFD'}},
		{"a" + r4[:2], []rune{'a'}, []rune{'\uFFFD', '\uFFFD'}},
		{"a" + r4[:3], []rune{'a'}, []rune{'\uFFFD', '\uFFFD', '\uFFFD'}},
		// Leading partial runes translated to U+FFFD.  The "b" suffix causes us to
		// discover that the encoding is invalid during Write.
		{r2[:1] + "b", []rune{'\uFFFD', 'b'}, nil},
		{r3[:1] + "b", []rune{'\uFFFD', 'b'}, nil},
		{r3[:2] + "b", []rune{'\uFFFD', '\uFFFD', 'b'}, nil},
		{r4[:1] + "b", []rune{'\uFFFD', 'b'}, nil},
		{r4[:2] + "b", []rune{'\uFFFD', '\uFFFD', 'b'}, nil},
		{r4[:3] + "b", []rune{'\uFFFD', '\uFFFD', '\uFFFD', 'b'}, nil},
		// Bracketed partial runes translated to U+FFFD.
		{"a" + r2[:1] + "b", []rune{'a', '\uFFFD', 'b'}, nil},
		{"a" + r3[:1] + "b", []rune{'a', '\uFFFD', 'b'}, nil},
		{"a" + r3[:2] + "b", []rune{'a', '\uFFFD', '\uFFFD', 'b'}, nil},
		{"a" + r4[:1] + "b", []rune{'a', '\uFFFD', 'b'}, nil},
		{"a" + r4[:2] + "b", []rune{'a', '\uFFFD', '\uFFFD', 'b'}, nil},
		{"a" + r4[:3] + "b", []rune{'a', '\uFFFD', '\uFFFD', '\uFFFD', 'b'}, nil},
	}
	for _, test := range tests {
		// Run with a variety of chunk sizes.
		for _, sizes := range [][]int{nil, {1}, {2}, {1, 2}, {2, 1}, {3}, {1, 2, 3}} {
			dec := new(UTF8ChunkDecoder)
			if got, want := writeRuneChunk(t, dec, test.Text, sizes), test.Write; !reflect.DeepEqual(got, want) {
				t.Errorf("%q write got %v, want %v", test.Text, got, want)
			}
			if got, want := flushRuneChunk(t, dec, test.Text), test.Flush; !reflect.DeepEqual(got, want) {
				t.Errorf("%q flush got %v, want %v", test.Text, got, want)
			}
		}
	}
}

func writeRuneChunk(t testing.TB, dec RuneChunkDecoder, text string, sizes []int) []rune {
	var runes []rune
	addRune := func(r rune) error {
		runes = append(runes, r)
		return nil
	}
	// Write chunks of different sizes until we've exhausted the input text.
	remain := []byte(text)
	for ix := 0; len(remain) > 0; ix++ {
		var chunk []byte
		chunk, remain = nextChunk(remain, sizes, ix)
		got, err := WriteRuneChunk(dec, addRune, chunk)
		if want := len(chunk); got != want || err != nil {
			t.Errorf("%q WriteRuneChunk(%q) got (%d,%v), want (%d,nil)", text, chunk, got, err, want)
		}
	}
	return runes
}

func flushRuneChunk(t testing.TB, dec RuneChunkDecoder, text string) []rune {
	var runes []rune
	addRune := func(r rune) error {
		runes = append(runes, r)
		return nil
	}
	// Flush the decoder.
	if err := FlushRuneChunk(dec, addRune); err != nil {
		t.Errorf("%q FlushRuneChunk got %v, want nil", text, err)
	}
	return runes
}

func nextChunk(text []byte, sizes []int, index int) (chunk, remain []byte) {
	if len(sizes) == 0 {
		return text, nil
	}
	size := sizes[index%len(sizes)]
	if size >= len(text) {
		return text, nil
	}
	return text[:size], text[size:]
}

// benchText contains a mix of 1, 2, 3 and 4 byte runes, and invalid encodings.
var benchText = strings.Repeat("a bc def ghij klmno pqrstu vwxyz A BC DEF GHIJ KLMNO PQRSTU VWXYZ 0123456789 !@#$%^&*()ΔΘΠΣΦ王普澤世界\U0001F680\U0001F681\U0001F682\U0001F683\uFFFD\xFF ", 100)

func benchRuneChunkDecoder(b *testing.B, dec RuneChunkDecoder, sizes []int) {
	for i := 0; i < b.N; i++ {
		writeRuneChunk(b, dec, benchText, sizes)
		flushRuneChunk(b, dec, benchText)
	}
}

func BenchmarkUTF8ChunkDecoder_Sizes_0(b *testing.B) {
	benchRuneChunkDecoder(b, new(UTF8ChunkDecoder), nil)
}
func BenchmarkUTF8ChunkDecoder_Sizes_1(b *testing.B) {
	benchRuneChunkDecoder(b, new(UTF8ChunkDecoder), []int{1})
}
func BenchmarkUTF8ChunkDecoder_Sizes_1_2_3(b *testing.B) {
	benchRuneChunkDecoder(b, new(UTF8ChunkDecoder), []int{1, 2, 3})
}
