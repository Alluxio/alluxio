// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package textutil

import (
	"bytes"
)

// TODO(toddw): Add UTF16 support.

const (
	EOF                = rune(-1) // Indicates the end of a rune stream.
	LineSeparator      = '\u2028' // Unicode line separator rune.
	ParagraphSeparator = '\u2029' // Unicode paragraph separator rune.
)

// RuneEncoder is the interface to an encoder of a stream of runes into
// bytes.Buffer.
type RuneEncoder interface {
	// Encode encodes r into buf.
	Encode(r rune, buf *bytes.Buffer)
}

// RuneChunkDecoder is the interface to a decoder of a stream of encoded runes
// that may be arbitrarily chunked.
//
// Implementations of RuneChunkDecoder are commonly used to implement io.Writer
// wrappers, to handle buffering when chunk boundaries may occur in the middle
// of an encoded rune.
type RuneChunkDecoder interface {
	// DecodeRune returns the next rune in chunk, and its width in bytes.  If
	// chunk represents a partial rune, the chunk is buffered and returns EOF and
	// the size of the chunk.  Subsequent calls to DecodeRune will combine
	// previously buffered data when decoding.
	DecodeRune(chunk []byte) (r rune, n int)
	// FlushRune returns the next buffered rune.  Returns EOF when all buffered
	// data is returned.
	FlushRune() rune
}

// WriteRuneChunk is a helper that repeatedly calls d.DecodeRune(chunk) until
// EOF, calling fn for every rune that is decoded.  Returns the number of bytes
// in data that were successfully processed.  If fn returns an error,
// WriteRuneChunk will return with that error, without processing any more data.
//
// This is a convenience for implementing io.Writer, given a RuneChunkDecoder.
func WriteRuneChunk(d RuneChunkDecoder, fn func(rune) error, chunk []byte) (int, error) {
	pos := 0
	for pos < len(chunk) {
		r, size := d.DecodeRune(chunk[pos:])
		pos += size
		if r == EOF {
			break
		}
		if err := fn(r); err != nil {
			return pos, err
		}
	}
	return pos, nil
}

// FlushRuneChunk is a helper that repeatedly calls d.FlushRune until EOF,
// calling fn for every rune that is decoded.  If fn returns an error, Flush
// will return with that error, without processing any more data.
//
// This is a convenience for implementing an additional Flush() call on an
// implementation of io.Writer, given a RuneChunkDecoder.
func FlushRuneChunk(d RuneChunkDecoder, fn func(rune) error) error {
	for {
		r := d.FlushRune()
		if r == EOF {
			return nil
		}
		if err := fn(r); err != nil {
			return err
		}
	}
}

// bytePos and runePos distinguish positions that are used in either domain;
// we're trying to avoid silly mistakes like adding a bytePos to a runePos.
type bytePos int
type runePos int

// byteRuneBuffer maintains a buffer with both byte and rune based positions.
type byteRuneBuffer struct {
	enc     RuneEncoder
	buf     bytes.Buffer
	runeLen runePos
}

func (b *byteRuneBuffer) ByteLen() bytePos { return bytePos(b.buf.Len()) }
func (b *byteRuneBuffer) RuneLen() runePos { return b.runeLen }
func (b *byteRuneBuffer) Bytes() []byte    { return b.buf.Bytes() }

func (b *byteRuneBuffer) Reset() {
	b.buf.Reset()
	b.runeLen = 0
}

// WriteRune writes r into b.
func (b *byteRuneBuffer) WriteRune(r rune) {
	b.enc.Encode(r, &b.buf)
	b.runeLen++
}

// WriteString writes str into b.
func (b *byteRuneBuffer) WriteString(str string) {
	for _, r := range str {
		b.WriteRune(r)
	}
}

// WriteString0Runes writes str into b, not incrementing the rune length.
func (b *byteRuneBuffer) WriteString0Runes(str string) {
	for _, r := range str {
		b.enc.Encode(r, &b.buf)
	}
}
