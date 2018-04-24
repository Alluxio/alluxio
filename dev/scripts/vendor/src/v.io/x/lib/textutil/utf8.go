// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package textutil

import (
	"bytes"
	"fmt"
	"unicode/utf8"
)

// UTF8Encoder implements RuneEncoder for the UTF-8 encoding.
type UTF8Encoder struct{}

var _ RuneEncoder = UTF8Encoder{}

// Encode encodes r into buf in the UTF-8 encoding.
func (UTF8Encoder) Encode(r rune, buf *bytes.Buffer) { buf.WriteRune(r) }

// UTF8ChunkDecoder implements RuneChunkDecoder for a stream of UTF-8 data that
// is arbitrarily chunked.
//
// UTF-8 is a byte-wise encoding that may use multiple bytes to encode a single
// rune.  This decoder buffers partial runes that have been split across chunks,
// so that a full rune is returned when the subsequent data chunk is provided.
//
// This is commonly used to implement an io.Writer wrapper over UTF-8 text.  It
// is useful since the data provided to Write calls may be arbitrarily chunked.
//
// The zero UTF8ChunkDecoder is a decoder with an empty buffer.
type UTF8ChunkDecoder struct {
	// The only state we keep is the last partial rune we've encountered.
	partial    [utf8.UTFMax]byte
	partialLen int
}

var _ RuneChunkDecoder = (*UTF8ChunkDecoder)(nil)

// DecodeRune implements the RuneChunkDecoder interface method.
//
// Invalid encodings are transformed into U+FFFD, one byte at a time.  See
// unicode/utf8.DecodeRune for details.
func (d *UTF8ChunkDecoder) DecodeRune(chunk []byte) (rune, int) {
	if d.partialLen > 0 {
		return d.decodeRunePartial(chunk)
	}
	r, size := utf8.DecodeRune(chunk)
	if r == utf8.RuneError && !utf8.FullRune(chunk) {
		// Initialize the partial rune buffer with chunk.
		d.partialLen = copy(d.partial[:], chunk)
		return d.verifyPartial(d.partialLen, chunk)
	}
	return r, size
}

// decodeRunePartial implements decodeRune when there is a previously buffered
// partial rune.
func (d *UTF8ChunkDecoder) decodeRunePartial(chunk []byte) (rune, int) {
	// Append as much as we can to the partial rune, and see if it's full.
	oldLen := d.partialLen
	d.partialLen += copy(d.partial[oldLen:], chunk)
	if !utf8.FullRune(d.partial[:d.partialLen]) {
		// We still don't have a full rune - keep waiting.
		return d.verifyPartial(d.partialLen-oldLen, chunk)
	}
	// We finally have a full rune.
	r, size := utf8.DecodeRune(d.partial[:d.partialLen])
	if size < oldLen {
		// This occurs when we have a multi-byte rune that has the right number of
		// bytes, but is an invalid code point.
		//
		// Say oldLen=2, and we just received the third byte of a 3-byte rune which
		// isn't a UTF-8 trailing byte.  In this case utf8.DecodeRune returns U+FFFD
		// and size=1, to indicate we should skip the first byte.
		//
		// We shift the unread portion of the old partial buffer forward, and update
		// the partial len so that it's strictly decreasing.  The strictly
		// decreasing property isn't necessary for correctness, but helps avoid
		// repeatedly copying into the partial buffer unecessarily.
		copy(d.partial[:], d.partial[size:oldLen])
		d.partialLen = oldLen - size
		return r, 0
	}
	// We've used all of the partial buffer.
	d.partialLen = 0
	return r, size - oldLen
}

// verifyPartial is called when we don't have a full rune, and ncopy bytes have
// been copied from data into the decoder partial rune buffer.  We expect that
// all data has been buffered and we return EOF and the total size of the data.
func (d *UTF8ChunkDecoder) verifyPartial(ncopy int, data []byte) (rune, int) {
	if ncopy < len(data) {
		// Something's very wrong if we managed to fill d.partial without copying
		// all the data; any sequence of utf8.UTFMax bytes must be a full rune.
		panic(fmt.Errorf("UTF8ChunkDecoder: partial rune %v with leftover data %v", d.partial[:d.partialLen], data[ncopy:]))
	}
	return EOF, len(data)
}

// FlushRune implements the RuneChunkDecoder interface method.
//
// Since the only data that is buffered is the final partial rune, the return
// value will only ever be U+FFFD or EOF.  No valid runes are ever returned by
// this method, but multiple U+FFFD may be returned before EOF.
func (d *UTF8ChunkDecoder) FlushRune() rune {
	if d.partialLen == 0 {
		return EOF
	}
	r, size := utf8.DecodeRune(d.partial[:d.partialLen])
	copy(d.partial[:], d.partial[size:])
	d.partialLen -= size
	return r
}
