// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package textutil

import (
	"bytes"
	"io"
	"unicode/utf8"
)

// WriteFlusher is the interface that groups the basic Write and Flush methods.
//
// Flush is typically provided when Write calls perform buffering; Flush
// immediately outputs the buffered data.  Flush must be called after the last
// call to Write, and may be called an arbitrary number of times before the last
// Write.
type WriteFlusher interface {
	io.Writer
	Flush() error
}

// PrefixWriter returns an io.Writer that wraps w, where the prefix is written
// out immediately before the first non-empty Write call.
func PrefixWriter(w io.Writer, prefix string) io.Writer {
	return &prefixWriter{w, []byte(prefix)}
}

type prefixWriter struct {
	w      io.Writer
	prefix []byte
}

func (w *prefixWriter) Write(data []byte) (int, error) {
	if w.prefix != nil && len(data) > 0 {
		w.w.Write(w.prefix)
		w.prefix = nil
	}
	return w.w.Write(data)
}

// PrefixLineWriter returns a WriteFlusher that wraps w.  Each occurrence of EOL
// (\f, \n, \r, \v, LineSeparator or ParagraphSeparator) causes the preceeding
// line to be written to w, with the given prefix, in a single Write call.  Data
// without EOL is buffered until the next EOL or Flush call.  Flush appends \n to
// buffered data that doesn't end in EOL.
//
// A single Write call on the returned WriteFlusher may result in zero or more
// Write calls on the underlying w.
//
// If w implements WriteFlusher, each Flush call on the returned WriteFlusher
// results in exactly one Flush call on the underlying w.
func PrefixLineWriter(w io.Writer, prefix string) WriteFlusher {
	return &prefixLineWriter{w, []byte(prefix), len(prefix)}
}

type prefixLineWriter struct {
	w         io.Writer
	buf       []byte
	prefixLen int
}

const eolRunesAsString = "\f\n\r\v" + string(LineSeparator) + string(ParagraphSeparator)

func (w *prefixLineWriter) Write(data []byte) (int, error) {
	// Write requires that the return arg is in the range [0, len(data)], and we
	// must return len(data) on success.
	totalLen := len(data)
	for len(data) > 0 {
		index := bytes.IndexAny(data, eolRunesAsString)
		if index == -1 {
			// No EOL: buffer remaining data.
			// TODO(toddw): Flush at a max size, to avoid unbounded growth?
			w.buf = append(w.buf, data...)
			return totalLen, nil
		}
		// Saw EOL: single Write of buffer + data including EOL.
		_, eolSize := utf8.DecodeRune(data[index:])
		dataEnd := index + eolSize
		w.buf = append(w.buf, data[:dataEnd]...)
		data = data[dataEnd:]
		_, err := w.w.Write(w.buf)
		w.buf = w.buf[:w.prefixLen] // reset buf to prepare for the next line.
		if err != nil {
			return totalLen - len(data), err
		}
	}
	return totalLen, nil
}

func (w *prefixLineWriter) Flush() (e error) {
	defer func() {
		if f, ok := w.w.(WriteFlusher); ok {
			if err := f.Flush(); err != nil && e == nil {
				e = err
			}
		}
	}()
	if len(w.buf) > w.prefixLen {
		w.buf = append(w.buf, '\n') // add EOL to unterminated line.
		_, err := w.w.Write(w.buf)
		w.buf = w.buf[:w.prefixLen] // reset buf to prepare for the next line.
		if err != nil {
			return err
		}
	}
	return nil
}

// ByteReplaceWriter returns an io.Writer that wraps w, where all occurrences of
// the old byte are replaced with the new string on Write calls.
func ByteReplaceWriter(w io.Writer, old byte, new string) io.Writer {
	return &byteReplaceWriter{w, []byte{old}, []byte(new)}
}

type byteReplaceWriter struct {
	w        io.Writer
	old, new []byte
}

func (w *byteReplaceWriter) Write(data []byte) (int, error) {
	replaced := bytes.Replace(data, w.old, w.new, -1)
	if len(replaced) == 0 {
		return len(data), nil
	}
	// Write the replaced data, and return the number of bytes in data that were
	// written out, based on the proportion of replaced data written.  The
	// important boundary cases are:
	//   If all replaced data was written, we return n=len(data).
	//   If not all replaced data was written, we return n<len(data).
	n, err := w.w.Write(replaced)
	return n * len(data) / len(replaced), err
}

// TODO(toddw): Add ReplaceWriter, which performs arbitrary string replacements.
// This will need to buffer data and have an extra Flush() method, since the old
// string may match across successive Write calls.
