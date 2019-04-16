// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package textutil

import (
	"fmt"
	"io"
	"unicode"
)

// WrapWriter implements an io.Writer filter that formats input text into output
// lines with a given target width in runes.
//
// Each input rune is classified into one of three kinds:
//   EOL:    end-of-line, consisting of \f, \n, \r, \v, U+2028 or U+2029
//   Space:  defined by unicode.IsSpace
//   Letter: everything else
//
// The input text is expected to consist of words, defined as sequences of
// letters.  Sequences of words form paragraphs, where paragraphs are separated
// by either blank lines (that contain no letters), or an explicit U+2029
// ParagraphSeparator.  Input lines with leading spaces are treated verbatim.
//
// Paragraphs are output as word-wrapped lines; line breaks only occur at word
// boundaries.  Output lines are usually no longer than the target width.  The
// exceptions are single words longer than the target width, which are output on
// their own line, and verbatim lines, which may be arbitrarily longer or
// shorter than the width.
//
// Output lines never contain trailing spaces.  Only verbatim output lines may
// contain leading spaces.  Spaces separating input words are output verbatim,
// unless it would result in a line with leading or trailing spaces.
//
// EOL runes within the input text are never written to the output; the output
// line terminator and paragraph separator may be configured, and some EOL may
// be output as a single space ' ' to maintain word separation.
//
// The algorithm greedily fills each output line with as many words as it can,
// assuming that all Unicode code points have the same width.  Invalid UTF-8 is
// silently transformed to the replacement character U+FFFD and treated as a
// single rune.
//
// Flush must be called after the last call to Write; the input is buffered.
//
//   Implementation note: line breaking is a complicated topic.  This approach
//   attempts to be simple and useful; a full implementation conforming to
//   Unicode Standard Annex #14 would be complicated, and is not implemented.
//   Languages that don't use spaces to separate words (e.g. CJK) won't work
//   well under the current approach.
//
//   http://www.unicode.org/reports/tr14 [Unicode Line Breaking Algorithm]
//   http://www.unicode.org/versions/Unicode4.0.0/ch05.pdf [5.8 Newline Guidelines]
type WrapWriter struct {
	// State configured by the user.
	w             io.Writer
	runeDecoder   RuneChunkDecoder
	width         runePos
	lineTerm      []byte
	paragraphSep  string
	indents       []string
	forceVerbatim bool

	// The buffer contains a single output line.
	lineBuf byteRuneBuffer

	// Keep track of the previous state and rune.
	prevState state
	prevRune  rune

	// Keep track of blank input lines.
	inputLineHasLetter bool

	// lineBuf positions where the line starts (after separators and indents), a
	// new word has started and the last word has ended.
	lineStart    bytePos
	newWordStart bytePos
	lastWordEnd  bytePos

	// Keep track of paragraph terminations and line indices, so we can output the
	// paragraph separator and indents correctly.
	terminateParagraph bool
	paragraphLineIndex int
	wroteFirstLine     bool
}

type state int

const (
	stateWordWrap  state = iota // Perform word-wrapping [start state]
	stateVerbatim               // Verbatim output-line, no word-wrapping
	stateSkipSpace              // Skip spaces in input line.
)

// NewWrapWriter returns a new WrapWriter with the given target width in runes,
// producing output on the underlying writer w.  The dec and enc are used to
// respectively decode runes from Write calls, and encode runes to w.
func NewWrapWriter(w io.Writer, width int, dec RuneChunkDecoder, enc RuneEncoder) *WrapWriter {
	ret := &WrapWriter{
		w:            w,
		runeDecoder:  dec,
		width:        runePos(width),
		lineTerm:     []byte("\n"),
		paragraphSep: "\n",
		prevState:    stateWordWrap,
		prevRune:     LineSeparator,
		lineBuf:      byteRuneBuffer{enc: enc},
	}
	ret.resetLine()
	return ret
}

// NewUTF8WrapWriter returns a new WrapWriter filter that implements io.Writer,
// and decodes and encodes runes in UTF-8.
func NewUTF8WrapWriter(w io.Writer, width int) *WrapWriter {
	return NewWrapWriter(w, width, &UTF8ChunkDecoder{}, UTF8Encoder{})
}

// Width returns the target width in runes.  If width < 0 the width is
// unlimited; each paragraph is output as a single line.
func (w *WrapWriter) Width() int { return int(w.width) }

// SetLineTerminator sets the line terminator for subsequent Write calls.  Every
// output line is terminated with term; EOL runes from the input are never
// written to the output.  A new WrapWriter instance uses "\n" as the default
// line terminator.
//
// Calls Flush internally, and returns any Flush error.
func (w *WrapWriter) SetLineTerminator(term string) error {
	if err := w.Flush(); err != nil {
		return err
	}
	w.lineTerm = []byte(term)
	w.resetLine()
	return nil
}

// SetParagraphSeparator sets the paragraph separator for subsequent Write
// calls.  Every consecutive pair of non-empty paragraphs is separated with sep;
// EOL runes from the input are never written to the output.  A new WrapWriter
// instance uses "\n" as the default paragraph separator.
//
// Calls Flush internally, and returns any Flush error.
func (w *WrapWriter) SetParagraphSeparator(sep string) error {
	if err := w.Flush(); err != nil {
		return err
	}
	w.paragraphSep = sep
	w.resetLine()
	return nil
}

// SetIndents sets the indentation for subsequent Write calls.  Multiple indents
// may be set, corresponding to the indent to use for the corresponding
// paragraph line.  E.g. SetIndents("AA", "BBB", C") means the first line in
// each paragraph is indented with "AA", the second line in each paragraph is
// indented with "BBB", and all subsequent lines in each paragraph are indented
// with "C".
//
// SetIndents() is equivalent to SetIndents(""), SetIndents("", ""), etc.
//
// A new WrapWriter instance has no indents by default.
//
// Calls Flush internally, and returns any Flush error.
func (w *WrapWriter) SetIndents(indents ...string) error {
	if err := w.Flush(); err != nil {
		return err
	}
	// Copy indents in case the user passed the slice via SetIndents(p...), and
	// canonicalize the all empty case to nil.
	allEmpty := true
	w.indents = make([]string, len(indents))
	for ix, indent := range indents {
		w.indents[ix] = indent
		if indent != "" {
			allEmpty = false
		}
	}
	if allEmpty {
		w.indents = nil
	}
	w.resetLine()
	return nil
}

// ForceVerbatim forces w to stay in verbatim mode if v is true, or lets w
// perform its regular line writing algorithm if v is false.  This is useful if
// there is a sequence of lines that should be written verbatim, even if the
// lines don't start with spaces.
//
// Calls Flush internally, and returns any Flush error.
func (w *WrapWriter) ForceVerbatim(v bool) error {
	w.forceVerbatim = v
	return w.Flush()
}

// Write implements io.Writer by buffering data into the WrapWriter w.  Actual
// writes to the underlying writer may occur, and may include data buffered in
// either this Write call or previous Write calls.
//
// Flush must be called after the last call to Write.
func (w *WrapWriter) Write(data []byte) (int, error) {
	return WriteRuneChunk(w.runeDecoder, w.addRune, data)
}

// Flush flushes any remaining buffered text, and resets the paragraph line
// count back to 0, so that indents will be applied starting from the first
// line.  It does not imply a paragraph separator; repeated calls to Flush with
// no intervening calls to other methods is equivalent to a single Flush.
//
// Flush must be called after the last call to Write, and may be called an
// arbitrary number of times before the last Write.
func (w *WrapWriter) Flush() error {
	if err := FlushRuneChunk(w.runeDecoder, w.addRune); err != nil {
		return err
	}
	// Add U+2028 to force the last line (if any) to be written.
	if err := w.addRune(LineSeparator); err != nil {
		return err
	}
	// Reset the paragraph line count.
	w.paragraphLineIndex = 0
	w.resetLine()
	return nil
}

// addRune is called every time w.runeDecoder decodes a full rune.
func (w *WrapWriter) addRune(r rune) error {
	state, lineBreak := w.nextState(r, w.updateRune(r))
	if lineBreak {
		if err := w.writeLine(); err != nil {
			return err
		}
	}
	w.bufferRune(r, state, lineBreak)
	w.prevState = state
	w.prevRune = r
	return nil
}

// We classify each incoming rune into three kinds for easier handling.
type kind int

const (
	kindEOL kind = iota
	kindSpace
	kindLetter
)

func runeKind(r rune) kind {
	switch r {
	case '\f', '\n', '\r', '\v', LineSeparator, ParagraphSeparator:
		return kindEOL
	}
	if unicode.IsSpace(r) {
		return kindSpace
	}
	return kindLetter
}

func (w *WrapWriter) updateRune(r rune) bool {
	forceLineBreak := false
	switch kind := runeKind(r); kind {
	case kindEOL:
		// Update lastWordEnd if the last word just ended.
		if w.newWordStart != -1 {
			w.newWordStart = -1
			w.lastWordEnd = w.lineBuf.ByteLen()
		}
		switch {
		case w.prevRune == '\r' && r == '\n':
			// Treat "\r\n" as a single EOL; we've already handled the logic for '\r',
			// so there's nothing to do when we see '\n'.
		case r == LineSeparator:
			// Treat U+2028 as a pure line break; it's never a paragraph break.
			forceLineBreak = true
		case r == ParagraphSeparator || !w.inputLineHasLetter:
			// The paragraph has just been terminated if we see an explicit U+2029, or
			// if we see a blank line, which may contain spaces.
			forceLineBreak = true
			w.terminateParagraph = true
		}
		w.inputLineHasLetter = false
	case kindSpace:
		// Update lastWordEnd if the last word just ended.
		if w.newWordStart != -1 {
			w.newWordStart = -1
			w.lastWordEnd = w.lineBuf.ByteLen()
		}
	case kindLetter:
		// Update newWordStart if a new word just started.
		if w.newWordStart == -1 {
			w.newWordStart = w.lineBuf.ByteLen()
		}
		w.inputLineHasLetter = true
		w.terminateParagraph = false
	default:
		panic(fmt.Errorf("textutil: updateRune unhandled kind %d", kind))
	}
	return forceLineBreak
}

// nextState returns the next state and whether we should break the line.
//
// Here's a handy table that describes all the scenarios in which we will line
// break input text, grouped by the reason for the break.  The current position
// is the last non-* rune in each pattern, which is where we decide to break.
//
//              w.prevState   Next state   Buffer reset
//              -----------   ----------   ------------
//   ===== Force line break (U+2028 / U+2029, blank line) =====
//   a..*|***   *             wordWrap     empty
//   a._.|***   *             wordWrap     empty
//   a+**|***   *             wordWrap     empty
//
//   ===== verbatim: wait for any EOL =====
//   _*.*|***   verbatim      wordWrap     empty
//
//   ===== wordWrap: switch to verbatim =====
//   a._*|***   wordWrap      verbatim     empty
//
//   ===== wordWrap: line is too wide =====
//   abc.|***   wordWrap      wordWrap     empty
//   abcd|.**   wordWrap      wordWrap     empty
//   abcd|e.*   wordWrap      wordWrap     empty
//   a_cd|.**   wordWrap      wordWrap     empty
//
//   abc_|***   wordWrap      skipSpace    empty
//   abcd|_**   wordWrap      skipSpace    empty
//   abcd|e_*   wordWrap      skipSpace    empty
//   a_cd|_**   wordWrap      skipSpace    empty
//
//   a_cd|e**   wordWrap      start        newWordStart
//
//   LEGEND
//     abcde  Letter
//     .      End-of-line
//     +      End-of-line (only U+2028 / U+2029)
//     _      Space
//     *      Any rune (letter, line-end or space)
//     |      Visual indication of width=4, has no width itself.
//
// Note that Flush calls behave exactly as if an explicit U+2028 line separator
// were added to the end of all buffered data.
func (w *WrapWriter) nextState(r rune, forceLineBreak bool) (state, bool) {
	kind := runeKind(r)
	if w.forceVerbatim {
		return stateVerbatim, forceLineBreak || kind == kindEOL
	}
	if forceLineBreak {
		return stateWordWrap, true
	}
	// Handle non word-wrap states, which are easy.
	switch w.prevState {
	case stateVerbatim:
		if kind == kindEOL {
			return stateWordWrap, true
		}
		return stateVerbatim, false
	case stateSkipSpace:
		if kind == kindSpace {
			return stateSkipSpace, false
		}
		return stateWordWrap, false
	}
	// Handle stateWordWrap, which is more complicated.

	// Switch to the verbatim state when we see a space right after an EOL.
	if runeKind(w.prevRune) == kindEOL && kind == kindSpace {
		return stateVerbatim, true
	}
	// Break on EOL or space when the line is too wide.  See above table.
	if w.width >= 0 && w.width <= w.lineBuf.RuneLen()+1 {
		switch kind {
		case kindEOL:
			return stateWordWrap, true
		case kindSpace:
			return stateSkipSpace, true
		}
		// case kindLetter falls through
	}
	// Handle the newWordStart case in the above table.
	if w.width >= 0 && w.width < w.lineBuf.RuneLen()+1 && w.newWordStart != w.lineStart {
		return stateWordWrap, true
	}
	// Stay in the wordWrap state and don't break the line.
	return stateWordWrap, false
}

func (w *WrapWriter) writeLine() error {
	if w.lastWordEnd == -1 {
		// Don't write blank lines, but we must reset the line in case the paragraph
		// has just been terminated.
		w.resetLine()
		return nil
	}
	// Write the line (without trailing spaces) followed by the line terminator.
	line := w.lineBuf.Bytes()[:w.lastWordEnd]
	if _, err := w.w.Write(line); err != nil {
		return err
	}
	if _, err := w.w.Write(w.lineTerm); err != nil {
		return err
	}
	// Reset the line buffer.
	w.wroteFirstLine = true
	w.paragraphLineIndex++
	if w.newWordStart != -1 {
		// If we have an unterminated new word, we must be in the newWordStart case
		// in the table above.  Handle the special buffer reset here.
		newWord := string(w.lineBuf.Bytes()[w.newWordStart:])
		w.resetLine()
		w.newWordStart = w.lineBuf.ByteLen()
		w.lineBuf.WriteString(newWord)
	} else {
		w.resetLine()
	}
	return nil
}

func (w *WrapWriter) resetLine() {
	w.lineBuf.Reset()
	w.newWordStart = -1
	w.lastWordEnd = -1
	// Write the paragraph separator if the previous paragraph has terminated.
	// This consumes no runes from the line width.
	if w.wroteFirstLine && w.terminateParagraph {
		w.lineBuf.WriteString0Runes(w.paragraphSep)
		w.paragraphLineIndex = 0
	}
	// Add indent; a non-empty indent consumes runes from the line width.
	var indent string
	switch {
	case w.paragraphLineIndex < len(w.indents):
		indent = w.indents[w.paragraphLineIndex]
	case len(w.indents) > 0:
		indent = w.indents[len(w.indents)-1]
	}
	w.lineBuf.WriteString(indent)
	w.lineStart = w.lineBuf.ByteLen()
}

func (w *WrapWriter) bufferRune(r rune, state state, lineBreak bool) {
	// Never add leading spaces to the buffer in the wordWrap state.
	wordWrapNoLeadingSpaces := state == stateWordWrap && !lineBreak
	switch kind := runeKind(r); kind {
	case kindEOL:
		// When we're word-wrapping and we see a letter followed by EOL, we convert
		// the EOL into a single space in the buffer, to break the previous word
		// from the next word.
		if wordWrapNoLeadingSpaces && runeKind(w.prevRune) == kindLetter {
			w.lineBuf.WriteRune(' ')
		}
	case kindSpace:
		if wordWrapNoLeadingSpaces || state == stateVerbatim {
			w.lineBuf.WriteRune(r)
		}
	case kindLetter:
		w.lineBuf.WriteRune(r)
	default:
		panic(fmt.Errorf("textutil: bufferRune unhandled kind %d", kind))
	}
}
