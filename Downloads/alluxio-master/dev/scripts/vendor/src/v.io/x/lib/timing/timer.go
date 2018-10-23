// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package timing implements utilities for tracking timing information.
package timing

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"time"
)

// nowFunc is used rather than direct calls to time.Now to allow tests to inject
// different clock functions.
var nowFunc = time.Now

// InvalidDuration is set on Interval.End to indicate that the end hasn't been
// set yet; i.e. the interval is open.
const InvalidDuration = time.Duration(-1 << 63)

// Interval represents a named time interval.  The start and end time of the
// interval are represented as durations from a fixed "zero" time, rather than
// time.Time, for a more compact and faster implementation.
type Interval struct {
	Name       string
	Depth      int
	Start, End time.Duration
}

// Timer provides support for tracking a tree of strictly hierarchical time
// intervals.  If you need to track overlapping time intervals, simply use
// separate Timers.
//
// Timer maintains a notion of a current interval, initialized to the root.  The
// tree of intervals is constructed by push and pop operations, which add and
// update intervals to the tree, while updating the currently referenced
// interval.  Finish should be called to finish all timing.
type Timer struct {
	Zero      time.Time  // Absolute start time of the timer.
	Intervals []Interval // List of intervals, in depth-first order.

	// The stack holds the path through the interval tree leading to the current
	// interval.  This makes it easy to determine the current interval, as well as
	// pop up to the parent interval.  The root is never held in the stack.
	stack []int
}

// NewTimer returns a new Timer, with the root interval set to the given name.
func NewTimer(name string) *Timer {
	return &Timer{
		Intervals: []Interval{{
			Name: name,
			End:  InvalidDuration,
		}},
		Zero: nowFunc(),
	}
}

// Push appends a child with the given name and an open interval to current, and
// updates the current interval to refer to the newly created child.
func (t *Timer) Push(name string) {
	depth := len(t.stack)
	if depth == 0 {
		// Unset the root end time, to handle Push after Finish.
		t.Intervals[0].End = InvalidDuration
	}
	t.Intervals = append(t.Intervals, Interval{
		Name:  name,
		Depth: depth + 1,
		Start: t.Now(),
		End:   InvalidDuration,
	})
	t.stack = append(t.stack, len(t.Intervals)-1)
}

// Pop closes the current interval, and updates the current interval to refer to
// its parent.  Pop does nothing if the current interval is the root.
func (t *Timer) Pop() {
	if last := len(t.stack) - 1; last >= 0 {
		t.Intervals[t.stack[last]].End = t.Now()
		t.stack = t.stack[:last]
	}
}

// Finish finishes all timing, closing all intervals including the root.
func (t *Timer) Finish() {
	end := t.Now()
	t.Intervals[0].End = end
	for _, index := range t.stack {
		t.Intervals[index].End = end
	}
	t.stack = t.stack[:0]
}

// Now returns the time now relative to timer.Zero.
func (t *Timer) Now() time.Duration {
	return nowFunc().Sub(t.Zero)
}

// String returns a formatted string describing the tree of time intervals.
func (t *Timer) String() string {
	var buf bytes.Buffer
	IntervalPrinter{Zero: t.Zero}.Print(&buf, t.Intervals, t.Now())
	return buf.String()
}

// IntervalPrinter is a pretty-printer for Intervals.  Example output:
//
//    00:00:01.000 root       98.000s       00:01:39.000
//    00:00:01.000    *           9.000s    00:00:10.000
//    00:00:10.000    foo        45.000s    00:00:55.000
//    00:00:10.000       *           5.000s 00:00:15.000
//    00:00:15.000       foo1       22.000s 00:00:37.000
//    00:00:37.000       foo2       18.000s 00:00:55.000
//    00:00:55.000    bar        25.000s    00:01:20.000
//    00:01:20.000    baz        19.000s    00:01:39.000
type IntervalPrinter struct {
	// Zero is the absolute start time to use for printing; all interval times are
	// computed relative to the zero time.  Typically this is set to Timer.Zero to
	// print actual timestamps, or time.Time{} to print relative timestamps
	// starting at the root.
	Zero time.Time
	// TimeFormat is passed to time.Format to format the start and end times.
	// Defaults to "15:04:05.000" if the value is empty.
	TimeFormat string
	// Indent is the number of spaces to indent each successive depth in the tree.
	// Defaults to 3 spaces if the value is 0; set to a negative value for no
	// indent.
	Indent int
	// MinGap is the minimum duration for gaps to be shown between successive
	// entries; only gaps that are larger than this threshold will be shown.
	// Defaults to 1 millisecond if the value is 0; set to a negative duration to
	// show all gaps.
	MinGap time.Duration
}

// Print writes formatted output to w representing the given intervals.  The
// intervals must be in depth-first order; i.e. any slice or subslice of
// intervals produced by Timer are valid.
//
// The time now is used as the end time for any open intervals, and is
// represented as a duration from the zero time for the intervals;
// e.g. use Timer.Now() for intervals collected by the Timer.
func (p IntervalPrinter) Print(w io.Writer, intervals []Interval, now time.Duration) error {
	if len(intervals) == 0 {
		return nil
	}
	// Set default options for zero fields.
	if p.TimeFormat == "" {
		p.TimeFormat = "15:04:05.000"
	}
	switch {
	case p.Indent < 0:
		p.Indent = 0
	case p.Indent == 0:
		p.Indent = 3
	}
	switch {
	case p.MinGap < 0:
		p.MinGap = InvalidDuration
	case p.MinGap == 0:
		p.MinGap = time.Millisecond
	}
	printer := printer{
		IntervalPrinter: p,
		w:               w,
		now:             now,
		intervals:       intervals,
	}
	return printer.print()
}

type printer struct {
	IntervalPrinter
	w         io.Writer
	now       time.Duration
	intervals []Interval
	stack     []int

	// Stats collected to help formatting.
	nowLabel   string
	depthMin   int
	depthRange int
	nameWidth  int
	durWidth   int
}

func (p *printer) print() error {
	p.stack = make([]int, 1)
	p.collectStats()
	return p.walkIntervals(p.printRow)
}

func (p *printer) walkIntervals(fn func(name string, start, end time.Duration, depth int) error) error {
	stack := p.stack[:1]
	stack[0] = 0
	prev := Interval{"", p.intervals[0].Depth, 0, 0}
	for index, i := range p.intervals {
		for i.Depth < prev.Depth && len(stack) > 1 {
			// Handle the normal case for gaps based on pops, where we have a full
			// subtree.  The gap end is based on the end of the parent, which is the
			// new previous interval.
			parent := p.intervals[stack[len(stack)-2]]
			start, end := prev.End, parent.End
			if gap := end - start; gap >= p.MinGap {
				if err := fn("*", start, end, prev.Depth); err != nil {
					return err
				}
			}
			stack = stack[:len(stack)-1]
			prev = parent
		}
		switch {
		case i.Depth < prev.Depth && len(stack) == 1:
			// Handle a corner-case for gaps based on pops, where we have a partial
			// subtree.  The gap end is based on the start of the current interval,
			// and we update the stack to start with the current interval.
			start, end := prev.End, i.Start
			if gap := end - start; gap >= p.MinGap {
				if err := fn("*", start, end, prev.Depth); err != nil {
					return err
				}
			}
			stack[0] = index
		case i.Depth == prev.Depth:
			// Handle the regular case for gaps based on pop/push siblings.
			start, end := prev.End, i.Start
			if gap := end - start; gap >= p.MinGap {
				if err := fn("*", start, end, i.Depth); err != nil {
					return err
				}
			}
			stack[len(stack)-1] = index
		case i.Depth > prev.Depth:
			// Handle the regular case for gaps based on push children.
			start, end := prev.Start, i.Start
			if gap := end - start; gap >= p.MinGap {
				if err := fn("*", start, end, i.Depth); err != nil {
					return err
				}
			}
			stack = append(stack, index)
		}
		// Visit the current interval.
		if err := fn(i.Name, i.Start, i.End, i.Depth); err != nil {
			return err
		}
		prev = i
	}
	// Handle leftover gaps (based on pops), if any exist.
	for len(stack) > 1 {
		parent := p.intervals[stack[len(stack)-2]]
		start, end := prev.End, parent.End
		if gap := end - start; gap >= p.MinGap {
			if err := fn("*", start, end, prev.Depth); err != nil {
				return err
			}
		}
		stack = stack[:len(stack)-1]
		prev = parent
	}
	p.stack = stack[:0]
	return nil
}

func (p *printer) printRow(name string, start, end time.Duration, depth int) error {
	depth -= p.depthMin
	pad := strings.Repeat(" ", p.Indent*depth)
	pad2 := strings.Repeat(" ", p.Indent*(p.depthRange-depth))
	startStr := p.Zero.Add(start).Format(p.TimeFormat)
	endStr, dur := p.nowLabel, p.now-start
	if end != InvalidDuration {
		endStr, dur = p.Zero.Add(end).Format(p.TimeFormat), end-start
	}
	_, err := fmt.Fprintf(p.w, "%s %-*s %s%*.3fs%s %s\n", startStr, p.nameWidth, pad+name, pad, p.durWidth, float64(dur)/float64(time.Second), pad2, endStr)
	return err
}

func (p *printer) collectStats() {
	p.nowLabel = strings.Repeat("-", len(p.TimeFormat)-3) + "now"
	depthMin, depthMax := p.intervals[0].Depth, p.intervals[0].Depth
	for _, i := range p.intervals[1:] {
		if x := i.Depth; x < depthMin {
			depthMin = x
		}
		if x := i.Depth; x > depthMax {
			depthMax = x
		}
	}
	p.depthMin = depthMin
	p.depthRange = depthMax - depthMin
	var durMax time.Duration
	p.walkIntervals(func(name string, start, end time.Duration, depth int) error {
		if x := len(name) + p.Indent*(depth-p.depthMin); x > p.nameWidth {
			p.nameWidth = x
		}
		dur := p.now - start
		if end != InvalidDuration {
			dur = end - start
		}
		if dur > durMax {
			durMax = dur
		}
		return nil
	})
	p.durWidth = len(fmt.Sprintf("%.3f", float64(durMax)/float64(time.Second)))
}
