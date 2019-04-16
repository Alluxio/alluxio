// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package timing

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"
)

func sec(d int) time.Duration {
	return time.Second * time.Duration(d)
}

func tsec(d int) time.Time {
	return time.Time{}.Add(sec(d))
}

// fakeNow is a simulated clock where now is set manually.
type fakeNow struct{ now int }

func (f *fakeNow) Now() time.Time { return tsec(f.now) }

// stepNow is a simulated clock where now increments in 1 second steps.
type stepNow struct{ now int }

func (s *stepNow) Now() time.Time {
	s.now++
	return tsec(s.now)
}

type (
	// op represents the operations that can be performed on a Timer, with a fake
	// clock.  This makes it easy to construct test cases.
	op interface {
		run(f *fakeNow, t *Timer)
	}
	push struct {
		now  int
		name string
	}
	pop struct {
		now int
	}
	finish struct {
		now int
	}
)

func (x push) run(f *fakeNow, t *Timer) {
	f.now = x.now
	t.Push(x.name)
}
func (x pop) run(f *fakeNow, t *Timer) {
	f.now = x.now
	t.Pop()
}
func (x finish) run(f *fakeNow, t *Timer) {
	f.now = x.now
	t.Finish()
}

// stripGaps strips out leading newlines, and also strips any line with an
// asterisk (*).  Asterisks appear in lines with gaps, as shown here:
//    00:00:01.000 root   98.000s    00:01:39.000
//    00:00:01.000    *       9.000s 00:00:10.000
//    00:00:10.000    abc    89.000s 00:01:39.000
func stripGaps(out string) string {
	out = strings.TrimLeft(out, "\n")
	var lines []string
	for _, line := range strings.Split(out, "\n") {
		if !strings.ContainsRune(line, '*') {
			lines = append(lines, line)
		}
	}
	return strings.Join(lines, "\n")
}

func TestTimer(t *testing.T) {
	tests := []struct {
		ops       []op
		intervals []Interval
		str       string
	}{
		{
			nil,
			[]Interval{{"root", 0, sec(0), InvalidDuration}},
			`
00:00:01.000 root 999.000s ---------now
`,
		},
		{
			[]op{pop{123}},
			[]Interval{{"root", 0, sec(0), InvalidDuration}},
			`
00:00:01.000 root 999.000s ---------now
`,
		},
		{
			[]op{finish{99}},
			[]Interval{{"root", 0, sec(0), sec(98)}},
			`
00:00:01.000 root 98.000s 00:01:39.000
`,
		},
		{
			[]op{finish{99}, pop{123}},
			[]Interval{{"root", 0, sec(0), sec(98)}},
			`
00:00:01.000 root 98.000s 00:01:39.000
`,
		},
		{
			[]op{push{10, "abc"}},
			[]Interval{
				{"root", 0, sec(0), InvalidDuration},
				{"abc", 1, sec(9), InvalidDuration},
			},
			`
00:00:01.000 root   999.000s    ---------now
00:00:01.000    *        9.000s 00:00:10.000
00:00:10.000    abc    990.000s ---------now
`,
		},
		{
			[]op{push{10, "abc"}, finish{99}},
			[]Interval{
				{"root", 0, sec(0), sec(98)},
				{"abc", 1, sec(9), sec(98)},
			},
			`
00:00:01.000 root   98.000s    00:01:39.000
00:00:01.000    *       9.000s 00:00:10.000
00:00:10.000    abc    89.000s 00:01:39.000
`,
		},
		{
			[]op{push{10, "abc"}, pop{20}, finish{99}},
			[]Interval{
				{"root", 0, sec(0), sec(98)},
				{"abc", 1, sec(9), sec(19)},
			},
			`
00:00:01.000 root   98.000s    00:01:39.000
00:00:01.000    *       9.000s 00:00:10.000
00:00:10.000    abc    10.000s 00:00:20.000
00:00:20.000    *      79.000s 00:01:39.000
`,
		},
		{
			[]op{push{10, "A1"}, push{20, "A1_1"}},
			[]Interval{
				{"root", 0, sec(0), InvalidDuration},
				{"A1", 1, sec(9), InvalidDuration},
				{"A1_1", 2, sec(19), InvalidDuration},
			},
			`
00:00:01.000 root       999.000s       ---------now
00:00:01.000    *            9.000s    00:00:10.000
00:00:10.000    A1         990.000s    ---------now
00:00:10.000       *           10.000s 00:00:20.000
00:00:20.000       A1_1       980.000s ---------now
`,
		},
		{
			[]op{push{10, "A1"}, push{20, "A1_1"}, finish{99}},
			[]Interval{
				{"root", 0, sec(0), sec(98)},
				{"A1", 1, sec(9), sec(98)},
				{"A1_1", 2, sec(19), sec(98)},
			},
			`
00:00:01.000 root       98.000s       00:01:39.000
00:00:01.000    *           9.000s    00:00:10.000
00:00:10.000    A1         89.000s    00:01:39.000
00:00:10.000       *          10.000s 00:00:20.000
00:00:20.000       A1_1       79.000s 00:01:39.000
`,
		},
		{
			[]op{push{10, "A1"}, push{20, "A1_1"}, pop{30}, finish{99}},
			[]Interval{
				{"root", 0, sec(0), sec(98)},
				{"A1", 1, sec(9), sec(98)},
				{"A1_1", 2, sec(19), sec(29)},
			},
			`
00:00:01.000 root       98.000s       00:01:39.000
00:00:01.000    *           9.000s    00:00:10.000
00:00:10.000    A1         89.000s    00:01:39.000
00:00:10.000       *          10.000s 00:00:20.000
00:00:20.000       A1_1       10.000s 00:00:30.000
00:00:30.000       *          69.000s 00:01:39.000
`,
		},
		{
			[]op{push{10, "A1"}, push{20, "A1_1"}, pop{30}, pop{40}, finish{99}},
			[]Interval{
				{"root", 0, sec(0), sec(98)},
				{"A1", 1, sec(9), sec(39)},
				{"A1_1", 2, sec(19), sec(29)},
			},
			`
00:00:01.000 root       98.000s       00:01:39.000
00:00:01.000    *           9.000s    00:00:10.000
00:00:10.000    A1         30.000s    00:00:40.000
00:00:10.000       *          10.000s 00:00:20.000
00:00:20.000       A1_1       10.000s 00:00:30.000
00:00:30.000       *          10.000s 00:00:40.000
00:00:40.000    *          59.000s    00:01:39.000
`,
		},
		{
			[]op{push{10, "A1"}, push{20, "A1_1"}, push{30, "A1_1_1"}},
			[]Interval{
				{"root", 0, sec(0), InvalidDuration},
				{"A1", 1, sec(9), InvalidDuration},
				{"A1_1", 2, sec(19), InvalidDuration},
				{"A1_1_1", 3, sec(29), InvalidDuration},
			},
			`
00:00:01.000 root            999.000s          ---------now
00:00:01.000    *                 9.000s       00:00:10.000
00:00:10.000    A1              990.000s       ---------now
00:00:10.000       *                10.000s    00:00:20.000
00:00:20.000       A1_1            980.000s    ---------now
00:00:20.000          *                10.000s 00:00:30.000
00:00:30.000          A1_1_1          970.000s ---------now
`,
		},
		{
			[]op{push{10, "A1"}, push{20, "A1_1"}, push{30, "A1_1_1"}, finish{99}},
			[]Interval{
				{"root", 0, sec(0), sec(98)},
				{"A1", 1, sec(9), sec(98)},
				{"A1_1", 2, sec(19), sec(98)},
				{"A1_1_1", 3, sec(29), sec(98)},
			},
			`
00:00:01.000 root            98.000s          00:01:39.000
00:00:01.000    *                9.000s       00:00:10.000
00:00:10.000    A1              89.000s       00:01:39.000
00:00:10.000       *               10.000s    00:00:20.000
00:00:20.000       A1_1            79.000s    00:01:39.000
00:00:20.000          *               10.000s 00:00:30.000
00:00:30.000          A1_1_1          69.000s 00:01:39.000
`,
		},
		{
			[]op{push{10, "A1"}, push{20, "A1_1"}, push{30, "A1_1_1"}, pop{40}, finish{99}},
			[]Interval{
				{"root", 0, sec(0), sec(98)},
				{"A1", 1, sec(9), sec(98)},
				{"A1_1", 2, sec(19), sec(98)},
				{"A1_1_1", 3, sec(29), sec(39)},
			},
			`
00:00:01.000 root            98.000s          00:01:39.000
00:00:01.000    *                9.000s       00:00:10.000
00:00:10.000    A1              89.000s       00:01:39.000
00:00:10.000       *               10.000s    00:00:20.000
00:00:20.000       A1_1            79.000s    00:01:39.000
00:00:20.000          *               10.000s 00:00:30.000
00:00:30.000          A1_1_1          10.000s 00:00:40.000
00:00:40.000          *               59.000s 00:01:39.000
`,
		},
		{
			[]op{push{10, "A1"}, push{20, "A1_1"}, push{30, "A1_1_1"}, pop{40}, pop{55}, finish{99}},
			[]Interval{
				{"root", 0, sec(0), sec(98)},
				{"A1", 1, sec(9), sec(98)},
				{"A1_1", 2, sec(19), sec(54)},
				{"A1_1_1", 3, sec(29), sec(39)},
			},
			`
00:00:01.000 root            98.000s          00:01:39.000
00:00:01.000    *                9.000s       00:00:10.000
00:00:10.000    A1              89.000s       00:01:39.000
00:00:10.000       *               10.000s    00:00:20.000
00:00:20.000       A1_1            35.000s    00:00:55.000
00:00:20.000          *               10.000s 00:00:30.000
00:00:30.000          A1_1_1          10.000s 00:00:40.000
00:00:40.000          *               15.000s 00:00:55.000
00:00:55.000       *               44.000s    00:01:39.000
`,
		},
		{
			[]op{push{10, "A1"}, push{20, "A1_1"}, push{30, "A1_1_1"}, pop{40}, pop{55}, pop{75}, finish{99}},
			[]Interval{
				{"root", 0, sec(0), sec(98)},
				{"A1", 1, sec(9), sec(74)},
				{"A1_1", 2, sec(19), sec(54)},
				{"A1_1_1", 3, sec(29), sec(39)},
			},
			`
00:00:01.000 root            98.000s          00:01:39.000
00:00:01.000    *                9.000s       00:00:10.000
00:00:10.000    A1              65.000s       00:01:15.000
00:00:10.000       *               10.000s    00:00:20.000
00:00:20.000       A1_1            35.000s    00:00:55.000
00:00:20.000          *               10.000s 00:00:30.000
00:00:30.000          A1_1_1          10.000s 00:00:40.000
00:00:40.000          *               15.000s 00:00:55.000
00:00:55.000       *               20.000s    00:01:15.000
00:01:15.000    *               24.000s       00:01:39.000
`,
		},
		{
			[]op{push{10, "A1"}, push{20, "A1_1"}, finish{30}, push{40, "B1"}},
			[]Interval{
				{"root", 0, sec(0), InvalidDuration},
				{"A1", 1, sec(9), sec(29)},
				{"A1_1", 2, sec(19), sec(29)},
				{"B1", 1, sec(39), InvalidDuration},
			},
			`
00:00:01.000 root       999.000s       ---------now
00:00:01.000    *            9.000s    00:00:10.000
00:00:10.000    A1          20.000s    00:00:30.000
00:00:10.000       *           10.000s 00:00:20.000
00:00:20.000       A1_1        10.000s 00:00:30.000
00:00:30.000    *           10.000s    00:00:40.000
00:00:40.000    B1         960.000s    ---------now
`,
		},
		{
			[]op{push{10, "A1"}, push{20, "A1_1"}, finish{30}, push{40, "B1"}, finish{99}},
			[]Interval{
				{"root", 0, sec(0), sec(98)},
				{"A1", 1, sec(9), sec(29)},
				{"A1_1", 2, sec(19), sec(29)},
				{"B1", 1, sec(39), sec(98)},
			},
			`
00:00:01.000 root       98.000s       00:01:39.000
00:00:01.000    *           9.000s    00:00:10.000
00:00:10.000    A1         20.000s    00:00:30.000
00:00:10.000       *          10.000s 00:00:20.000
00:00:20.000       A1_1       10.000s 00:00:30.000
00:00:30.000    *          10.000s    00:00:40.000
00:00:40.000    B1         59.000s    00:01:39.000
`,
		},
		{
			[]op{push{10, "foo"}, push{15, "foo1"}, pop{37}, push{37, "foo2"}, pop{55}, pop{55}, push{55, "bar"}, pop{80}, push{80, "baz"}, pop{99}, finish{99}},
			[]Interval{
				{"root", 0, sec(0), sec(98)},
				{"foo", 1, sec(9), sec(54)},
				{"foo1", 2, sec(14), sec(36)},
				{"foo2", 2, sec(36), sec(54)},
				{"bar", 1, sec(54), sec(79)},
				{"baz", 1, sec(79), sec(98)},
			},
			`
00:00:01.000 root       98.000s       00:01:39.000
00:00:01.000    *           9.000s    00:00:10.000
00:00:10.000    foo        45.000s    00:00:55.000
00:00:10.000       *           5.000s 00:00:15.000
00:00:15.000       foo1       22.000s 00:00:37.000
00:00:37.000       foo2       18.000s 00:00:55.000
00:00:55.000    bar        25.000s    00:01:20.000
00:01:20.000    baz        19.000s    00:01:39.000
`,
		},
		{
			[]op{push{10, "foo"}, push{15, "foo1"}, pop{30}, push{37, "foo2"}, pop{50}, pop{53}, push{55, "bar"}, pop{75}, push{80, "baz"}, pop{90}, finish{99}},
			[]Interval{
				{"root", 0, sec(0), sec(98)},
				{"foo", 1, sec(9), sec(52)},
				{"foo1", 2, sec(14), sec(29)},
				{"foo2", 2, sec(36), sec(49)},
				{"bar", 1, sec(54), sec(74)},
				{"baz", 1, sec(79), sec(89)},
			},
			`
00:00:01.000 root       98.000s       00:01:39.000
00:00:01.000    *           9.000s    00:00:10.000
00:00:10.000    foo        43.000s    00:00:53.000
00:00:10.000       *           5.000s 00:00:15.000
00:00:15.000       foo1       15.000s 00:00:30.000
00:00:30.000       *           7.000s 00:00:37.000
00:00:37.000       foo2       13.000s 00:00:50.000
00:00:50.000       *           3.000s 00:00:53.000
00:00:53.000    *           2.000s    00:00:55.000
00:00:55.000    bar        20.000s    00:01:15.000
00:01:15.000    *           5.000s    00:01:20.000
00:01:20.000    baz        10.000s    00:01:30.000
00:01:30.000    *           9.000s    00:01:39.000
`,
		},
	}
	for _, test := range tests {
		// Run all ops.
		now := &fakeNow{1}
		nowFunc = now.Now
		timer := NewTimer("root")
		for _, op := range test.ops {
			op.run(now, timer)
		}
		name := fmt.Sprintf("%#v", test.ops)
		// Check all intervals.
		if got, want := timer.Intervals, test.intervals; !reflect.DeepEqual(got, want) {
			t.Errorf("%s got intervals %v, want %v", name, got, want)
		}
		// Check string output.
		now.now = 1000
		if got, want := timer.String(), strings.TrimLeft(test.str, "\n"); got != want {
			t.Errorf("%s GOT STRING\n%sWANT\n%s", name, got, want)
		}
		// Check print output hiding all gaps.
		var buf bytes.Buffer
		printer := IntervalPrinter{Zero: timer.Zero, MinGap: time.Hour}
		if err := printer.Print(&buf, timer.Intervals, nowFunc().Sub(timer.Zero)); err != nil {
			t.Errorf("%s got printer error: %v", name, err)
		}
		if got, want := buf.String(), stripGaps(test.str); got != want {
			t.Errorf("%s GOT PRINT\n%sWANT\n%s", name, got, want)
		}
	}
	nowFunc = time.Now
}

// TestIntervalPrinterCornerCases tests corner cases for the printer.  These are
// all cases where only a subset of the full Timer intervals is printed.
func TestIntervalPrinterCornerCases(t *testing.T) {
	tests := []struct {
		intervals []Interval
		str       string
	}{
		{
			[]Interval{{"abc", 1, sec(9), InvalidDuration}},
			`
00:00:01.000 *     9.000s 00:00:10.000
00:00:10.000 abc 990.000s ---------now
`,
		},
		{
			[]Interval{{"abc", 1, sec(9), sec(98)}},
			`
00:00:01.000 *    9.000s 00:00:10.000
00:00:10.000 abc 89.000s 00:01:39.000
`,
		},
		{
			[]Interval{
				{"A1", 1, sec(9), InvalidDuration},
				{"A1_1", 2, sec(19), InvalidDuration},
			},
			`
00:00:01.000 *         9.000s    00:00:10.000
00:00:10.000 A1      990.000s    ---------now
00:00:10.000    *        10.000s 00:00:20.000
00:00:20.000    A1_1    980.000s ---------now
`,
		},
		{
			[]Interval{
				{"A1", 1, sec(9), InvalidDuration},
				{"A1_1", 2, sec(19), sec(49)},
			},
			`
00:00:01.000 *         9.000s    00:00:10.000
00:00:10.000 A1      990.000s    ---------now
00:00:10.000    *        10.000s 00:00:20.000
00:00:20.000    A1_1     30.000s 00:00:50.000
00:00:50.000    *       950.000s ---------now
`,
		},
		{
			[]Interval{
				{"A1", 1, sec(9), sec(98)},
				{"A1_1", 2, sec(19), sec(49)},
			},
			`
00:00:01.000 *        9.000s    00:00:10.000
00:00:10.000 A1      89.000s    00:01:39.000
00:00:10.000    *       10.000s 00:00:20.000
00:00:20.000    A1_1    30.000s 00:00:50.000
00:00:50.000    *       49.000s 00:01:39.000
`,
		},
		{
			[]Interval{
				{"A1_1", 2, sec(9), sec(19)},
				{"B1", 1, sec(39), InvalidDuration},
			},
			`
00:00:01.000    *         9.000s 00:00:10.000
00:00:10.000    A1_1     10.000s 00:00:20.000
00:00:20.000    *        20.000s 00:00:40.000
00:00:40.000 B1      960.000s    ---------now
`,
		},
		{
			[]Interval{
				{"A1_1", 2, sec(9), sec(19)},
				{"B1", 1, sec(39), sec(64)},
			},
			`
00:00:01.000    *        9.000s 00:00:10.000
00:00:10.000    A1_1    10.000s 00:00:20.000
00:00:20.000    *       20.000s 00:00:40.000
00:00:40.000 B1      25.000s    00:01:05.000
`,
		},
		{
			[]Interval{
				{"A1_1", 2, sec(9), sec(19)},
				{"B1", 1, sec(39), sec(64)},
				{"C1", 1, sec(69), sec(84)},
			},
			`
00:00:01.000    *        9.000s 00:00:10.000
00:00:10.000    A1_1    10.000s 00:00:20.000
00:00:20.000    *       20.000s 00:00:40.000
00:00:40.000 B1      25.000s    00:01:05.000
00:01:05.000 *        5.000s    00:01:10.000
00:01:10.000 C1      15.000s    00:01:25.000
`,
		},
		{
			[]Interval{
				{"A1_1", 2, sec(9), sec(19)},
				{"B1", 1, sec(39), sec(84)},
				{"B1_1", 2, sec(64), sec(69)},
			},
			`
00:00:01.000    *        9.000s 00:00:10.000
00:00:10.000    A1_1    10.000s 00:00:20.000
00:00:20.000    *       20.000s 00:00:40.000
00:00:40.000 B1      45.000s    00:01:25.000
00:00:40.000    *       25.000s 00:01:05.000
00:01:05.000    B1_1     5.000s 00:01:10.000
00:01:10.000    *       15.000s 00:01:25.000
`,
		},
		{
			[]Interval{
				{"A1_1", 2, sec(9), sec(19)},
				{"B1", 1, sec(39), sec(89)},
				{"B1_1", 2, sec(64), sec(69)},
				{"B1_2", 2, sec(79), sec(87)},
			},
			`
00:00:01.000    *        9.000s 00:00:10.000
00:00:10.000    A1_1    10.000s 00:00:20.000
00:00:20.000    *       20.000s 00:00:40.000
00:00:40.000 B1      50.000s    00:01:30.000
00:00:40.000    *       25.000s 00:01:05.000
00:01:05.000    B1_1     5.000s 00:01:10.000
00:01:10.000    *       10.000s 00:01:20.000
00:01:20.000    B1_2     8.000s 00:01:28.000
00:01:28.000    *        2.000s 00:01:30.000
`,
		},
		{
			[]Interval{
				{"A1_1_1", 3, sec(9), sec(19)},
				{"B1", 1, sec(39), InvalidDuration},
			},
			`
00:00:01.000       *              9.000s 00:00:10.000
00:00:10.000       A1_1_1        10.000s 00:00:20.000
00:00:20.000       *             20.000s 00:00:40.000
00:00:40.000 B1           960.000s       ---------now
`,
		},
		{
			[]Interval{
				{"A1_1_1", 3, sec(9), sec(19)},
				{"B1", 1, sec(39), sec(64)},
			},
			`
00:00:01.000       *             9.000s 00:00:10.000
00:00:10.000       A1_1_1       10.000s 00:00:20.000
00:00:20.000       *            20.000s 00:00:40.000
00:00:40.000 B1           25.000s       00:01:05.000
`,
		},
		{
			[]Interval{
				{"A1_1_1", 3, sec(9), sec(19)},
				{"B1", 1, sec(39), sec(64)},
				{"C1", 1, sec(69), sec(84)},
			},
			`
00:00:01.000       *             9.000s 00:00:10.000
00:00:10.000       A1_1_1       10.000s 00:00:20.000
00:00:20.000       *            20.000s 00:00:40.000
00:00:40.000 B1           25.000s       00:01:05.000
00:01:05.000 *             5.000s       00:01:10.000
00:01:10.000 C1           15.000s       00:01:25.000
`,
		},
		{
			[]Interval{
				{"A1_1_1", 3, sec(9), sec(19)},
				{"B1", 1, sec(39), sec(84)},
				{"B1_1", 2, sec(59), sec(69)},
			},
			`
00:00:01.000       *             9.000s 00:00:10.000
00:00:10.000       A1_1_1       10.000s 00:00:20.000
00:00:20.000       *            20.000s 00:00:40.000
00:00:40.000 B1           45.000s       00:01:25.000
00:00:40.000    *            20.000s    00:01:00.000
00:01:00.000    B1_1         10.000s    00:01:10.000
00:01:10.000    *            15.000s    00:01:25.000
`,
		},
		{
			[]Interval{
				{"A1_1", 2, sec(9), sec(84)},
				{"A1_1_1", 3, sec(39), sec(79)},
				{"A1_1_1_1", 4, sec(54), sec(69)},
				{"B1", 1, sec(89), sec(99)},
			},
			`
00:00:01.000    *                  9.000s       00:00:10.000
00:00:10.000    A1_1              75.000s       00:01:25.000
00:00:10.000       *                 30.000s    00:00:40.000
00:00:40.000       A1_1_1            40.000s    00:01:20.000
00:00:40.000          *                 15.000s 00:00:55.000
00:00:55.000          A1_1_1_1          15.000s 00:01:10.000
00:01:10.000          *                 10.000s 00:01:20.000
00:01:20.000       *                  5.000s    00:01:25.000
00:01:25.000    *                  5.000s       00:01:30.000
00:01:30.000 B1                10.000s          00:01:40.000
`,
		},
	}
	for _, test := range tests {
		name := fmt.Sprintf("%#v", test.intervals)
		// Check print output with gaps.
		var buf bytes.Buffer
		printer := IntervalPrinter{Zero: tsec(1)}
		if err := printer.Print(&buf, test.intervals, sec(999)); err != nil {
			t.Errorf("%s got printer error: %v", name, err)
		}
		if got, want := buf.String(), strings.TrimLeft(test.str, "\n"); got != want {
			t.Errorf("%s GOT STRING\n%sWANT\n%s", name, got, want)
		}
	}
}

func BenchmarkTimerPush(b *testing.B) {
	t := NewTimer("root")
	for i := 0; i < b.N; i++ {
		t.Push("child")
	}
}

func BenchmarkTimerPushPop(b *testing.B) {
	t := NewTimer("root")
	for i := 0; i < b.N; i++ {
		timerPushPop(t)
	}
}
func timerPushPop(t *Timer) {
	t.Push("child1")
	t.Pop()
	t.Push("child2")
	t.Push("child2_1")
	t.Pop()
	t.Push("child2_2")
	t.Pop()
	t.Pop()
	t.Push("child3")
	t.Pop()
}

var randSource = rand.NewSource(123)

func BenchmarkTimerRandom(b *testing.B) {
	t, rng := NewTimer("root"), rand.New(randSource)
	for i := 0; i < b.N; i++ {
		timerRandom(rng, t)
	}
}
func timerRandom(rng *rand.Rand, t *Timer) {
	switch pct := rng.Intn(100); {
	case pct < 60:
		timerPushPop(t)
	case pct < 90:
		t.Push("foo")
	case pct < 99:
		t.Pop()
	default:
		t.Finish()
	}
}

func BenchmarkTimerString(b *testing.B) {
	t, rng, now := NewTimer("root"), rand.New(randSource), &stepNow{0}
	nowFunc = now.Now
	for i := 0; i < 1000; i++ {
		timerRandom(rng, t)
	}
	t.Finish() // Make sure all intervals are closed.
	want := t.String()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := t.String(); got != want {
			b.Fatalf("GOT\n%sWANT\n%s\nTIMER\n%#v", got, want, t)
		}
	}
	nowFunc = time.Now
}
