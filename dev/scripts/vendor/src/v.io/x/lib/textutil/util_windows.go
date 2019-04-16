// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build windows

package textutil

import "fmt"

func TerminalSize() (row, col int, _ error) {
	return 0, 0, fmt.Errorf("not implemented")
}
