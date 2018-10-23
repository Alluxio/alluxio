// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Command testbin is a test program of the -v23.metadata command line flag.
package main

import (
	"flag"

	_ "v.io/x/lib/metadata"
)

func main() {
	flag.Parse()
}
