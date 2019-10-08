// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package textutil implements utilities for handling human-readable text.
//
// This package includes a combination of low-level and high-level utilities.
// The main high-level utilities are:
//   NewUTF8WrapWriter: Text formatter with line-based word wrapping.
//   PrefixWriter:      Add prefix to output.
//   PrefixLineWriter:  Add prefix to each line in output.
//   ByteReplaceWriter: Replace single byte with bytes in output.
package textutil
