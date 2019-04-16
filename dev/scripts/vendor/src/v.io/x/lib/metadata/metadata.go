// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package metadata implements a mechanism for setting and retrieving metadata
// stored in program binaries.
//
// Metadata is a flat mapping of unique string identifiers to their associated
// string values.  Both the ids and the values should be human-readable strings,
// since many uses of metadata involve textual output for humans; e.g. dumping
// metadata from the program on the command-line, or prepending metadata to log
// files.
//
// The ids must be unique, and avoiding collisions is up to the users of this
// package; it is recommended to prefix your identifiers with your project name.
//
// There are typically two sources for metadata, both supported by this package:
//
// 1) External metadata gathered by a build tool, e.g. the build timestamp.
//
// 2) Internal metadata compiled into the program, e.g. version numbers.
//
// External metadata is injected into the program binary by the linker.  The Go
// ld linker provides a -X option that may be used for this purpose; see LDFlag
// for more details.
//
// Internal metadata is already compiled into the program binary, but the
// metadata package must still be made aware of it.  Call the Insert function in
// an init function to accomplish this:
//
//   package mypkg
//   import "v.io/x/lib/metadata"
//
//   func init() {
//     metadata.Insert("myproject.myid", "value")
//   }
//
// The built-in metadata comes pre-populated with the Go architecture, operating
// system and version.
//
// This package registers a flag -metadata via an init function.  Setting
// -metadata on the command-line causes the program to dump metadata in the
// XML format and exit.
package metadata

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strings"
)

// T represents the metadata for a program binary.
type T struct {
	entries map[string]string
}

// String returns a human-readable representation; the same as ToXML.
func (x *T) String() string {
	return x.ToXML()
}

// Insert sets the metadata entry for id to value, and returns the previous
// value.  Whitespace is trimmed from either end of the value.
func (x *T) Insert(id, value string) string {
	if x.entries == nil {
		x.entries = make(map[string]string)
	}
	old := x.entries[id]
	x.entries[id] = strings.TrimSpace(value)
	return old
}

// Lookup retrieves the value for the given id from x.
func (x *T) Lookup(id string) string {
	return x.entries[id]
}

// FromMap returns new metadata initialized with the given entries.  Calls
// Insert on each element of entries.
func FromMap(entries map[string]string) *T {
	x := new(T)
	for id, value := range entries {
		x.Insert(id, value)
	}
	return x
}

// ToMap returns a copy of the entries in x.  Mutating the returned map has no
// effect on x.
func (x *T) ToMap() map[string]string {
	if len(x.entries) == 0 {
		return nil
	}
	ret := make(map[string]string, len(x.entries))
	for id, value := range x.entries {
		ret[id] = value
	}
	return ret
}

type xmlMetaData struct {
	XMLName struct{}   `xml:"metadata"`
	Entries []xmlEntry `xml:"md"`
}

type xmlEntry struct {
	ID string `xml:"id,attr"`
	// When marshalling only one of Value or ValueCDATA is set; Value is normally
	// used, and ValueCDATA is used to add explicit "<![CDATA[...]]>" wrapping.
	//
	// When unmarshalling Value is set to the unescaped data, while ValueCDATA is
	// set to the raw XML.
	Value      string `xml:",chardata"`
	ValueCDATA string `xml:",innerxml"`
}

// FromXML returns new metadata initialized with the given XML encoded data.
// The expected schema is described in ToXML.
func FromXML(data []byte) (*T, error) {
	x := new(T)
	if len(data) == 0 {
		return x, nil
	}
	var xmlData xmlMetaData
	if err := xml.Unmarshal(data, &xmlData); err != nil {
		return nil, err
	}
	for _, entry := range xmlData.Entries {
		x.Insert(entry.ID, entry.Value)
	}
	return x, nil
}

// ToXML returns the XML encoding of x, using the schema described below.
//
//   <metadata>
//     <md id="A">a value</md>
//     <md id="B"><![CDATA[
//       foo
//       bar
//     ]]></md>
//     <md id="C">c value</md>
//   </metadata>
func (x *T) ToXML() string {
	return x.toXML(true)
}

func (x *T) toXML(indent bool) string {
	// Write each XML <md> entry ordered by id.
	var ids []string
	for id, _ := range x.entries {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var data xmlMetaData
	for _, id := range ids {
		entry := xmlEntry{ID: id}
		value := x.entries[id]
		if xmlUseCDATASection(value) {
			entry.ValueCDATA = cdataStart + "\n" + value + "\n  " + cdataEnd
		} else {
			entry.Value = value
		}
		data.Entries = append(data.Entries, entry)
	}
	var dataXML []byte
	if indent {
		dataXML, _ = xml.MarshalIndent(data, "", "  ")
	} else {
		dataXML, _ = xml.Marshal(data)
	}
	return string(dataXML)
}

const (
	cdataStart = "<![CDATA["
	cdataEnd   = "]]>"
)

func xmlUseCDATASection(value string) bool {
	// Cannot use CDATA if "]]>" appears since that's the CDATA terminator.
	if strings.Contains(value, cdataEnd) {
		return false
	}
	// The choice at this point is a heuristic; it only determines how "pretty"
	// the output looks.
	b := []byte(value)
	var buf bytes.Buffer
	xml.EscapeText(&buf, b)
	return !bytes.Equal(buf.Bytes(), b)
}

// FromBase64 returns new metadata initialized with the given base64 encoded
// data.  The data is expected to have started as a valid XML representation of
// metadata, then zlib compressed, and finally base64 encoded.
func FromBase64(data []byte) (*T, error) {
	if len(data) == 0 {
		return new(T), nil
	}
	dataXML := make([]byte, base64.StdEncoding.DecodedLen(len(data)))
	n, err := base64.StdEncoding.Decode(dataXML, data)
	if err != nil {
		return nil, err
	}
	var b bytes.Buffer
	r, err := zlib.NewReader(bytes.NewReader(dataXML[:n]))
	if err != nil {
		return nil, err
	}
	_, errCopy := io.Copy(&b, r)
	errClose := r.Close()
	switch {
	case errCopy != nil:
		return nil, err
	case errClose != nil:
		return nil, err
	}
	return FromXML(b.Bytes())
}

// ToBase64 returns the base64 encoding of x.  First x is XML encoded, then zlib
// compressed, and finally base64 encoded.
func (x *T) ToBase64() string {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	w.Write([]byte(x.toXML(false)))
	w.Close()
	return base64.StdEncoding.EncodeToString(b.Bytes())
}

var thisPkgPath = reflect.TypeOf(T{}).PkgPath()

// LDFlag returns the flag to pass to the Go ld linker to initialize the
// built-in metadata with x.  Calls LDFlagExternal with the appropriate package
// path and unexported variable name to initialize BuiltIn.
func LDFlag(x *T) string {
	return LDFlagExternal(thisPkgPath, "initBuiltIn", x)
}

// LDFlagExternal returns the flag to pass to the Go ld linker to initialize the
// string variable defined in pkgpath to the base64 encoding of x.  See the
// documentation of the -X option at https://golang.org/cmd/ld
//
// The base64 encoding is used to avoid quoting and escaping issues when passing
// the flag through the go toolchain.  An example of using the result to install
// a Go binary with metadata x:
//
//   LDFlagExternal("main", "myvar", x) == "-X main.myvar=eJwBAAD//wAAAAE="
//
//   $ go install -ldflags="-X main.myvar=eJwBAAD//wAAAAE=" mypackage
func LDFlagExternal(pkgpath, variable string, x *T) string {
	return fmt.Sprintf("-X %s.%s=%s", pkgpath, variable, x.ToBase64())
}

// Insert sets the built-in metadata entry for id to value, and returns the
// previous value.  Whitespace is trimmed from either end of the value.
//
// The built-in metadata is initialized by the Go ld linker.  See the LDFlag
// function for more details.
func Insert(id, value string) string { return BuiltIn.Insert(id, value) }

// Lookup retrieves the value for the given id from the built-in metadata.
func Lookup(id string) string { return BuiltIn.Lookup(id) }

// ToBase64 returns the base64 encoding of the built-in metadata.  First the
// metadata is XML encoded, then zlib compressed, and finally base64 encoded.
func ToBase64() string { return BuiltIn.ToBase64() }

// ToXML returns the XML encoding of the built-in metadata.  The schema is
// defined in T.ToXML.
func ToXML() string { return BuiltIn.ToXML() }

// ToMap returns a copy of the entries in the built-in metadata.  Mutating the
// returned map has no effect on the built-in metadata.
func ToMap() map[string]string { return BuiltIn.ToMap() }

// BuiltIn represents the metadata built-in to the Go program.  The top-level
// functions such as Insert, ToBase64, and so on are wrappers for the methods of
// BuiltIn.
var BuiltIn T

// initBuiltIn is expected to be initialized by the Go ld linker.
var initBuiltIn string

func init() {
	// First initialize the BuiltIn metadata based on linker-injected metadata.
	if x, err := FromBase64([]byte(initBuiltIn)); err != nil {
		// Don't panic, since a binary without metadata is more useful than a binary
		// that always panics with invalid metadata.
		fmt.Fprintf(os.Stderr, `
metadata: built-in initialization failed (%v) from base64 data: %v
`, err, initBuiltIn)
	} else {
		BuiltIn = *x
	}
	// Now set values from the runtime.  These may not be overridden by the
	// linker-injected metadata, and should not be overridden by user packages.
	BuiltIn.Insert("go.Arch", runtime.GOARCH)
	BuiltIn.Insert("go.OS", runtime.GOOS)
	BuiltIn.Insert("go.Version", runtime.Version())

	flag.Var(metadataFlag{}, "metadata", "Displays metadata for the program and exits.")
}

// metadataFlag implements a flag that dumps the default metadata and exits the
// program when it is set.
type metadataFlag struct{}

func (metadataFlag) IsBoolFlag() bool { return true }
func (metadataFlag) String() string   { return "<just specify -metadata to activate>" }
func (metadataFlag) Set(string) error {
	fmt.Println(BuiltIn.String())
	os.Exit(0)
	return nil
}
