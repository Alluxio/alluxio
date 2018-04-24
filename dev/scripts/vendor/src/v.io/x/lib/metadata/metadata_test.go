// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package metadata

import (
	"os/exec"
	"reflect"
	"runtime"
	"testing"
)

var allTests = []struct {
	MD  *T
	XML string
	B64 []string
}{
	{
		MD:  FromMap(nil),
		XML: `<metadata></metadata>`,
		B64: []string{`eJyyyU0tSUxJLEm0s9GHMwEBAAD//1RmB6Y=`},
	},
	{
		MD: FromMap(map[string]string{
			"A": `a value`,
			"B": `b value`,
			"C": `c value`,
		}),
		XML: `<metadata>
  <md id="A">a value</md>
  <md id="B">b value</md>
  <md id="C">c value</md>
</metadata>`,
		B64: []string{`eJyyyU0tSUxJLEm0s8lNUchMsVVyVLJLVChLzClNtdHPTYELOynZJWERdlayS0YW1oebBwgAAP//H2Qc4g==`},
	},
	{
		MD: FromMap(map[string]string{
			"A": `a value`,
			"B": `
b value
   has newlines
 galore
`,
			"C": `c value`,
		}),
		XML: `<metadata>
  <md id="A">a value</md>
  <md id="B"><![CDATA[
b value
   has newlines
 galore
  ]]></md>
  <md id="C">c value</md>
</metadata>`,
		B64: []string{`eJyyyU0tSUxJLEm0s8lNUchMsVVyVLJLVChLzClNtdHPTYELOynZ2ShGO7s4hjhGcyVBFHApKChkJBYr5KWW52TmpRZzKaQn5uQXgcRjY+1QtDsr2SUjm6oPtxcQAAD//0qVKG0=`},
	},
	{
		MD: FromMap(map[string]string{
			"BuildPlatform": `amd64unknown-linux-unknown`,
			"BuildTime":     `2015-05-01T01:33:35Z`,
			"Manifest": `
<manifest label="">
  <projects>
    <project exclude="false" name="release.go.v23" path="release/go/src/v.io/v23" protocol="git" remote="https://vanadium.googlesource.com/release.go.v23" revision="16889ec00eb4008849057a5e9014a025a231f836"></project>
    <project exclude="false" name="release.go.x.devtools" path="release/go/src/v.io/x/devtools" protocol="git" remote="https://vanadium.googlesource.com/release.go.x.devtools" revision="de0bd3a5f0f9b30532c41bdb01661cfe8c24df76"></project>
  </projects>
</manifest>
`,
			"ZZZ": `zzz`,
		}),
		XML: `<metadata>
  <md id="BuildPlatform">amd64unknown-linux-unknown</md>
  <md id="BuildTime">2015-05-01T01:33:35Z</md>
  <md id="Manifest"><![CDATA[
<manifest label="">
  <projects>
    <project exclude="false" name="release.go.v23" path="release/go/src/v.io/v23" protocol="git" remote="https://vanadium.googlesource.com/release.go.v23" revision="16889ec00eb4008849057a5e9014a025a231f836"></project>
    <project exclude="false" name="release.go.x.devtools" path="release/go/src/v.io/x/devtools" protocol="git" remote="https://vanadium.googlesource.com/release.go.x.devtools" revision="de0bd3a5f0f9b30532c41bdb01661cfe8c24df76"></project>
  </projects>
</manifest>
  ]]></md>
  <md id="ZZZ">zzz</md>
</metadata>`,
		B64: []string{
			`eJyskrFu2zAQhvc8hcrdIilKimxIBNJ2LdDBk4MMJ/KksCV1hkiphp++COI6cYYCBTp+/x1x+MC/DZjAQgLdBps527HPi/P2u4c00ByYhmDrcpl+TvRr2ng3LafNhVoe7O2rvQvIdCFktRHVRsi9kDuldqo63Ox+g8kNGBPT7afHL18f9g+Pd224hJmHHn3HmL7LsvY40w80Kb7AG2Z4Mn6x2LEBfESWTRCwYzN6hIj5SPlaKJYdIT1fUz4Sj7Pha+6Iv45nSmTId2x0iWUzBkrYseeUjnHH+QoTWLeEfCQaPUZaZoO5ocA/3plxddHR1DFZN80WjRDYl0I0TbkV1T1UuBWyBFFUUCg5NKpmuuUXl381O+UW10Tk498ET/zd1n/wfH/1Tdei6K2CahDDtleiUoUpZW97IetamgEbU5R2uP+oe4Wo71r+5+NfJk9P+qYph8OB6fP5/Brya1d/BwAA//8zuuV+`,
			`eJyskjFv2zAQhff8CpW7RVKUFNmQBaTtWqCDJwcZTuTJYUvqDJFSDf/6MojhxBkKFAjA5d0d7+HDvdZjBAMRutabzJot+zpbZ346iANNnnXgTV3O4++R/owrZ8f5tLqolntz+2tnPbKuELJaifTkTsiNUhtV7W9mf8BoBwyRde2Xx2/fH3YPj3etvxQzBz26LWPdXZa1x4l+oY7hRbzJDE/azQa3bAAXkGUj+CQmdAgB8wPlS6FYdoT4fK3yA/Ewab7klvhre6JImpLXwUaWTegppi3PMR7DhvMFRjB29mkdHRwGmieNuSbPP/pMuNhgadwyWTfNGrUQ2JdCNE25FtU9VLgWsgRRVFAoOTSqTuT8wvK/ZKfc4BKJXPgX4Im/m/oEzveub7gGRW8UVIMY1r0SlSp0KXvTC1nXUg/Y6KI0w/1H3KtIZ03BuBz+pfP01N0kZb/fs+58Pr8W+TWrfwMAAP//M7rlfg==`,
		},
	},
}

func TestToMap(t *testing.T) {
	for _, test := range allTests {
		if got, want := test.MD.ToMap(), test.MD.entries; !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestFromMap(t *testing.T) {
	for _, test := range allTests {
		if got, want := FromMap(test.MD.entries), test.MD; !reflect.DeepEqual(got, want) {
			t.Errorf("got %#v, want %#v", got, want)
		}
	}
}

func TestToXML(t *testing.T) {
	for _, test := range allTests {
		if got, want := test.MD.ToXML(), test.XML; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestFromXML(t *testing.T) {
	for _, test := range allTests {
		got, err := FromXML([]byte(test.XML))
		if err != nil {
			t.Errorf("%v FromXML failed: %v", test.XML, err)
		}
		if want := test.MD; !reflect.DeepEqual(got, want) {
			t.Errorf("got %#v, want %#v", got, want)
		}
		// Add some garbage and make sure it fails.
		bad := "<notclosed" + test.XML
		if got, err := FromXML([]byte(bad)); got != nil || err == nil {
			t.Errorf("%v FromXML should have failed: (%#v, %v)", bad, got, err)
		}
	}
}

func TestToBase64(t *testing.T) {
	for _, test := range allTests {
		got := test.MD.ToBase64()
		found := false
		for _, want := range test.B64 {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("got %q, want one of %q", got, test.B64)
		}
	}
}

func TestFromBase64(t *testing.T) {
	for _, test := range allTests {
		for _, b64 := range test.B64 {
			got, err := FromBase64([]byte(b64))
			if err != nil {
				t.Errorf("%v FromBase64 failed: %v", b64, err)
			}
			if want := test.MD; !reflect.DeepEqual(got, want) {
				t.Errorf("got %#v, want %#v", got, want)
			}
			// Add some garbage and make sure it fails.
			bad := "!@#$%^&*()_+" + b64
			if got, err := FromBase64([]byte(bad)); got != nil || err == nil {
				t.Errorf("%v FromBase64 should have failed: (%#v, %v)", bad, got, err)
			}
		}
	}
}

func TestInsertLookup(t *testing.T) {
	tests := []struct {
		ID, Value, Old string
		Map            map[string]string
	}{
		{"A", "abc", "", map[string]string{"A": "abc"}},
		{"B", "123", "", map[string]string{"A": "abc", "B": "123"}},
		{"A", "xyz", "abc", map[string]string{"A": "xyz", "B": "123"}},
		{"C", "s p a c e s", "", map[string]string{"A": "xyz", "B": "123", "C": "s p a c e s"}},
	}
	var x T
	for _, test := range tests {
		if got, want := x.Lookup(test.ID), test.Old; got != want {
			t.Errorf("(%q, %q) Lookup got %q, want %q", test.ID, test.Value, got, want)
		}
		if got, want := x.Insert(test.ID, test.Value), test.Old; got != want {
			t.Errorf("(%q, %q) Insert got %q, want %q", test.ID, test.Value, got, want)
		}
		if got, want := x.Lookup(test.ID), test.Value; got != want {
			t.Errorf("(%q, %q) Lookup got %q, want %q", test.ID, test.Value, got, want)
		}
		// Add some leading and trailing spaces, which will be stripped.
		value2 := "ZZ" + test.Value + "ZZ"
		if got, want := x.Insert(test.ID, " \n\t"+value2+"\n\t "), test.Value; got != want {
			t.Errorf("(%q, %q) Insert got %q, want %q", test.ID, value2, got, want)
		}
		if got, want := x.Lookup(test.ID), value2; got != want {
			t.Errorf("(%q, %q) Lookup got %q, want %q", test.ID, value2, got, want)
		}
		// Set the value back.
		if got, want := x.Insert(test.ID, test.Value), value2; got != want {
			t.Errorf("(%q, %q) Insert got %q, want %q", test.ID, test.Value, got, want)
		}
		// Check the map form.
		if got, want := x.ToMap(), test.Map; !reflect.DeepEqual(got, want) {
			t.Errorf("(%q, %q) ToMap got %q, want %q", test.ID, test.Value, got, want)
		}
	}
}

func TestLDFlag(t *testing.T) {
	for _, test := range allTests {
		got := LDFlag(test.MD)
		found := false
		for _, b64 := range test.B64 {
			want := "-X " + thisPkgPath + ".initBuiltIn=" + b64
			if got == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("got %q, want one of %v", got, test.B64)
		}
	}
}

// TestBuiltIn tests the package-level functions that operate on BuiltIn.
func TestBuiltIn(t *testing.T) {
	const id, value1, value2 = "TestID", "testvalue1", "testvalue2"
	if got, want := Lookup(id), ""; got != want {
		t.Errorf("Lookup %s got %q, want %q", id, got, want)
	}
	if got, want := Insert(id, value1), ""; got != want {
		t.Errorf("Insert %s got %q, want %q", id, got, want)
	}
	if got, want := Lookup(id), value1; got != want {
		t.Errorf("Lookup %s got %q, want %q", id, got, want)
	}
	if got, want := Insert(id, value2), value1; got != want {
		t.Errorf("Insert %s got %q, want %q", id, got, want)
	}
	wantMap := map[string]string{
		id:           value2,
		"go.Arch":    runtime.GOARCH,
		"go.OS":      runtime.GOOS,
		"go.Version": runtime.Version(),
	}
	if got, want := ToMap(), wantMap; !reflect.DeepEqual(got, want) {
		t.Errorf("got map %q, want %q", got, want)
	}
	if got, want := ToXML(), FromMap(wantMap).ToXML(); got != want {
		t.Errorf("got xml %q, want %q", got, want)
	}
	if got, want := ToBase64(), FromMap(wantMap).ToBase64(); got != want {
		t.Errorf("got base64 %q, want %q", got, want)
	}
}

// TestInitAndFlag builds a test binary with some metadata, and invokes the
// -metadata flag to make sure it dumps the expected metadata.
func TestInitAndFlag(t *testing.T) {
	// Run the test binary.
	const id, value = "zzzTestID", "abcdefg"
	x := FromMap(map[string]string{id: value})
	cmdRun := exec.Command("go", "run", "-ldflags="+LDFlag(x), "./testdata/testbin.go", "-metadata")
	outXML, err := cmdRun.CombinedOutput()
	if err != nil {
		t.Errorf("%v failed: %v\n%s", cmdRun.Args, err, outXML)
	}
	wantXML := `<metadata>
  <md id="go.Arch">` + runtime.GOARCH + `</md>
  <md id="go.OS">` + runtime.GOOS + `</md>
  <md id="go.Version">` + runtime.Version() + `</md>
  <md id="` + id + `">` + value + `</md>
</metadata>
`
	if got, want := string(outXML), wantXML; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
