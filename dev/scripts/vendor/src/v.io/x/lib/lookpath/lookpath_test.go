// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lookpath_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"v.io/x/lib/lookpath"
)

func mkdir(t *testing.T, d ...string) string {
	path := filepath.Join(d...)
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	return path
}

func mkfile(t *testing.T, dir, file string, perm os.FileMode) string {
	path := filepath.Join(dir, file)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, perm)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func initTmpDir(t *testing.T) (string, func()) {
	tmpDir, err := ioutil.TempDir("", "envvar_lookpath")
	if err != nil {
		t.Fatal(err)
	}
	return tmpDir, func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Error(err)
		}
	}
}

func pathEnv(dir ...string) map[string]string {
	return map[string]string{"PATH": strings.Join(dir, string(filepath.ListSeparator))}
}

func isNotFoundError(err error, name string) bool {
	e, ok := err.(*exec.Error)
	return ok && e.Name == name && e.Err == exec.ErrNotFound
}

func TestLook(t *testing.T) {
	tmpDir, cleanup := initTmpDir(t)
	defer cleanup()
	dirA, dirB := mkdir(t, tmpDir, "a"), mkdir(t, tmpDir, "b")
	aFoo, aBar := mkfile(t, dirA, "foo", 0755), mkfile(t, dirA, "bar", 0755)
	bBar, bBaz := mkfile(t, dirB, "bar", 0755), mkfile(t, dirB, "baz", 0755)
	aExe, bExe := mkfile(t, dirA, "exe", 0644), mkfile(t, dirB, "exe", 0755)
	tests := []struct {
		Env  map[string]string
		Name string
		Want string
	}{
		{nil, "", ""},
		{nil, "foo", ""},
		{pathEnv(dirA), "foo", aFoo},
		{pathEnv(dirA), "bar", aBar},
		{pathEnv(dirA), "baz", ""},
		{pathEnv(dirB), "foo", ""},
		{pathEnv(dirB), "bar", bBar},
		{pathEnv(dirB), "baz", bBaz},
		{pathEnv(dirA, dirB), "foo", aFoo},
		{pathEnv(dirA, dirB), "bar", aBar},
		{pathEnv(dirA, dirB), "baz", bBaz},
		// Make sure we find bExe, since aExe isn't executable.
		{pathEnv(dirA, dirB), "exe", bExe},
		// Absolute name lookups.
		{nil, dirA, ""},
		{nil, dirB, ""},
		{nil, aFoo, aFoo},
		{nil, aBar, aBar},
		{nil, bBar, bBar},
		{nil, bBaz, bBaz},
		{nil, aExe, ""},
		{nil, bExe, bExe},
	}
	for _, test := range tests {
		hdr := fmt.Sprintf("env=%v name=%v", test.Env, test.Name)
		look, err := lookpath.Look(test.Env, test.Name)
		if got, want := look, test.Want; got != want {
			t.Errorf("%s got %v, want %v", hdr, got, want)
		}
		if (look == "") == (err == nil) {
			t.Errorf("%s got mismatched look=%v err=%v", hdr, look, err)
		}
		if err != nil && !isNotFoundError(err, test.Name) {
			t.Errorf("%s got wrong error %v", hdr, err)
		}
	}
}

func TestLookPrefix(t *testing.T) {
	tmpDir, cleanup := initTmpDir(t)
	defer cleanup()
	dirA, dirB := mkdir(t, tmpDir, "a"), mkdir(t, tmpDir, "b")
	aFoo, aBar := mkfile(t, dirA, "foo", 0755), mkfile(t, dirA, "bar", 0755)
	bBar, bBaz := mkfile(t, dirB, "bar", 0755), mkfile(t, dirB, "baz", 0755)
	aBzz, bBaa := mkfile(t, dirA, "bzz", 0755), mkfile(t, dirB, "baa", 0755)
	aExe, bExe := mkfile(t, dirA, "exe", 0644), mkfile(t, dirB, "exe", 0755)
	tests := []struct {
		Env    map[string]string
		Prefix string
		Names  map[string]bool
		Want   []string
	}{
		{nil, "", nil, nil},
		{nil, "foo", nil, nil},
		{pathEnv(dirA), "foo", nil, []string{aFoo}},
		{pathEnv(dirA), "bar", nil, []string{aBar}},
		{pathEnv(dirA), "baz", nil, nil},
		{pathEnv(dirA), "f", nil, []string{aFoo}},
		{pathEnv(dirA), "b", nil, []string{aBar, aBzz}},
		{pathEnv(dirB), "foo", nil, nil},
		{pathEnv(dirB), "bar", nil, []string{bBar}},
		{pathEnv(dirB), "baz", nil, []string{bBaz}},
		{pathEnv(dirB), "f", nil, nil},
		{pathEnv(dirB), "b", nil, []string{bBaa, bBar, bBaz}},
		{pathEnv(dirA, dirB), "foo", nil, []string{aFoo}},
		{pathEnv(dirA, dirB), "bar", nil, []string{aBar}},
		{pathEnv(dirA, dirB), "baz", nil, []string{bBaz}},
		{pathEnv(dirA, dirB), "f", nil, []string{aFoo}},
		{pathEnv(dirA, dirB), "b", nil, []string{bBaa, aBar, bBaz, aBzz}},
		// Don't find baz, since it's already provided.
		{pathEnv(dirA, dirB), "b", map[string]bool{"baz": true}, []string{bBaa, aBar, aBzz}},
		// Make sure we find bExe, since aExe isn't executable.
		{pathEnv(dirA, dirB), "exe", nil, []string{bExe}},
		{pathEnv(dirA, dirB), "e", nil, []string{bExe}},
		// Absolute prefix lookups.
		{nil, dirA, nil, nil},
		{nil, dirB, nil, nil},
		{nil, aFoo, nil, []string{aFoo}},
		{nil, aBar, nil, []string{aBar}},
		{nil, bBar, nil, []string{bBar}},
		{nil, bBaz, nil, []string{bBaz}},
		{nil, aBzz, nil, []string{aBzz}},
		{nil, bBaa, nil, []string{bBaa}},
		{nil, filepath.Join(dirA, "f"), nil, []string{aFoo}},
		{nil, filepath.Join(dirA, "b"), nil, []string{aBar, aBzz}},
		{nil, filepath.Join(dirB, "f"), nil, nil},
		{nil, filepath.Join(dirB, "b"), nil, []string{bBaa, bBar, bBaz}},
		{nil, filepath.Join(dirB, "b"), map[string]bool{"baz": true}, []string{bBaa, bBar}},
		{nil, aExe, nil, nil},
		{nil, filepath.Join(dirA, "e"), nil, nil},
		{nil, bExe, nil, []string{bExe}},
		{nil, filepath.Join(dirB, "e"), nil, []string{bExe}},
	}
	for _, test := range tests {
		hdr := fmt.Sprintf("env=%v prefix=%v names=%v", test.Env, test.Prefix, test.Names)
		look, err := lookpath.LookPrefix(test.Env, test.Prefix, test.Names)
		if got, want := look, test.Want; !reflect.DeepEqual(got, want) {
			t.Errorf("%s got %v, want %v", hdr, got, want)
		}
		if (look == nil) == (err == nil) {
			t.Errorf("%s got mismatched look=%v err=%v", hdr, look, err)
		}
		if err != nil && !isNotFoundError(err, test.Prefix+"*") {
			t.Errorf("%s got wrong error %v", hdr, err)
		}
	}
}
