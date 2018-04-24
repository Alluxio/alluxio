// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package lookpath implements utilities to find executables.
package lookpath

// TODO(toddw): implement for non-unix systems.

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

func splitPath(env map[string]string) []string {
	var dirs []string
	for _, dir := range strings.Split(env["PATH"], string(filepath.ListSeparator)) {
		if dir != "" {
			dirs = append(dirs, dir)
		}
	}
	return dirs
}

func isExecutable(info os.FileInfo) bool {
	mode := info.Mode()
	return !mode.IsDir() && mode&0111 != 0
}

// Look returns the absolute path of the executable with the given name.  If
// name only contains a single path component, the dirs in env["PATH"] are
// consulted, and the first match is returned.  Otherwise, for multi-component
// paths, the absolute path of the name is looked up directly.
//
// The behavior is the same as LookPath in the os/exec package, but allows the
// env to be passed in explicitly.
func Look(env map[string]string, name string) (string, error) {
	var dirs []string
	base := filepath.Base(name)
	if base == name {
		dirs = splitPath(env)
	} else {
		dirs = []string{filepath.Dir(name)}
	}
	for _, dir := range dirs {
		file, err := filepath.Abs(filepath.Join(dir, base))
		if err != nil {
			continue
		}
		info, err := os.Stat(file)
		if err != nil {
			continue
		}
		if !isExecutable(info) {
			continue
		}
		return file, nil
	}
	return "", &exec.Error{Name: name, Err: exec.ErrNotFound}
}

// LookPrefix returns the absolute paths of all executables with the given name
// prefix.  If prefix only contains a single path component, the directories in
// env["PATH"] are consulted.  Otherwise, for multi-component prefixes, only the
// directory containing the prefix is consulted.  If multiple executables with
// the same base name match the prefix in different directories, the first match
// is returned.  Returns a list of paths sorted by base name.
//
// The names are filled in as the method runs, to ensure the first matching
// property.  As a consequence, you may pass in a pre-populated names map to
// prevent matching those names.  It is fine to pass in a nil names map.
func LookPrefix(env map[string]string, prefix string, names map[string]bool) ([]string, error) {
	if names == nil {
		names = make(map[string]bool)
	}
	var dirs []string
	if filepath.Base(prefix) == prefix {
		dirs = splitPath(env)
	} else {
		dirs = []string{filepath.Dir(prefix)}
	}
	var all []string
	for _, dir := range dirs {
		dir, err := filepath.Abs(dir)
		if err != nil {
			continue
		}
		infos, err := ioutil.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, info := range infos {
			if !isExecutable(info) {
				continue
			}
			name := info.Name()
			file := filepath.Join(dir, name)
			index := strings.LastIndex(file, prefix)
			if index == -1 || strings.ContainsRune(file[index+len(prefix):], filepath.Separator) {
				continue
			}
			if names[name] {
				continue
			}
			names[name] = true
			all = append(all, file)
		}
	}
	if len(all) > 0 {
		sort.Sort(byBase(all))
		return all, nil
	}
	return nil, &exec.Error{Name: prefix + "*", Err: exec.ErrNotFound}
}

type byBase []string

func (x byBase) Len() int           { return len(x) }
func (x byBase) Less(i, j int) bool { return filepath.Base(x[i]) < filepath.Base(x[j]) }
func (x byBase) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }
