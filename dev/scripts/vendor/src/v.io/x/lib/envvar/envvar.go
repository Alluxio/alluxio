// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package envvar implements utilities for processing environment variables.
// There are three representations of environment variables:
//   1) []"key=value"  # hard to get and set, used by standard Go packages
//   2) map[key]value  # simple to get and set, nicest syntax
//   3) *envvar.Vars   # simple to get and set, also tracks deltas
//
// The slice form (1) is used by standard Go packages, presumably since it's
// similar to the underlying OS representation.  The map form (2) is convenient
// to use, and has native Go map syntax.  The Vars form (3) is also convenient
// to use, and tracks deltas when mutations are performed.
//
// This package provides utilities to easily use and convert between the three
// representations.
//
// Empty keys are invalid and silently skipped in operations over all
// representations.
package envvar

import (
	"os"
	"sort"
	"strings"
)

// MergeMaps merges together maps, and returns a new map with the merged result.
// If the same key appears in more than one input map, the last one "wins"; the
// value is set based on the last map containing that key.
//
// As a result of its semantics, MergeMaps called with a single map returns a
// copy of the map, with empty keys dropped.
func MergeMaps(maps ...map[string]string) map[string]string {
	merged := make(map[string]string)
	for _, m := range maps {
		for key, value := range m {
			if key != "" {
				merged[key] = value
			}
		}
	}
	return merged
}

// CopyMap returns a copy of from, with empty keys dropped.
func CopyMap(from map[string]string) map[string]string {
	return MergeMaps(from)
}

// MergeSlices merges together slices, and returns a new slice with the merged
// result.  If the same key appears more than once in a single input slice, or
// in more than one input slice, the last one "wins"; the value is set based on
// the last slice element in the last slice containing that key.
//
// As a result of its semantics, MergeSlices called with a single slice returns
// a copy of the slice, with empty keys dropped.
func MergeSlices(slices ...[]string) []string {
	merged := make(map[string]string)
	for _, slice := range slices {
		for _, kv := range slice {
			if key, value := SplitKeyValue(kv); key != "" {
				merged[key] = value
			}
		}
	}
	return MapToSlice(merged)
}

// CopySlice returns a copy of from, with empty keys dropped, and ordered by
// key.  If the same key appears more than once the last one "wins"; the value
// is set based on the last slice element containing that key.
func CopySlice(from []string) []string {
	return MergeSlices(from)
}

// MapToSlice converts from the map to the slice representation.  The returned
// slice is in sorted order.
func MapToSlice(from map[string]string) []string {
	to := make([]string, 0, len(from))
	for key, value := range from {
		if key != "" {
			to = append(to, JoinKeyValue(key, value))
		}
	}
	SortByKey(to)
	return to
}

// SliceToMap converts from the slice to the map representation.  If the same
// key appears more than once, the last one "wins"; the value is set based on
// the last slice element containing that key.
func SliceToMap(from []string) map[string]string {
	to := make(map[string]string, len(from))
	for _, kv := range from {
		if key, value := SplitKeyValue(kv); key != "" {
			to[key] = value
		}
	}
	return to
}

// SplitKeyValue splits kv into its key and value components.  The format of kv
// is "key=value"; the split is performed on the first '=' character.
func SplitKeyValue(kv string) (string, string) {
	split := strings.SplitN(kv, "=", 2)
	if len(split) == 2 {
		return split[0], split[1]
	}
	return split[0], ""
}

// JoinKeyValue joins key and value into a single string "key=value".
func JoinKeyValue(key, value string) string {
	return key + "=" + value
}

// SplitTokens is like strings.Split(value, separator), but also filters out
// empty tokens.  Thus SplitTokens("", ":") returns a nil slice, unlike
// strings.SplitTokens which returns a slice with a single empty string.
func SplitTokens(value, separator string) []string {
	var tokens []string
	for _, token := range strings.Split(value, separator) {
		if token != "" {
			tokens = append(tokens, token)
		}
	}
	return tokens
}

// JoinTokens is like strings.Join(tokens, separator), but also filters out
// empty tokens.
func JoinTokens(tokens []string, separator string) string {
	var value string
	for _, token := range tokens {
		if token == "" {
			continue
		}
		if value != "" {
			value += separator
		}
		value += token
	}
	return value
}

// UniqueTokens returns a new slice containing tokens that are not empty or
// duplicated, and in the same relative order as the original slice.
func UniqueTokens(tokens []string) []string {
	var unique []string
	seen := make(map[string]bool)
	for _, token := range tokens {
		if token == "" || seen[token] {
			continue
		}
		seen[token] = true
		unique = append(unique, token)
	}
	return unique
}

// FilterToken returns a new slice containing tokens that are not empty or match
// the target, and in the same relative order as the original slice.
func FilterToken(tokens []string, target string) []string {
	var filtered []string
	for _, token := range tokens {
		if token == "" || token == target {
			continue
		}
		filtered = append(filtered, token)
	}
	return filtered
}

// PrependUniqueToken prepends token to value, which is separated by separator,
// removing all empty and duplicate tokens.  Returns a string where token only
// occurs once, and is first.
func PrependUniqueToken(value, separator, token string) string {
	result := SplitTokens(value, separator)
	result = append([]string{token}, result...)
	return JoinTokens(UniqueTokens(result), separator)
}

// AppendUniqueToken appends token to value, which is separated by separator,
// and removes all empty and duplicate tokens.  Returns a string where token
// only occurs once, and is last.
func AppendUniqueToken(value, separator, token string) string {
	result := SplitTokens(value, separator)
	result = FilterToken(result, token)
	result = append(result, token)
	return JoinTokens(UniqueTokens(result), separator)
}

// SortByKey sorts vars into ascending key order, where vars is expected to be
// in the []"key=value" slice representation.
func SortByKey(vars []string) {
	sort.Sort(keySorter(vars))
}

type keySorter []string

func (s keySorter) Len() int      { return len(s) }
func (s keySorter) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s keySorter) Less(i, j int) bool {
	ikey, _ := SplitKeyValue(s[i])
	jkey, _ := SplitKeyValue(s[j])
	return ikey < jkey
}

// Vars is a mutable set of environment variables that tracks deltas.
//
// Vars are initialized with a base environment, and may be mutated with calls
// to Set and SetTokens.  The resulting environment is retrieved with calls to
// ToMap and ToSlice.
//
// Mutations are tracked separately from the base environment; call Deltas to
// retrieve only the environment variables that have been changed.
//
// The zero Vars has an empty environment, and supports all methods.
type Vars struct {
	base        map[string]string
	deltaInsert map[string]string
	deltaRemove map[string]bool
}

// VarsFromMap returns a new Vars initialized from the given base map.
func VarsFromMap(base map[string]string) *Vars { return &Vars{base: MergeMaps(base)} }

// VarsFromSlice returns a new Vars initialized from the given base slice.
func VarsFromSlice(base []string) *Vars { return &Vars{base: SliceToMap(base)} }

// VarsFromOS returns a new Vars initialized from os.Environ.
func VarsFromOS() *Vars { return VarsFromSlice(os.Environ()) }

// Contains returns true iff the key exists in the current set of variables.
func (x *Vars) Contains(key string) bool {
	if x.deltaRemove[key] {
		return false
	}
	if _, ok := x.deltaInsert[key]; ok {
		return true
	}
	_, ok := x.base[key]
	return ok
}

// Get returns the value associated with key.  Returns "" if the key doesn't
// exist, or if the key has an empty value.  Use Contains to test for existence.
func (x *Vars) Get(key string) string {
	if x.deltaRemove[key] {
		return ""
	}
	if value, ok := x.deltaInsert[key]; ok {
		return value
	}
	return x.base[key]
}

// GetTokens is a convenience that calls SplitTokens(x.Get(key), separator).
func (x *Vars) GetTokens(key, separator string) []string {
	return SplitTokens(x.Get(key), separator)
}

// Set assigns key to the given value.
func (x *Vars) Set(key, value string) {
	if key != "" {
		if x.deltaInsert == nil {
			x.deltaInsert = make(map[string]string)
		}
		x.deltaInsert[key] = value
		delete(x.deltaRemove, key)
	}
}

// SetTokens is a convenience that calls x.Set(key, JoinTokens(tokens, separator)).
func (x *Vars) SetTokens(key string, tokens []string, separator string) {
	x.Set(key, JoinTokens(tokens, separator))
}

// Delete removes the given keys.  Subsequent calls to Contains on each key
// will return false.
func (x *Vars) Delete(keys ...string) {
	for _, key := range keys {
		if key != "" {
			if x.deltaRemove == nil {
				x.deltaRemove = make(map[string]bool)
			}
			x.deltaRemove[key] = true
			delete(x.deltaInsert, key)
		}
	}
}

// ToMap returns the map representation of the current set of variables.
//
// Mutating the returned map does not affect x.
func (x *Vars) ToMap() map[string]string {
	snapshot := MergeMaps(x.base, x.deltaInsert)
	for key, _ := range x.deltaRemove {
		delete(snapshot, key)
	}
	return snapshot
}

// ToSlice returns the slice representation of the current set of variables.
//
// Mutating the returned slice does not affect x.
func (x *Vars) ToSlice() []string {
	return MapToSlice(x.ToMap())
}

// Base returns a copy of the original base environment.
//
// Mutating the returned map does not affect x.
func (x *Vars) Base() map[string]string {
	return MergeMaps(x.base)
}

// Deltas returns the set of variables that have been mutated after
// initialization.
//
// If the last mutation for key K was Set or SetTokens, map[K] contains a
// non-nil pointer to the last value that was set.  If the last mutation for key
// K was Delete, map[K] contains a nil pointer.
//
// Mutating the returned map does not affect x.
func (x *Vars) Deltas() map[string]*string {
	deltas := make(map[string]*string, len(x.deltaInsert)+len(x.deltaRemove))
	for key, value := range x.deltaInsert {
		cp := value
		deltas[key] = &cp
	}
	for key, _ := range x.deltaRemove {
		deltas[key] = nil
	}
	return deltas
}

// UpdateOS updates the OS with the current set of variables.  All variables are
// visited in sorted order, and os.Setenv is called for each variable.
//
// Returns the first error encountered, if any.
func (x *Vars) UpdateOS() error {
	var firstErr error
	for _, kv := range x.ToSlice() {
		key, value := SplitKeyValue(kv)
		if err := os.Setenv(key, value); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
