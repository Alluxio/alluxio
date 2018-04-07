// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package envvar

import (
	"os"
	"reflect"
	"testing"
)

func TestMapSliceFuncs(t *testing.T) {
	tests := []struct {
		FromMap, Map     map[string]string
		FromSlice, Slice []string
	}{
		{nil, map[string]string{}, nil, []string{}},
		{ // Empty keys are silently dropped.
			map[string]string{"": ""}, map[string]string{},
			[]string{"", "=", "=123"}, []string{},
		},
		{ // Values may be empty, slices may be missing equal sign.
			map[string]string{"A": "", "B": "", "": ""},
			map[string]string{"A": "", "B": ""},
			[]string{"A=", "B", ""},
			[]string{"A=", "B="},
		},
		{ // Values may contain equal sign.
			map[string]string{"A": "=", "B": "==", "C": "=0=", "": ""},
			map[string]string{"A": "=", "B": "==", "C": "=0="},
			[]string{"A==", "B===", "C==0=", ""},
			[]string{"A==", "B===", "C==0="},
		},
		{ // Duplicates in slice takes last element.
			map[string]string{"A": "1", "": ""},
			map[string]string{"A": "1"},
			[]string{"A=9", "A=1", ""},
			[]string{"A=1"},
		},
		{ // Slice items are sorted.
			map[string]string{"A": "2", "B": "1", "C": "3", "": ""},
			map[string]string{"A": "2", "B": "1", "C": "3"},
			[]string{"C=3", "A=9", "B=1", "A=2", ""},
			[]string{"A=2", "B=1", "C=3"},
		},
		{ // A bunch of different items.
			map[string]string{"A": "2", "B": "", "C": "=3=", "": ""},
			map[string]string{"A": "2", "B": "", "C": "=3="},
			[]string{"C==3=", "A=9", "B=1", "B", "", "A=2"},
			[]string{"A=2", "B=", "C==3="},
		},
	}
	for _, test := range tests {
		// Test map to slice conversions.
		if got, want := MapToSlice(test.FromMap), test.Slice; !reflect.DeepEqual(got, want) {
			t.Errorf("MapToSlice(%v) got %v, want %v", test.FromMap, got, want)
		}
		if got, want := MapToSlice(test.Map), test.Slice; !reflect.DeepEqual(got, want) {
			t.Errorf("MapToSlice(%v) got %v, want %v", test.Map, got, want)
		}
		// Test slice to map conversions.
		if got, want := SliceToMap(test.FromSlice), test.Map; !reflect.DeepEqual(got, want) {
			t.Errorf("SliceToMap(%v) got %v, want %v", test.FromSlice, got, want)
		}
		if got, want := SliceToMap(test.Slice), test.Map; !reflect.DeepEqual(got, want) {
			t.Errorf("SliceToMap(%v) got %v, want %v", test.Slice, got, want)
		}
		// Test MergeMaps merging each map multiple times.
		var maps []map[string]string
		for ix := 0; ix < 3; ix++ {
			maps = append(maps, test.FromMap, test.Map)
		}
		if got, want := MergeMaps(maps...), test.Map; !reflect.DeepEqual(got, want) {
			t.Errorf("MergeMaps got %v, want %v", got, want)
		}
		// Test MergeSlices merging each slice multiple times.
		var slices [][]string
		for ix := 0; ix < 3; ix++ {
			slices = append(slices, test.FromSlice, test.Slice)
		}
		if got, want := MergeSlices(slices...), test.Slice; !reflect.DeepEqual(got, want) {
			t.Errorf("MergeSlices got %v, want %v", got, want)
		}
		// Test CopyMap actually returns a copy.
		copyMap := CopyMap(test.Map)
		if !reflect.DeepEqual(copyMap, test.Map) {
			t.Errorf("CopyMap got %v, want %v", copyMap, test.Map)
		}
		copyMap["Z"] = "zzz"
		if reflect.DeepEqual(copyMap, test.Map) {
			t.Errorf("CopyMap(%v) failed copy semantics", copyMap)
		}
		// Test CopySlice actually returns a copy.
		copySlice := CopySlice(test.Slice)
		if len(copySlice) > 0 {
			if !reflect.DeepEqual(copySlice, test.Slice) {
				t.Errorf("CopySlice got %v, want %v", copySlice, test.Slice)
			}
			copySlice[0] = "Z=zzz"
			if reflect.DeepEqual(copySlice, test.Slice) {
				t.Errorf("CopySlice(%v) failed copy semantics", copySlice)
			}
		}
	}
}

func TestSplitJoinKeyValue(t *testing.T) {
	tests := []struct {
		KV, Key, Value string
	}{
		{"", "", ""},
		{"=", "", ""},
		{"=123", "", "123"},
		{"A=123", "A", "123"},
		{"A==123", "A", "=123"},
		{"A==123=", "A", "=123="},
		{"A=a b c", "A", "a b c"},
		{"A==a b c", "A", "=a b c"},
	}
	for _, test := range tests {
		key, value := SplitKeyValue(test.KV)
		if got, want := key, test.Key; got != want {
			t.Errorf("SplitKeyValue got key %q, want %q", got, want)
		}
		if got, want := value, test.Value; got != want {
			t.Errorf("SplitKeyValue got value %q, want %q", got, want)
		}
		if test.KV == "" {
			continue
		}
		if got, want := JoinKeyValue(key, value), test.KV; got != want {
			t.Errorf("JoinKeyValue got %q, want %q", got, want)
		}
	}
}

func TestSplitJoinTokens(t *testing.T) {
	tests := []struct {
		Sep, FromValue, Value string
		FromTokens, Tokens    []string
	}{
		{":", "", "", nil, nil},
		{":", ":::", "", nil, nil},
		{":", ":ABC:", "ABC", []string{"", "ABC", ""}, []string{"ABC"}},
		{":", ":A:B:C:", "A:B:C", []string{"", "A", "", "B", "", "C", ""}, []string{"A", "B", "C"}},
	}
	for _, test := range tests {
		if got, want := SplitTokens(test.FromValue, test.Sep), test.Tokens; !reflect.DeepEqual(got, want) {
			t.Errorf("SplitTokens(%v) got %v, want %v", test.FromValue, got, want)
		}
		if got, want := SplitTokens(test.Value, test.Sep), test.Tokens; !reflect.DeepEqual(got, want) {
			t.Errorf("SplitTokens(%v) got %v, want %v", test.Value, got, want)
		}
		if got, want := JoinTokens(test.FromTokens, test.Sep), test.Value; got != want {
			t.Errorf("JoinTokens(%v) got %v, want %v", test.FromTokens, got, want)
		}
		if got, want := JoinTokens(test.Tokens, test.Sep), test.Value; got != want {
			t.Errorf("JoinTokens(%v) got %v, want %v", test.Tokens, got, want)
		}
	}
}

func TestUniqueTokens(t *testing.T) {
	tests := []struct {
		Tokens, Want []string
	}{
		{nil, nil},
		{[]string{""}, nil},
		{[]string{"A"}, []string{"A"}},
		{[]string{"A", "A"}, []string{"A"}},
		{[]string{"A", "B"}, []string{"A", "B"}},
		{[]string{"A", "B", "A", "B"}, []string{"A", "B"}},
	}
	for _, test := range tests {
		if got, want := UniqueTokens(test.Tokens), test.Want; !reflect.DeepEqual(got, want) {
			t.Errorf("UniqueTokens(%q) got %q, want %q", test.Tokens, got, want)
		}
	}
}

func TestFilterToken(t *testing.T) {
	tests := []struct {
		Tokens []string
		Target string
		Want   []string
	}{
		{nil, "", nil},
		{nil, "A", nil},
		{[]string{""}, "", nil},
		{[]string{""}, "A", nil},
		{[]string{"A"}, "", []string{"A"}},
		{[]string{"A"}, "A", nil},
		{[]string{"A"}, "B", []string{"A"}},
		{[]string{"A", "A"}, "", []string{"A", "A"}},
		{[]string{"A", "A"}, "A", nil},
		{[]string{"A", "A"}, "B", []string{"A", "A"}},
		{[]string{"A", "B"}, "", []string{"A", "B"}},
		{[]string{"A", "B"}, "A", []string{"B"}},
		{[]string{"A", "B"}, "B", []string{"A"}},
		{[]string{"A", "B", "A", "B"}, "", []string{"A", "B", "A", "B"}},
		{[]string{"A", "B", "A", "B"}, "A", []string{"B", "B"}},
		{[]string{"A", "B", "A", "B"}, "B", []string{"A", "A"}},
	}
	for _, test := range tests {
		if got, want := FilterToken(test.Tokens, test.Target), test.Want; !reflect.DeepEqual(got, want) {
			t.Errorf("FilterToken(%q, %q) got %q, want %q", test.Tokens, test.Target, got, want)
		}
	}
}

func TestPrependUniqueToken(t *testing.T) {
	tests := []struct {
		Sep, Value, Token, Want string
	}{
		{":", "", "", ""},
		{":", "", "Z", "Z"},
		{":", "Z", "", "Z"},
		{":", "Z", "Z", "Z"},
		{":", ":Z:Z:", "Z", "Z"},
		{":", ":A:B", "Z", "Z:A:B"},
		{":", "A:B", "Z", "Z:A:B"},
		{":", "A:::B", "Z", "Z:A:B"},
		{":", "A:::B:", "Z", "Z:A:B"},
		{":", "Z:A:Z:B:Z", "Z", "Z:A:B"},
		{":", "Z:A:Z:B:Z:A:Z:B:Z", "Z", "Z:A:B"},
	}
	for _, test := range tests {
		if got, want := PrependUniqueToken(test.Value, test.Sep, test.Token), test.Want; got != want {
			t.Errorf("PrependUniqueToken(%q, %q, %q) got %v, want %v", test.Value, test.Sep, test.Token, got, want)
		}
	}
}

func TestAppendUniqueToken(t *testing.T) {
	tests := []struct {
		Sep, Value, Token, Want string
	}{
		{":", "", "", ""},
		{":", "", "Z", "Z"},
		{":", "Z", "", "Z"},
		{":", "Z", "Z", "Z"},
		{":", ":Z:Z:", "Z", "Z"},
		{":", ":A:B:", "Z", "A:B:Z"},
		{":", "A:B", "Z", "A:B:Z"},
		{":", "A:::B", "Z", "A:B:Z"},
		{":", "Z:A:Z:B:Z", "Z", "A:B:Z"},
		{":", "Z:A:Z:B:Z:A:Z:B:Z", "Z", "A:B:Z"},
	}
	for _, test := range tests {
		if got, want := AppendUniqueToken(test.Value, test.Sep, test.Token), test.Want; got != want {
			t.Errorf("AppendUniqueToken(%q, %q, %q) got %v, want %v", test.Value, test.Sep, test.Token, got, want)
		}
	}
}

func TestSortByKey(t *testing.T) {
	tests := []struct {
		In, Sorted []string
	}{
		{nil, nil},
		{[]string{}, []string{}},
		{[]string{"A"}, []string{"A"}},
		{
			[]string{"C=foo", "A", "B="},
			[]string{"A", "B=", "C=foo"},
		},
		{
			[]string{"A1=b", "AB=d", "A=a", "AZ=e", "A2=c"},
			[]string{"A=a", "A1=b", "A2=c", "AB=d", "AZ=e"},
		},
	}
	for _, test := range tests {
		var in []string
		if test.In != nil {
			in = make([]string, len(test.In))
			copy(in, test.In)
		}
		SortByKey(in)
		if got, want := in, test.Sorted; !reflect.DeepEqual(got, want) {
			t.Errorf("got %#v, want %#v", got, want)
		}
	}
}

// expectVarsA expects vars to contain A=123
func expectVarsA(t *testing.T, vars *Vars) {
	if got, want := vars.Contains("A"), true; got != want {
		t.Errorf(`Contains("A") got %v, want %v`, got, want)
	}
	if got, want := vars.Contains("B"), false; got != want {
		t.Errorf(`Contains("B") got %v, want %v`, got, want)
	}
	if got, want := vars.Contains("C"), false; got != want {
		t.Errorf(`Contains("C") got %v, want %v`, got, want)
	}
	if got, want := vars.Contains("D"), false; got != want {
		t.Errorf(`Contains("D") got %v, want %v`, got, want)
	}
	if got, want := vars.Get("A"), "123"; got != want {
		t.Errorf(`Get("A") got %v, want %v`, got, want)
	}
	if got, want := vars.Get("B"), ""; got != want {
		t.Errorf(`Get("B") got %v, want %v`, got, want)
	}
	if got, want := vars.Get("C"), ""; got != want {
		t.Errorf(`Get("C") got %v, want %v`, got, want)
	}
	if got, want := vars.Get("D"), ""; got != want {
		t.Errorf(`Get("D") got %v, want %v`, got, want)
	}
	if got, want := vars.GetTokens("A", ":"), []string{"123"}; !reflect.DeepEqual(got, want) {
		t.Errorf(`GetTokens("A") got %v, want %v`, got, want)
	}
	if got, want := vars.GetTokens("B", ":"), []string(nil); !reflect.DeepEqual(got, want) {
		t.Errorf(`GetTokens("B") got %v, want %v`, got, want)
	}
	if got, want := vars.GetTokens("C", ":"), []string(nil); !reflect.DeepEqual(got, want) {
		t.Errorf(`GetTokens("C") got %v, want %v`, got, want)
	}
	if got, want := vars.GetTokens("D", ":"), []string(nil); !reflect.DeepEqual(got, want) {
		t.Errorf(`GetTokens("D") got %v, want %v`, got, want)
	}
	if got, want := vars.ToMap(), map[string]string{"A": "123"}; !reflect.DeepEqual(got, want) {
		t.Errorf(`ToMap() got %v, want %v`, got, want)
	}
	if got, want := vars.ToSlice(), []string{"A=123"}; !reflect.DeepEqual(got, want) {
		t.Errorf(`ToSlice() got %v, want %v`, got, want)
	}
}

// expectVarsAB expects vars to contain A=123 B=x:y:z
func expectVarsAB(t *testing.T, vars *Vars) {
	if got, want := vars.Contains("A"), true; got != want {
		t.Errorf(`Contains("A") got %v, want %v`, got, want)
	}
	if got, want := vars.Contains("B"), true; got != want {
		t.Errorf(`Contains("B") got %v, want %v`, got, want)
	}
	if got, want := vars.Contains("C"), false; got != want {
		t.Errorf(`Contains("C") got %v, want %v`, got, want)
	}
	if got, want := vars.Contains("D"), false; got != want {
		t.Errorf(`Contains("D") got %v, want %v`, got, want)
	}
	if got, want := vars.Get("A"), "123"; got != want {
		t.Errorf(`Get("A") got %v, want %v`, got, want)
	}
	if got, want := vars.Get("B"), "x:y:z"; got != want {
		t.Errorf(`Get("B") got %v, want %v`, got, want)
	}
	if got, want := vars.Get("C"), ""; got != want {
		t.Errorf(`Get("C") got %v, want %v`, got, want)
	}
	if got, want := vars.Get("D"), ""; got != want {
		t.Errorf(`Get("D") got %v, want %v`, got, want)
	}
	if got, want := vars.GetTokens("A", ":"), []string{"123"}; !reflect.DeepEqual(got, want) {
		t.Errorf(`GetTokens("A") got %v, want %v`, got, want)
	}
	if got, want := vars.GetTokens("B", ":"), []string{"x", "y", "z"}; !reflect.DeepEqual(got, want) {
		t.Errorf(`GetTokens("B") got %v, want %v`, got, want)
	}
	if got, want := vars.GetTokens("C", ":"), []string(nil); !reflect.DeepEqual(got, want) {
		t.Errorf(`GetTokens("C") got %v, want %v`, got, want)
	}
	if got, want := vars.GetTokens("D", ":"), []string(nil); !reflect.DeepEqual(got, want) {
		t.Errorf(`GetTokens("D") got %v, want %v`, got, want)
	}
	if got, want := vars.ToMap(), map[string]string{"A": "123", "B": "x:y:z"}; !reflect.DeepEqual(got, want) {
		t.Errorf(`ToMap() got %v, want %v`, got, want)
	}
	if got, want := vars.ToSlice(), []string{"A=123", "B=x:y:z"}; !reflect.DeepEqual(got, want) {
		t.Errorf(`ToSlice() got %v, want %v`, got, want)
	}
}

// expectVarsAC expects vars to contain A=123 C=foo
func expectVarsAC(t *testing.T, vars *Vars) {
	if got, want := vars.Contains("A"), true; got != want {
		t.Errorf(`Contains("A") got %v, want %v`, got, want)
	}
	if got, want := vars.Contains("B"), false; got != want {
		t.Errorf(`Contains("B") got %v, want %v`, got, want)
	}
	if got, want := vars.Contains("C"), true; got != want {
		t.Errorf(`Contains("C") got %v, want %v`, got, want)
	}
	if got, want := vars.Contains("D"), false; got != want {
		t.Errorf(`Contains("D") got %v, want %v`, got, want)
	}
	if got, want := vars.Get("A"), "123"; got != want {
		t.Errorf(`Get("A") got %v, want %v`, got, want)
	}
	if got, want := vars.Get("B"), ""; got != want {
		t.Errorf(`Get("B") got %v, want %v`, got, want)
	}
	if got, want := vars.Get("C"), "foo"; got != want {
		t.Errorf(`Get("C") got %v, want %v`, got, want)
	}
	if got, want := vars.Get("D"), ""; got != want {
		t.Errorf(`Get("D") got %v, want %v`, got, want)
	}
	if got, want := vars.GetTokens("A", ":"), []string{"123"}; !reflect.DeepEqual(got, want) {
		t.Errorf(`GetTokens("A") got %v, want %v`, got, want)
	}
	if got, want := vars.GetTokens("B", ":"), []string(nil); !reflect.DeepEqual(got, want) {
		t.Errorf(`GetTokens("B") got %v, want %v`, got, want)
	}
	if got, want := vars.GetTokens("C", ":"), []string{"foo"}; !reflect.DeepEqual(got, want) {
		t.Errorf(`GetTokens("C") got %v, want %v`, got, want)
	}
	if got, want := vars.GetTokens("D", ":"), []string(nil); !reflect.DeepEqual(got, want) {
		t.Errorf(`GetTokens("D") got %v, want %v`, got, want)
	}
	if got, want := vars.ToMap(), map[string]string{"A": "123", "C": "foo"}; !reflect.DeepEqual(got, want) {
		t.Errorf(`ToMap() got %v, want %v`, got, want)
	}
	if got, want := vars.ToSlice(), []string{"A=123", "C=foo"}; !reflect.DeepEqual(got, want) {
		t.Errorf(`ToSlice() got %v, want %v`, got, want)
	}
}

var v123, vxyz, vfoo = "123", "x:y:z", "foo"

func TestZeroVars(t *testing.T) {
	var vars Vars
	if got, want := vars.Contains("A"), false; got != want {
		t.Errorf(`Contains("A") got %v, want %v`, got, want)
	}
	if got, want := vars.Get("A"), ""; got != want {
		t.Errorf(`Get("A") got %v, want %v`, got, want)
	}
	if got, want := vars.GetTokens("A", ":"), []string(nil); !reflect.DeepEqual(got, want) {
		t.Errorf(`GetTokens("A") got %v, want %v`, got, want)
	}
	if got, want := vars.ToMap(), map[string]string{}; !reflect.DeepEqual(got, want) {
		t.Errorf(`ToMap() got %v, want %v`, got, want)
	}
	if got, want := vars.ToSlice(), []string{}; !reflect.DeepEqual(got, want) {
		t.Errorf(`ToSlice() got %v, want %v`, got, want)
	}
	if got, want := vars.Deltas(), map[string]*string{}; !reflect.DeepEqual(got, want) {
		t.Errorf(`Deltas() got %v, want %v`, got, want)
	}
	// Mutate some values.
	vars.Set("A", "123")
	vars.SetTokens("B", []string{"x", "y", "z"}, ":")
	vars.Delete("C")
	expectVarsAB(t, &vars)
	if got, want := vars.Deltas(), map[string]*string{"A": &v123, "B": &vxyz, "C": nil}; !reflect.DeepEqual(got, want) {
		t.Errorf(`Deltas() got %v, want %v`, got, want)
	}
	// Delete an existing value.
	vars.Delete("B")
	expectVarsA(t, &vars)
	if got, want := vars.Deltas(), map[string]*string{"A": &v123, "B": nil, "C": nil}; !reflect.DeepEqual(got, want) {
		t.Errorf(`Deltas() got %v, want %v`, got, want)
	}
	// Set a previously deleted value.
	vars.Set("C", "foo")
	expectVarsAC(t, &vars)
	if got, want := vars.Deltas(), map[string]*string{"A": &v123, "B": nil, "C": &vfoo}; !reflect.DeepEqual(got, want) {
		t.Errorf(`Deltas() got %v, want %v`, got, want)
	}
}

func TestVarsFromMap(t *testing.T) {
	vars := VarsFromMap(map[string]string{"A": "123", "B": "x:y:z"})
	expectVarsAB(t, vars)
	if got, want := vars.Deltas(), map[string]*string{}; !reflect.DeepEqual(got, want) {
		t.Errorf(`Deltas() got %v, want %v`, got, want)
	}
	// Delete a non-existent value.
	vars.Delete("C")
	expectVarsAB(t, vars)
	if got, want := vars.Deltas(), map[string]*string{"C": nil}; !reflect.DeepEqual(got, want) {
		t.Errorf(`Deltas() got %v, want %v`, got, want)
	}
	// Delete an existing value.
	vars.Delete("B")
	expectVarsA(t, vars)
	if got, want := vars.Deltas(), map[string]*string{"B": nil, "C": nil}; !reflect.DeepEqual(got, want) {
		t.Errorf(`Deltas() got %v, want %v`, got, want)
	}
	// Set a previously deleted value.
	vars.Set("C", "foo")
	expectVarsAC(t, vars)
	if got, want := vars.Deltas(), map[string]*string{"B": nil, "C": &vfoo}; !reflect.DeepEqual(got, want) {
		t.Errorf(`Deltas() got %v, want %v`, got, want)
	}
}

func TestVarsFromSlice(t *testing.T) {
	vars := VarsFromSlice([]string{"A=123", "B=x:y:z"})
	expectVarsAB(t, vars)
	if got, want := vars.Deltas(), map[string]*string{}; !reflect.DeepEqual(got, want) {
		t.Errorf(`Deltas() got %v, want %v`, got, want)
	}
	// Delete a non-existent value.
	vars.Delete("C")
	expectVarsAB(t, vars)
	if got, want := vars.Deltas(), map[string]*string{"C": nil}; !reflect.DeepEqual(got, want) {
		t.Errorf(`Deltas() got %v, want %v`, got, want)
	}
	// Delete an existing value.
	vars.Delete("B")
	expectVarsA(t, vars)
	if got, want := vars.Deltas(), map[string]*string{"B": nil, "C": nil}; !reflect.DeepEqual(got, want) {
		t.Errorf(`Deltas() got %v, want %v`, got, want)
	}
	// Set a previously deleted value.
	vars.Set("C", "foo")
	expectVarsAC(t, vars)
	if got, want := vars.Deltas(), map[string]*string{"B": nil, "C": &vfoo}; !reflect.DeepEqual(got, want) {
		t.Errorf(`Deltas() got %v, want %v`, got, want)
	}
}

func TestVarsFromOS(t *testing.T) {
	// Set an environment variable and make sure it shows up.
	const testKey, testValue = "OS_ENV_TEST_KEY", "OS_ENV_TEST_VAL"
	if err := os.Setenv(testKey, testValue); err != nil {
		t.Fatalf("Setenv(%q, %q) failed: %v", testKey, testValue, err)
	}
	defer os.Unsetenv(testKey)
	vars := VarsFromOS()
	if got, want := vars.Contains(testKey), true; got != want {
		t.Errorf(`Contains(%q) got %v, want %v`, testKey, got, want)
	}
	if got, want := vars.Get(testKey), testValue; got != want {
		t.Errorf(`Get(%q) got %v, want %v`, testKey, got, want)
	}
	if got, want := vars.GetTokens(testKey, ":"), []string{testValue}; !reflect.DeepEqual(got, want) {
		t.Errorf(`GetTokens(%q) got %v, want %v`, testKey, got, want)
	}
	if got, want := vars.Deltas(), map[string]*string{}; !reflect.DeepEqual(got, want) {
		t.Errorf(`Deltas() got %v, want %v`, got, want)
	}
}
