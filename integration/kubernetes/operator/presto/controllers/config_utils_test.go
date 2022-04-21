package controllers

import "testing"

func TestConfigDataHashOnMap(t *testing.T) {
	mapA := map[string]string{"K": "foo", "V": "1"}
	mapAReordered := map[string]string{"V": "1", "K": "foo"}
	mapB := map[string]string{"K": "foo", "V": "2"}

	if ConfigDataHash(mapA) != ConfigDataHash(mapA) {
		t.Fatalf("Want consistent for the same map, %x, %x", ConfigDataHash(mapA), ConfigDataHash(mapA))
	}

	if ConfigDataHash(mapA) != ConfigDataHash(mapAReordered) {
		t.Fatalf("Want matching for the reordered map, %x, %x", ConfigDataHash(mapA), ConfigDataHash(mapAReordered))
	}

	if ConfigDataHash(mapA) == ConfigDataHash(mapB) {
		t.Fatalf("Want not matching with different structs , %x, %x", ConfigDataHash(mapA), ConfigDataHash(mapB))
	}

	if ConfigDataHash(mapA) == ConfigDataHash(nil) {
		t.Fatalf("Want not matching with nil struct, %x, %x", ConfigDataHash(mapA), ConfigDataHash(nil))
	}
}

func TestUnorderedKeyValuePairHashing(t *testing.T) {
	var hashResultA uint32 = 0
	hashResultA ^= hashKeyValuePair("a", "1")
	hashResultA ^= hashKeyValuePair("b", "2")
	hashResultA ^= hashKeyValuePair("c", "3")

	var hashResultB uint32 = 0
	hashResultB ^= hashKeyValuePair("b", "2")
	hashResultA ^= hashKeyValuePair("c", "3")
	hashResultB ^= hashKeyValuePair("a", "1")

	if hashResultA != hashResultB {
		t.Fatalf("Want consistent for the same pairs with different order, %x, %x", hashResultA, hashResultB)
	}
}
