package utils

import (
	"math/rand"
	"time"
)

func StringWithCharset(length int) string {
	charset := "abcdefghijklmnopqrstuvwxyz" +
		"0123456789"
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}
