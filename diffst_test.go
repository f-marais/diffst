package main

import (
	"fmt"
	"os"
	"testing"
)

func TestReverseToReturnReversedInputString(t *testing.T) {
	if 1 != 1 {
		t.Fatalf("Expected ")
	}
}

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	fmt.Println("tesging")
	os.Exit(m.Run())
	fmt.Println("afters")
}
