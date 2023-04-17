package qcow2

import (
	"os"
	"testing"
)

func TestQcow2_Read(t *testing.T) {
	f, err := os.Open(qcowFile)
	if err != nil {
		t.Fatalf("os.Open: %v", err)
	}
	defer f.Close()
	qcow, err := Read(f, 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	_ = qcow
}
