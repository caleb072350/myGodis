package dict

import (
	"strconv"
	"testing"
)

func TestPut(t *testing.T) {
	d := MakeConcurrent(0)
	d.Put("key", "value")
	val, ok := d.Get("key")
	if !ok || val != "value" {
		t.Error("Put failed")
	}

	// test update
	ret := d.Put("key", "2")
	if ret != 0 {
		t.Error("put test failed: expected result 0, actual: " + strconv.Itoa(ret))
	}
}

func TestRemove(t *testing.T) {
	d := MakeConcurrent(0)
	d.Put("key", "value")
	d.Remove("key")
	val, ok := d.Get("key")
	if ok {
		t.Error("Remove failed")
	}
	if val != nil {
		t.Error("Remove failed")
	}
}
