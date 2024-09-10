package list

import (
	"strconv"
	"strings"
	"testing"
)

func ToString(list *LinkedList) string {
	arr := make([]string, list.size)
	list.ForEach(func(i int, v interface{}) bool {
		integer, _ := v.(int)
		arr[i] = strconv.Itoa(integer)
		return true
	})
	return "[" + strings.Join(arr, ", ") + "]"
}

func TestAdd(t *testing.T) {
	list := Make()
	for i := 0; i < 10; i++ {
		list.Add(i)
	}
	list.ForEach(func(i int, v interface{}) bool {
		integer, _ := v.(int)
		if integer != i {
			t.Errorf("Expected %d, got %d", i, integer)
		}
		return true
	})
}

func TestGet(t *testing.T) {
	list := Make()
	for i := 0; i < 10; i++ {
		list.Add(i)
	}
	for i := 0; i < 10; i++ {
		v := list.Get(i)
		integer, _ := v.(int)
		if integer != i {
			t.Errorf("Expected %d, got %d", i, integer)
		}
	}
}
