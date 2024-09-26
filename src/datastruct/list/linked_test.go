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

func TestRemove(t *testing.T) {
	list := Make()
	for i := 0; i < 10; i++ {
		list.Add(i)
	}
	for i := 9; i >= 0; i-- {
		list.Remove(i)
		if i != list.Len() {
			t.Errorf("remove test fail: expected size " + strconv.Itoa(i) + " but got " + strconv.Itoa(list.Len()))
		}
		list.ForEach(func(i int, v interface{}) bool {
			integer, _ := v.(int)
			if integer != i {
				t.Errorf("Expected %d, got %d", i, integer)
			}
			return true
		})
	}
}

func TestRemoveVal(t *testing.T) {
	list := Make()
	for i := 0; i < 10; i++ {
		list.Add(i)
		list.Add(i)
	}

	for index := 0; index < list.Len(); index++ {
		list.RemoveAllByVal(index)
		list.ForEach(func(i int, v interface{}) bool {
			integer, _ := v.(int)
			if integer == index {
				t.Errorf("Expected %d to be removed", index)
			}
			return true
		})
	}

	list = Make()
	for i := 0; i < 10; i++ {
		list.Add(i)
		list.Add(i)
	}
	for i := 0; i < 10; i++ {
		list.RemoveByVal(i, 1)
	}
	list.ForEach(func(i int, v interface{}) bool {
		integer, _ := v.(int)
		if integer != i {
			t.Errorf("Expected %d, got %d", i, integer)
		}
		return true
	})

	for i := 0; i < 10; i++ {
		list.RemoveByVal(i, 1)
	}
	if list.Len() != 0 {
		t.Error("test fail: expected 0, got: " + strconv.Itoa(list.Len()))
	}

	list = Make()
	for i := 0; i < 10; i++ {
		list.Add(i)
		list.Add(i)
	}
	for i := 0; i < 10; i++ {
		list.ReverseRemoveByVal(i, 1)
	}
	list.ForEach(func(i int, v interface{}) bool {
		integer, _ := v.(int)
		if integer != i {
			t.Errorf("Expected %d, got %d", i, integer)
		}
		return true
	})
	for i := 0; i < 10; i++ {
		list.ReverseRemoveByVal(i, 1)
	}
	if list.Len() != 0 {
		t.Error("test fail: expected 0, got: " + strconv.Itoa(list.Len()))
	}
}

func TestInsert(t *testing.T) {
	list := Make()
	for i := 0; i < 10; i++ {
		list.Add(i)
	}
	for i := 0; i < 10; i++ {
		list.Insert(i*2, i)

		list.ForEach(func(j int, v interface{}) bool {
			var expected int
			if j < (i+1)*2 {
				if j%2 == 0 {
					expected = j / 2
				} else {
					expected = (j - 1) / 2
				}
			} else {
				expected = j - i - 1
			}
			actual, _ := list.Get(j).(int)
			if actual != expected {
				t.Errorf("Expected %d, got %d", expected, actual)
			}
			return true
		})

		for j := 0; j < list.Len(); j++ {
			var expected int
			if j < (i+1)*2 {
				if j%2 == 0 {
					expected = j / 2
				} else {
					expected = (j - 1) / 2
				}
			} else {
				expected = j - i - 1
			}
			actual, _ := list.Get(j).(int)
			if actual != expected {
				t.Errorf("Expected %d, got %d", expected, actual)
			}
		}
	}
}

func TestRemoveLast(t *testing.T) {
	list := Make()
	for i := 0; i < 10; i++ {
		list.Add(i)
	}
	for i := 9; i >= 0; i-- {
		v := list.RemoveLast()
		integer, _ := v.(int)
		if integer != i {
			t.Errorf("Expected %d, got %d", i, integer)
		}
	}
	if list.Len() != 0 {
		t.Error("test fail: expected 0, got: " + strconv.Itoa(list.Len()))
	}
}

func TestRange(t *testing.T) {
	list := Make()
	for i := 0; i < 10; i++ {
		list.Add(i)
	}
	for start := 0; start < 10; start++ {
		for stop := start; stop < 10; stop++ {
			slice := list.Range(start, stop)
			if len(slice) != stop-start {
				t.Error("test fail: expected length " + strconv.Itoa(stop-start) + " but got " + strconv.Itoa(len(slice)))
			}
			sliceIndex := 0
			for i := start; i < stop; i++ {
				val := slice[sliceIndex]
				intVal, _ := val.(int)
				if intVal != i {
					t.Error("test fail: expected " + strconv.Itoa(i) + " but got " + strconv.Itoa(intVal))
				}
				sliceIndex++
			}
		}
	}
}
