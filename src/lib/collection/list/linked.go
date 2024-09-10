// 链表的实现

package list

type LinkedList struct {
	first *node
	last  *node
	size  int
}

type node struct {
	val  interface{}
	prev *node
	next *node
}

// 这里如果多个进程往列表里添加元素，会不会有冲突？
func (list *LinkedList) Add(val interface{}) {
	if list == nil {
		panic("list is nil")
	}
	n := &node{val: val}
	if list.last == nil {
		list.first = n
		list.last = n
	} else {
		list.last.next = n
		n.prev = list.last
		list.last = n
	}
	list.size++
}

func (list *LinkedList) find(index int) (val *node) {
	if index < list.size/2 {
		n := list.first
		for i := 0; i < index; i++ {
			n = n.next
		}
		return n
	} else {
		n := list.last
		for i := list.size - 1; i > index; i-- {
			n = n.prev
		}
		return n
	}
}

func (list *LinkedList) Get(index int) (val interface{}) {
	if list == nil {
		panic("list is nil")
	}
	if index < 0 || index >= list.size {
		panic("index out of bounds")
	}
	return list.find(index).val
}

func (list *LinkedList) Insert(index int, val interface{}) {
	if list == nil {
		panic("list is nil")
	}
	if index < 0 || index >= list.size {
		panic("index out of bounds")
	}
	if index == list.size {
		list.Add(val)
	} else {
		// 找到index位置的元素，把新元素插在index位置上，原来的元素往后移
		pivot := list.find(index)
		n := &node{val: val, prev: pivot.prev, next: pivot}
		if pivot.prev == nil {
			list.first = n
		} else {
			pivot.prev.next = n
		}
		pivot.prev = n
	}
	list.size++
}

func (list *LinkedList) Remove(index int) {
	if list == nil {
		panic("list is nil")
	}
	if index < 0 || index >= list.size {
		panic("index out of bounds")
	}
	n := list.find(index)
	if n.prev == nil {
		list.first = n.next
	} else {
		n.prev.next = n.next
	}
	if n.next == nil {
		list.last = n.prev
	} else {
		n.next.prev = n.prev
	}

	// for gc
	n.prev = nil
	n.next = nil
	n.val = nil

	list.size--
}

func (list *LinkedList) Size() int {
	if list == nil {
		panic("list is nil")
	}
	return list.size
}

func (list *LinkedList) ForEach(consumer func(int, interface{}) bool) {
	if list == nil {
		panic("list is nil")
	}
	n := list.first
	for i := 0; i < list.size; i++ {
		if !consumer(i, n.val) {
			break
		}
		n = n.next
	}
}

func Make(vals ...interface{}) *LinkedList {
	list := &LinkedList{}
	for _, val := range vals {
		list.Add(val)
	}
	return list
}
