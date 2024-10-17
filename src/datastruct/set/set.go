package set

import "myGodis/src/datastruct/dict"

type Set struct {
	dict *dict.Dict
}

func Make(shardCountHint int) *Set {
	return &Set{
		dict: dict.Make(shardCountHint),
	}
}

func MakeFromVals(members ...string) *Set {
	set := &Set{
		dict: dict.Make(len(members)),
	}
	for _, member := range members {
		set.Add(member)
	}
	return set
}

func (set *Set) Add(val string) int {
	return set.dict.Put(val, struct{}{})
}

func (set *Set) Remove(val string) int {
	return set.dict.Remove(val)
}

func (set *Set) Has(val string) bool {
	_, exists := set.dict.Get(val)
	return exists
}

func (set *Set) Len() int {
	return set.dict.Len()
}

func (set *Set) ToSlice() []string {
	slice := make([]string, set.dict.Len())
	i := 0
	set.dict.ForEach(func(key string, val interface{}) bool {
		if i < len(slice) {
			slice[i] = key
		} else {
			// set extended during traversal
			slice = append(slice, key)
		}
		i++
		return true
	})
	return slice
}

func (set *Set) ForEach(consumer func(member string) bool) {
	set.dict.ForEach(func(key string, val interface{}) bool {
		return consumer(key)
	})
}

// 交集运算
func (set *Set) Intersect(another *Set) *Set {
	if set == nil {
		panic("set is nil")
	}
	setSize := set.Len()
	anotherSize := another.Len()
	if setSize > anotherSize {
		set, another = another, set
	}
	newSet := Make(setSize)
	set.ForEach(func(member string) bool {
		if another.Has(member) {
			newSet.Add(member)
		}
		return true
	})
	return newSet
}

// 并集运算
func (set *Set) Union(another *Set) *Set {
	if set == nil {
		panic("set is nil")
	}
	newSet := Make(set.Len() + another.Len())
	set.ForEach(func(member string) bool {
		newSet.Add(member)
		return true
	})
	another.ForEach(func(member string) bool {
		newSet.Add(member)
		return true
	})
	return newSet
}

// 差集运算
func (set *Set) Diff(another *Set) *Set {
	if set == nil {
		panic("set is nil")
	}
	newSet := Make(set.Len())
	set.ForEach(func(member string) bool {
		if !another.Has(member) {
			newSet.Add(member)
		}
		return true
	})
	return newSet
}

// func (set *Set) RandomMembers(limit int) []string {
// 	return set.dict.RandomKeys(limit)
// }
