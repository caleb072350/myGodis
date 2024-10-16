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
