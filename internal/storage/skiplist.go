package storage

import (
	"math/rand"
)

const (
	MaxLevel    = 32
	Probability = 0.5
)

type Node struct {
	key         string
	value       []byte
	flags       uint32
	isTombstone bool
	next        []*Node
}

type SkipList struct {
	head  *Node
	level int
	size  int64
}

func NewSkipList() *SkipList {
	return &SkipList{
		head:  &Node{next: make([]*Node, MaxLevel)},
		level: 1,
	}
}

func (s *SkipList) randomLevel() int {
	lvl := 1
	for rand.Float64() < Probability && lvl < MaxLevel {
		lvl++
	}

	return lvl
}

func (s *SkipList) Set(key string, value []byte, flags uint32, isTombstone bool) {
	update := make([]*Node, MaxLevel)
	current := s.head

	for i := s.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
		update[i] = current
	}

	target := current.next[0]

	if target != nil && target.key == key {
		target.value = value
		target.flags = flags
		target.isTombstone = isTombstone
		return
	}

	newLevel := s.randomLevel()
	if newLevel > s.level {
		for i := s.level; i < newLevel; i++ {
			update[i] = s.head
		}
		s.level = newLevel
	}

	newNode := &Node{
		key:         key,
		value:       value,
		flags:       flags,
		isTombstone: isTombstone,
		next:        make([]*Node, newLevel),
	}

	for i := 0; i < newLevel; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}

	s.size += int64(len(key) + len(value) + 12)
}

func (s *SkipList) Get(key string) ([]byte, uint32, bool, bool) {
	current := s.head
	for i := s.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
	}

	target := current.next[0]
	if target != nil && target.key == key {
		return target.value, target.flags, target.isTombstone, true
	}

	return nil, 0, false, false
}

func (s *SkipList) Delete(key string) {
	s.Set(key, nil, 0, true)
}
