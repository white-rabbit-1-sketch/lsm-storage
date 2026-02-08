package storage

import (
	"hash/fnv"
	"math"
)

type BloomFilter []byte

func NewBloomFilter(n int, p float64) BloomFilter {
	m := uint32(-float64(n) * math.Log(p) / (math.Log(2) * math.Log(2)))
	return make(BloomFilter, (m+7)/8)
}

func (bf BloomFilter) Add(key []byte) {
	h := hashKey(key)
	m := uint32(len(bf) * 8)
	for i := 0; i < 4; i++ {
		idx := (h[0] + uint32(i)*h[1]) % m
		bf[idx/8] |= (1 << (idx % 8))
	}
}

func (bf BloomFilter) Contains(key []byte) bool {
	if len(bf) == 0 {
		return true
	}
	h := hashKey(key)
	m := uint32(len(bf) * 8)
	for i := 0; i < 4; i++ {
		idx := (h[0] + uint32(i)*h[1]) % m
		if bf[idx/8]&(1<<(idx%8)) == 0 {
			return false
		}
	}
	return true
}

func hashKey(key []byte) [2]uint32 {
	h := fnv.New64a()
	h.Write(key)
	u := h.Sum64()
	return [2]uint32{uint32(u), uint32(u >> 32)}
}
