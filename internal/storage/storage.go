package storage

import (
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Storage struct {
	tablesMutex sync.RWMutex
	flushMutex  sync.Mutex
	shards      []*Shard
	shardsSize  int64
	tables      []*SSTable
	dataDir     string
	blockSize   int64
	maxMemSize  int64
	shardsCount uint32
}

type Shard struct {
	mu       sync.RWMutex
	skipList *SkipList
}

func NewStorage(dataDir string, blockSize int64, maxMemSize int64, shardsCount uint32) (*Storage, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	s := &Storage{
		shardsCount: shardsCount,
		dataDir:     dataDir,
		blockSize:   blockSize,
		maxMemSize:  maxMemSize,
		shards:      make([]*Shard, shardsCount),
	}

	for i := 0; i < int(shardsCount); i++ {
		s.shards[i] = &Shard{
			skipList: NewSkipList(),
		}
	}

	if err := s.loadSSTables(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Storage) getShard(key string) (*Shard, error) {
	h := fnv.New32a()
	_, err := h.Write([]byte(key))
	if err != nil {
		return nil, err
	}

	return s.shards[h.Sum32()%s.shardsCount], nil
}

func (s *Storage) Close() error {
	err := s.flush(false)
	if err != nil {
		return err
	}

	err = s.closeTables()
	if err != nil {
		return err
	}

	return nil
}

func (s *Storage) loadSSTables() error {
	files, err := os.ReadDir(s.dataDir)
	if err != nil {
		return err
	}

	var sstFiles []string
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".sst") {
			sstFiles = append(sstFiles, filepath.Join(s.dataDir, f.Name()))
		}
	}

	sort.Strings(sstFiles)

	for _, path := range sstFiles {
		err = s.loadSSTable(path)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Storage) loadSSTable(path string) error {
	s.tablesMutex.Lock()
	defer s.tablesMutex.Unlock()

	table, err := OpenSSTable(path, s.blockSize)
	if err != nil {
		return err
	}

	s.tables = append(s.tables, table)

	return nil
}

func (s *Storage) Set(key string, value []byte, flags uint32) error {
	shard, err := s.getShard(key)
	if err != nil {
		return err
	}

	shard.mu.Lock()
	oldSize := shard.skipList.size
	shard.skipList.Set(key, value, flags, false)
	newSize := shard.skipList.size
	shard.mu.Unlock()

	shardsSize := atomic.AddInt64(&s.shardsSize, newSize-oldSize)

	if shardsSize >= s.maxMemSize {
		err = s.flush(true)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Storage) Get(key string) ([]byte, uint32, bool, error) {
	shard, err := s.getShard(key)
	if err != nil {
		return nil, 0, false, err
	}

	shard.mu.RLock()
	val, flags, isTomb, found := shard.skipList.Get(key)
	shard.mu.RUnlock()

	if found {
		if isTomb {
			return nil, 0, false, nil
		}

		return val, flags, true, nil
	}

	s.tablesMutex.RLock()
	defer s.tablesMutex.RUnlock()

	for i := len(s.tables) - 1; i >= 0; i-- {
		val, flags, isTomb, err = s.tables[i].Get(key)
		if err != nil {
			return nil, 0, false, err
		}

		if isTomb {
			return nil, 0, false, nil
		}

		if val != nil {
			return val, flags, true, nil
		}
	}

	return nil, 0, false, nil
}

func (s *Storage) Delete(key string) error {
	shard, err := s.getShard(key)
	if err != nil {
		return err
	}

	shard.mu.Lock()
	shard.skipList.Delete(key)
	shard.mu.Unlock()

	return nil
}

func (s *Storage) flush(load bool) error {
	lock := s.flushMutex.TryLock()
	if lock {
		defer s.flushMutex.Unlock()

		shardsSize := atomic.LoadInt64(&s.shardsSize)
		if shardsSize > 0 {
			log.Println("Starting data flush...")

			for i := 0; i < int(s.shardsCount); i++ {
				s.shards[i].mu.Lock()

				if s.shards[i].skipList.size == 0 {
					s.shards[i].mu.Unlock()
					continue
				}

				name := fmt.Sprintf("%d.%d.sst", i, time.Now().UnixNano())
				path := filepath.Join(s.dataDir, name)

				err := CreateSSTable(path, s.blockSize, s.shards[i].skipList)
				if err != nil {
					s.shards[i].mu.Unlock()
					return err
				}

				atomic.AddInt64(&s.shardsSize, -s.shards[i].skipList.size)

				s.shards[i].skipList = NewSkipList()
				s.shards[i].mu.Unlock()

				if load {
					err = s.loadSSTable(path)
					if err != nil {
						return err
					}
				}

			}

			log.Println("Data flush is end")
		}
	}

	return nil
}

func (s *Storage) closeTables() error {
	s.tablesMutex.Lock()
	defer s.tablesMutex.Unlock()

	log.Println("Starting tables close...")
	for i := range s.tables {
		err := s.tables[i].Close()
		if err != nil {
			return err
		}
	}
	log.Println("Tables are closed")

	return nil
}
