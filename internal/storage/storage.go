package storage

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type Storage struct {
	mu         sync.RWMutex
	skipList   *SkipList
	tables     []*SSTable
	dataDir    string
	blockSize  int64
	maxMemSize int64
}

func NewStorage(dataDir string, blockSize int64, maxMemSize int64) (*Storage, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	s := &Storage{
		skipList:   NewSkipList(),
		dataDir:    dataDir,
		blockSize:  blockSize,
		maxMemSize: maxMemSize,
	}

	if err := s.loadSSTables(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.skipList != nil && s.skipList.size > 0 {
		log.Println("Starting data flush...")
		_, err := s.flush()
		if err != nil {
			return err
		}
		log.Println("Data flush is end")
	}

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
	table, err := OpenSSTable(path, s.blockSize)
	if err != nil {
		return err
	}

	s.tables = append(s.tables, table)

	return nil
}

func (s *Storage) Set(key string, value []byte, flags uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.skipList.Set(key, value, flags, false)

	if s.skipList.size >= s.maxMemSize {
		path, err := s.flush()
		if err != nil {
			return err
		}

		err = s.loadSSTable(path)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Storage) Get(key string) ([]byte, uint32, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, flags, isTomb, found := s.skipList.Get(key)
	if found {
		if isTomb {
			return nil, 0, false, nil
		}

		return val, flags, true, nil
	}

	var err error
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

func (s *Storage) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.skipList.Delete(key)
}

func (s *Storage) flush() (string, error) {
	name := fmt.Sprintf("%d.sst", time.Now().UnixNano())
	path := filepath.Join(s.dataDir, name)

	err := CreateSSTable(path, s.blockSize, s.skipList)
	if err != nil {
		return "", err
	}

	s.skipList = NewSkipList()

	return path, nil
}
