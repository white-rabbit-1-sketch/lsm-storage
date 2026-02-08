package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"io"
	"os"
	"sort"
)

type SSTable struct {
	f                      *os.File
	writer                 *bufio.Writer
	index                  []IndexEntry
	indexStartOffset       int64
	bloomFilterStartOffset int64
	blockSize              int64
	filter                 BloomFilter
	hashIndex              map[uint64]int64
}

type IndexEntry struct {
	Key    string
	Offset int64
}

func Flush(path string, blockSize int64, skipList *SkipList) (*SSTable, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}

	table := &SSTable{
		f:         f,
		writer:    bufio.NewWriter(f),
		blockSize: blockSize,
	}

	err = table.Write(skipList)
	if err != nil {
		closeError := table.Close()
		if closeError != nil {
			return nil, err
		}

		return nil, err
	}

	err = f.Close()
	if err != nil {
		return nil, err
	}

	table.f, err = os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	return table, nil
}

func OpenSSTable(path string, blockSize int64) (*SSTable, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	t := &SSTable{f: f, blockSize: blockSize}

	err = t.readBloomFilter()
	if err != nil {
		closeError := f.Close()
		if closeError != nil {
			return nil, err
		}

		return nil, err
	}

	err = t.readIndex()
	if err != nil {
		closeError := f.Close()
		if closeError != nil {
			return nil, err
		}

		return nil, err
	}

	err = t.rebuildHashIndex()
	if err != nil {
		closeError := f.Close()
		if closeError != nil {
			return nil, err
		}

		return nil, err
	}

	return t, nil
}

func (t *SSTable) Close() error {
	return t.f.Close()
}

func (t *SSTable) Get(searchKey string) ([]byte, uint32, bool, error) {
	if !t.filter.Contains([]byte(searchKey)) {
		return nil, 0, false, nil
	}

	h, err := t.hashString(searchKey)
	if err != nil {
		return nil, 0, false, err
	}

	if offset, ok := t.hashIndex[h]; ok {
		val, flags, isTombstone, err := t.readEntryAt(offset, searchKey)
		if err == nil && val != nil {
			return val, flags, isTombstone, nil
		}
	}

	if len(t.index) == 0 {
		return nil, 0, false, nil
	}

	i := sort.Search(len(t.index), func(i int) bool {
		return t.index[i].Key > searchKey
	})

	targetIdx := 0
	if i > 0 {
		targetIdx = i - 1
	}

	startOffset := t.index[targetIdx].Offset
	var endOffset int64
	if targetIdx+1 < len(t.index) {
		endOffset = t.index[targetIdx+1].Offset
	} else {
		endOffset = t.bloomFilterStartOffset
	}

	blockLen := endOffset - startOffset
	blockBuf := make([]byte, blockLen)
	_, err = t.f.ReadAt(blockBuf, startOffset)
	if err != nil {
		return nil, 0, false, err
	}

	var pos int64 = 0
	searchKeyBytes := []byte(searchKey)

	for pos < blockLen {
		kLen := binary.BigEndian.Uint16(blockBuf[pos : pos+2])
		vLen := binary.BigEndian.Uint32(blockBuf[pos+2 : pos+6])
		flags := binary.BigEndian.Uint32(blockBuf[pos+6 : pos+10])
		isTombstone := binary.BigEndian.Uint16(blockBuf[pos+10:pos+12]) == 1
		pos += 12

		key := blockBuf[pos : pos+int64(kLen)]
		pos += int64(kLen)

		res := bytes.Compare(key, searchKeyBytes)
		if res == 0 {
			val := make([]byte, vLen)
			copy(val, blockBuf[pos:pos+int64(vLen)])
			return val, flags, isTombstone, nil
		}

		if res > 0 {
			break
		}

		pos += int64(vLen)
	}

	return nil, 0, false, nil
}

func (t *SSTable) readEntryAt(offset int64, searchKey string) ([]byte, uint32, bool, error) {
	header := make([]byte, 12)
	_, err := t.f.ReadAt(header, offset)
	if err != nil {
		return nil, 0, false, err
	}

	kLen := binary.BigEndian.Uint16(header[0:2])
	vLen := binary.BigEndian.Uint32(header[2:6])
	flags := binary.BigEndian.Uint32(header[6:10])
	isT := binary.BigEndian.Uint16(header[10:12]) == 1

	data := make([]byte, int(kLen)+int(vLen))
	_, err = t.f.ReadAt(data, offset+12)
	if err != nil {
		return nil, 0, false, err
	}

	key := string(data[:kLen])
	if key != searchKey {
		return nil, 0, false, nil
	}

	val := make([]byte, vLen)
	copy(val, data[kLen:])

	return val, flags, isT, nil
}

func (t *SSTable) hashString(s string) (uint64, error) {
	h := fnv.New64a()
	_, err := h.Write([]byte(s))
	if err != nil {
		return 0, err
	}

	return h.Sum64(), nil
}

func (t *SSTable) readBloomFilter() error {
	_, err := t.f.Seek(-16, io.SeekEnd)
	if err != nil {
		return err
	}

	err = binary.Read(t.f, binary.BigEndian, &t.bloomFilterStartOffset)
	if err != nil {
		return err
	}

	_, err = t.f.Seek(t.bloomFilterStartOffset, io.SeekStart)
	if err != nil {
		return err
	}

	var filterLen uint32
	err = binary.Read(t.f, binary.BigEndian, &filterLen)
	if err != nil {
		return err
	}

	filterBuf := make([]byte, filterLen)
	_, err = io.ReadFull(t.f, filterBuf)
	if err != nil {
		return err
	}
	t.filter = BloomFilter(filterBuf)

	return nil
}

func (t *SSTable) rebuildHashIndex() error {
	t.hashIndex = make(map[uint64]int64)

	var pos int64 = 0

	for pos < t.bloomFilterStartOffset {
		entryOffset := pos

		header := make([]byte, 12)
		_, err := t.f.ReadAt(header, pos)
		if err != nil {
			return err
		}

		kLen := binary.BigEndian.Uint16(header[0:2])
		vLen := binary.BigEndian.Uint32(header[2:6])
		keyBuf := make([]byte, kLen)
		_, err = t.f.ReadAt(keyBuf, pos+12)
		if err != nil {
			return err
		}

		h, err := t.hashString(string(keyBuf))
		if err != nil {
			return err
		}

		t.hashIndex[h] = entryOffset

		pos += 12 + int64(kLen) + int64(vLen)
	}

	return nil
}

func (t *SSTable) readIndex() error {
	info, err := t.f.Stat()
	if err != nil {
		return err
	}

	_, err = t.f.Seek(-8, io.SeekEnd)
	if err != nil {
		return err
	}

	err = binary.Read(t.f, binary.BigEndian, &t.indexStartOffset)
	if err != nil {
		return err
	}

	t.index = make([]IndexEntry, 0, (info.Size()-t.indexStartOffset)/30)

	_, err = t.f.Seek(t.indexStartOffset, io.SeekStart)
	if err != nil {
		return err
	}

	for {
		curr, _ := t.f.Seek(0, io.SeekCurrent)
		if curr >= info.Size()-16 {
			break
		}

		var kLen uint16
		err = binary.Read(t.f, binary.BigEndian, &kLen)
		if err != nil {
			return err
		}

		keyBuf := make([]byte, kLen)
		_, err = io.ReadFull(t.f, keyBuf)
		if err != nil {
			return err
		}

		var offset int64
		err = binary.Read(t.f, binary.BigEndian, &offset)
		if err != nil {
			return err
		}

		t.index = append(t.index, IndexEntry{Key: string(keyBuf), Offset: offset})
	}

	return nil
}

func (t *SSTable) Write(skipList *SkipList) error {
	var lastIndexEntryOffset int64 = 0

	t.index = make([]IndexEntry, 0, skipList.size/t.blockSize)
	t.filter = NewBloomFilter(int(skipList.size/64), 0.01)
	t.hashIndex = make(map[uint64]int64, skipList.size)

	curr := skipList.head.next[0]
	var offset int64
	for curr != nil {
		if offset == 0 || (offset-lastIndexEntryOffset) >= t.blockSize {
			t.index = append(t.index, IndexEntry{
				Key:    curr.key,
				Offset: offset,
			})

			lastIndexEntryOffset = offset
		}

		h, err := t.hashString(curr.key)
		if err != nil {
			return err
		}

		t.hashIndex[h] = offset

		size, err := t.writeEntry(curr.key, curr.value, curr.flags, curr.isTombstone)
		if err != nil {
			return err
		}

		offset += size

		t.filter.Add([]byte(curr.key))

		curr = curr.next[0]
	}

	t.bloomFilterStartOffset = offset
	err := t.writeBloomFilter()
	if err != nil {
		return err
	}

	filterSize := int64(4 + len([]byte(t.filter)))
	t.indexStartOffset = t.bloomFilterStartOffset + filterSize
	err = t.writeIndex()
	if err != nil {
		return err
	}

	err = t.writer.Flush()
	if err != nil {
		return err
	}

	return t.f.Sync()
}

func (t *SSTable) writeEntry(key string, value []byte, flags uint32, isTombstone bool) (int64, error) {
	var size int64

	err := binary.Write(t.writer, binary.BigEndian, uint16(len(key)))
	if err != nil {
		return 0, err
	}

	err = binary.Write(t.writer, binary.BigEndian, uint32(len(value)))
	if err != nil {
		return 0, err
	}

	err = binary.Write(t.writer, binary.BigEndian, flags)
	if err != nil {
		return 0, err
	}

	var isTombstoneVal uint16 = 0
	if isTombstone {
		isTombstoneVal = 1
	}
	err = binary.Write(t.writer, binary.BigEndian, isTombstoneVal)
	if err != nil {
		return 0, err
	}
	size += 12

	var keySize int
	keySize, err = t.writer.Write([]byte(key))
	if err != nil {
		return 0, err
	}
	size += int64(keySize)

	var valueSize int
	valueSize, err = t.writer.Write(value)
	if err != nil {
		return 0, err
	}
	size += int64(valueSize)

	return size, nil
}

func (t *SSTable) writeBloomFilter() error {
	filterData := []byte(t.filter)
	err := binary.Write(t.writer, binary.BigEndian, uint32(len(filterData)))
	if err != nil {
		return err
	}
	_, err = t.writer.Write(filterData)
	if err != nil {
		return err
	}

	return nil
}

func (t *SSTable) writeIndex() error {
	for i := range t.index {
		err := binary.Write(t.writer, binary.BigEndian, uint16(len(t.index[i].Key)))
		if err != nil {
			return err
		}

		_, err = t.writer.WriteString(t.index[i].Key)
		if err != nil {
			return err
		}

		err = binary.Write(t.writer, binary.BigEndian, t.index[i].Offset)
		if err != nil {
			return err
		}
	}

	err := binary.Write(t.writer, binary.BigEndian, t.bloomFilterStartOffset)
	if err != nil {
		return err
	}

	err = binary.Write(t.writer, binary.BigEndian, t.indexStartOffset)
	if err != nil {
		return err
	}

	return nil
}
