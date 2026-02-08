package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
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
}

type IndexEntry struct {
	Key    string
	Offset int64
}

func CreateSSTable(path string, blockSize int64, skipList *SkipList) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
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
			return err
		}

		return err
	}

	err = table.Close()
	if err != nil {
		return err
	}

	return nil
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

	return t, nil
}

func (t *SSTable) Close() error {
	return t.f.Close()
}

func (t *SSTable) Get(searchKey string) ([]byte, uint32, bool, error) {
	if !t.filter.Contains([]byte(searchKey)) {
		return nil, 0, false, nil
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
	_, err := t.f.ReadAt(blockBuf, startOffset)
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
