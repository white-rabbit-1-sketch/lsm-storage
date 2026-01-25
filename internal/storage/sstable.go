package storage

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"sort"
)

type SSTable struct {
	f                *os.File
	writer           *bufio.Writer
	index            []IndexEntry
	indexStartOffset int64
	blockSize        int64
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
	if len(t.index) == 0 {
		return nil, 0, false, nil
	}

	i := sort.Search(len(t.index), func(i int) bool {
		return t.index[i].Key > searchKey
	})

	if i == 0 && t.index[0].Key > searchKey {
		return nil, 0, false, nil
	}

	targetIndexEntryPosition := 0
	if i > 0 {
		targetIndexEntryPosition = i - 1
	}

	currOffset := t.index[targetIndexEntryPosition].Offset
	for {
		if currOffset >= t.indexStartOffset {
			break
		}

		var header [12]byte
		_, err := t.f.ReadAt(header[:], currOffset)
		if err != nil {
			return nil, 0, false, err
		}

		kLen := binary.BigEndian.Uint16(header[0:2])
		vLen := binary.BigEndian.Uint32(header[2:6])
		flags := binary.BigEndian.Uint32(header[6:10])
		isTombstoneVal := binary.BigEndian.Uint16(header[10:12])
		isTombstone := false
		if isTombstoneVal == 1 {
			isTombstone = true
		}

		currOffset += 12

		keyBuf := make([]byte, kLen)
		_, err = t.f.ReadAt(keyBuf, currOffset)
		if err != nil {
			return nil, 0, false, err
		}
		currOffset += int64(kLen)

		currentKey := string(keyBuf)
		if currentKey == searchKey {
			valBuf := make([]byte, vLen)
			_, err = t.f.ReadAt(valBuf, currOffset)
			if err != nil {
				return nil, 0, false, err
			}

			return valBuf, flags, isTombstone, nil
		}

		if currentKey > searchKey {
			break
		}

		currOffset += int64(vLen)
	}

	return nil, 0, false, nil
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
		if curr >= info.Size()-8 {
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

		curr = curr.next[0]
	}

	t.indexStartOffset = offset

	err := t.writeIndex()
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

	err := binary.Write(t.writer, binary.BigEndian, t.indexStartOffset)
	if err != nil {
		return err
	}

	return nil
}
