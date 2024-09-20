package log

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())

	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED); err != nil {
		return nil, err
	}

	return idx, nil
}

func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, fmt.Errorf("index is empty")
	}
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
		pos = uint64(binary.BigEndian.Uint64(i.mmap[i.size-posWidth:]))
		return out, pos, nil
	}
	if uint64(in+1)*entWidth > i.size {
		return 0, 0, fmt.Errorf("index out of bounds")
	}
	out = binary.BigEndian.Uint32(i.mmap[uint64(in*entWidth) : uint64(in*entWidth)+offWidth])
	pos = binary.BigEndian.Uint64(i.mmap[uint64(in*entWidth)+offWidth : uint64(in*entWidth)+entWidth])
	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	if i.size+entWidth > uint64(len(i.mmap)) {
		return fmt.Errorf("index is full")
	}
	binary.BigEndian.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	binary.BigEndian.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += entWidth
	return nil
}

func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}
