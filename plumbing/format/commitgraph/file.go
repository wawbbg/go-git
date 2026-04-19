package commitgraph

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/go-git/go-git/v5/plumbing"
)

const (
	commitGraphMagic      = "CGPH"
	commitGraphVersion    = 1
	chunkOIDFanout        = 0x4f494446 // "OIDF"
	chunkOIDLookup        = 0x4f49444c // "OIDL"
	chunkCommitData       = 0x43444154 // "CDAT"
	chunkExtraEdgeList    = 0x45444745 // "EDGE"
	commitGraphHeaderSize = 8
	chunkTableEntrySize   = 12
)

// fileIndex is a commit-graph index backed by a binary file.
type fileIndex struct {
	reader        io.ReaderAt
	hasher        plumbing.Hash
	oidFanoutOff  int64
	oidLookupOff  int64
	commitDataOff int64
	extraEdgeOff  int64
	count         uint32
}

// OpenFileIndex opens a commit-graph file and returns an Index backed by it.
func OpenFileIndex(r io.ReaderAt) (Index, error) {
	f := &fileIndex{reader: r}
	if err := f.readHeader(); err != nil {
		return nil, err
	}
	return f, nil
}

func (f *fileIndex) readHeader() error {
	header := make([]byte, commitGraphHeaderSize)
	if _, err := f.reader.ReadAt(header, 0); err != nil {
		return fmt.Errorf("reading commit-graph header: %w", err)
	}
	if string(header[:4]) != commitGraphMagic {
		return fmt.Errorf("invalid commit-graph magic: %x", header[:4])
	}
	if header[4] != commitGraphVersion {
		return fmt.Errorf("unsupported commit-graph version: %d", header[4])
	}
	numChunks := int(header[6])
	return f.readChunkTable(numChunks)
}

func (f *fileIndex) readChunkTable(numChunks int) error {
	table := make([]byte, (numChunks+1)*chunkTableEntrySize)
	if _, err := f.reader.ReadAt(table, commitGraphHeaderSize); err != nil {
		return fmt.Errorf("reading chunk table: %w", err)
	}
	r := bufio.NewReader(bytes.NewReader(table))
	for i := 0; i < numChunks; i++ {
		id := make([]byte, 4)
		if _, err := io.ReadFull(r, id); err != nil {
			return err
		}
		offBuf := make([]byte, 8)
		if _, err := io.ReadFull(r, offBuf); err != nil {
			return err
		}
		off := int64(binary.BigEndian.Uint64(offBuf))
		chunkID := binary.BigEndian.Uint32(id)
		switch chunkID {
		case chunkOIDFanout:
			f.oidFanoutOff = off
		case chunkOIDLookup:
			f.oidLookupOff = off
		case chunkCommitData:
			f.commitDataOff = off
		case chunkExtraEdgeList:
			f.extraEdgeOff = off
		}
	}
	if f.oidFanoutOff == 0 || f.oidLookupOff == 0 || f.commitDataOff == 0 {
		return fmt.Errorf("commit-graph missing required chunks")
	}
	// Total count is the last entry of the fanout table (index 255).
	// The fanout table has 256 entries, each 4 bytes wide.
	// Note: fanout[255] gives the total number of commits in the graph.
	countBuf := make([]byte, 4)
	if _, err := f.reader.ReadAt(countBuf, f.oidFanoutOff+255*4); err != nil {
		return fmt.Errorf("reading fanout count: %w", err)
	}
	f.count = binary.BigEndian.Uint32(countBuf)
	return nil
}

// GetIndexByHash returns the position of the given hash in the OID lookup table
// using binary search. Returns an error if the hash is not found.
func (f *fileIndex) GetIndexByHash(h plumbing.Hash) (uint32, error) {
	// Use the fanout table to narrow the search range to hashes sharing the
	// same first byte, reducing the binary search space significantly.
	var lo, hi uint32 = 0, f.count

	// Narrow range using fanout table.
	if h[0] > 0 {
		fanoutBuf := make([]byte, 4)
		if _, err := f.reader.ReadAt(fanoutBuf, f.oidFanoutOff+int64(h[0]-1)*4); err != nil {
			return 0, fmt.Errorf("reading fanout entry: %w", err)
		}
		lo = binary.BigEndian.Uint32(fanoutBuf)
	}
	{
		fanoutBuf := make([]byte, 4)
		if _, err := f.reader.ReadAt(fanoutBuf, f.oidFanoutOff+int64(h[0])*4); err != nil {
			return 0, fmt.Errorf("reading fanout entry: %w", err)
		}
		hi = binary.BigEndian.Uint32(fanoutBuf)
	}

	for lo < hi {
		mid := (lo + hi) / 2
		var entry plumbing.Hash
		if _, err := f.reader.ReadAt(entry[:], f.oidLookupOff+int64(mid)*20); err != nil {
			return 0, fmt.Errorf("reading OID entry: %w", err)
		}
		cmp := bytes.Compare(entry[:], h[:])
		if cmp == 0 {
			return mid, nil
		} else if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return 0, plumbing.ErrObjectNotFound
}
