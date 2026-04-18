package commitgraph

import (
	"encoding/binary"
	"errors"
	"io"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
)

const (
	Signature  = "CGPH"
	Version    = 1
	HashLength = 20
)

var (
	ErrMalformedCommitGraph = errors.New("malformed commit-graph file")
	ErrUnsupportedVersion   = errors.New("unsupported commit-graph version")
)

// CommitData holds parsed data for a single commit node.
type CommitData struct {
	Hash           plumbing.Hash
	ParentHashes   []plumbing.Hash
	TreeHash       plumbing.Hash
	Generation     uint32
	When           time.Time
}

// MemoryIndex holds commit graph data in memory.
type MemoryIndex struct {
	commits map[plumbing.Hash]*CommitData
}

// NewMemoryIndex creates an empty in-memory commit graph index.
func NewMemoryIndex() *MemoryIndex {
	return &MemoryIndex{
		commits: make(map[plumbing.Hash]*CommitData),
	}
}

// Add inserts a CommitData entry into the index.
func (m *MemoryIndex) Add(c *CommitData) {
	m.commits[c.Hash] = c
}

// GetCommitDataByHash returns commit data for the given hash.
func (m *MemoryIndex) GetCommitDataByHash(h plumbing.Hash) (*CommitData, error) {
	c, ok := m.commits[h]
	if !ok {
		return nil, plumbing.ErrObjectNotFound
	}
	return c, nil
}

// Hashes returns all hashes stored in the index.
func (m *MemoryIndex) Hashes() []plumbing.Hash {
	hashes := make([]plumbing.Hash, 0, len(m.commits))
	for h := range m.commits {
		hashes = append(hashes, h)
	}
	return hashes
}

// readUint32 is a helper to read a big-endian uint32 from a reader.
func readUint32(r io.Reader) (uint32, error) {
	buf := make([]byte, 4)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(buf), nil
}
