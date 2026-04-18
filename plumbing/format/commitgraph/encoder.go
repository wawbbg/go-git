package commitgraph

import (
	"crypto/sha1"
	"encoding/binary"
	"io"

	"github.com/go-git/go-git/v5/plumbing"
)

const (
	signature        = "CGPH"
	version          = 1
	chunkOIDFanout   = 0x4f494446 // OIDF
	chunkOIDLookup   = 0x4f49444c // OIDL
	chunkCommitData  = 0x43444154 // CDAT
	chunkExtraEdges  = 0x45444745 // EDGE
	chunkTableOfContentsTerminator = 0x0
)

// Encoder writes commit-graph files.
type Encoder struct {
	w   io.Writer
	hash hash.Hash
}

// NewEncoder returns a new Encoder that writes to w.
func NewEncoder(w io.Writer) *Encoder {
	h := sha1.New()
	return &Encoder{
		w:    io.MultiWriter(w, h),
		hash: h,
	}
}

// Encode serializes a MemoryIndex to the commit-graph binary format.
func (e *Encoder) Encode(idx *MemoryIndex) error {
	hashes := idx.Hashes()
	n := len(hashes)

	// Compute chunk sizes
	oidFanoutSize := 256 * 4
	oidLookupSize := n * 20
	commitDataSize := n * 36

	// Header: 4-byte signature + 1-byte version + 1-byte hash version + 1-byte num chunks + 1-byte reserved
	if _, err := io.WriteString(e.w, signature); err != nil {
		return err
	}
	if err := e.writeByte(version); err != nil {
		return err
	}
	if err := e.writeByte(1); err != nil { // SHA1
		return err
	}
	numChunks := byte(3)
	if err := e.writeByte(numChunks); err != nil {
		return err
	}
	if err := e.writeByte(0); err != nil { // reserved
		return err
	}

	// Table of contents: each entry is 12 bytes (4-byte chunk ID + 8-byte offset)
	baseOffset := int64(8 + int(numChunks+1)*12)
	offsets := []int64{
		baseOffset,
		baseOffset + int64(oidFanoutSize),
		baseOffset + int64(oidFanoutSize) + int64(oidLookupSize),
		baseOffset + int64(oidFanoutSize) + int64(oidLookupSize) + int64(commitDataSize),
	}
	chunks := []uint32{chunkOIDFanout, chunkOIDLookup, chunkCommitData}
	for i, id := range chunks {
		if err := e.writeUint32(id); err != nil {
			return err
		}
		if err := e.writeUint64(uint64(offsets[i])); err != nil {
			return err
		}
	}
	// Terminator
	if err := e.writeUint32(chunkTableOfContentsTerminator); err != nil {
		return err
	}
	if err := e.writeUint64(uint64(offsets[len(offsets)-1])); err != nil {
		return err
	}

	// OID Fanout chunk
	fanout := [256]uint32{}
	for _, h := range hashes {
		fanout[h[0]]++
	}
	for i := 1; i < 256; i++ {
		fanout[i] += fanout[i-1]
	}
	for _, f := range fanout {
		if err := e.writeUint32(f); err != nil {
			return err
		}
	}

	// OID Lookup chunk
	for _, h := range hashes {
		if _, err := e.w.Write(h[:]); err != nil {
			return err
		}
	}

	// Commit Data chunk
	for _, h := range hashes {
		node, err := idx.GetIndexByHash(h)
		if err != nil {
			return err
		}
		commit, err := idx.GetCommitDataByIndex(node)
		if err != nil {
			return err
		}
		if _, err := e.w.Write(commit.TreeHash[:]); err != nil {
			return err
		}
		p1 := uint32(0x70000000) // no parent
		if len(commit.ParentIndexes) > 0 {
			p1 = uint32(commit.ParentIndexes[0])
		}
		if err := e.writeUint32(p1); err != nil {
			return err
		}
		p2 := uint32(0x70000000)
		if len(commit.ParentIndexes) > 1 {
			p2 = uint32(commit.ParentIndexes[1])
		}
		if err := e.writeUint32(p2); err != nil {
			return err
		}
		generation := uint64(commit.Generation)<<34 | uint64(commit.When.Unix())
		if err := e.writeUint64(generation); err != nil {
			return err
		}
	}

	// Write checksum
	_, err := e.w.(io.Writer).Write(e.hash.Sum(nil))
	return err
}

func (e *Encoder) writeByte(b byte) error {
	_, err := e.w.Write([]byte{b})
	return err
}

func (e *Encoder) writeUint32(v uint32) error {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, v)
	_, err := e.w.Write(buf)
	return err
}

func (e *Encoder) writeUint64(v uint64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	_, err := e.w.Write(buf)
	return err
}

// Ensure plumbing is imported (used for type references in other files)
var _ = plumbing.ZeroHash
