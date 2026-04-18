package commitgraph

import (
	"testing"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
)

func TestMemoryIndex_AddAndGet(t *testing.T) {
	idx := NewMemoryIndex()

	h := plumbing.NewHash("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	parent := plumbing.NewHash("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	tree := plumbing.NewHash("cccccccccccccccccccccccccccccccccccccccc")

	cd := &CommitData{
		Hash:         h,
		ParentHashes: []plumbing.Hash{parent},
		TreeHash:     tree,
		Generation:   1,
		When:         time.Unix(1609459200, 0),
	}

	idx.Add(cd)

	got, err := idx.GetCommitDataByHash(h)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Hash != h {
		t.Errorf("expected hash %v, got %v", h, got.Hash)
	}
	if len(got.ParentHashes) != 1 || got.ParentHashes[0] != parent {
		t.Errorf("unexpected parent hashes: %v", got.ParentHashes)
	}
	if got.Generation != 1 {
		t.Errorf("expected generation 1, got %d", got.Generation)
	}
}

func TestMemoryIndex_NotFound(t *testing.T) {
	idx := NewMemoryIndex()
	h := plumbing.NewHash("dddddddddddddddddddddddddddddddddddddddd")

	_, err := idx.GetCommitDataByHash(h)
	if err == nil {
		t.Fatal("expected error for missing hash, got nil")
	}
}

func TestMemoryIndex_Hashes(t *testing.T) {
	idx := NewMemoryIndex()
	h1 := plumbing.NewHash("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	h2 := plumbing.NewHash("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

	idx.Add(&CommitData{Hash: h1})
	idx.Add(&CommitData{Hash: h2})

	hashes := idx.Hashes()
	if len(hashes) != 2 {
		t.Errorf("expected 2 hashes, got %d", len(hashes))
	}
}
