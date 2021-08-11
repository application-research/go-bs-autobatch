package autobatch

import (
	"math/rand"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func randBlock() blocks.Block {
	data := make([]byte, 2048)
	rand.Read(data)
	return blocks.NewBlock(data)
}

func TestBasicAutobatching(t *testing.T) {
	cold := blockstore.NewBlockstore(datastore.NewMapDatastore())
	wal := blockstore.NewBlockstore(datastore.NewMapDatastore())

	ab, err := NewBlockstore(cold, wal, 100, 100, false)
	if err != nil {
		t.Fatal(err)
	}

	blk1 := randBlock()
	if err := ab.PutMany([]blocks.Block{blk1}); err != nil {
		t.Fatal(err)
	}

	has, err := ab.Has(blk1.Cid())
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Fatal("should have")
	}

	_, err = ab.Get(blk1.Cid())
	if err != nil {
		t.Fatal(err)
	}
}
