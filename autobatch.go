package autobatch

import (
	"context"
	"fmt"
	"strings"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	logging "github.com/ipfs/go-log"
)

var log = logging.Logger("bs-wal")

type Blockstore struct {
	child blockstore.Blockstore

	writeLog blockstore.Blockstore
	buffer   map[cid.Cid]blocks.Block
	lk       sync.Mutex

	// number of items we buffer in the writeAheadLog before we flush them to
	// the child blockstore
	bufferLimit int

	// threshold for number of items in a batch put call that we will skip
	// putting in the write ahead log, and write directly and sychronously to
	// the child blockstore
	putManySyncThreshold int
}

func NewBlockstore(child, writelog blockstore.Blockstore, bufferlimit int) (blockstore.Blockstore, error) {
	bs := &Blockstore{
		child:    child,
		writeLog: writelog,
		buffer:   make(map[cid.Cid]blocks.Block),

		bufferLimit:          bufferlimit,
		putManySyncThreshold: 20,
	}

	if err := bs.recoverWriteLog(); err != nil {
		return nil, err
	}

	return bs, nil
}

func (bs *Blockstore) recoverWriteLog() error {
	ch, err := bs.writeLog.AllKeysChan(context.TODO())
	if err != nil {
		return err
	}

	for c := range ch {
		blk, err := bs.writeLog.Get(c)
		if err != nil {
			return fmt.Errorf("write log appears to be corrupt, read of advertised content failed: %s", err)
		}

		bs.buffer[c] = blk
	}
	log.Infof("recovered %d entries from the write log", len(bs.buffer))

	return nil
}

func (bs *Blockstore) DeleteBlock(c cid.Cid) error {
	if err := bs.writeLogClear(c); err != nil {
		return err
	}

	return bs.child.DeleteBlock(c)
}

func (bs *Blockstore) writeLogClear(c cid.Cid) error {
	bs.lk.Lock()
	_, ok := bs.buffer[c]
	bs.lk.Unlock()

	if !ok {
		return nil
	}

	bs.lk.Lock()
	defer bs.lk.Unlock()
	_, ok = bs.buffer[c]
	if !ok {
		return nil
	}

	if err := bs.writeLog.DeleteBlock(c); err != nil {
		return err
	}

	delete(bs.buffer, c)

	return nil
}

func (bs *Blockstore) Has(c cid.Cid) (bool, error) {
	bs.lk.Lock()
	_, ok := bs.buffer[c]
	bs.lk.Unlock()
	if ok {
		return true, nil
	}

	return bs.child.Has(c)
}

func (bs *Blockstore) Get(c cid.Cid) (blocks.Block, error) {
	bs.lk.Lock()
	blk, ok := bs.buffer[c]
	bs.lk.Unlock()
	if ok {
		return blk, nil
	}

	return bs.child.Get(c)
}

// GetSize returns the CIDs mapped BlockSize
func (bs *Blockstore) GetSize(c cid.Cid) (int, error) {
	bs.lk.Lock()
	blk, ok := bs.buffer[c]
	bs.lk.Unlock()
	if ok {
		return len(blk.RawData()), nil
	}

	return bs.child.GetSize(c)
}

// Put puts a given block to the underlying datastore
func (bs *Blockstore) Put(blk blocks.Block) error {
	if err := bs.writeLog.Put(blk); err != nil {
		return err
	}
	bs.addToBuffer([]blocks.Block{blk})

	return nil
}

func (bs *Blockstore) addToBuffer(blks []blocks.Block) {
	bs.lk.Lock()
	defer bs.lk.Unlock()

	for _, blk := range blks {
		bs.buffer[blk.Cid()] = blk
	}
	if len(bs.buffer) >= bs.bufferLimit {
		go bs.flushWriteLog()
	}
}

func (bs *Blockstore) flushWriteLog() {
	bs.lk.Lock()
	buf := bs.buffer
	bs.buffer = make(map[cid.Cid]blocks.Block)
	bs.lk.Unlock()

	log.Debugf("flushing write log (%d blocks)", len(buf))

	blks := make([]blocks.Block, 0, len(buf))
	for _, blk := range buf {
		blks = append(blks, blk)
	}

	if err := bs.child.PutMany(blks); err != nil {
		// very annoying case to handle, for now, just put buffered entries
		// back in the main buffer.  this will trigger another flush attempt on
		// the next write, and that might have bad consequences, but not yet
		// sure what else to do

		bs.lk.Lock()
		for _, blk := range blks {
			bs.buffer[blk.Cid()] = blk
		}
		bs.lk.Unlock()
		log.Errorf("failed to flush write log (%d entries) to underlying datastore: %s", len(buf), err)
		return
	}

	// now clear properly persisted entries out of the write ahead log
	for _, blk := range blks {
		if err := bs.writeLog.DeleteBlock(blk.Cid()); err != nil {
			log.Errorf("failed to delete block %s from write ahead log: %s", blk.Cid(), err)
		}
	}

	if gcer, ok := bs.writeLog.(bstoreGCer); ok {
		log.Debugf("running gc on write log")
		if err := gcer.CollectGarbage(); err != nil {
			// hacky way to avoid a dependency on badger for a specific error check so i can have less noisy logs
			if !strings.Contains(err.Error(), "request rejected") {
				log.Warningf("failed to run garbage collection on write log: %s", err)
			}
		}
	}
}

type bstoreGCer interface {
	CollectGarbage() error
}

// PutMany puts a slice of blocks at the same time using batching
// capabilities of the underlying datastore whenever possible.
func (bs *Blockstore) PutMany(blks []blocks.Block) error {
	if len(blks) > bs.putManySyncThreshold {
		return bs.child.PutMany(blks)
	}

	if err := bs.writeLog.PutMany(blks); err != nil {
		return err
	}

	bs.addToBuffer(blks)
	return nil
}

// AllKeysChan returns a channel from which
// the CIDs in the Blockstore can be read. It should respect
// the given context, closing the channel if it becomes Done.
func (bs *Blockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	childch, err := bs.child.AllKeysChan(ctx)
	if err != nil {
		return nil, err
	}

	bufcopy := make(map[cid.Cid]struct{})
	bs.lk.Lock()
	for c := range bs.buffer {
		bufcopy[c] = struct{}{}
	}
	bs.lk.Unlock()

	out := make(chan cid.Cid, 32)
	go func() {
		defer close(out)

		for c := range bufcopy {
			select {
			case out <- c:
			case <-ctx.Done():
				return
			}
		}

		for c := range childch {
			select {
			case out <- c:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, nil
}

// HashOnRead specifies if every read block should be
// rehashed to make sure it matches its CID.
func (bs *Blockstore) HashOnRead(enabled bool) {
	bs.child.HashOnRead(enabled)
}

func (bs *Blockstore) View(c cid.Cid, f func([]byte) error) error {
	bs.lk.Lock()
	blk, ok := bs.buffer[c]
	bs.lk.Unlock()
	if ok {
		return f(blk.RawData())
	}

	if cview, ok := bs.child.(blockstore.Viewer); ok {
		return cview.View(c, f)
	}

	blk, err := bs.child.Get(c)
	if err != nil {
		return err
	}

	return f(blk.RawData())
}
