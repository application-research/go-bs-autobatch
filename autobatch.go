package autobatch

import (
	"context"
	"strings"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log"
	"golang.org/x/xerrors"
)

var log = logging.Logger("bs-wal")

type Blockstore struct {
	child blockstore.Blockstore

	writeLog blockstore.Blockstore
	buffer   map[cid.Cid]struct{}
	lk       sync.Mutex

	// number of items we buffer in the writeAheadLog before we flush them to
	// the child blockstore
	bufferLimit int

	// threshold for number of items in a batch put call that we will skip
	// putting in the write ahead log, and write directly and sychronously to
	// the child blockstore
	putManySyncThreshold int
}

func NewBlockstore(child, writelog blockstore.Blockstore, bufferlimit, putmanythreshold int, norecover bool) (*Blockstore, error) {
	bs := &Blockstore{
		child:    child,
		writeLog: writelog,
		buffer:   make(map[cid.Cid]struct{}),

		bufferLimit:          bufferlimit,
		putManySyncThreshold: putmanythreshold,
	}

	if !norecover {
		if err := bs.recoverWriteLog(); err != nil {
			return nil, err
		}
	}

	return bs, nil
}

func (bs *Blockstore) recoverWriteLog() error {
	ch, err := bs.writeLog.AllKeysChan(context.TODO())
	if err != nil {
		return err
	}

	go func() {
		var count int
		for c := range ch {
			bs.addToBuffer(c)
			count++
		}
		log.Infof("recovered %d entries from the write log", count)
	}()

	return nil
}

func (bs *Blockstore) Flush(ctx context.Context) error {
	ch, err := bs.writeLog.AllKeysChan(ctx)
	if err != nil {
		return err
	}

	var count int
	for c := range ch {
		count++
		blk, err := bs.writeLog.Get(ctx, c)
		if err != nil {
			log.Errorf("failed to get expected block from write log: %s", err)
			continue
		}

		if err := bs.child.Put(ctx, blk); err != nil {
			log.Errorf("failed to write block to child datastore: %s", err)
			continue
		}

		if err := bs.writeLog.DeleteBlock(ctx, blk.Cid()); err != nil {
			log.Errorf("failed to delete block: %s", err)
			continue
		}
		log.Infof("flush progress: %d", count)

		if count%100 == 99 {
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
	}
	log.Infof("flushed %d entries from the write log", count)

	if gcer, ok := bs.writeLog.(bstoreGCer); ok {
		log.Debugf("running gc on write log")
		if err := gcer.CollectGarbage(); err != nil {
			// hacky way to avoid a dependency on badger for a specific error check so i can have less noisy logs
			if !strings.Contains(err.Error(), "request rejected") {
				log.Warningf("failed to run garbage collection on write log: %s", err)
			}
		}
	}

	return nil

}

func (bs *Blockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	if err := bs.writeLogClear(ctx, c); err != nil {
		return err
	}

	return bs.child.DeleteBlock(ctx, c)
}

type batchDeleter interface {
	DeleteMany(context.Context, []cid.Cid) error
}

func (bs *Blockstore) DeleteMany(ctx context.Context, cids []cid.Cid) error {
	for _, c := range cids {
		if err := bs.writeLogClear(ctx, c); err != nil {
			return err
		}
	}

	if dm, ok := bs.child.(batchDeleter); ok {
		return dm.DeleteMany(ctx, cids)
	}

	for _, c := range cids {
		if err := bs.child.DeleteBlock(ctx, c); err != nil {
			return err
		}
	}

	return nil
}

func (bs *Blockstore) writeLogClear(ctx context.Context, c cid.Cid) error {
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

	if err := bs.writeLog.DeleteBlock(ctx, c); err != nil {
		return err
	}

	delete(bs.buffer, c)

	return nil
}

func (bs *Blockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	bs.lk.Lock()
	_, ok := bs.buffer[c]
	bs.lk.Unlock()
	if ok {
		return true, nil
	}

	has, err := bs.child.Has(ctx, c)
	if err != nil {
		return false, err
	}

	if !has {
		return bs.writeLog.Has(ctx, c)
	}
	return has, nil
}

func (bs *Blockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	bs.lk.Lock()
	_, ok := bs.buffer[c]
	bs.lk.Unlock()
	if ok {
		// TODO: technically a race condition here...
		return bs.writeLog.Get(ctx, c)
	}

	blk, err := bs.child.Get(ctx, c)
	switch {
	case err == nil:
		return blk, nil
	default:
		return nil, err
	case xerrors.Is(err, ipld.ErrNotFound{Cid: c}):
		// This is a weird edgecase that really should be fixed some other way
		return bs.writeLog.Get(ctx, c)
	}
}

// GetSize returns the CIDs mapped BlockSize
func (bs *Blockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	bs.lk.Lock()
	_, ok := bs.buffer[c]
	bs.lk.Unlock()
	if ok {
		// TODO: technically a race condition here too
		return bs.writeLog.GetSize(ctx, c)
	}

	s, err := bs.child.GetSize(ctx, c)
	switch {
	case err == nil:
		return s, nil
	default:
		return 0, err
	case xerrors.Is(err, ipld.ErrNotFound{Cid: c}):
		// This is a weird edgecase that really should be fixed some other way
		return bs.writeLog.GetSize(ctx, c)
	}
}

// Put puts a given block to the underlying datastore
func (bs *Blockstore) Put(ctx context.Context, blk blocks.Block) error {
	if err := bs.writeLog.Put(ctx, blk); err != nil {
		return err
	}
	bs.addToBuffer(blk.Cid())

	return nil
}

func (bs *Blockstore) addToBuffer(c cid.Cid) {
	bs.lk.Lock()
	defer bs.lk.Unlock()

	bs.buffer[c] = struct{}{}

	if len(bs.buffer) >= bs.bufferLimit {
		go bs.flushWriteLog(context.Background())
	}
}

func (bs *Blockstore) flushWriteLog(ctx context.Context) {
	bs.lk.Lock()
	buf := bs.buffer
	bs.buffer = make(map[cid.Cid]struct{})
	bs.lk.Unlock()

	log.Debugf("flushing write log (%d blocks)", len(buf))

	blks := make([]blocks.Block, 0, len(buf))
	for c := range buf {
		blk, err := bs.writeLog.Get(ctx, c)
		if err != nil {
			log.Errorf("failed to get expected block from write log: %s", err)
			continue
		}
		blks = append(blks, blk)
	}

	if err := bs.child.PutMany(ctx, blks); err != nil {
		// very annoying case to handle, for now, just put buffered entries
		// back in the main buffer.  this will trigger another flush attempt on
		// the next write, and that might have bad consequences, but not yet
		// sure what else to do

		bs.lk.Lock()
		for _, blk := range blks {
			bs.buffer[blk.Cid()] = struct{}{}
		}
		bs.lk.Unlock()
		log.Errorf("failed to flush write log (%d entries) to underlying datastore: %s", len(buf), err)
		return
	}

	// now clear properly persisted entries out of the write ahead log
	for _, blk := range blks {
		if err := bs.writeLog.DeleteBlock(ctx, blk.Cid()); err != nil {
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
func (bs *Blockstore) PutMany(ctx context.Context, blks []blocks.Block) error {
	if len(blks) > bs.putManySyncThreshold {
		return bs.child.PutMany(ctx, blks)
	}

	if err := bs.writeLog.PutMany(ctx, blks); err != nil {
		return err
	}

	for _, blk := range blks {
		bs.addToBuffer(blk.Cid())
	}
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

func (bs *Blockstore) View(ctx context.Context, c cid.Cid, f func([]byte) error) error {
	bs.lk.Lock()
	_, ok := bs.buffer[c]
	bs.lk.Unlock()
	if ok {
		return maybeView(ctx, bs.writeLog, c, f)
	}

	err := maybeView(ctx, bs.child, c, f)
	if err == nil {
		return nil
	}
	if !xerrors.Is(err, ipld.ErrNotFound{Cid: c}) {
		return err
	}

	return maybeView(ctx, bs.writeLog, c, f)
}

func maybeView(ctx context.Context, bs blockstore.Blockstore, c cid.Cid, f func([]byte) error) error {
	if cview, ok := bs.(blockstore.Viewer); ok {
		return cview.View(ctx, c, f)
	}

	blk, err := bs.Get(ctx, c)
	if err != nil {
		return err
	}

	return f(blk.RawData())
}
