// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package storage

import (
	"fmt"

	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
)

/*
NetStore is a cloud storage access abstaction layer for swarm
it contains the shared logic of network served chunk store/retrieval requests
both local (coming from DPA api) and remote (coming from peers via bzz protocol)
it implements the ChunkStore interface and embeds LocalStore

It is called by the bzz protocol instances via Depo (the store/retrieve request handler)
a protocol instance is running on each peer, so this is heavily parallelised.
NetStore falls back to a backend (CloudStorage interface)
implemented by bzz/network/forwarder. forwarder or IPFS or IPΞS
*/
type NetStore struct {
	localStore *LocalStore
	RetrieveC  chan *Chunk
	DeliverC   chan *Chunk
	StoreC     chan *Chunk
}

// netstore contructor, takes path argument that is used to initialise dbStore,
// the persistent (disk) storage component of LocalStore
func NewNetStore(lstore *LocalStore, params *StoreParams) *NetStore {
	return &NetStore{
		localStore: lstore,
		RetrieveC:  make(chan *Chunk, params.RequestBufferSize),
		DeliverC:   make(chan *Chunk, params.DeliveryBufferSize),
		StoreC:     make(chan *Chunk, params.StorageBufferSize),
	}
}

// store logic common to local and network chunk store requests
// ~ unsafe put in localdb no check if exists no extra copy no hash validation
// the chunk is forced to propagate (Cloud.Store) even if locally found!
// caller needs to make sure if that is wanted
func (self *NetStore) Put(chunk *Chunk) {
	// if chunk is constructed anew then localstore put should copy the
	// req state to the current chunk
	self.localStore.Put(chunk)

	// handle deliveries
	var err error
	if chunk.Req != nil {
		glog.V(logger.Detail).Infof("NetStore.Put: localStore.Put %v hit existing request...delivering", chunk.Key.Log())
		// closing C signals to other routines (local requests)
		// that the chunk is has been retrieved
		close(chunk.Req.C)
		// deliver the chunk to requesters upstream
		err = self.Deliver(chunk)
	} else {
		glog.V(logger.Detail).Infof("NetStore.Put: localStore.Put %v stored locally", chunk.Key.Log())
		// handle propagating store requests
		err = pushChunk(self.StoreC, chunk)
	}
	chunk.ErrC <- fmt.Errorf("netstore storage error: %v", err)
}

// retrieve logic common for local and network chunk retrieval requests
func (self *NetStore) Get(key Key) (*Chunk, error) {
	var err error
	chunk, err := self.localStore.Get(key)
	if err == nil {
		if chunk.Req == nil {
			glog.V(logger.Detail).Infof("NetStore.Get: %v found locally", key)
		} else {
			glog.V(logger.Detail).Infof("NetStore.Get: %v hit on an existing request", key)
			// no need to launch again
		}
		return chunk, err
	}
	// no data and no request status
	glog.V(logger.Detail).Infof("NetStore.Get: %v not found locally. open new request", key)
	chunk = NewChunk(key, NewRequestStatus(key))
	self.localStore.memStore.Put(chunk)
	err = pushChunk(self.RetrieveC, chunk)
	return chunk, fmt.Errorf("netstore retrieval error: %v", err)
}

// Close netstore
func (self *NetStore) Close() {
	return
}

func (self *NetStore) Deliver(chunk *Chunk) error {
	return pushChunk(self.DeliverC, chunk)
}

func pushChunk(c chan *Chunk, chunk *Chunk) error {
	select {
	case c <- chunk:
		return nil
	default:
	}
	return fmt.Errorf("buffer contention")
}
