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

package network

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/swarm/storage"
)

// implements an OutgoingStreamer for chunk request deliveries
// offered streams:
// * live request delivery with or without checkback
type RequestDeliverySyncer struct {
	// netStore.StoreC
	inC    chan *storage.Chunk
	params *StreamRequestMsgData
}

// netStore.DeliverC
func NewRequestDeliverySyncer(params *StreamRequestMsgData, inC chan *storage.Chunk) *RequestDeliverySyncer {
	return &RequestDeliverySyncer{
		params: params,
		inC:    inC,
	}
}

// implements an OutgoingStreamer for history syncing on bins
// offered streams:
// * live request delivery with or without checkback
// * (live/non-live historical) chunk syncing per proximity bin
type ProxBinSyncer struct {
	// netStore.StoreC
	inC chan *storage.Chunk
}

// netStore.DeliverC
func NewProxBinSyncer(params *StreamRequestMsgData, inC chan *storage.Chunk) *RequestDeliverySyncer {
	return &RequestDeliverySyncer{
		params: params,
		inC:    inC,
	}
}

type ProxBinPeerSyncer struct {
	*SubscribeMsgData
}

type SwarmSyncer struct {
	out    chan interface{}
	root   func() (storage.Key, *storage.LazyChunkReader)
	hashes []byte
	chunks []*Chunk
}

func NewSwarmSyncer(req *SubscribeMsgData, root func() (storage.Key, *storage.LazyChunkReader), out chan interface{}) *SwarmSyncer {
	return &SwarmSyncer{
		root:   root,
		out:    out,
		chunks: nil,
	}
}

func (self *SwarmSyncer) Handle(req *WantedKeysMsgData) error {
	for i, hash := range self.unsyncedKeys { //bits over req.want
		if Bit(req.Want, i) {
			go func() {
				// retrieve *locally*
				chunk, err := self.localStore.Get(hash)
				self.out <- storeRequestMsgData{
					Key:   chunk.Key,
					SData: chunk.SData,
				}
			}()

		}
	}
	if len(req.Receipt) > 0 {
		// validate receipt
	}
}

type LiveSyncer struct {
	root []byte
	cursors
	hashes []byte
	chunks []*Chunk
}

func NewLiveSyncer(req *SubscribeMsgData, root func() (storage.Key, *storage.LazyChunkReader), out chan interface{}) *SwarmSyncer {
	return &LiveSyncer{
		root:   root,
		out:    out,
		chunks: nil,
	}
}

func (self *LiveSyncer) Handle(req *WantedKeysMsgData) error {
	for i, hash := range self.unsyncedKeys { //bits over req.want
		if Bit(req.Want, i) {
			go func() {
				// retrieve *locally*
				// if
				// if not found send receipt instead
				self.out <- storeRequestMsgData{
					Key:   chunk.Key,
					SData: chunk.SData,
				}
			}()

		}
	}
	if len(req.Receipt) > 0 {
		// validate receipt
	}
}
func (self *ProxBinSyncer) New(req *SubscribeMsgData, p Peer) (OutgoingPeerStreamer, error) {
	queue := p.Queues[req.Priority]
	return OutgoingPeerStreamer(NewSwarmSyncer(req, self.GetCurrentHead, queue))
}

func (self *ProxBinPeerSyncer) Handle(req *WantedKeysMsgData) error {

}
