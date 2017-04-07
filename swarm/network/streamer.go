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

// stream manager
type Streamer struct {
	inlock   sync.Mutex
	outlock  sync.Mutex
	outgoing map[string]OutgoingStreamer
	incoming map[string]IncomingStreamer
}

type SubscribeMsgData struct {
	Stream   string //name of stream
	Offset   uint64 // offset to stream from
	Live     bool
	Priority uint8 // delivered on priority channel
	Rate     uint8 // step in the stream (in case it is multirate)
	MaxLen   uint8 // size of batch to be sent
}

// sent to inquire about a stream
type StreamRequestMsgData struct {
	Stream string
	Offset uint64
}

// sent as a response from a provider
type StreamResponseMsgData struct {
	Stream string
	Offset uint64
	Peers  []*peerAddr
}

func (self StreamRequestMsgData) String() string {
	return fmt.Sprintf("Query for stream '%v' from offset %v till %v", self.Stream, self.MinOffset, self.MaxOffset)
}

type UnsyncedKeysMsgData struct {
	Stream string // name of stream
	Batch  uint64 // id of the batch
	Hashes []byte // stream of hashes (128)
	Root   []byte // integrity check and request for receipt at Offset
	Offset uint64 // the offset after the last hash in Hashes
}

type WantedKeysMsgData struct {
	Stream  string // name of stream
	Batch   uint64 // id of the batch
	Want    []byte // bitvector indicating which keys of the batch needed
	Receipt []byte
}

type OutgoingStreamer interface {
	Params() *StreamResponseMsgData
	New(*StreamRequestMsgData, *StreamerPeer) (OutgoingPeerStreamer, error)
}

type IncomingStreamer interface {
	Params() *StreamRequestMsgData
	New(*StreamResponseMsgData, *StreamerPeer) (IncomingPeerStreamer, error)
	AddProvider(...Peer) error
}

// interface for outgoing peer Streamer
// handles WatntedKeysMsgData
type OutgoingPeerStreamer interface {
	Handle(*WantedKeysMsgData) error
}

// interface for incoming peer Streamer
// handles UnsyncedKeysMsgData
type IncomingPeerStreamer interface {
	Handle(*UnsyncedKeysMsgData) (*WantedKeysMsgData, error)
}

const (
	Top = iota
	High
	Mid
	Low
)

type StreamerPeer struct {
	queues [Top + 1]chan interface{}
	Peer
	outgoing map[string]OutgoingPeerStreamer
	incoming map[string]IncomingPeerStreamer
	quit     chan bool
}

func NewStreamerPeer() *StreamerPeer {
	var queues = make([Top + 1]chan interface{})
	for i, _ := range queues {
		queues[i] = make(chan interface{})
	}
	self := &StreamerPeer{
		queues:   queues,
		Peer:     p,
		outgoing: make(map[string]OutgoingPeerStreamer),
		incoming: make(map[string]IncomingPeerStreamer),
		quit:     make(chan bool),
	}

	go func() {
		var priority int = Top
		var q chan interface{}
		for {
			q = self.queues[priority]
			select {
			case <-self.quit:
				return
			case msg <- q:
				p.Send(msg)
				priority = Top
			default:
				priority--
				if priority < 0 {
					priority = Top
				}
				select {
					<-
				}
			}
		}
	}()

	return self
}

func (self *Streamer) GetStreamerPeer(p) *StreamerPeer {
	id := p.ID().String
	sp := &self.peers[id]
	if sp == nil {
		sp = NewStreamerPeer(p)
		self.peers[id] = sp
	}
	return sp
}

func (self *Streamer) GetOutgoingStreamer(req *StreamRequestMsgData, p Peer) (OutgoingStreamer, error) {
	sp := self.GetStreamerPeer()
	streamer := sp.outgoing[req.Stream]
	if steamer == nil {
		s := self.outgoing[req.Stream]
		if s == nil {
			return nil, fmt.Errorf("stream '%v' not provided", req.Stream)
		}
		var err error
		streamer, err = s.New(req, sp)
		if err != nil {
			return nil, err
		}
		sp.outgoing[req.Stream] = streamer
	}
	return streamer, nil
}

func (self *Streamer) GetInomingStreamer(req *StreamRequestMsgData, p Peer) (IncomingStreamer, error) {
	sp := self.GetStreamerPeer()
	streamer := sp.incoming[req.Stream]
	if streamer == nil {
		s := self.incoming[req.Stream]
		if s == nil {
			return nil, fmt.Errorf("stream '%v' not provided", req.Stream)
		}
		var err error
		streamer, err = s.New(req, sp)
		if err != nil {
			return nil, err
		}
		sp.incoming[req.Stream] = streamer
	}
	return streamer, nil
}

func (self *Streamer) handleSubscribeMsg(msg interface{}, p Peer) error {
	req := msg.(*SubscribeMsgData)
	_, err := self.GetOutgoingStreamer(req.Stream, p)
	return err
}

func (self *Streamer) handleUnsyncedKeysMsg(msg interface{}, p Peer) error {
	req := msg.(*UnsyncedKeysMsgData)
	want, err := self.GetIncomingStreamer(req.Stream, p).Handle(req)
	if err != nil {
		return err
	}
	return p.Send(&WantedKeysMsgData{
		Stream: req.Stream,
		Batch:  req.Batch,
		Want:   want,
	})
}

func (self *Streamer) handleWantedKeysMsg(msg interface{}, p Peer) error {
	req := msg.(*WantedKeysMsgData)
	want, err := self.GetOutgoingStreamer(req.Stream, p).Handle(req)
}

func (self *Streamer) Register(stream string, f func(Peer) *OutgoingStreamer) error {
	self.outgoingLock.Lock()
	defer self.outgoingLock.Unlock()
	f := self.outgoing[stream]
	if f != nil {
		return fmt.Errorf("stream %v already registered")
	}
	self.outgoing[stream] = f
	return nil
}

func (self *Streamer) StreamRequest(stream string, offset uint64, p Peer) bool {
	p.Send(&StreamRequestMsgData{
		Stream: stream,
		Offset: offset,
	})
}

func (self *Streamer) handleStreamRequestMsg(msg interface{}, p Peer) error {
	req := msg.(*StreamRequestMsgData)
	streamer := self.outgoing[req.Stream]
	if Streamer == nil {
		return nil
	}
	p.Send(Streamer.StreamResponseMsgData)
}

func (self *Streamer) handleStreamResponseMsg(msg interface{}, p Peer) error {
	req := msg.(*StreamResponseMsgData)
	streamer := self.incoming[req.Stream]
	if err != nil {
		return nil
	}
	streamer.AddProviders(p, req)
}

func (self *Streamer) Subscribe(req *SubscribeMsgData, p Peer) error {
	p.Send(req)
}
