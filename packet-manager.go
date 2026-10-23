package sftp

import (
	"encoding"
	"sort"
	"sync"
)

type (
	packetManager struct {
		sender    packetSender
		requests  chan orderedPacket
		responses chan orderedPacket
		fini      chan struct{}
		working   *sync.WaitGroup

		alloc       *allocator
		incoming    orderedPackets
		outgoing    orderedPackets
		packetCount uint32
	}

	packetSender interface {
		sendPacket(encoding.BinaryMarshaler) error
	}

	orderedRequest struct {
		requestPacket
		orderid uint32
	}

	orderedResponse struct {
		responsePacket
		orderid uint32
	}

	orderedPacket interface {
		id() uint32
		orderID() uint32
	}

	orderedPackets []orderedPacket
)

func newPktMgr(sender packetSender) *packetManager {
	s := &packetManager{
		requests:  make(chan orderedPacket, SftpServerWorkerCount),
		responses: make(chan orderedPacket, SftpServerWorkerCount),
		fini:      make(chan struct{}),
		incoming:  make([]orderedPacket, 0, SftpServerWorkerCount),
		outgoing:  make([]orderedPacket, 0, SftpServerWorkerCount),
		sender:    sender,
		working:   &sync.WaitGroup{},
	}
	go s.controller()
	return s
}

func (s *packetManager) newOrderID() uint32 {
	s.packetCount++
	return s.packetCount
}

func (s *packetManager) getNextOrderID() uint32 {
	return s.packetCount + 1
}

func (s *packetManager) newOrderedRequest(p requestPacket) orderedRequest {
	return orderedRequest{requestPacket: p, orderid: s.newOrderID()}
}
func (p orderedRequest) orderID() uint32 { return p.orderid }

func (s *packetManager) newOrderedResponse(p responsePacket, id uint32,
) orderedResponse {
	return orderedResponse{responsePacket: p, orderid: id}
}
func (p orderedResponse) orderID() uint32 { return p.orderid }

func (o orderedPackets) Sort() {
	sort.Slice(o, func(i, j int) bool {
		return o[i].orderID() < o[j].orderID()
	})
}

func (s *packetManager) incomingPacket(pkt orderedRequest) {
	s.working.Add(1)
	s.requests <- pkt
}

func (s *packetManager) readyPacket(pkt orderedResponse) {
	s.responses <- pkt
	s.working.Done()
}

func (s *packetManager) close() {
	s.working.Wait()
	close(s.fini)
}

func (s *packetManager) workerChan(runWorker func(chan orderedRequest),
) chan orderedRequest {
	rwChan := make(chan orderedRequest, SftpServerWorkerCount)
	for range SftpServerWorkerCount {
		runWorker(rwChan)
	}

	cmdChan := make(chan orderedRequest)
	runWorker(cmdChan)

	pktChan := make(chan orderedRequest, SftpServerWorkerCount)
	go func() {
		for pkt := range pktChan {
			switch pkt.requestPacket.(type) {
			case *sshFxpReadPacket, *sshFxpWritePacket:
				s.incomingPacket(pkt)
				rwChan <- pkt
				continue
			case *sshFxpClosePacket:

				s.working.Wait()
			}

			s.incomingPacket(pkt)

			cmdChan <- pkt
		}

		close(rwChan)
		close(cmdChan)
		s.close()
	}()

	return pktChan
}

func (s *packetManager) controller() {
	for {
		select {
		case pkt := <-s.requests:
			debug("incoming id (oid): %v (%v)", pkt.id(), pkt.orderID())
			s.incoming = append(s.incoming, pkt)
			s.incoming.Sort()
		case pkt := <-s.responses:
			debug("outgoing id (oid): %v (%v)", pkt.id(), pkt.orderID())
			s.outgoing = append(s.outgoing, pkt)
			s.outgoing.Sort()
		case <-s.fini:
			return
		}

		s.maybeSendPackets()
	}
}

func (s *packetManager) maybeSendPackets() {
	for {
		if len(s.outgoing) == 0 || len(s.incoming) == 0 {
			debug("break! -- outgoing: %v; incoming: %v",
				len(s.outgoing), len(s.incoming))
			break
		}

		out := s.outgoing[0]
		in := s.incoming[0]

		if in.orderID() != out.orderID() {
			break
		}

		debug("Sending packet: %v", out.id())
		s.sender.sendPacket(out.(encoding.BinaryMarshaler))
		if s.alloc != nil {
			s.alloc.ReleasePages(in.orderID())
		}

		copy(s.incoming, s.incoming[1:])
		s.incoming[len(s.incoming)-1] = nil
		s.incoming = s.incoming[:len(s.incoming)-1]
		copy(s.outgoing, s.outgoing[1:])
		s.outgoing[len(s.outgoing)-1] = nil
		s.outgoing = s.outgoing[:len(s.outgoing)-1]
	}
}
