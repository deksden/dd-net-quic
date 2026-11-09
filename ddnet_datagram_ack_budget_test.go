package quic

import (
	"testing"

	"github.com/sagernet/quic-go/internal/ackhandler"
	"github.com/sagernet/quic-go/internal/monotime"
	"github.com/sagernet/quic-go/internal/protocol"
	"github.com/sagernet/quic-go/internal/utils"
	"github.com/sagernet/quic-go/internal/wire"
)

type ddnetEmptyFramer struct{}

func (ddnetEmptyFramer) HasData() bool { return false }
func (ddnetEmptyFramer) Append(f []ackhandler.Frame, s []ackhandler.StreamFrame, _ protocol.ByteCount, _ monotime.Time, _ protocol.Version) ([]ackhandler.Frame, []ackhandler.StreamFrame, protocol.ByteCount) {
	return f, s, 0
}

type ddnetLargeACK struct{}

func (ddnetLargeACK) GetAckFrame(_ protocol.EncryptionLevel, _ monotime.Time, _ bool) *wire.AckFrame {
	a := &wire.AckFrame{}
	for i := 0; i < 100; i++ {
		n := protocol.PacketNumber(1000 - 3*i)
		a.AckRanges = append(a.AckRanges, wire.AckRange{Smallest: n, Largest: n})
	}
	return a
}
func TestDDNetLargeACKDoesNotSilentlyDiscardDatagram(t *testing.T) {
	q := newDatagramQueue(func() {}, utils.DefaultLogger)
	frame := &wire.DatagramFrame{Data: make([]byte, 1016)}
	if err := q.Add(frame); err != nil {
		t.Fatal(err)
	}
	p := &packetPacker{framer: ddnetEmptyFramer{}, acks: ddnetLargeACK{}, retransmissionQueue: newRetransmissionQueue(), datagramQueue: q}
	const budget = protocol.ByteCount(1100)
	for i := 0; i < DatagramFrameMaxPeekTimes+2; i++ {
		result := p.composeNextPacket(budget, false, true, monotime.Now(), protocol.Version1)
		if result.length > budget {
			t.Fatalf("packet exceeded budget: %d", result.length)
		}
		for _, f := range result.frames {
			if f.Frame == frame {
				if result.ack == nil {
					t.Fatal("ACK was lost")
				}
				return
			}
		}
		if q.Peek() == nil {
			t.Fatalf("DATAGRAM silently discarded after %d large ACKs", i+1)
		}
	}
	t.Fatal("DATAGRAM never became sendable with repeated ACKs")
}

func TestDDNetACKKeepsValidMinimumWhenDatagramCannotFit(t *testing.T) {
	q := newDatagramQueue(func() {}, utils.DefaultLogger)
	frame := &wire.DatagramFrame{Data: make([]byte, 1098)}
	if err := q.Add(frame); err != nil {
		t.Fatal(err)
	}
	p := &packetPacker{framer: ddnetEmptyFramer{}, acks: ddnetLargeACK{}, retransmissionQueue: newRetransmissionQueue(), datagramQueue: q}
	const budget = protocol.ByteCount(1100)
	result := p.composeNextPacket(budget, false, true, monotime.Now(), protocol.Version1)
	if result.ack == nil || len(result.ack.AckRanges) == 0 || result.length > budget {
		t.Fatal("ACK did not fit safely")
	}
	if len(result.frames) != 0 || q.Peek() != frame {
		t.Fatal("unfittable DATAGRAM was sent or lost")
	}
}
