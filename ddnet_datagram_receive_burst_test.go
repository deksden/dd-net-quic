package quic

import (
 "context"
 "testing"
 "time"
 "github.com/sagernet/quic-go/internal/protocol"
 "github.com/sagernet/quic-go/internal/utils"
 "github.com/sagernet/quic-go/internal/wire"
)

// One allowed connection-input burst must survive a paused application reader.
func TestDDNetDatagramReceiveBurst(t *testing.T) {
 q:=newDatagramQueue(func(){},utils.DefaultLogger)
 for n:=0;n<protocol.MaxConnUnprocessedPackets;n++ {
  data:=make([]byte,1016);data[0],data[1]=byte(n>>8),byte(n)
  q.HandleDatagramFrame(&wire.DatagramFrame{Data:data})
 }
 ctx,cancel:=context.WithTimeout(context.Background(),time.Second);defer cancel()
 for n:=0;n<protocol.MaxConnUnprocessedPackets;n++ {
  data,err:=q.Receive(ctx)
  if err!=nil {t.Fatalf("datagram %d lost inside the receive queue: %v",n,err)}
  if len(data)!=1016 || int(data[0])<<8|int(data[1])!=n {t.Fatalf("datagram %d reordered or corrupted",n)}
 }
 if len(q.rcvQueue) != 0 {
  t.Fatalf("expected empty rcvQueue after draining, got %d", len(q.rcvQueue))
 }
}

func TestDDNetDatagramQueueZeroesDequeuedElements(t *testing.T) {
 q := newDatagramQueue(func() {}, utils.DefaultLogger)
 q.HandleDatagramFrame(&wire.DatagramFrame{Data: []byte("first")})
 q.HandleDatagramFrame(&wire.DatagramFrame{Data: []byte("second")})
 backing := q.rcvQueue[:cap(q.rcvQueue)]

 ctx, cancel := context.WithTimeout(context.Background(), time.Second)
 defer cancel()
 got, err := q.Receive(ctx)
 if err != nil || string(got) != "first" {
  t.Fatalf("unexpected first receive: %q %v", got, err)
 }
 if backing[0] != nil {
  t.Fatal("dequeued slice slot was not zeroed")
 }
 got, err = q.Receive(ctx)
 if err != nil || string(got) != "second" {
  t.Fatalf("unexpected second receive: %q %v", got, err)
 }
 if backing[1] != nil || len(q.rcvQueue) != 0 {
  t.Fatalf("expected second slot nil and len 0, got slot=%v len=%d", backing[1], len(q.rcvQueue))
 }
}
