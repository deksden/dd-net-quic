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
}
