//nolint:all
package session

import (
	"encoding/binary"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/xflash-panda/server-anytls/internal/pkg/proxy/padding"
)

// RED: Measure writeConnLocked with padding active under concurrent stream writes.
func BenchmarkWriteConnLocked_ConcurrentWithPadding(b *testing.B) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	sess := NewClientSession(serverConn, &padding.DefaultPaddingFactory)
	sess.sendPadding = true

	go func() {
		buf := make([]byte, 65536)
		for {
			if _, err := clientConn.Read(buf); err != nil {
				return
			}
		}
	}()

	writers := runtime.GOMAXPROCS(0)
	payload := make([]byte, 100)

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(writers)
	each := b.N / writers
	for g := 0; g < writers; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				sess.pktCounter.Store(uint32(i % 8))
				sess.connLock.Lock()
				sess.writeConnLocked(payload)
				sess.connLock.Unlock()
			}
		}()
	}
	wg.Wait()
}

// RED: writeDataFrame allocates a *Buffer struct per call via buf.NewSize.
func BenchmarkWriteDataFrame_Allocs(b *testing.B) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	sess := NewClientSession(serverConn, &padding.DefaultPaddingFactory)
	sess.sendPadding = false

	go func() {
		buf := make([]byte, 65536)
		for {
			if _, err := clientConn.Read(buf); err != nil {
				return
			}
		}
	}()

	payload := make([]byte, 256)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sess.writeDataFrame(1, payload)
	}
}

// RED: Measure streamLock contention.
func BenchmarkStreamLookup_UnderContention(b *testing.B) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	sess := NewClientSession(serverConn, &padding.DefaultPaddingFactory)

	for i := uint32(1); i <= 100; i++ {
		sess.streams[i] = newStream(i, sess)
	}

	writers := runtime.GOMAXPROCS(0) / 2
	if writers < 1 {
		writers = 1
	}

	b.ReportAllocs()
	b.ResetTimer()

	var wg sync.WaitGroup

	wg.Add(writers)
	each := b.N / writers
	for g := 0; g < writers; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				sid := uint32(i%100 + 1)
				sess.streamLock.RLock()
				_ = sess.streams[sid]
				sess.streamLock.RUnlock()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < each; i++ {
			sid := uint32(i%100 + 1)
			sess.streamLock.Lock()
			sess.streams[sid] = newStream(sid, sess)
			sess.streamLock.Unlock()
		}
	}()

	wg.Wait()
}

// RED: Measure padding assembly cost isolated from I/O (uses GenerateRecordPayloadSizes).
func BenchmarkPaddingAssembly_CPUOnly(b *testing.B) {
	paddingF := padding.DefaultPaddingFactory.Load()

	payload := make([]byte, 200)
	var paddingBuf []byte
	var pktSizesBuf []int
	var pktCounter atomic.Uint32

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pkt := pktCounter.Add(1)
		if pkt >= paddingF.Stop {
			pktCounter.Store(0)
			pkt = pktCounter.Add(1)
		}
		pktSizes := paddingF.GenerateRecordPayloadSizes(pkt)

		data := payload
		overhead := len(pktSizes) * (headerOverHeadSize + 1024)
		needed := len(data) + overhead
		if cap(paddingBuf) < needed {
			paddingBuf = make([]byte, 0, needed)
		}
		assembled := paddingBuf[:0]
		for _, l := range pktSizes {
			remaining := len(data)
			if l == padding.CheckMark {
				if remaining == 0 {
					break
				}
				continue
			}
			if remaining > l {
				assembled = append(assembled, data[:l]...)
				data = data[l:]
			} else if remaining > 0 {
				paddingLen := l - remaining - headerOverHeadSize
				if paddingLen > 0 {
					assembled = append(assembled, data...)
					var wasteHdr [headerOverHeadSize]byte
					wasteHdr[0] = cmdWaste
					binary.BigEndian.PutUint16(wasteHdr[5:7], uint16(paddingLen))
					assembled = append(assembled, wasteHdr[:]...)
					assembled = append(assembled, zeroBuf[:paddingLen]...)
				} else {
					assembled = append(assembled, data...)
				}
				data = nil
			} else {
				var wasteHdr [headerOverHeadSize]byte
				wasteHdr[0] = cmdWaste
				binary.BigEndian.PutUint16(wasteHdr[5:7], uint16(l))
				assembled = append(assembled, wasteHdr[:]...)
				assembled = append(assembled, zeroBuf[:l]...)
				data = nil
			}
		}
		if len(data) > 0 {
			assembled = append(assembled, data...)
		}
		paddingBuf = assembled
		_ = pktSizesBuf
	}
}

// GREEN: Padding assembly using AppendRecordPayloadSizes (zero-alloc padding sizes).
func BenchmarkPaddingAssembly_CPUOnly_AppendAPI(b *testing.B) {
	paddingF := padding.DefaultPaddingFactory.Load()

	payload := make([]byte, 200)
	var paddingBuf []byte
	var pktSizesBuf []int
	var pktCounter atomic.Uint32

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pkt := pktCounter.Add(1)
		if pkt >= paddingF.Stop {
			pktCounter.Store(0)
			pkt = pktCounter.Add(1)
		}
		pktSizesBuf = paddingF.AppendRecordPayloadSizes(pkt, pktSizesBuf[:0])

		data := payload
		overhead := len(pktSizesBuf) * (headerOverHeadSize + 1024)
		needed := len(data) + overhead
		if cap(paddingBuf) < needed {
			paddingBuf = make([]byte, 0, needed)
		}
		assembled := paddingBuf[:0]
		for _, l := range pktSizesBuf {
			remaining := len(data)
			if l == padding.CheckMark {
				if remaining == 0 {
					break
				}
				continue
			}
			if remaining > l {
				assembled = append(assembled, data[:l]...)
				data = data[l:]
			} else if remaining > 0 {
				paddingLen := l - remaining - headerOverHeadSize
				if paddingLen > 0 {
					assembled = append(assembled, data...)
					var wasteHdr [headerOverHeadSize]byte
					wasteHdr[0] = cmdWaste
					binary.BigEndian.PutUint16(wasteHdr[5:7], uint16(paddingLen))
					assembled = append(assembled, wasteHdr[:]...)
					assembled = append(assembled, zeroBuf[:paddingLen]...)
				} else {
					assembled = append(assembled, data...)
				}
				data = nil
			} else {
				var wasteHdr [headerOverHeadSize]byte
				wasteHdr[0] = cmdWaste
				binary.BigEndian.PutUint16(wasteHdr[5:7], uint16(l))
				assembled = append(assembled, wasteHdr[:]...)
				assembled = append(assembled, zeroBuf[:l]...)
				data = nil
			}
		}
		if len(data) > 0 {
			assembled = append(assembled, data...)
		}
		paddingBuf = assembled
	}
}
