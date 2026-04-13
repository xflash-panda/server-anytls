package session

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	scommon "github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/buf"
	"github.com/sirupsen/logrus"
	"github.com/xflash-panda/server-anytls/internal/pkg/proxy/padding"
	"github.com/xflash-panda/server-anytls/internal/pkg/util"
)

var clientDebugPaddingScheme = os.Getenv("CLIENT_DEBUG_PADDING_SCHEME") == "1"

// zeroBuf is a pre-allocated zero-filled buffer used for waste padding,
// avoiding per-write heap allocation of make([]byte, paddingLen).
// Max padding length is uint16 (65535).
var zeroBuf [65536]byte

type Session struct {
	conn        net.Conn
	connLock    sync.Mutex
	streams     map[uint32]*Stream
	streamId    atomic.Uint32
	streamLock  sync.RWMutex
	dieOnce     sync.Once
	die         chan struct{}
	dieHook     func()
	synDone     func()
	synDoneLock sync.Mutex
	seq         uint64
	idleSince   time.Time
	padding     *scommon.TypedValue[*padding.PaddingFactory]
	peerVersion byte
	isClient    bool
	sendPadding bool
	buffering   bool
	buffer      []byte
	pktCounter  atomic.Uint32
	paddingBuf  []byte
	onNewStream func(stream *Stream)
}

func NewClientSession(conn net.Conn, _padding *scommon.TypedValue[*padding.PaddingFactory]) *Session {
	s := &Session{
		conn:        conn,
		isClient:    true,
		sendPadding: true,
		padding:     _padding,
	}
	s.die = make(chan struct{})
	s.streams = make(map[uint32]*Stream)
	return s
}

func NewServerSession(conn net.Conn, onNewStream func(stream *Stream), _padding *scommon.TypedValue[*padding.PaddingFactory]) *Session {
	s := &Session{
		conn:        conn,
		onNewStream: onNewStream,
		padding:     _padding,
	}
	s.die = make(chan struct{})
	s.streams = make(map[uint32]*Stream)
	return s
}

func (s *Session) Run() {
	if !s.isClient {
		_ = s.recvLoop()
		return
	}

	paddingF := s.padding.Load()
	if paddingF == nil {
		return
	}
	settings := util.StringMap{
		"v":           "2",
		"padding-md5": paddingF.Md5,
	}

	f := newFrame(cmdSettings, 0)
	f.data = settings.ToBytes()
	s.buffering = true
	_, _ = s.writeControlFrame(f)
	go func() { _ = s.recvLoop() }()
}

func (s *Session) IsClosed() bool {
	select {
	case <-s.die:
		return true
	default:
		return false
	}
}

// SetDieHook sets a callback function that will be called when the session closes
func (s *Session) SetDieHook(hook func()) {
	s.dieHook = hook
}

func (s *Session) Close() error {
	var once bool
	s.dieOnce.Do(func() {
		close(s.die)
		once = true
	})

	if once {
		if s.dieHook != nil {
			s.dieHook()
			s.dieHook = nil
		}
		s.streamLock.Lock()
		for _, stream := range s.streams {
			stream.closeLocally()
		}
		s.streams = make(map[uint32]*Stream)
		s.streamLock.Unlock()
		return s.conn.Close()
	} else {
		return io.ErrClosedPipe
	}
}

func (s *Session) OpenStream() (*Stream, error) {
	if s.IsClosed() {
		return nil, io.ErrClosedPipe
	}

	sid := s.streamId.Add(1)
	stream := newStream(sid, s)
	if sid >= 2 && s.peerVersion >= 2 {
		s.synDoneLock.Lock()
		if s.synDone != nil {
			s.synDone()
		}

		s.synDone = util.NewDeadlineWatcher(time.Second*3, func() {
			_ = s.Close()
		})
		s.synDoneLock.Unlock()
	}

	if _, err := s.writeControlFrame(newFrame(cmdSYN, sid)); err != nil {
		return nil, err
	}

	s.buffering = false
	s.streamLock.Lock()
	defer s.streamLock.Unlock()

	select {
	case <-s.die:
		return nil, io.ErrClosedPipe
	default:
		s.streams[sid] = stream
		return stream, nil
	}
}

func (s *Session) recvLoop() error {
	defer func() {
		if r := recover(); r != nil {
			logrus.Errorln("[BUG]", r, string(debug.Stack()))
		}
	}()
	defer func() { _ = s.Close() }()
	var receivedSettingsFromClient bool
	var hdr rawHeader
	var readBuf []byte
	for {
		if _, err := io.ReadFull(s.conn, hdr[:]); err == nil {
			sid := hdr.StreamID()
			switch hdr.Cmd() {
			case cmdPSH:
				if hdr.Length() > 0 {
					frameLen := int(hdr.Length())
					if cap(readBuf) < frameLen {
						readBuf = make([]byte, frameLen)
					} else {
						readBuf = readBuf[:frameLen]
					}
					if _, err := io.ReadFull(s.conn, readBuf); err == nil {
						s.streamLock.RLock()
						stream, ok := s.streams[sid]
						s.streamLock.RUnlock()
						if ok {
							_, _ = stream.pipeW.Write(readBuf)
						}
					} else {
						return err
					}
				}
			case cmdSYN:
				if !s.isClient && !receivedSettingsFromClient {
					f := newFrame(cmdAlert, 0)
					f.data = []byte("client did not send its settings")
					_, _ = s.writeControlFrame(f)
					return nil
				}
				s.streamLock.Lock()
				if _, ok := s.streams[sid]; !ok {
					stream := newStream(sid, s)
					s.streams[sid] = stream
					go func() {
						if s.onNewStream != nil {
							s.onNewStream(stream)
						} else {
							_ = stream.Close()
						}
					}()
				}
				s.streamLock.Unlock()
			case cmdSYNACK:
				s.synDoneLock.Lock()
				if s.synDone != nil {
					s.synDone()
					s.synDone = nil
				}
				s.synDoneLock.Unlock()
				if hdr.Length() > 0 {
					buffer := buf.Get(int(hdr.Length()))
					if _, err := io.ReadFull(s.conn, buffer); err != nil {
						_ = buf.Put(buffer)
						return err
					}
					s.streamLock.RLock()
					stream, ok := s.streams[sid]
					s.streamLock.RUnlock()
					if ok {
						_ = stream.closeWithError(fmt.Errorf("remote: %s", string(buffer)))
					}
					_ = buf.Put(buffer)
				}
			case cmdFIN:
				s.streamLock.Lock()
				stream, ok := s.streams[sid]
				delete(s.streams, sid)
				s.streamLock.Unlock()
				if ok {
					stream.closeLocally()
				}
			case cmdWaste:
				if hdr.Length() > 0 {
					buffer := buf.Get(int(hdr.Length()))
					if _, err := io.ReadFull(s.conn, buffer); err != nil {
						_ = buf.Put(buffer)
						return err
					}
					_ = buf.Put(buffer)
				}
			case cmdSettings:
				if hdr.Length() > 0 {
					buffer := buf.Get(int(hdr.Length()))
					if _, err := io.ReadFull(s.conn, buffer); err != nil {
						_ = buf.Put(buffer)
						return err
					}
					if !s.isClient {
						receivedSettingsFromClient = true
						m := util.StringMapFromBytes(buffer)
						paddingF := s.padding.Load()
						if paddingF == nil {
							_ = buf.Put(buffer)
							return nil
						}
						if m["padding-md5"] != paddingF.Md5 {
							f := newFrame(cmdUpdatePaddingScheme, 0)
							f.data = paddingF.RawScheme
							_, err = s.writeControlFrame(f)
							if err != nil {
								_ = buf.Put(buffer)
								return err
							}
						}
						if v, err := strconv.Atoi(m["v"]); err == nil && v >= 2 {
							s.peerVersion = byte(v)
							f := newFrame(cmdServerSettings, 0)
							f.data = util.StringMap{
								"v": "2",
							}.ToBytes()
							_, err = s.writeControlFrame(f)
							if err != nil {
								_ = buf.Put(buffer)
								return err
							}
						}
					}
					_ = buf.Put(buffer)
				}
			case cmdAlert:
				if hdr.Length() > 0 {
					buffer := buf.Get(int(hdr.Length()))
					if _, err := io.ReadFull(s.conn, buffer); err != nil {
						_ = buf.Put(buffer)
						return err
					}
					if s.isClient {
						logrus.Errorln("[Alert from server]", string(buffer))
					}
					_ = buf.Put(buffer)
					return nil
				}
			case cmdUpdatePaddingScheme:
				if hdr.Length() > 0 {
					rawScheme := make([]byte, int(hdr.Length()))
					if _, err := io.ReadFull(s.conn, rawScheme); err != nil {
						return err
					}
					if s.isClient && !clientDebugPaddingScheme {
						if padding.UpdatePaddingScheme(rawScheme) {
							logrus.Infof("[Update padding succeed] %x\n", md5.Sum(rawScheme))
						} else {
							logrus.Warnf("[Update padding failed] %x\n", md5.Sum(rawScheme))
						}
					}
				}
			case cmdHeartRequest:
				if _, err := s.writeControlFrame(newFrame(cmdHeartResponse, sid)); err != nil {
					return err
				}
			case cmdHeartResponse:
				break
			case cmdServerSettings:
				if hdr.Length() > 0 {
					buffer := buf.Get(int(hdr.Length()))
					if _, err := io.ReadFull(s.conn, buffer); err != nil {
						_ = buf.Put(buffer)
						return err
					}
					if s.isClient {
						m := util.StringMapFromBytes(buffer)
						if v, err := strconv.Atoi(m["v"]); err == nil {
							s.peerVersion = byte(v)
						}
					}
					_ = buf.Put(buffer)
				}
			default:
			}
		} else {
			return err
		}
	}
}

func (s *Session) streamClosed(sid uint32) error {
	if s.IsClosed() {
		return io.ErrClosedPipe
	}
	_, err := s.writeControlFrame(newFrame(cmdFIN, sid))
	s.streamLock.Lock()
	delete(s.streams, sid)
	s.streamLock.Unlock()
	return err
}

// maxFramePayload is the largest data payload per frame, chosen so that
// chunkLen + headerOverHeadSize <= 65536, keeping frames within the pool.
const maxFramePayload = 65536 - headerOverHeadSize

func (s *Session) writeDataFrame(sid uint32, data []byte) (int, error) {
	total := len(data)
	for len(data) > 0 {
		chunkLen := min(len(data), maxFramePayload)

		buffer := buf.NewSize(chunkLen + headerOverHeadSize)
		_ = buffer.WriteByte(cmdPSH)
		binary.BigEndian.PutUint32(buffer.Extend(4), sid)
		binary.BigEndian.PutUint16(buffer.Extend(2), uint16(chunkLen))
		_, _ = buffer.Write(data[:chunkLen])
		_, err := s.writeConn(buffer.Bytes())
		buffer.Release()
		if err != nil {
			return 0, err
		}
		data = data[chunkLen:]
	}
	return total, nil
}

func (s *Session) writeControlFrame(frame frame) (int, error) {
	dataLen := len(frame.data)

	buffer := buf.NewSize(dataLen + headerOverHeadSize)
	_ = buffer.WriteByte(frame.cmd)
	binary.BigEndian.PutUint32(buffer.Extend(4), frame.sid)
	binary.BigEndian.PutUint16(buffer.Extend(2), uint16(dataLen))
	_, _ = buffer.Write(frame.data)

	_, err := s.writeConnWithDeadline(buffer.Bytes(), time.Second*5)
	buffer.Release()
	if err != nil {
		_ = s.Close()
		return 0, err
	}

	return dataLen, nil
}

func (s *Session) writeConnWithDeadline(b []byte, deadline time.Duration) (n int, err error) {
	s.connLock.Lock()
	defer s.connLock.Unlock()
	_ = s.conn.SetWriteDeadline(time.Now().Add(deadline))
	defer func() { _ = s.conn.SetWriteDeadline(time.Time{}) }()
	return s.writeConnLocked(b)
}

func (s *Session) writeConn(b []byte) (n int, err error) {
	s.connLock.Lock()
	defer s.connLock.Unlock()
	return s.writeConnLocked(b)
}

func (s *Session) writeConnLocked(b []byte) (n int, err error) {
	if s.buffering {
		s.buffer = append(s.buffer, b...)
		return len(b), nil
	} else if len(s.buffer) > 0 {
		s.buffer = append(s.buffer, b...)
		b = s.buffer
		s.buffer = nil
	}
	if s.sendPadding {
		pkt := s.pktCounter.Add(1)
		paddingF := s.padding.Load()
		if paddingF == nil {
			s.sendPadding = false
			return s.conn.Write(b)
		}
		if pkt < paddingF.Stop {
			payloadLen := len(b)
			pktSizes := paddingF.GenerateRecordPayloadSizes(pkt)
			// Reuse per-session buffer for padding assembly (safe: called under connLock)
			overhead := len(pktSizes) * (headerOverHeadSize + 1024)
			needed := payloadLen + overhead
			if cap(s.paddingBuf) < needed {
				s.paddingBuf = make([]byte, 0, needed)
			}
			assembled := s.paddingBuf[:0]
			for _, l := range pktSizes {
				remaining := len(b)
				if l == padding.CheckMark {
					if remaining == 0 {
						break
					}
					continue
				}
				if remaining > l {
					assembled = append(assembled, b[:l]...)
					b = b[l:]
				} else if remaining > 0 {
					paddingLen := l - remaining - headerOverHeadSize
					if paddingLen > 0 {
						assembled = append(assembled, b...)
						var wasteHdr [headerOverHeadSize]byte
						wasteHdr[0] = cmdWaste
						binary.BigEndian.PutUint16(wasteHdr[5:7], uint16(paddingLen))
						assembled = append(assembled, wasteHdr[:]...)
						assembled = append(assembled, zeroBuf[:paddingLen]...)
					} else {
						assembled = append(assembled, b...)
					}
					b = nil
				} else {
					var wasteHdr [headerOverHeadSize]byte
					wasteHdr[0] = cmdWaste
					binary.BigEndian.PutUint16(wasteHdr[5:7], uint16(l))
					assembled = append(assembled, wasteHdr[:]...)
					assembled = append(assembled, zeroBuf[:l]...)
					b = nil
				}
			}
			if len(b) > 0 {
				assembled = append(assembled, b...)
			}
			s.paddingBuf = assembled
			_, err = s.conn.Write(assembled)
			if err != nil {
				return 0, err
			}
			return payloadLen, nil
		}
		s.sendPadding = false
		s.paddingBuf = nil
	}
	return s.conn.Write(b)
}
