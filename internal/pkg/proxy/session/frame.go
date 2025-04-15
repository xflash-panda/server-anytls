package session

import (
	"encoding/binary"
)

const ( 
	cmdWaste               = 0 
	cmdSYN                 = 1 
	cmdPSH                 = 2 
	cmdFIN                 = 3 
	cmdSettings            = 4 
	cmdAlert               = 5 
	cmdUpdatePaddingScheme = 6 
	
	cmdSYNACK         = 7  
	cmdHeartRequest   = 8  
	cmdHeartResponse  = 9  
	cmdServerSettings = 10 
)

const (
	headerOverHeadSize = 1 + 4 + 2
)

type frame struct {
	cmd  byte   
	sid  uint32 
	data []byte 
}

func newFrame(cmd byte, sid uint32) frame {
	return frame{cmd: cmd, sid: sid}
}

type rawHeader [headerOverHeadSize]byte

func (h rawHeader) Cmd() byte {
	
	return h[0]
}

func (h rawHeader) StreamID() uint32 {
	
	return binary.BigEndian.Uint32(h[1:])
}

func (h rawHeader) Length() uint16 {
	
	return binary.BigEndian.Uint16(h[5:])
}
