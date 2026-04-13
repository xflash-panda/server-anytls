//nolint:all
package session

import (
	"net"
	"testing"

	scommon "github.com/sagernet/sing/common"
	"github.com/xflash-panda/server-anytls/internal/pkg/proxy/padding"
)

// TestWriteDataFrameLargeData verifies that writeDataFrame does not corrupt
// the protocol when data exceeds uint16 max (65535 bytes).
// Bug: uint16(dataLen) silently truncates, causing header/payload mismatch
// which leads to protocol desync and potential panics on the receiver.
func TestWriteDataFrameLargeData(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	var p scommon.TypedValue[*padding.PaddingFactory]
	sess := NewServerSession(serverConn, nil, &p)

	// Drain writes so they don't block
	go func() {
		buf := make([]byte, 256*1024)
		for {
			if _, err := clientConn.Read(buf); err != nil {
				return
			}
		}
	}()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("writeDataFrame panicked: %v", r)
		}
	}()

	// Write data larger than uint16 max
	bigData := make([]byte, 70000)
	n, err := sess.writeDataFrame(1, bigData)
	if err != nil {
		t.Fatalf("writeDataFrame returned error: %v", err)
	}
	// The function should write all bytes correctly, not truncate via uint16
	if n != len(bigData) {
		t.Fatalf("expected n=%d, got n=%d", len(bigData), n)
	}
}

// TestWriteDataFrameChunking verifies that writeDataFrame properly chunks
// data larger than 65535 bytes so each frame's length field fits in uint16.
func TestWriteDataFrameChunking(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	var p scommon.TypedValue[*padding.PaddingFactory]
	sess := NewServerSession(serverConn, nil, &p)

	// Collect all written bytes
	done := make(chan []byte, 1)
	go func() {
		var all []byte
		buf := make([]byte, 256*1024)
		for {
			n, err := clientConn.Read(buf)
			if n > 0 {
				all = append(all, buf[:n]...)
			}
			if err != nil {
				break
			}
			// Read enough for the chunked frames + overhead
			if len(all) > 70000+3*headerOverHeadSize {
				break
			}
		}
		done <- all
	}()

	bigData := make([]byte, 70000)
	for i := range bigData {
		bigData[i] = byte(i % 256)
	}

	n, err := sess.writeDataFrame(1, bigData)
	if err != nil {
		t.Fatalf("writeDataFrame error: %v", err)
	}
	if n != len(bigData) {
		t.Fatalf("expected n=%d, got n=%d", len(bigData), n)
	}

	_ = serverConn.Close()
	written := <-done

	// Verify each frame header has a valid uint16 length (no truncation)
	offset := 0
	totalPayload := 0
	for offset < len(written) {
		if offset+headerOverHeadSize > len(written) {
			break
		}
		// cmd byte
		cmd := written[offset]
		if cmd != cmdPSH {
			t.Fatalf("unexpected cmd %d at offset %d", cmd, offset)
		}
		// length field (uint16) at offset+5
		frameDataLen := int(written[offset+5])<<8 | int(written[offset+6])
		if frameDataLen == 0 || frameDataLen > 65535 {
			t.Fatalf("invalid frame length %d at offset %d", frameDataLen, offset)
		}
		offset += headerOverHeadSize + frameDataLen
		totalPayload += frameDataLen
	}

	if totalPayload != len(bigData) {
		t.Fatalf("total payload mismatch: expected %d, got %d", len(bigData), totalPayload)
	}
}
