package couchbase

import (
	"encoding/binary"
	"fmt"
	mcd "github.com/couchbase/gomemcached"
)

const opaqueOpen = 0xBEAF0001
const opaqueFailoverRequest = 0xBEAF0002

type transporter interface {
	Transmit(*mcd.MCRequest) error
	Receive() (*mcd.MCResponse, error)
	Send(*mcd.MCRequest) (*mcd.MCResponse, error)
}

// uprOpen sends uprOPEN request with `name`. The API will wait for a
// response before returning back to the caller.
func uprOpen(conn transporter, name string, flags uint32) error {
	if len(name) > 65535 {
		return fmt.Errorf("uprOpen: name cannot exceed 65535")
	}

	req := &mcd.MCRequest{Opcode: uprOPEN, Opaque: opaqueOpen}
	req.Key = []byte(name) // #Key

	req.Extras = make([]byte, 8)
	binary.BigEndian.PutUint32(req.Extras[:4], 0) // #Extras.sequenceNo
	// while consumer is opening the connection Type flag needs to be cleared.
	binary.BigEndian.PutUint32(req.Extras[4:], flags) // #Extras.flags

	if res, err := conn.Send(req); err != nil { // send and receive packet
		return fmt.Errorf("connection error, %v", err)
	} else if res.Opcode != uprOPEN {
		return fmt.Errorf("unexpected #opcode %v", res.Opcode)
	} else if req.Opaque != res.Opaque {
		return fmt.Errorf("opaque mismatch, %v over %v", res.Opaque, res.Opaque)
	} else if res.Status != mcd.SUCCESS {
		return fmt.Errorf("status %v", res.Status)
	}
	return nil
}

// requestFailoverLog sends uprFailoverLOG request for `vbucket` and waits
// for a response before returning.
func requestFailoverLog(conn transporter, vbucket uint16) (flog FailoverLog, err error) {
	var res *mcd.MCResponse

	req := &mcd.MCRequest{
		Opcode:  uprFailoverLOG,
		VBucket: vbucket,
		Opaque:  uint32(vbucket),
	}

	if res, err = conn.Send(req); err != nil { // Send and receive packet
		return nil, fmt.Errorf("connection error, %v", err)
	} else if res.Opcode != uprFailoverLOG {
		return nil, fmt.Errorf("unexpected #opcode %v", res.Opcode)
	} else if req.Opaque != res.Opaque {
		err = fmt.Errorf("opaque mismatch, %v over %v", res.Opaque, res.Opaque)
		return nil, err
	} else if res.Status != mcd.SUCCESS {
		return nil, fmt.Errorf("status %v", res.Status)
	}
	if flog, err = parseFailoverLog(res.Body); err != nil {
		return nil, err
	}
	return flog, err
}

// requestStream sends uprStreamREQ request for `vbucket`. The call will
// return back immediate to the caller, it is upto the caller to handle the
// response sent back from the producer.
func requestStream(conn transporter, flags, opq uint32, vb uint16,
	vuuid, startSeqno, endSeqno, highSeqno uint64) error {

	req := &mcd.MCRequest{Opcode: uprStreamREQ, Opaque: opq, VBucket: vb}
	req.Extras = make([]byte, 40) // #Extras
	binary.BigEndian.PutUint32(req.Extras[:4], flags)
	binary.BigEndian.PutUint32(req.Extras[4:8], uint32(0))
	binary.BigEndian.PutUint64(req.Extras[8:16], startSeqno)
	binary.BigEndian.PutUint64(req.Extras[16:24], endSeqno)
	binary.BigEndian.PutUint64(req.Extras[24:32], vuuid)
	binary.BigEndian.PutUint64(req.Extras[32:40], highSeqno)

	return conn.Transmit(req)
}

// endStream sends uprStreamEND request for `vbucket`. The call will
// return back immediate to the caller, it is upto the caller to handle the
// response sent back from the producer.
func endStream(conn transporter, flags uint32, vbucket uint16) error {
	req := &mcd.MCRequest{
		Opcode:  uprStreamEND,
		Opaque:  uint32(vbucket),
		VBucket: vbucket,
	}
	req.Extras = make([]byte, 4) // #Extras
	binary.BigEndian.PutUint32(req.Extras, flags)
	return conn.Transmit(req)
}

// parseFailoverLog parses response body from uprFailvoerLOG and
// uprStreamREQ response.
func parseFailoverLog(body []byte) (FailoverLog, error) {
	if len(body)%16 != 0 {
		err := fmt.Errorf("invalid body length %v, in failover-log", len(body))
		return nil, err
	}
	log := make(FailoverLog, len(body)/16)
	for i, j := 0, 0; i < len(body); i += 16 {
		vuuid := binary.BigEndian.Uint64(body[i : i+8])
		seqno := binary.BigEndian.Uint64(body[i+8 : i+16])
		log[j] = [2]uint64{vuuid, seqno}
		j++
	}
	return log, nil
}

// request2Response converts will interpret VBucket field as Status field and
// re-interpret the request packate as response packet. This is required for
// UPR streams which are full duplex.
func request2Response(req *mcd.MCRequest) *mcd.MCResponse {
	return &mcd.MCResponse{
		Opcode: req.Opcode,
		Cas:    req.Cas,
		Opaque: req.Opaque,
		Status: mcd.Status(req.VBucket),
		Extras: req.Extras,
		Key:    req.Key,
		Body:   req.Body,
	}
}
