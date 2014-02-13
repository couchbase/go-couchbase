package couchbase

import (
	"encoding/binary"
	"fmt"
	mcd "github.com/dustin/gomemcached"
	"testing"
)

type testConn struct {
	req      *mcd.MCRequest
	transmit func(*mcd.MCRequest) error
	receive  func() (*mcd.MCResponse, error)
	send     func(*mcd.MCRequest) (*mcd.MCResponse, error)
}

func (conn *testConn) Transmit(req *mcd.MCRequest) error {
	conn.req = req
	return conn.transmit(req)
}

func (conn *testConn) Receive() (*mcd.MCResponse, error) {
	return conn.receive()
}

func (conn *testConn) Send(req *mcd.MCRequest) (*mcd.MCResponse, error) {
	return conn.send(req)
}

func TestParseFailoverLog(t *testing.T) {
	flogsIn := [][2]uint64{
		{0x1234567812345671, 10},
		{0x1234567812345672, 20},
	}
	buf, offset := make([]byte, 32), 0
	for _, x := range flogsIn {
		binary.BigEndian.PutUint64(buf[offset:], x[0])
		binary.BigEndian.PutUint64(buf[offset+8:], x[1])
		offset += 16
	}
	if flogsOut, err := ParseFailoverLog(buf); err != nil {
		t.Fatal(err)
	} else if flogsOut[0][0] != flogsIn[0][0] || flogsOut[0][1] != flogsIn[0][1] ||
		flogsOut[1][0] != flogsIn[1][0] || flogsOut[1][1] != flogsIn[1][1] {
		e := fmt.Errorf("Mismatch in flogs %v, expected %v", flogsOut, flogsIn)
		t.Fatal(e)
	}
}

func TestUprOpen(t *testing.T) {
	name := "upr-open"
	conn := &testConn{}
	conn.send = func(req *mcd.MCRequest) (*mcd.MCResponse, error) {
		if string(req.Key) != name {
			t.Fatal("name mistmatch")
		} else if req.Opcode != uprOPEN {
			t.Fatal("opcode is not uprOPEN")
		}
		res := Request2Response(req)
		res.Status = mcd.SUCCESS
		return res, nil
	}
	UprOpen(conn, name, 0x1 /*flags*/)
}

func TestRequestFailoverLog(t *testing.T) {
	flogsIn := [][2]uint64{
		{0x1234567812345671, 10},
		{0x1234567812345672, 20},
	}
	buf, offset := make([]byte, 32), 0
	for _, x := range flogsIn {
		binary.BigEndian.PutUint64(buf[offset:], x[0])
		binary.BigEndian.PutUint64(buf[offset+8:], x[1])
		offset += 16
	}

	conn := &testConn{}
	vbucket := uint16(0x15)
	conn.send = func(req *mcd.MCRequest) (*mcd.MCResponse, error) {
		if req.Opcode != uprFailoverLOG {
			t.Fatal("opcode is not uprOPEN")
		} else if req.VBucket != vbucket {
			t.Fatal("mismatch in requested vbucket")
		} else if req.VBucket != uint16(req.Opaque) {
			t.Fatal("vbucket and opaque are expected to be same")
		}
		res := Request2Response(req)
		res.Body = buf
		res.Status = mcd.SUCCESS
		return res, nil
	}
	if flogsOut, err := RequestFailoverLog(conn, vbucket); err != nil {
		t.Fatal(err)
	} else if flogsOut[0][0] != flogsIn[0][0] || flogsOut[0][1] != flogsIn[0][1] ||
		flogsOut[1][0] != flogsIn[1][0] || flogsOut[1][1] != flogsIn[1][1] {
		e := fmt.Errorf("Mismatch in flogs %v, expected %v", flogsOut, flogsIn)
		t.Fatal(e)
	}
}

func TestRequestStream(t *testing.T) {
	conn := &testConn{}
	vbucket, vuuid := uint16(0x15), uint64(0)
	flags := uint32(0)
	s, e, h := uint64(0), uint64(0xFFFFFFFFFFFFFFFF), uint64(0)
	conn.transmit = func(req *mcd.MCRequest) error {
		if req.Opcode != uprStreamREQ {
			t.Fatal("opcode is not uprStreamREQ")
		} else if req.VBucket != vbucket {
			t.Fatal("mismatch in requested vbucket")
		} else if req.VBucket != uint16(req.Opaque) {
			t.Fatal("vbucket and opaque are expected to be same")
		} else if len(req.Extras) != 40 {
			t.Fatal("extras need to be 40 bytes long")
		} else if binary.BigEndian.Uint32(req.Extras) != flags {
			t.Fatal("mismatch in flags")
		} else if binary.BigEndian.Uint32(req.Extras[4:]) != 0 {
			t.Fatal("mismatch in reserved")
		} else if binary.BigEndian.Uint64(req.Extras[8:]) != s {
			t.Fatal("mismatch in start sequence")
		} else if binary.BigEndian.Uint64(req.Extras[16:]) != e {
			t.Fatal("mismatch in end sequence")
		} else if binary.BigEndian.Uint64(req.Extras[24:]) != vuuid {
			t.Fatal("mismatch in vb-uuid")
		} else if binary.BigEndian.Uint64(req.Extras[32:]) != h {
			t.Fatal("mismatch in high sequence")
		}
		return nil
	}
	RequestStream(conn, flags, uint32(vbucket), vbucket, vuuid, s, e, h)
}
