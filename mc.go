package couchbase

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"
	"net"
	"runtime"
)

const bufsize = 1024

type memcachedClient struct {
	Conn   net.Conn
	writer *bufio.Writer

	hdrBuf []byte
}

func connect(prot, dest string) (rv *memcachedClient) {
	conn, err := net.Dial(prot, dest)
	if err != nil {
		log.Fatalf("Failed to connect: %s", err)
	}
	rv = new(memcachedClient)
	rv.Conn = conn
	rv.writer = bufio.NewWriterSize(rv.Conn, bufsize)
	rv.hdrBuf = make([]byte, HDR_LEN)
	return rv
}

func (client *memcachedClient) send(req mcRequest) (rv mcResponse) {
	transmitRequest(client.writer, req)
	rv = client.getResponse()
	return
}

func (client *memcachedClient) Get(vb uint16, key string) mcResponse {
	var req mcRequest
	req.Opcode = mcGET
	req.VBucket = vb
	req.Key = []byte(key)
	req.Cas = 0
	req.Opaque = 0
	req.Extras = []byte{}
	req.Body = []byte{}
	return client.send(req)
}

func (client *memcachedClient) Del(vb uint16, key string) mcResponse {
	var req mcRequest
	req.Opcode = mcDELETE
	req.VBucket = vb
	req.Key = []byte(key)
	req.Cas = 0
	req.Opaque = 0
	req.Extras = []byte{}
	req.Body = []byte{}
	return client.send(req)
}

func (client *memcachedClient) store(opcode uint8, vb uint16,
	key string, flags int, exp int, body []byte) mcResponse {

	var req mcRequest
	req.Opcode = opcode
	req.VBucket = vb
	req.Cas = 0
	req.Opaque = 0
	req.Key = []byte(key)
	req.Extras = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint64(req.Extras, uint64(flags)<<32|uint64(exp))
	req.Body = body
	return client.send(req)
}

func (client *memcachedClient) Add(vb uint16, key string, flags int, exp int,
	body []byte) mcResponse {
	return client.store(mcADD, vb, key, flags, exp, body)
}

func (client *memcachedClient) Set(vb uint16, key string, flags int, exp int,
	body []byte) mcResponse {
	return client.store(mcSET, vb, key, flags, exp, body)
}

func (client *memcachedClient) getResponse() mcResponse {
	bytesRead, err := io.ReadFull(client.Conn, client.hdrBuf)
	if err != nil || bytesRead != HDR_LEN {
		log.Printf("Error reading message: %s (%d bytes)", err, bytesRead)
		runtime.Goexit()
	}
	res := grokHeader(client.hdrBuf)
	readContents(client.Conn, res)
	return res
}

func readContents(s net.Conn, res mcResponse) {
	readOb(s, res.Extras)
	readOb(s, res.Key)
	readOb(s, res.Body)
}

func grokHeader(hdrBytes []byte) (rv mcResponse) {
	if hdrBytes[0] != mcRES_MAGIC {
		log.Printf("Bad magic: %x", hdrBytes[0])
		runtime.Goexit()
	}
	// rv.Opcode = hdrBytes[1]
	rv.Key = make([]byte, binary.BigEndian.Uint16(hdrBytes[2:]))
	rv.Extras = make([]byte, hdrBytes[4])
	rv.Status = uint16(hdrBytes[7])
	bodyLen := binary.BigEndian.Uint32(hdrBytes[8:]) - uint32(len(rv.Key)) - uint32(len(rv.Extras))
	rv.Body = make([]byte, bodyLen)
	// rv.Opaque = binary.BigEndian.Uint32(hdrBytes[12:])
	rv.Cas = binary.BigEndian.Uint64(hdrBytes[16:])
	return
}

func transmitRequest(o *bufio.Writer, req mcRequest) {
	// 0
	writeByte(o, mcREQ_MAGIC)
	writeByte(o, req.Opcode)
	writeUint16(o, uint16(len(req.Key)))
	// 4
	writeByte(o, uint8(len(req.Extras)))
	writeByte(o, 0)
	writeUint16(o, req.VBucket)
	// 8
	writeUint32(o, uint32(len(req.Body))+
		uint32(len(req.Key))+
		uint32(len(req.Extras)))
	// 12
	writeUint32(o, req.Opaque)
	// 16
	writeUint64(o, req.Cas)
	// The rest
	writeBytes(o, req.Extras)
	writeBytes(o, req.Key)
	writeBytes(o, req.Body)
	o.Flush()
}

func writeBytes(s *bufio.Writer, data []byte) {
	if len(data) > 0 {
		written, err := s.Write(data)
		if err != nil || written != len(data) {
			log.Printf("Error writing bytes:  %s", err)
			runtime.Goexit()
		}
	}
	return

}

func writeByte(s *bufio.Writer, b byte) {
	data := make([]byte, 1)
	data[0] = b
	writeBytes(s, data)
}

func writeUint16(s *bufio.Writer, n uint16) {
	data := []byte{0, 0}
	binary.BigEndian.PutUint16(data, n)
	writeBytes(s, data)
}

func writeUint32(s *bufio.Writer, n uint32) {
	data := []byte{0, 0, 0, 0}
	binary.BigEndian.PutUint32(data, n)
	writeBytes(s, data)
}

func writeUint64(s *bufio.Writer, n uint64) {
	data := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint64(data, n)
	writeBytes(s, data)
}

func readOb(s net.Conn, buf []byte) {
	x, err := io.ReadFull(s, buf)
	if err != nil || x != len(buf) {
		log.Printf("Error reading part: %s", err)
		runtime.Goexit()
	}
}
