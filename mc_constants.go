package couchbase

import "fmt"

type HeaderMagic int

const (
	REQ_MAGIC = 0x80
	RES_MAGIC = 0x81
)

type CommandCode int

const (
	GET        = 0x00
	SET        = 0x01
	ADD        = 0x02
	REPLACE    = 0x03
	DELETE     = 0x04
	INCREMENT  = 0x05
	DECREMENT  = 0x06
	QUIT       = 0x07
	FLUSH      = 0x08
	GETQ       = 0x09
	NOOP       = 0x0a
	VERSION    = 0x0b
	GETK       = 0x0c
	GETKQ      = 0x0d
	APPEND     = 0x0e
	PREPEND    = 0x0f
	STAT       = 0x10
	SETQ       = 0x11
	ADDQ       = 0x12
	REPLACEQ   = 0x13
	DELETEQ    = 0x14
	INCREMENTQ = 0x15
	DECREMENTQ = 0x16
	QUITQ      = 0x17
	FLUSHQ     = 0x18
	APPENDQ    = 0x19
	PREPENDQ   = 0x1a
	RGET       = 0x30
	RSET       = 0x31
	RSETQ      = 0x32
	RAPPEND    = 0x33
	RAPPENDQ   = 0x34
	RPREPEND   = 0x35
	RPREPENDQ  = 0x36
	RDELETE    = 0x37
	RDELETEQ   = 0x38
	RINCR      = 0x39
	RINCRQ     = 0x3a
	RDECR      = 0x3b
	RDECRQ     = 0x3c
)

type ResponseStatus int

const (
	SUCCESS         = 0x00
	KEY_ENOENT      = 0x01
	KEY_EEXISTS     = 0x02
	E2BIG           = 0x03
	EINVAL          = 0x04
	NOT_STORED      = 0x05
	DELTA_BADVAL    = 0x06
	UNKNOWN_COMMAND = 0x81
	ENOMEM          = 0x82
)

type MCRequest struct {
	Opcode            uint8
	Cas               uint64
	Opaque            uint32
	VBucket           uint16
	Extras, Key, Body []byte
	ResponseChannel   chan MCResponse
}

func (req MCRequest) String() string {
	return fmt.Sprintf("{MCRequest opcode=%x, key='%s'}",
		req.Opcode, req.Key)
}

type MCResponse struct {
	Status            uint16
	Cas               uint64
	Extras, Key, Body []byte
	Fatal             bool
}

func (res MCResponse) String() string {
	return fmt.Sprintf("{MCResponse status=%x keylen=%d, extralen=%d, bodylen=%d}",
		res.Status, len(res.Key), len(res.Extras), len(res.Body))
}

func (res MCResponse) Error() string {
	return fmt.Sprintf("MCResponse status=%x, msg: %s",
		res.Status, string(res.Body))
}

type MCItem struct {
	Cas               uint64
	Flags, Expiration uint32
	Data              []byte
}

const HDR_LEN = 24
