package couchbase

import "fmt"

type HeaderMagic int

const (
	mcREQ_MAGIC = 0x80
	mcRES_MAGIC = 0x81
)

type mcCommandCode int

const (
	mcGET        = 0x00
	mcSET        = 0x01
	mcADD        = 0x02
	mcREPLACE    = 0x03
	mcDELETE     = 0x04
	mcINCREMENT  = 0x05
	mcDECREMENT  = 0x06
	mcQUIT       = 0x07
	mcFLUSH      = 0x08
	mcGETQ       = 0x09
	mcNOOP       = 0x0a
	mcVERSION    = 0x0b
	mcGETK       = 0x0c
	mcGETKQ      = 0x0d
	mcAPPEND     = 0x0e
	mcPREPEND    = 0x0f
	mcSTAT       = 0x10
	mcSETQ       = 0x11
	mcADDQ       = 0x12
	mcREPLACEQ   = 0x13
	mcDELETEQ    = 0x14
	mcINCREMENTQ = 0x15
	mcDECREMENTQ = 0x16
	mcQUITQ      = 0x17
	mcFLUSHQ     = 0x18
	mcAPPENDQ    = 0x19
	mcPREPENDQ   = 0x1a
	mcRGET       = 0x30
	mcRSET       = 0x31
	mcRSETQ      = 0x32
	mcRAPPEND    = 0x33
	mcRAPPENDQ   = 0x34
	mcRPREPEND   = 0x35
	mcRPREPENDQ  = 0x36
	mcRDELETE    = 0x37
	mcRDELETEQ   = 0x38
	mcRINCR      = 0x39
	mcRINCRQ     = 0x3a
	mcRDECR      = 0x3b
	mcRDECRQ     = 0x3c
)

type mcResponseStatus int

const (
	mcSUCCESS         = 0x00
	mcKEY_ENOENT      = 0x01
	mcKEY_EEXISTS     = 0x02
	mcE2BIG           = 0x03
	mcEINVAL          = 0x04
	mcNOT_STORED      = 0x05
	mcDELTA_BADVAL    = 0x06
	mcUNKNOWN_COMMAND = 0x81
	mcENOMEM          = 0x82
)

type mcRequest struct {
	Opcode            uint8
	Cas               uint64
	Opaque            uint32
	VBucket           uint16
	Extras, Key, Body []byte
	ResponseChannel   chan mcResponse
}

func (req mcRequest) String() string {
	return fmt.Sprintf("{mcRequest opcode=%x, key='%s'}",
		req.Opcode, req.Key)
}

type mcResponse struct {
	Status            uint16
	Cas               uint64
	Extras, Key, Body []byte
	Fatal             bool
}

func (res mcResponse) String() string {
	return fmt.Sprintf("{mcResponse status=%x keylen=%d, extralen=%d, bodylen=%d}",
		res.Status, len(res.Key), len(res.Extras), len(res.Body))
}

func (res mcResponse) Error() string {
	return fmt.Sprintf("mcResponse status=%x, msg: %s",
		res.Status, string(res.Body))
}

type mcItem struct {
	Cas               uint64
	Flags, Expiration uint32
	Data              []byte
}

const HDR_LEN = 24
