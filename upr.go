// goupr package provides client sdk for UPR connections and streaming.
package couchbase

import (
	"encoding/binary"
	"fmt"
	mcd "github.com/dustin/gomemcached"
	mc "github.com/dustin/gomemcached/client"
	"log"
	"time"
)

const (
	UPR_OPEN         = mcd.CommandCode(0x50) // Open a upr connection with `name`
	UPR_ADD_STREAM   = mcd.CommandCode(0x51) // Sent by ebucketmigrator to upr consumer
	UPR_CLOSE_STREAM = mcd.CommandCode(0x52) // Sent by ebucketmigrator to upr consumer
	UPR_FAILOVER_LOG = mcd.CommandCode(0x54) // Request all known failover ids for restart
	UPR_STREAM_REQ   = mcd.CommandCode(0x53) // Stream request from consumer to producer
	UPR_STREAM_END   = mcd.CommandCode(0x55) // Sent by producer when it is going to end stream
	UPR_SNAPSHOTM    = mcd.CommandCode(0x56) // Sent by producer for a new snapshot
	UPR_MUTATION     = mcd.CommandCode(0x57) // Notifies SET/ADD/REPLACE/etc.  on the server
	UPR_DELETION     = mcd.CommandCode(0x58) // Notifies DELETE on the server
	UPR_EXPIRATION   = mcd.CommandCode(0x59) // Notifies key expiration
	UPR_FLUSH        = mcd.CommandCode(0x5a) // Notifies vbucket flush
)

const (
	ROLLBACK = mcd.Status(0x23)
)

// UprStream will maintain stream information per vbucket
type UprStream struct {
	Vbucket  uint16 // vbucket id
	Vuuid    uint64 // vbucket uuid
	Opaque   uint32 // messages from producer to this stream have same value
	Highseq  uint64 // to be supplied by the application
	Startseq uint64 // to be supplied by the application
	Endseq   uint64 // to be supplied by the application
	Flog     FailoverLog
}

// UprEvent objects will be created for stream mutations and deletions and
// published on UprFeed:C channel.
type UprEvent struct {
	Bucket  string // bucket name for this event
	Opstr   string // TODO: Make this consistent with TAP
	Vbucket uint16 // vbucket number
	Seqno   uint64 // sequence number
	Key     []byte
	Value   []byte
}

// UprFeed is per bucket structure managing connections to all nodes and
// vbucket streams.
type UprFeed struct {
	// Exported channel where an aggregate of all UprEvent are sent to app.
	C <-chan UprEvent
	c chan UprEvent

	bucket  *Bucket                   // upr client for bucket
	name    string                    // name of the connection used in UPR_OPEN
	vbmap   map[uint16]*uprConnection // vbucket-number->connection mapping
	streams map[uint16]*UprStream     // vbucket-number->stream mapping
	// `quit` channel is used to signal that a Close() is called on UprFeed
	quit chan bool
}

// uprConnection structure maintains an active memcached connection.
type uprConnection struct {
	host string // host to which `mc` is connected.
	conn *mc.Client
}

type msgT struct {
	uprconn *uprConnection
	pkt     mcd.MCRequest
	err     error
}

var eventTypes = map[mcd.CommandCode]string{ // Refer UprEvent
	UPR_MUTATION: "UPR_MUTATION",
	UPR_DELETION: "UPR_DELETION",
}

// StartUprFeed creates a feed that aggregates all mutations for the bucket
// and publishes them as UprEvent on UprFeed:C channel.
func StartUprFeed(b *Bucket, name string,
	streams map[uint16]*UprStream) (feed *UprFeed, err error) {

	uprconns, err := connectToNodes(b, name)
	if err == nil {
		vbmap := vbConns(b, uprconns)
		if streams == nil {
			streams = freshStreams(vbmap)
		}
		streams, err = startStreams(streams, vbmap)
		if err != nil {
			closeConnections(uprconns)
			return nil, err
		}
		feed = &UprFeed{
			bucket:  b,
			name:    name,
			vbmap:   vbmap,
			streams: streams,
			quit:    make(chan bool),
			c:       make(chan UprEvent, 16),
		}
		feed.C = feed.c
		go feed.doSession(uprconns)
	}
	return feed, err
}

// GetFailoverLogs return a list of vuuid and sequence number for all
// vbuckets.
func GetFailoverLogs(b *Bucket, name string) ([]FailoverLog, error) {

	var flog FailoverLog
	var err error
	uprconns, err := connectToNodes(b, name)
	if err == nil {
		flogs := make([]FailoverLog, 0)
		vbmap := vbConns(b, uprconns)
		for vb, uprconn := range vbmap {
			if flog, err = RequestFailoverLog(uprconn.conn, vb); err != nil {
				return nil, err
			}
			flogs = append(flogs, flog)
		}
		closeConnections(uprconns)
		return flogs, nil
	}
	return nil, err
}

// Close UprFeed. Does the opposite of StartUprFeed()
func (feed *UprFeed) Close() {
	if feed.hasQuit() {
		return
	}
	close(feed.quit)
	feed.vbmap = nil
	feed.streams = nil
}

// Get stream will return the UprStream structure for vbucket `vbucket`.
// Caller can use the UprStream information to restart the stream later.
func (feed *UprFeed) GetStream(vbno uint16) *UprStream {
	return feed.streams[vbno]
}

func freshStreams(vbmaps map[uint16]*uprConnection) map[uint16]*UprStream {
	streams := make(map[uint16]*UprStream)
	for vbno := range vbmaps {
		streams[vbno] = &UprStream{
			Vbucket:  vbno,
			Vuuid:    0,
			Opaque:   uint32(vbno),
			Highseq:  0,
			Startseq: 0,
			Endseq:   0xFFFFFFFFFFFFFFFF,
		}
	}
	return streams
}

// connect with all servers holding data for `feed.bucket` and try reconnecting
// until `feed` is closed.
func (feed *UprFeed) doSession(uprconns []*uprConnection) {
	msgch := make(chan msgT)
	killSwitch := make(chan bool)

	for _, uprconn := range uprconns {
		go doReceive(uprconn, uprconn.host, msgch, killSwitch)
	}
	feed.doEvents(msgch)

	close(killSwitch)
	closeConnections(uprconns)
	close(feed.c)
}

// TODO: This function is not used at present.
func (feed *UprFeed) retryConnections(
	uprconns []*uprConnection) ([]*uprConnection, bool) {

	var err error

	log.Println("Retrying connections ...")
	retryInterval := initialRetryInterval
	for {
		closeConnections(uprconns)
		uprconns, err = connectToNodes(feed.bucket, feed.name)
		if err == nil {
			feed.vbmap = vbConns(feed.bucket, uprconns)
			feed.streams, err = startStreams(feed.streams, feed.vbmap)
			if err == nil {
				return uprconns, false
			}
		} else {
			log.Println("retry:", err)
		}
		log.Printf("Retrying after %v seconds ...", retryInterval)
		select {
		case <-time.After(retryInterval):
		case <-feed.quit:
			return nil, true
		}
		if retryInterval *= 2; retryInterval > maximumRetryInterval {
			retryInterval = maximumRetryInterval
		}
	}
	return uprconns, false
}

func (feed *UprFeed) doEvents(msgch <-chan msgT) bool {
	for {
		select {
		case msg, ok := <-msgch:
			if !ok {
				return false
			}
			if msg.err != nil {
				log.Printf(
					"Received error from %v: %v\n", msg.uprconn.host, msg.err)
				return false
			} else if err := handleUprMessage(feed, &msg.pkt); err != nil {
				log.Println(err)
				return false
			}
		case <-feed.quit:
			return true
		}
	}
}

func handleUprMessage(feed *UprFeed, req *mcd.MCRequest) (err error) {
	vb := uint16(req.Opaque)
	stream := feed.streams[uint16(req.Opaque)]
	switch req.Opcode {
	case UPR_STREAM_REQ:
		rollb, flog, err := handleStreamResponse(Request2Response(req))
		if err == nil {
			if flog != nil {
				stream.Flog = flog
			} else if rollb > 0 {
				uprconn := feed.vbmap[vb]
				log.Println("Requesting a rollback for %v to sequence %v",
					vb, rollb)
				flags := uint32(0)
				err = RequestStream(
					uprconn.conn, flags, req.Opaque, vb, stream.Vuuid,
					rollb, stream.Endseq, stream.Highseq)
			}
		}
	case UPR_MUTATION, UPR_DELETION:
		e := feed.makeUprEvent(req)
		stream.Startseq = e.Seqno
		feed.c <- e
	case UPR_STREAM_END:
		res := Request2Response(req)
		err = fmt.Errorf("Stream %v is ending", uint16(res.Opaque))
	case UPR_SNAPSHOTM:
	case UPR_CLOSE_STREAM, UPR_EXPIRATION, UPR_FLUSH, UPR_ADD_STREAM:
		err = fmt.Errorf("Opcode %v not implemented", req.Opcode)
	default:
		err = fmt.Errorf("ERROR: un-known opcode received %v", req)
	}
	return
}

func handleStreamResponse(res *mcd.MCResponse) (uint64, FailoverLog, error) {
	var rollback uint64
	var err error
	var flog FailoverLog

	switch {
	case res.Status == ROLLBACK && len(res.Extras) != 8:
		err = fmt.Errorf("invalid rollback %v\n", res.Extras)
	case res.Status == ROLLBACK:
		rollback = binary.BigEndian.Uint64(res.Extras)
	case res.Status != mcd.SUCCESS:
		err = fmt.Errorf("Unexpected status %v", res.Status)
	}
	if err == nil {
		if rollback > 0 {
			return rollback, flog, err
		} else {
			flog, err = ParseFailoverLog(res.Body[:])
		}
	}
	return rollback, flog, err
}

func connectToNodes(b *Bucket, name string) ([]*uprConnection, error) {
	var conn *mc.Client
	var err error

	uprconns := make([]*uprConnection, 0)
	for _, cp := range b.getConnPools() {
		if cp == nil {
			return nil, fmt.Errorf("go-couchbase: no connection pool")
		}
		if conn, err = cp.Get(); err != nil {
			return nil, err
		}
		// A connection can't be used after UPR; Dont' count it against the
		// connection pool capacity
		<-cp.createsem
		if uprconn, err := connectToNode(b, conn, name, cp.host); err != nil {
			return nil, err
		} else {
			uprconns = append(uprconns, uprconn)
		}
	}
	return uprconns, nil
}

func connectToNode(b *Bucket, conn *mc.Client,
	name, host string) (uprconn *uprConnection, err error) {
	if err = UprOpen(conn, name, uint32(0x1) /*flags*/); err != nil {
		return
	}
	uprconn = &uprConnection{
		host: host,
		conn: conn,
	}
	log.Printf("Connected to host %v (%p)\n", host, conn)
	return
}

func vbConns(b *Bucket, uprconns []*uprConnection) map[uint16]*uprConnection {

	servers, vbmaps := b.VBSMJson.ServerList, b.VBSMJson.VBucketMap
	vbconns := make(map[uint16]*uprConnection)
	for _, uprconn := range uprconns {
		// Collect vbuckets under this connection
		for vbno := range vbmaps {
			host := servers[vbmaps[int(vbno)][0]]
			if uprconn.host == host {
				vbconns[uint16(vbno)] = uprconn
			}
		}
	}
	return vbconns
}

func startStreams(streams map[uint16]*UprStream,
	vbmap map[uint16]*uprConnection) (map[uint16]*UprStream, error) {

	for vb, uprconn := range vbmap {
		stream := streams[vb]
		flags := uint32(0)
		err := RequestStream(
			uprconn.conn, flags, uint32(vb), vb, stream.Vuuid,
			stream.Startseq, stream.Endseq, stream.Highseq)
		if err != nil {
			return nil, err
		}
		log.Printf(
			"Posted stream request for vbucket %v from %v (vuuid:%v)\n",
			vb, stream.Startseq, stream.Vuuid)
	}
	return streams, nil
}

func doReceive(uprconn *uprConnection, host string,
	msgch chan msgT, killSwitch chan bool) {

	var hdr [mcd.HDR_LEN]byte
	var msg msgT
	var pkt mcd.MCRequest
	var err error

	mcconn := uprconn.conn.Hijack()

loop:
	for {
		if _, err = pkt.Receive(mcconn, hdr[:]); err != nil {
			msg = msgT{uprconn: uprconn, err: err}
		} else {
			msg = msgT{uprconn: uprconn, pkt: pkt}
		}
		select {
		case msgch <- msg:
		case <-killSwitch:
			break loop
		}
	}
	return
}

func (feed *UprFeed) makeUprEvent(req *mcd.MCRequest) UprEvent {
	bySeqno := binary.BigEndian.Uint64(req.Extras[:8])
	e := UprEvent{
		Bucket:  feed.bucket.Name,
		Opstr:   eventTypes[req.Opcode],
		Vbucket: req.VBucket,
		Seqno:   bySeqno,
		Key:     req.Key,
		Value:   req.Body,
	}
	return e
}

func (feed *UprFeed) hasQuit() bool {
	select {
	case <-feed.quit:
		return true
	default:
		return false
	}
}

func closeConnections(uprconns []*uprConnection) {
	for _, uprconn := range uprconns {
		log.Printf("Closing connection for %v: %p\n", uprconn.host, uprconn.conn)
		if uprconn.conn != nil {
			uprconn.conn.Close()
		}
		uprconn.conn = nil
	}
}
