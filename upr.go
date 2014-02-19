// provides client sdk for UPR connections and streaming.
//
// concurrency model for smart UPR client.
//
//     doSession() ----------------*
//         |                       |
//      (spawns)               doEvents()
//         |
//         *--> doReceive()
//         |
//         *--> doReceive()
//         |    ...
//         |    ...
//         *--> doReceive()
//
// a call to StartUprFeed spawns doSession() routine, which further spawns
// doReceive() for each upr-connection. A single upr connection can stream all
// vbucket mutations that are active on the node.
//
// doSession() acts as the supervisor routine for doReceive() worker routines
// and listens for messages and anamolies from doReceive()

package couchbase

// TODO:
//  performance consideration,
//      try to adjust buffer size for feed.C channel

import (
	"encoding/binary"
	"fmt"
	mcd "github.com/dustin/gomemcached"
	mc "github.com/dustin/gomemcached/client"
	"log"
	"time"
)

// constants used for memcached protocol
const (
	uprOPEN        = mcd.CommandCode(0x50) // Open a upr connection with `name`
	uprAddSTREAM   = mcd.CommandCode(0x51) // Sent by ebucketmigrator to upr consumer
	uprCloseSTREAM = mcd.CommandCode(0x52) // Sent by ebucketmigrator to upr consumer
	uprFailoverLOG = mcd.CommandCode(0x54) // Request all known failover ids for restart
	uprStreamREQ   = mcd.CommandCode(0x53) // Stream request from consumer to producer
	uprStreamEND   = mcd.CommandCode(0x55) // Sent by producer when it is going to end stream
	uprSnapshotM   = mcd.CommandCode(0x56) // Sent by producer for a new snapshot
	uprMUTATION    = mcd.CommandCode(0x57) // Notifies SET/ADD/REPLACE/etc.  on the server
	uprDELETION    = mcd.CommandCode(0x58) // Notifies DELETE on the server
	uprEXPIRATION  = mcd.CommandCode(0x59) // Notifies key expiration
	uprFLUSH       = mcd.CommandCode(0x5a) // Notifies vbucket flush
)
const (
	rollBack = mcd.Status(0x23)
)

// FailoverLog is a slice of 2 element array, containing a list of,
// [[vuuid, sequence-no], [vuuid, sequence-no] ...]
type FailoverLog [][2]uint64

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
	name    string                    // name of the connection used in uprOPEN
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
	uprMUTATION: "UPR_MUTATION",
	uprDELETION: "UPR_DELETION",
}

// GetFailoverLogs return a list of vuuid and sequence number for all vbuckets.
func (b *Bucket) GetFailoverLogs(name string) ([]FailoverLog, error) {
	var flog FailoverLog
	var err error

	uprconns, err := connectToNodes(b, name)
	if err != nil {
		return nil, err
	}
	flogs := make([]FailoverLog, 0)
	for vb, uprconn := range vbConns(b, uprconns) {
		if flog, err = requestFailoverLog(uprconn.conn, vb); err != nil {
			break
		}
		flogs = append(flogs, flog)
	}
	if err != nil {
		closeConnections(uprconns)
		return nil, err
	}
	return flogs, nil
}

// StartUprFeed creates a feed that aggregates all mutations for the bucket
// and publishes them as UprEvent on UprFeed:C channel.
func StartUprFeed(
	b *Bucket, name string, streams map[uint16]*UprStream) (*UprFeed, error) {
	var feed *UprFeed
	var err error

	uprconns, err := connectToNodes(b, name)
	if err != nil {
		return nil, err
	}

	vbmap := vbConns(b, uprconns)
	if streams == nil { // Start from beginning
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
		c:       make(chan UprEvent, 10000),
	}
	feed.C = feed.c
	go feed.doSession(uprconns)
	return feed, err
}

// Close UprFeed. Does the opposite of StartUprFeed()
func (feed *UprFeed) Close() {
	close(feed.quit)
	feed.vbmap = nil
	feed.streams = nil
}

// Get stream will return the UprStream structure for vbucket `vbucket`.
// Caller can use the UprStream information to restart the stream later.
func (feed *UprFeed) GetStream(vbno uint16) *UprStream {
	return feed.streams[vbno]
}

// CalculateVector will return a 3-element tuple of
// (vbucket-uuid, startSeq, highSeq) for a give failover-log and last known
// sequence number.
func CalculateVector(lastSeq uint64, flog FailoverLog) (
	vuuid uint64, startseq uint64, highseq uint64) {

	if lastSeq != 0 {
		for _, log := range flog {
			if lastSeq >= log[1] {
				vuuid, startseq, highseq = log[0], lastSeq, log[1]
			}
		}
		vuuid, startseq, highseq = flog[0][0], flog[0][1], flog[0][1]
	}
	return vuuid, startseq, highseq
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

// connect with all servers holding data for `feed.bucket`.
func (feed *UprFeed) doSession(uprconns []*uprConnection) {
	msgch := make(chan msgT, 10000)
	killSwitch := make(chan bool)
	for _, uprconn := range uprconns {
		go doReceive(uprconn, uprconn.host, msgch, killSwitch)
	}

	feed.doEvents(msgch) // Blocking call

	close(killSwitch)
	close(feed.c)
	closeConnections(uprconns)
}

// TODO: This function is not used at present.
func (feed *UprFeed) retryConnections(uprconns []*uprConnection) ([]*uprConnection, bool) {
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

// Upr feed for a bucket will be looping inside this function listening for
// UprEvents from vbucket connections.
func (feed *UprFeed) doEvents(msgch <-chan msgT) (err error) {
loop:
	for {
		select {
		case msg, ok := <-msgch:
			if !ok {
				err = fmt.Errorf("doevents: msgch closed")
			} else if msg.err != nil {
				err = fmt.Errorf("doevents: %v, %v", msg.uprconn.host, msg.err)
			} else {
				err = handleUprMessage(feed, &msg.pkt)
			}
			if err != nil {
				break loop
			}
		case <-feed.quit:
			break loop
		}
	}
	return
}

func handleUprMessage(feed *UprFeed, req *mcd.MCRequest) (err error) {
	vb := uint16(req.Opaque)
	stream := feed.streams[uint16(req.Opaque)]
	switch req.Opcode {
	case uprStreamREQ:
		rollb, flog, err := handleStreamResponse(request2Response(req))
		if err == nil {
			if flog == nil {
				log.Println("ERROR: flog cannot be empty")
			}
			stream.Flog = flog
		} else if err.Error() == "ROLLBACK" {
			uprconn := feed.vbmap[vb]
			flags := uint32(0)
			if stream.Startseq == 0 {
				err := fmt.Errorf("ERROR: Unexpected rollback for start-sequence 0")
				log.Println(err)
				panic(err) // TODO: Can be removed
			}
			stream.Vuuid, stream.Startseq, stream.Highseq =
				CalculateVector(rollb, stream.Flog)
			log.Println("Requesting a rollback for %v to sequence %v", vb, rollb)
			err = requestStream(
				uprconn.conn, flags, req.Opaque, vb, stream.Vuuid,
				stream.Startseq, stream.Endseq, stream.Highseq)
		} else if err != nil {
			log.Println(err)
		}
	case uprMUTATION, uprDELETION:
		e := feed.makeUprEvent(req)
		stream.Startseq = e.Seqno
		feed.c <- e
	case uprStreamEND:
		res := request2Response(req)
		err = fmt.Errorf("Stream %v is ending", uint16(res.Opaque))
	case uprSnapshotM:
	case uprCloseSTREAM, uprEXPIRATION, uprFLUSH, uprAddSTREAM:
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
	case res.Status == rollBack && len(res.Extras) != 8:
		err = fmt.Errorf("invalid rollback %v\n", res.Extras)
	case res.Status == rollBack:
		rollback = binary.BigEndian.Uint64(res.Extras)
		log.Printf("Rollback %v for vb %v\n", rollback, res.Opaque /*vb*/)
		return rollback, flog, fmt.Errorf("ROLLBACK")
	case res.Status != mcd.SUCCESS:
		err = fmt.Errorf("Unexpected status %v", res.Status)
	}
	if err == nil {
		flog, err = parseFailoverLog(res.Body[:])
	}
	return rollback, flog, err
}

// connect to all nodes where bucket `b` is stored. `name` will be passed to
// the UPR producer during UPR_OPEN handshake.
func connectToNodes(b *Bucket, name string) ([]*uprConnection, error) {
	var conn *mc.Client
	var err error

	uprconns := make([]*uprConnection, 0)
	for _, cp := range b.getConnPools() {
		if cp == nil {
			err = fmt.Errorf("go-couchbase: no connection pool")
			break
		}
		if conn, err = cp.Get(); err != nil {
			break
		}
		// A connection can't be used after UPR; Dont' count it against the
		// connection pool capacity
		<-cp.createsem
		if uprconn, err := connectToNode(b, conn, name, cp.host); err == nil {
			uprconns = append(uprconns, uprconn)
		} else {
			break
		}
	}
	if err != nil {
		closeConnections(uprconns)
	}
	return uprconns, nil
}

// connect to individual node.
func connectToNode(
	b *Bucket, conn *mc.Client, name, host string) (*uprConnection, error) {
	var uprconn *uprConnection
	var err error

	if err = uprOpen(conn, name, uint32(0x1) /*flags*/); err != nil {
		return nil, err
	}
	uprconn = &uprConnection{
		host: host,
		conn: conn,
	}
	return uprconn, err
}

// gather a map of vbucket -> uprConnection
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
		err := requestStream(
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

func doReceive(
	uprconn *uprConnection, host string, msgch chan msgT, killSwitch chan bool) {

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

// close memcached connections and set it to nil
func closeConnections(uprconns []*uprConnection) {
	for _, uprconn := range uprconns {
		if uprconn.conn != nil {
			uprconn.conn.Close()
		}
		uprconn.conn = nil
	}
}
