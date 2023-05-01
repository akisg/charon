package breakwater

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/golang-collections/go-datastructures/queue"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// SERVER

var (
	errMissingMetadata = status.Errorf(codes.InvalidArgument, "missing metadata")
)

// logger is to mock a sophisticated logging system. To simplify the example, we just print out the content.
func logger(format string, a ...interface{}) {
	fmt.Printf("LOG:\t"+format+"\n", a...)
}

/*
DATA STRUCTURES:
1. A global map of all active connections, which stores cIssued, cOC and cDemand
2. A queue of all pending requests, also tracking what time the earliest request is (queue head)
3. cTotal
4. cIssued
*/
type Connection struct {
	cUnused int64 // Balance of credits
	cOC     int64 // Overcommit amount
	cDemand int64 // number of requests pending
	cX      int64 // number of credits issued
}

type Breakwater struct {
	clientMap    sync.Map
	numClients   int64
	requestQueue *queue.Queue
	cTotal       int64
	cIssued      int64
	aFactor      float64
	bFactor      float64
	SLA          int64
}

// Todo: Add fields for gRPC contexts
type request struct {
	clientID int64
	time     int64
}

func newBreakwater(bFactor float64, aFactor float64, SLA int64) *Breakwater {
	return &Breakwater{
		clientMap:    sync.Map{},
		numClients:   0,
		requestQueue: queue.New(0),
		cTotal:       0,
		cIssued:      0,
		bFactor:      bFactor,
		aFactor:      aFactor,
		SLA:          SLA,
	}
}

/*
Helper to get current time delay
TODO: Test time delay in seconds
*/
func getDelay(q *queue.Queue) int64 {
	if q.Empty() {
		return 0
	} else {
		oldestReq, _ := (q.Get(1))
		oldestTime := oldestReq[0].(request).time
		return time.Now().UnixNano() - oldestTime
	}
}

/*
Function: Update cOC
Runs once every RTT
1. Check the queueing delay against SLA
2. If queueing delay is within SLA, increase cTotal additively
3. If queueing delay is beyond SLA, decrease cTotal multiplicatively
*/
func (b *Breakwater) updateCTotal() {
	delay := getDelay(b.requestQueue)
	addFactor := b.getAdditiveFactor()

	if delay < b.SLA {
		b.cTotal += int64(addFactor) // TODO: How does casting round?
	} else {
		b.cTotal = int64(math.Max(1.0-b.bFactor*(float64(delay)/float64(b.SLA)), 0.5)) // TODO: How does casting round?
		// TODO: Do I need to send negative credits here? Breakwater is unclear
	}
}

// Helper to calculate A additive Factor
func (b *Breakwater) getAdditiveFactor() (addFactor float64) {
	addFactor = math.Max(b.aFactor*float64(b.numClients), 1)
	return
}

/*
Function: Update cX (credits issued to a connection)
Runs once every time a request is issued
1. Retrieve demandX from metadata
2. Calculate cOC (the new overcommitment value, which is proportional to leftover, or 1)
3. If cIssued < cTotal:
Ideal to be issued is demandX + cOC, but limited by total available (cTotal - cIssued)
4. If cIssued >= cTotal:
We need to rate limit, so we issue demandX + cOC, OR just cX - 1 (ie we do not grant any new credits)
*/
func (b *Breakwater) updateCX(clientID int64, demandX int64) (cXNew int64) {
	// TODO: Get cX from clientMap
	connection, _ := b.clientMap.Load(clientID)
	cXOld := connection.(Connection).cX
	cOC := b.calculateCOC()

	if b.cIssued < b.cTotal {
		cAvail := b.cTotal - b.cIssued
		cXNew = int64(math.Min(float64(demandX+cOC), float64(cXOld)+float64(cAvail)))
	} else {
		cXNew = int64(math.Min(float64(demandX+cOC), float64(cXOld)-1))
	}
	return
}

func (b *Breakwater) calculateCOC() (cOC int64) {
	cOC = int64(math.Max(float64(b.cTotal-b.cIssued)/float64(b.numClients), 1))
	return
}

// Include incorperates (add to own price, so far) the downstream price table to its own price table.
func (b *Breakwater) Include(ctx context.Context, method string, downstreamPrice int64) (int64, error) {

	// Update the downstream price.

	return 0, nil
}

func (b *Breakwater) UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// This is the server side interceptor, it should check tokens, update price, do overload handling and attach price to response
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errMissingMetadata
	}

	// getMethodInfo(ctx)
	logger("[Received Req]:	The method name for request is ")
	logger(info.FullMethod)

	logger("[Received Req]:	demand is %s\n", md["demand"])
	logger("[Received Req]:	demand is %s\n", md["id"])
	// Jiali: overload handler, do AQM, deduct the tokens on the request, update price info

	tok, err := strconv.ParseInt(md["demand"][0], 10, 64)
	id, err := strconv.ParseInt(md["id"][0], 10, 64) // TODO: Retriving ID from metadata
	// TODO: Error handling

	// Update CX
	cXNew := b.updateCX(id, tok)

	// TODO: Update global cIssued and cTotals

	// TODO: Update cX

	// TODO: What goes in header, what goes in MD?
	cXNew_string := strconv.FormatInt(cXNew, 10)
	header := metadata.Pairs("cXNew", cXNew_string)
	logger("[Preparing Resp]:	Total price is %s\n", cXNew_string)
	grpc.SendHeader(ctx, header)

	// TO DO: Left with updating metadata
	tok_string := strconv.FormatInt(100000000, 10)
	// [critical] Jiali: Being outgoing seems to be critical for us.
	ctx = metadata.AppendToOutgoingContext(ctx, "tokens", tok_string)
	// ctx = metadata.NewOutgoingContext(ctx, md)

	logger("[Preparing Sub Req]:	Token left is %s\n", tok_string)
	m, err := handler(ctx, req)

	if err != nil {
		logger("RPC failed with error %v", err)
	}
	return m, err
}

/*
TODO List (harder problems):
1. How to get ID of endusers / clients (similar hard-coded message)
2. How to check metrics against SLAs (prometheus, and chaining unary incerceptors)
3. AQM - code written
4. How to check queue of requests client side (may have to use metrics)
*/
