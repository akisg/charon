package breakwater

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

/**
TODO:
0. Update greatestDelay in main loop
0. Find a way to update RTT (how often is a RTT)?
1. Make breakwater thread safe
2. Find efficient way for polling map of longest request times
3. What to do for clients when not enough credits? Use a channel for credits
- A channel for requests (once done, remove one from channel)
- For sleeping requests in 'queue', get waken up whenever there is a change in credits (via a channel)
- Store the number of credits in the channel, and decrement increment whenever done
*/

const RTTInMicrosecond = 5000        // RTT in milliseconds
const TARGETPERCENTAGE float64 = 0.4 // target is 0.4 of SLA as per paper
const STARTCREDITS int64 = 100       // start with 1000 credits

var (
	errMissingMetadata = status.Errorf(codes.InvalidArgument, "missing metadata")
)

// logger is to mock a sophisticated logging system. To simplify the example, we just print out the content.
func logger(format string, a ...interface{}) {
	fmt.Printf("LOG:\t"+format+"\n", a...)
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

/*
DATA STRUCTURES:
1. A global map of all active connections, which stores cIssued, cOC and cDemand
2. A queue of all pending requests, also tracking what time the earliest request is (queue head)
3. cTotal
4. cIssued
*/
type Connection struct {
	issued int64 // issued credits
	demand int64 // number of requests pending
	id     uuid.UUID
}

type Breakwater struct {
	clientMap         sync.Map
	requestMap        sync.Map
	lastUpdateTime    time.Time
	numClients        int64
	cTotal            int64
	cIssued           int64
	aFactor           float64
	bFactor           float64
	SLA               int64
	currGreatestDelay int64
	prevGreatestDelay int64
	id                uuid.UUID
	pendingOutgoing   chan int64
	noCreditBlocker   chan int64
	outgoingCredits   chan int64
}

// Todo: Add fields for gRPC contexts
type request struct {
	reqID          uuid.UUID
	timeDeductions int64
}

func InitBreakwater(bFactor float64, aFactor float64, SLA int64) (bw *Breakwater) {
	bw = &Breakwater{
		clientMap:         sync.Map{},
		lastUpdateTime:    time.Now(),
		numClients:        0,
		cTotal:            STARTCREDITS,
		cIssued:           0,
		bFactor:           bFactor,
		aFactor:           aFactor,
		SLA:               SLA,
		currGreatestDelay: 0,
		prevGreatestDelay: 0,
		id:                uuid.New(),
		// Outgoing buffer drops requests if > 50 requests in queue
		pendingOutgoing: make(chan int64, 50),
		noCreditBlocker: make(chan int64, 1),
		outgoingCredits: make(chan int64, 1),
	}
	// Give a minimum credit of 1, and
	bw.noCreditBlocker <- 1
	bw.outgoingCredits <- 1
	return
}

/*
Register a client if it is not already registered
*/
func (b *Breakwater) RegisterClient(id uuid.UUID, demand int64) (Connection, bool) {

	var c *Connection = &Connection{
		issued: 0,
		demand: demand,
		id:     id,
	}
	storedConn, loaded := b.clientMap.LoadOrStore(id, *c)
	if !loaded {
		b.numClients++
	}
	return storedConn.(Connection), loaded
}

/*
Helper to get current time delay
TODO: Test time delay in nanoseconds
*/
func (b *Breakwater) getDelay() int64 {
	return max(b.currGreatestDelay, b.prevGreatestDelay)
}

/*
Every RTT, update cIssued for consistency and
reset greatestDelay
*/
func (b *Breakwater) rttUpdate() {
	timeSinceLastUpdate := time.Now().Sub(b.lastUpdateTime)
	if timeSinceLastUpdate.Microseconds() > RTTInMicrosecond {
		logger("[Updating credits]: Updating cTotal for this RTT")
		b.lastUpdateTime = time.Now()

		// Re-calculate total issued (should not be too expensive as # clients are limited)
		var totalIssued int64 = 0
		b.clientMap.Range(func(key, value interface{}) bool {
			totalIssued += value.(Connection).issued
			return true
		})
		b.cIssued = totalIssued

		b.updateTotalCredits()

		// Reset greatest delay
		b.prevGreatestDelay = b.currGreatestDelay
		b.currGreatestDelay = 0

		logger("[Updating credits]: cTotal: %d, cIssued: %d", b.cTotal, b.cIssued)
	}
}

/*
Helper to get current demand (not exact due to race conditions, but gives a
fairly precise idea of number of outgoing requests in queue)
*/
func (b *Breakwater) getDemand() (demand int) {
	return len(b.pendingOutgoing)
}

/*
Adds request to the outgoing queue, returns false
and drops request if there are > 50 elements in channel
*/
func (b *Breakwater) queueRequest() bool {
	select {
	case b.pendingOutgoing <- 1:
		return true
	default:
		return false
	}
}

/*
Dequeues request to the outgoing queue,
returns false if queue channel is empty
*/
func (b *Breakwater) dequeueRequest() bool {
	select {
	case <-b.pendingOutgoing:
		return true
	default:
		return false
	}
}

/*
Unblocks blockingCreditQueue
*/
func (b *Breakwater) unblockNoCreditBlock() {
	select {
	case b.noCreditBlocker <- 1:
		return
	default:
		return
	}
}

func (b *Breakwater) UnaryInterceptorClient(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	logger("[Before Req]:	The method name for price table is %s\n", method)

	// retrieve price table for downstream clients queueing delay
	var isDownstream bool = false
	var reqid uuid.UUID
	timeStart := time.Now()
	var reqTimeData request
	md, ok := metadata.FromIncomingContext(ctx)
	if ok && len(md["reqid"]) > 0 {
		logger("[Before queue]:	is downstream request\n")
		reqid, _ := uuid.Parse(md["reqid"][0])
		r, ok := b.requestMap.Load(reqid)
		isDownstream = true
		if ok {
			reqTimeData = r.(request)
		} else {
			b.requestMap.Store(reqid, request{reqid, 0})
		}
	} else {
		// This is first upstream client / end user
		reqid = uuid.New()
	}

	// Check if queue is too long
	var added bool = b.queueRequest()
	if !added {
		return status.Errorf(codes.ResourceExhausted, "queue too long")
	}

	for {
		// Unblock if credits are available
		logger("[Waiting in queue]:	Checking if unblock available\n")
		<-b.noCreditBlocker
		logger("[Waiting in queue]:	Unblock available, checking if credits are sufficient\n")
		// Check actual number of credits (channel for binary semaphore)
		creditBalance := <-b.outgoingCredits
		if creditBalance > 0 {
			// Decrement credit balance
			creditBalance--
			// Send updated credit balance
			b.outgoingCredits <- creditBalance

			// If there are still credits, unblock other requests
			if creditBalance > 0 {
				b.noCreditBlocker <- 1
			}
			logger("[Waiting in queue]:	Unblocked with credit balance %d\n", creditBalance)
			break
		} else {
			// Else, return to binary semaphore and keep looping
			b.outgoingCredits <- creditBalance
			// TODO: Add a timeout here
		}

		// noCreditBlocker will unblock again when another request returns with
		// more credits
	}

	// Get demand
	demand := b.getDemand()
	logger("[Waiting in queue]:	demand is %d\n", demand)
	ctx = metadata.AppendToOutgoingContext(ctx, "demand", strconv.Itoa(demand), "id", b.id.String(), "reqid", reqid.String())

	// After breaking out of request loop, remove request from queue and send request
	// This should never be blocked
	logger("[Waiting in queue]:	Dequeueing and handling request\n")
	b.dequeueRequest()

	var header metadata.MD // variable to store header and trailer
	err := invoker(ctx, method, req, reply, cc, grpc.Header(&header))
	if err != nil {
		// The request failed. This error should be logged and examined.
		// log.Println(err)
		return err
	}

	if len(header["credits"]) > 0 {
		cXNew, _ := strconv.ParseInt(header["credits"][0], 10, 64)
		logger("[Received Resp]:	Updated spend credits is %d\n", cXNew)
		// fmt.Print("This is the header for okok lmao ", header["okok"][0])

		// Update credits and unblock other requests
		<-b.outgoingCredits
		b.outgoingCredits <- max(cXNew, 1)
		b.unblockNoCreditBlock()
	} else {
		logger("[Received Resp]:	No spend credits in response\n")
		// If no response, then just put to 1
		outgoingCredits := <-b.outgoingCredits
		b.outgoingCredits <- max(outgoingCredits, 1)
		b.unblockNoCreditBlock()
	}

	// Update time deductions
	timeEnd := time.Now()
	timeElapsed := timeEnd.Sub(timeStart).Microseconds()
	if isDownstream {
		reqTimeData.timeDeductions += timeElapsed
		b.requestMap.Store(reqTimeData.reqID, reqTimeData)
		logger("[Received Resp]:	Downstream client - total time deduction %d\n", reqTimeData.timeDeductions)
	}

	return err
}

// Helper to calculate A additive Factor
func (b *Breakwater) getAdditiveFactor() int64 {
	return max(int64(math.Round(b.aFactor*float64(b.numClients))), 1)
}

/*
Function: Update cOC
Runs once every RTT
1. Check the queueing delay against SLA
2. If queueing delay is within SLA, increase cTotal additively
3. If queueing delay is beyond SLA, decrease cTotal multiplicatively
*/
func (b *Breakwater) updateTotalCredits() {
	delay := float64(b.getDelay())

	var target float64 = float64(b.SLA) * TARGETPERCENTAGE

	if delay < target {
		addFactor := b.getAdditiveFactor()
		b.cTotal += addFactor
	} else {
		halfTotal := int64(math.Round(0.5 * float64(b.cTotal)))
		adjustingFactor := 1.0 - b.bFactor*((delay-target)/target)
		adjustedTotal := int64(math.Round(float64(b.cTotal) * adjustingFactor))
		b.cTotal = max(halfTotal, adjustedTotal)
		// If cTotal is 0, set to 1 (should have no need since on client side minimum credits is 1)
		// b.cTotal = max(b.cTotal, 1)
		// TODO: Is there need to send negative credits here? Breakwater is unclear
		// But likely not
	}
}

/*
Function: Update credits issued to a connection
Runs once every time a request is issued
1. Retrieve demand from metadata
2. Calculate cOC (the new overcommitment value, which is proportional to leftover, or 1)
3. If cIssued < cTotal:
Ideal to be issued is demandX + cOC, but limited by total available (cTotal - cIssued)
4. If cIssued >= cTotal:
We need to rate limit, so we issue demandX + cOC, OR just cX - 1 (ie we do not grant any new credits)
*/
func (b *Breakwater) updateCreditsToIssue(clientID uuid.UUID, demand int64) (cNew int64) {

	connection, _ := b.clientMap.Load(clientID)
	c := connection.(Connection)
	cOld := float64(c.issued)
	fmt.Println("Old credits: ", cOld)
	cOC := b.calculateCreditsOvercommitted()
	fmt.Println("Credits overcommitted: ", cOC)

	fmt.Println("total Issued credits: ", b.cIssued)
	fmt.Println("total credits: ", b.cTotal)
	fmt.Println("Num clients: ", b.numClients)

	// Here, b.cIssued is OVERALL issued credits, while c.issued is credits issued to a connection
	if b.cIssued < b.cTotal {
		// There is still space to issue credits
		cAvail := float64(b.cTotal - b.cIssued)
		cNew = int64(math.Min(float64(demand+cOC), cOld+cAvail))
		fmt.Println("there was available credits. cNew: ", cNew)
	} else {
		// At credit limit, so we only decrease
		cNew = int64(math.Min(float64(demand+cOC), float64(cOld)-1))
		fmt.Println("no available credits. cNew: ", cNew)
	}
	c.issued = cNew
	// fmt.Println("Issued credits: ", cNew)
	b.clientMap.Store(clientID, c)

	b.cIssued = b.cIssued + (cNew - int64(cOld))
	fmt.Println("new total issued credits: ", b.cIssued)
	return
}

/*
Number of over-committed credits per client
*/
func (b *Breakwater) calculateCreditsOvercommitted() int64 {
	return int64(math.Round(math.Max(float64(b.cTotal-b.cIssued)/float64(b.numClients), 1)))
}

/*
The server side interceptor
It should
1. Manage connections and register requests
2. Check for queueing delays
3. Update credits issued
4. Occassionally update cTotal
*/
func (b *Breakwater) UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errMissingMetadata
	}
	// getMethodInfo(ctx)
	logger("[Received Req]:	The method name for request is %s", info.FullMethod)

	fmt.Println("Printing metadata: ", md)

	demand, err1 := strconv.ParseInt(md["demand"][0], 10, 64)
	clientId, err2 := uuid.Parse(md["id"][0])
	reqId, err3 := uuid.Parse(md["reqid"][0])

	if err1 != nil || err2 != nil || err3 != nil {
		logger("[Received Req]:	Error: malformed metadata")
		return nil, errMissingMetadata
	}

	logger("[Received Req]:	The demand is %d\n", demand)
	logger("[Received Req]:	The clientid is %s\n", clientId)
	logger("[Received Req]:	reqid is %s\n", reqId)

	// Register client if unregistered
	b.RegisterClient(clientId, demand)

	issuedCredits := b.updateCreditsToIssue(clientId, demand)
	logger("[Received Req]:	issued credits is %d\n", issuedCredits)

	// Piggyback updated credits issued
	header := metadata.Pairs("credits", strconv.FormatInt(issuedCredits, 10))
	grpc.SendHeader(ctx, header)

	// Start the timer
	b.requestMap.Store(reqId, request{reqID: reqId, timeDeductions: 0})
	time_start := time.Now()

	// Call the handler
	logger("[Handling Req]:	Handling req \n")
	m, err := handler(ctx, req)

	// End the timer
	time_end := time.Now()
	elapsed := time_end.Sub(time_start).Microseconds()
	reqTimer, _ := b.requestMap.Load(reqId)
	timeDeductions := reqTimer.(request).timeDeductions
	b.requestMap.Delete(reqId)
	// Account for deductions of outgoing calls
	delay := elapsed - timeDeductions

	logger("[Req handled]: Time delay was %d after deduction of %d\n", delay, timeDeductions)

	// Update delay as neccessary
	if delay > b.currGreatestDelay {
		b.currGreatestDelay = delay
	}

	// Does update once every rtt
	b.rttUpdate()

	if err != nil {
		logger("RPC failed with error %v", err)
	}
	return m, err
}

// unaryInterceptor is an example unary interceptor.

/*
TODO List (harder problems):
1. How to get ID of endusers / clients (similar hard-coded message) (DONE)
2. How to check metrics against SLAs (prometheus, and chaining unary incerceptors) (DONE)
3. AQM - code written (DONE)
4. How to check queue of requests client side (may have to use metrics)
*/
