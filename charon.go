package charon

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"strconv"
	"sync"

	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	// InsufficientTokens is returned by the Limiter in case the number of requests overflows the capacity of a Limiter.
	InsufficientTokens = errors.New("Received insufficient tokens, trigger load shedding.")
	RateLimited        = errors.New("Insufficient tokens to send, trigger rate limit.")
)

// PriceTable implements the Charon price table
type PriceTable struct {
	// The following lockfree hashmap should contain total price, selfprice and downstream price
	// initprice is the price table's initprice.
	initprice          int64
	nodeName           string
	callMap            map[string]interface{}
	priceTableMap      sync.Map
	rateLimiting       bool
	loadShedding       bool
	pinpointThroughput bool
	pinpointLatency    bool
	rateLimiter        chan int64
	// updateRate is the rate at which price should be updated at least once.
	tokensLeft         int64
	tokenUpdateRate    time.Duration
	lastUpdateTime     time.Time
	tokenUpdateStep    int64
	throughtputCounter int64
	priceUpdateRate    time.Duration
	latencyThreshold   time.Duration
	debug              bool
}

// NewPriceTable creates a new instance of PriceTable.
// func NewPriceTable(initprice int64, callmap sync.Map, pricetable sync.Map) *PriceTable {
func NewPriceTable(initprice int64, nodeName string, callmap map[string]interface{}) (priceTable *PriceTable) {
	priceTable = &PriceTable{
		initprice:          initprice,
		nodeName:           nodeName,
		callMap:            callmap,
		priceTableMap:      sync.Map{},
		rateLimiting:       false,
		loadShedding:       true,
		pinpointThroughput: false,
		pinpointLatency:    true,
		rateLimiter:        make(chan int64, 1),
		tokensLeft:         10,
		tokenUpdateRate:    time.Millisecond * 10,
		lastUpdateTime:     time.Now(),
		tokenUpdateStep:    1,
		throughtputCounter: 0,
		priceUpdateRate:    time.Millisecond * 10,
		latencyThreshold:   time.Millisecond * 20,
		debug:              false,
	}
	// priceTable.rateLimiter <- 1
	// Only refill the tokens when the interceptor is for enduser.
	if priceTable.nodeName == "client" {
		go priceTable.tokenRefill()
	} else if priceTable.pinpointThroughput {
		go priceTable.decrementCounter()
	}

	return priceTable
}

func (pt *PriceTable) Increment() {
	atomic.AddInt64(&pt.throughtputCounter, 1)
}

func (pt *PriceTable) Decrement(step int64) {
	atomic.AddInt64(&pt.throughtputCounter, -step)
}

func (pt *PriceTable) GetCount() int64 {
	// return atomic.LoadInt64(&cc.throughtputCounter)
	return atomic.SwapInt64(&pt.throughtputCounter, 0)
}

// decrementCounter decrements the counter by 2x every x milliseconds.
func (pt *PriceTable) decrementCounter() {
	for range time.Tick(pt.priceUpdateRate) {
		pt.Decrement(20)

		// Create an empty context
		ctx := context.Background()
		pt.UpdateOwnPrice(ctx, pt.GetCount() > 0)
	}
}

// tokenRefill is a goroutine that refills the tokens in the price table.
func (pt *PriceTable) tokenRefill() {
	for range time.Tick(pt.tokenUpdateRate) {
		pt.tokensLeft += pt.tokenUpdateStep
		pt.lastUpdateTime = time.Now()
		pt.unblockRateLimiter()
		logger("[TokenRefill]: Tokens refilled. Tokens left: %d\n", pt.tokensLeft)
	}
}

/*
Unblocks rateLimiter channel.
*/
func (pt *PriceTable) unblockRateLimiter() {
	select {
	case pt.rateLimiter <- 1:
		return
	default:
		return
	}
}

// RateLimiting is for the end user (human client) to check the price and ratelimit their calls when tokens < prices.
func (pt *PriceTable) RateLimiting(ctx context.Context, tokens int64, methodName string) error {
	// downstreamName, _ := t.callMap.Load(methodName)
	downstreamName, _ := pt.callMap[methodName]
	servicePrice_string, _ := pt.priceTableMap.LoadOrStore(downstreamName, pt.initprice)
	servicePrice := servicePrice_string.(int64)

	extratoken := tokens - servicePrice
	logger("[Ratelimiting]: Checking Request. Token is %d, %s price is %d\n", tokens, downstreamName, servicePrice)

	if extratoken < 0 {
		logger("[Prepare Req]: Request blocked for lack of tokens.")
		return RateLimited
	}
	return nil
}

func (pt *PriceTable) RetrieveDSPrice(ctx context.Context, methodName string) (int64, error) {
	// retrive downstream node name involved in the request from callmap.
	// downstreamNames, _ := t.callMap.Load(methodName)
	downstreamNames, _ := pt.callMap[methodName]
	var downstreamPriceSum int64
	// var downstreamPrice int64
	if downstreamNamesSlice, ok := downstreamNames.([]string); ok {
		for _, downstreamName := range downstreamNamesSlice {
			downstreamPriceString, _ := pt.priceTableMap.LoadOrStore(downstreamName, int64(0))
			downstreamPrice := downstreamPriceString.(int64)
			downstreamPriceSum += downstreamPrice
		}
	}
	// fmt.Println("Total Price:", downstreamPriceSum)
	return downstreamPriceSum, nil
}

func (pt *PriceTable) RetrieveTotalPrice(ctx context.Context, methodName string) (string, error) {
	ownPrice_string, _ := pt.priceTableMap.LoadOrStore("ownprice", pt.initprice)
	ownPrice := ownPrice_string.(int64)
	downstreamPrice, _ := pt.RetrieveDSPrice(ctx, methodName)
	totalPrice := ownPrice + downstreamPrice
	price_string := strconv.FormatInt(totalPrice, 10)
	return price_string, nil
}

// Assume that own price is per microservice and it does not change across different types of requests/interfaces.
func (pt *PriceTable) UpdateOwnPrice(ctx context.Context, congestion bool) error {
	// fmt.Println("Throughtput counter:", atomic.LoadInt64(&t.throughtputCounter))

	ownPrice_string, _ := pt.priceTableMap.LoadOrStore("ownprice", pt.initprice)
	ownPrice := ownPrice_string.(int64)
	// The following code has been moved to decrementCounter() for pinpointThroughput.
	if congestion {
		ownPrice += 1
	} else if ownPrice > 0 {
		ownPrice -= 1
	}
	pt.priceTableMap.Store("ownprice", ownPrice)
	return nil
}

// LoadShedding takes tokens from the request according to the price table,
// then it updates the price table according to the tokens on the req.
// It returns #token left from ownprice, and a nil error if the request has sufficient amount of tokens.
// It returns ErrLimitExhausted if the amount of available tokens is less than requested.
func (pt *PriceTable) LoadShedding(ctx context.Context, tokens int64, methodName string) (int64, error) {
	ownPrice_string, _ := pt.priceTableMap.LoadOrStore("ownprice", pt.initprice)
	ownPrice := ownPrice_string.(int64)
	downstreamPrice, _ := pt.RetrieveDSPrice(ctx, methodName)
	totalPrice := ownPrice + downstreamPrice
	// downstreamName, _ := t.cmap.Load("echo")
	// downstreamPrice_string, _ := t.ptmap.LoadOrStore(downstreamName, int64(0))
	// downstreamPrice := downstreamPrice_string.(int64)
	// totalPrice_string, _ := t.ptmap.LoadOrStore("totalprice", t.initprice)
	// totalPrice := totalPrice_string.(int64)
	var extratoken int64
	extratoken = tokens - totalPrice

	logger("[Received Req]:	Total price is %d, ownPrice is %d downstream price is %d\n", totalPrice, ownPrice, downstreamPrice)

	if extratoken < 0 {
		logger("[Received Req]: Request rejected for lack of tokens. ownPrice is %d downstream price is %d\n", ownPrice, downstreamPrice)
		return 0, InsufficientTokens
	}

	// I'm thinking about moving it to a separate go routine, and have it run periodically for better performance.
	// or maybe run it whenever there's a congestion detected, by latency for example.
	// t.UpdateOwnPrice(ctx, extratoken < 0, tokens, ownPrice)

	if pt.pinpointThroughput {
		pt.Increment()
	}

	// Take the tokens from the req.
	var tokenleft int64
	tokenleft = tokens - ownPrice

	logger("[Received Req]:	Own price updated to %d\n", ownPrice)

	// totalPrice = ownPrice + downstreamPrice
	// t.ptmap.Store("totalprice", totalPrice)
	return tokenleft, nil
}

// SplitTokens splits the tokens left on the request to the downstream services.
// It returns a map, with the downstream service names as keys, and tokens left for them as values.
func (pt *PriceTable) SplitTokens(ctx context.Context, tokenleft int64, methodName string) ([]string, error) {
	downstreamNames, _ := pt.callMap[methodName]
	// downstreamNames, _ := t.callMap.Load(methodName)
	downstreamTokens := []string{}
	downstreamPriceSum, _ := pt.RetrieveDSPrice(ctx, methodName)
	logger("[Split tokens]:	downstream total price is %d\n", downstreamPriceSum)

	if downstreamNamesSlice, ok := downstreamNames.([]string); ok {
		size := len(downstreamNamesSlice)
		tokenleftPerDownstream := (tokenleft - downstreamPriceSum) / int64(size)
		logger("[Split tokens]:	extra token left for each ds is %d\n", tokenleftPerDownstream)
		for _, downstreamName := range downstreamNamesSlice {
			downstreamPriceString, _ := pt.priceTableMap.LoadOrStore(downstreamName, int64(0))
			downstreamPrice := downstreamPriceString.(int64)
			downstreamToken := tokenleftPerDownstream + downstreamPrice
			downstreamTokens = append(downstreamTokens, "tokens-"+downstreamName, strconv.FormatInt(downstreamToken, 10))
			logger("[Split tokens]:	token for %s is %d + %d\n", downstreamName, tokenleftPerDownstream, downstreamPrice)
		}
	}
	return downstreamTokens, nil
}

// UpdatePrice incorperates the downstream price table to its own price table.
func (pt *PriceTable) UpdatePrice(ctx context.Context, method string, downstreamPrice int64) (int64, error) {

	// Update the downstream price.
	pt.priceTableMap.Store(method, downstreamPrice)
	logger("[Received Resp]:	Downstream price of %s updated to %d\n", method, downstreamPrice)

	// var totalPrice int64
	// ownPrice, _ := t.ptmap.LoadOrStore("ownprice", t.initprice)
	// totalPrice = ownPrice.(int64) + downstreamPrice
	// t.ptmap.Store("totalprice", totalPrice)
	// logger("[Received Resp]:	Total price updated to %d\n", totalPrice)
	return downstreamPrice, nil
}

// unaryInterceptor is an example unary interceptor.
func (pt *PriceTable) UnaryInterceptorClient(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {

	// Jiali: the following line print the method name of the req/response, will be used to update the
	// logger("[Before Req]:	The method name for price table is ")
	// logger(method)
	// Jiali: before sending. check the price, calculate the #tokens to add to request, update the total tokens
	ctx = metadata.AppendToOutgoingContext(ctx, "name", pt.nodeName)
	var header metadata.MD // variable to store header and trailer
	err := invoker(ctx, method, req, reply, cc, grpc.Header(&header))
	// err := invoker(ctx, method, req, reply, cc, opts...)

	// Jiali: after replied. update and store the price info for future
	if len(header["price"]) > 0 {
		priceDownstream, _ := strconv.ParseInt(header["price"][0], 10, 64)
		pt.UpdatePrice(ctx, header["name"][0], priceDownstream)
		logger("[After Resp]:	The price table is from %s\n", header["name"])
	}

	return err
}

// unaryInterceptor is an example unary interceptor.
func (pt *PriceTable) UnaryInterceptorEnduser(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {

	// logger("[Before Req]:	The method name for price table is ")
	// logger(method)

	// rand.Seed(time.Now().UnixNano())
	// tok := rand.Intn(30)
	// tok_string := strconv.Itoa(tok)

	var tok int64

	// Jiali: before sending. check the price, calculate the #tokens to add to request, update the total tokens
	for {
		// right now let's assume that client uses all the tokens on her next request.
		tok = pt.tokensLeft
		if !pt.rateLimiting {
			break
		}
		ratelimit := pt.RateLimiting(ctx, tok, "echo")
		if ratelimit == RateLimited {
			// return ratelimit
			<-pt.rateLimiter
		} else {
			break
		}
	}

	pt.tokensLeft -= tok
	tok_string := strconv.FormatInt(tok, 10)
	ctx = metadata.AppendToOutgoingContext(ctx, "tokens", tok_string, "name", pt.nodeName)

	var header metadata.MD // variable to store header and trailer
	err := invoker(ctx, method, req, reply, cc, grpc.Header(&header))

	// Jiali: after replied. update and store the price info for future
	if len(header["price"]) > 0 {
		priceDownstream, _ := strconv.ParseInt(header["price"][0], 10, 64)
		pt.UpdatePrice(ctx, header["name"][0], priceDownstream)
		logger("[After Resp]:	The price table is from %s\n", header["name"])
	}
	return err
}

var (
	errMissingMetadata = status.Errorf(codes.InvalidArgument, "missing metadata")
)

// logger is to mock a sophisticated logging system. To simplify the example, we just print out the content.
func logger(format string, a ...interface{}) {
	// fmt.Printf("LOG:\t"+format+"\n", a...)
}

func getMethodInfo(ctx context.Context) {
	methodName, _ := grpc.Method(ctx)
	logger(methodName)
}

func (pt *PriceTable) UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// This is the server side interceptor, it should check tokens, update price, do overload handling and attach price to response
	startTime := time.Now()

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errMissingMetadata
	}

	// getMethodInfo(ctx)
	logger("[Received Req]:	The sender's name for request is %s\n", md["name"])
	// logger(info.FullMethod)

	// Jiali: overload handler, do AQM, deduct the tokens on the request, update price info
	// logger("[Received Req]:	tokens are %s\n", md["tokens"])
	// tok, err := strconv.ParseInt(md["tokens"][0], 10, 64)
	var tok int64
	var err error

	if val, ok := md["tokens-"+pt.nodeName]; ok {
		logger("[Received Req]:	tokens for %s are %s\n", pt.nodeName, val)
		tok, err = strconv.ParseInt(val[0], 10, 64)
	} else {
		logger("[Received Req]:	tokens are %s\n", md["tokens"])
		tok, err = strconv.ParseInt(md["tokens"][0], 10, 64)
	}

	// overload handler:
	tokenleft, err := pt.LoadShedding(ctx, tok, "echo")
	if err == InsufficientTokens && pt.loadShedding {
		price_string, _ := pt.RetrieveTotalPrice(ctx, "echo")
		header := metadata.Pairs("price", price_string, "name", pt.nodeName)
		logger("[Sending Error Resp]:	Total price is %s\n", price_string)
		grpc.SendHeader(ctx, header)
		// return nil, status.Errorf(codes.ResourceExhausted, "req dropped, try again later")
		return nil, status.Errorf(codes.ResourceExhausted, "%d token for %s price. req dropped, try again later", tok, price_string)
	} else if err != nil {
		// The limiter failed. This error should be logged and examined.
		log.Println(err)
		return nil, status.Error(codes.Internal, "internal error")
	}

	tok_string := strconv.FormatInt(tokenleft, 10)
	logger("[Preparing Sub Req]:	Token left is %s\n", tok_string)

	// [critical] Jiali: Being outgoing seems to be critical for us.
	// Jiali: we need to attach the token info to the context, so that the downstream can retrieve it.
	// ctx = metadata.AppendToOutgoingContext(ctx, "tokens", tok_string)
	// Jiali: we actually need multiple kv pairs for the token information, because one context is sent to multiple downstreams.
	downstreamTokens, _ := pt.SplitTokens(ctx, tokenleft, "echo")
	ctx = metadata.AppendToOutgoingContext(ctx, downstreamTokens...)

	// ctx = metadata.NewOutgoingContext(ctx, md)

	m, err := handler(ctx, req)

	// Attach the price info to response before sending
	// right now let's just propagate the corresponding price of the RPC method rather than a whole pricetable.
	// totalPrice_string, _ := PriceTableInstance.ptmap.Load("totalprice")
	price_string, _ := pt.RetrieveTotalPrice(ctx, "echo")

	header := metadata.Pairs("price", price_string, "name", pt.nodeName)
	logger("[Preparing Resp]:	Total price is %s\n", price_string)
	grpc.SendHeader(ctx, header)

	totalLatency := time.Since(startTime)
	logger("[Server-side Timer] Processing Duration is: %.2d milliseconds\n", totalLatency.Milliseconds())

	if pt.pinpointLatency {
		pt.UpdateOwnPrice(ctx, totalLatency > pt.latencyThreshold)
	}

	if err != nil {
		logger("RPC failed with error %v", err)
	}
	return m, err
}

// wrappedStream wraps around the embedded grpc.ServerStream, and intercepts the RecvMsg and
// SendMsg method call.
type wrappedStream struct {
	grpc.ServerStream
}

func (w *wrappedStream) RecvMsg(m interface{}) error {
	logger("Receive a message (Type: %T) at %s", m, time.Now().Format(time.RFC3339))
	return w.ServerStream.RecvMsg(m)
}

func (w *wrappedStream) SendMsg(m interface{}) error {
	logger("Send a message (Type: %T) at %v", m, time.Now().Format(time.RFC3339))
	return w.ServerStream.SendMsg(m)
}

func newWrappedStream(s grpc.ServerStream) grpc.ServerStream {
	return &wrappedStream{s}
}

func StreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	// authentication (token verification)
	_, ok := metadata.FromIncomingContext(ss.Context())
	if !ok {
		return errMissingMetadata
	}

	err := handler(srv, newWrappedStream(ss))
	if err != nil {
		logger("RPC failed with error %v", err)
	}
	return err
}
