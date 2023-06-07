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
	initprice     int64
	nodeName      string
	callMap       map[string]interface{}
	priceTableMap sync.Map
	rateLimiting  bool
	rateLimiter   chan int64
	// updateRate is the rate at which price should be updated at least once.
	tokensLeft         int64
	tokenUpdateRate    time.Duration
	lastUpdateTime     time.Time
	tokenUpdateStep    int64
	throughtputCounter int64
	priceUpdateRate    time.Duration
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
		rateLimiting:       true,
		rateLimiter:        make(chan int64, 1),
		tokensLeft:         10,
		tokenUpdateRate:    time.Millisecond * 10,
		lastUpdateTime:     time.Now(),
		tokenUpdateStep:    1,
		throughtputCounter: 0,
		priceUpdateRate:    time.Millisecond * 100,
		debug:              false,
	}
	// priceTable.rateLimiter <- 1
	// Only refill the tokens when the interceptor is for enduser.
	if priceTable.nodeName == "client" {
		go priceTable.tokenRefill()
	} else {
		go priceTable.decrementCounter()
	}

	return priceTable
}

func (cc *PriceTable) Increment() {
	atomic.AddInt64(&cc.throughtputCounter, 1)
}

func (cc *PriceTable) Decrement(step int64) {
	atomic.AddInt64(&cc.throughtputCounter, -step)
}

func (cc *PriceTable) GetCount() int64 {
	// return atomic.LoadInt64(&cc.throughtputCounter)
	return atomic.SwapInt64(&cc.throughtputCounter, 0)
}

// decrementCounter decrements the counter by 200 every 100 milliseconds.
func (pt *PriceTable) decrementCounter() {
	for range time.Tick(pt.priceUpdateRate) {
		pt.Decrement(200)

		ownPrice_string, _ := pt.priceTableMap.LoadOrStore("ownprice", pt.initprice)
		ownPrice := ownPrice_string.(int64)
		if pt.GetCount() > 10 {
			// ownPrice += 2
		} else if ownPrice > 0 {
			ownPrice -= 1
		}
		pt.priceTableMap.Store("ownprice", ownPrice)

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
func (t *PriceTable) RateLimiting(ctx context.Context, tokens int64, methodName string) error {
	// downstreamName, _ := t.callMap.Load(methodName)
	downstreamName, _ := t.callMap[methodName]
	servicePrice_string, _ := t.priceTableMap.LoadOrStore(downstreamName, t.initprice)
	servicePrice := servicePrice_string.(int64)

	extratoken := tokens - servicePrice
	logger("[Ratelimiting]: Checking Request. Token is %d, %s price is %d\n", tokens, downstreamName, servicePrice)

	if extratoken < 0 {
		logger("[Prepare Req]: Request blocked for lack of tokens.")
		return RateLimited
	}
	return nil
}

func (t *PriceTable) RetrieveDSPrice(ctx context.Context, methodName string) (int64, error) {
	// retrive downstream node name involved in the request from callmap.
	// downstreamNames, _ := t.callMap.Load(methodName)
	downstreamNames, _ := t.callMap[methodName]
	var downstreamPriceSum int64
	// var downstreamPrice int64
	if downstreamNamesSlice, ok := downstreamNames.([]string); ok {
		for _, downstreamName := range downstreamNamesSlice {
			downstreamPriceString, _ := t.priceTableMap.LoadOrStore(downstreamName, int64(0))
			downstreamPrice := downstreamPriceString.(int64)
			downstreamPriceSum += downstreamPrice
		}
	}
	// fmt.Println("Total Price:", downstreamPriceSum)
	return downstreamPriceSum, nil
}

func (t *PriceTable) RetrieveTotalPrice(ctx context.Context, methodName string) (string, error) {
	ownPrice_string, _ := t.priceTableMap.LoadOrStore("ownprice", t.initprice)
	ownPrice := ownPrice_string.(int64)
	downstreamPrice, _ := t.RetrieveDSPrice(ctx, methodName)
	totalPrice := ownPrice + downstreamPrice
	price_string := strconv.FormatInt(totalPrice, 10)
	return price_string, nil
}

// Assume that own price is per microservice and it does not change across different types of requests/interfaces.
func (t *PriceTable) UpdateOwnPrice(ctx context.Context, reqDropped bool, tokens int64, ownPrice int64) error {
	t.Increment()
	// fmt.Println("Throughtput counter:", atomic.LoadInt64(&t.throughtputCounter))
	// The following code has been moved to decrementCounter().
	// if t.GetCount() > 10 {
	// 	ownPrice += 1
	// 	atomic.SwapInt64(&t.throughtputCounter, 0)
	// } else if ownPrice > 0 {
	// 	ownPrice -= 1
	// }
	// t.priceTableMap.Store("ownprice", ownPrice)
	return nil
}

// LoadShading takes tokens from the request according to the price table,
// then it updates the price table according to the tokens on the req.
// It returns #token left from ownprice, and a nil error if the request has sufficient amount of tokens.
// It returns ErrLimitExhausted if the amount of available tokens is less than requested.
func (t *PriceTable) LoadShading(ctx context.Context, tokens int64, methodName string) (int64, error) {
	ownPrice_string, _ := t.priceTableMap.LoadOrStore("ownprice", t.initprice)
	ownPrice := ownPrice_string.(int64)
	downstreamPrice, _ := t.RetrieveDSPrice(ctx, methodName)
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

	t.UpdateOwnPrice(ctx, extratoken < 0, tokens, ownPrice)

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
func (t *PriceTable) SplitTokens(ctx context.Context, tokenleft int64, methodName string) ([]string, error) {
	downstreamNames, _ := t.callMap[methodName]
	// downstreamNames, _ := t.callMap.Load(methodName)
	downstreamTokens := []string{}
	downstreamPriceSum, _ := t.RetrieveDSPrice(ctx, methodName)
	logger("[Split tokens]:	downstream total price is %d\n", downstreamPriceSum)

	if downstreamNamesSlice, ok := downstreamNames.([]string); ok {
		size := len(downstreamNamesSlice)
		tokenleftPerDownstream := (tokenleft - downstreamPriceSum) / int64(size)
		logger("[Split tokens]:	extra token left for each ds is %d\n", tokenleftPerDownstream)
		for _, downstreamName := range downstreamNamesSlice {
			downstreamPriceString, _ := t.priceTableMap.LoadOrStore(downstreamName, int64(0))
			downstreamPrice := downstreamPriceString.(int64)
			downstreamToken := tokenleftPerDownstream + downstreamPrice
			downstreamTokens = append(downstreamTokens, "tokens-"+downstreamName, strconv.FormatInt(downstreamToken, 10))
			logger("[Split tokens]:	token for %s is %d + %d\n", downstreamName, tokenleftPerDownstream, downstreamPrice)
		}
	}
	return downstreamTokens, nil
}

// UpdatePrice incorperates the downstream price table to its own price table.
func (t *PriceTable) UpdatePrice(ctx context.Context, method string, downstreamPrice int64) (int64, error) {

	// Update the downstream price.
	t.priceTableMap.Store(method, downstreamPrice)
	logger("[Received Resp]:	Downstream price of %s updated to %d\n", method, downstreamPrice)

	// var totalPrice int64
	// ownPrice, _ := t.ptmap.LoadOrStore("ownprice", t.initprice)
	// totalPrice = ownPrice.(int64) + downstreamPrice
	// t.ptmap.Store("totalprice", totalPrice)
	// logger("[Received Resp]:	Total price updated to %d\n", totalPrice)
	return downstreamPrice, nil
}

// unaryInterceptor is an example unary interceptor.
func (PriceTableInstance *PriceTable) UnaryInterceptorClient(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {

	// Jiali: the following line print the method name of the req/response, will be used to update the
	// logger("[Before Req]:	The method name for price table is ")
	// logger(method)
	// Jiali: before sending. check the price, calculate the #tokens to add to request, update the total tokens
	ctx = metadata.AppendToOutgoingContext(ctx, "name", PriceTableInstance.nodeName)
	var header metadata.MD // variable to store header and trailer
	err := invoker(ctx, method, req, reply, cc, grpc.Header(&header))
	// err := invoker(ctx, method, req, reply, cc, opts...)

	// Jiali: after replied. update and store the price info for future
	if len(header["price"]) > 0 {
		priceDownstream, _ := strconv.ParseInt(header["price"][0], 10, 64)
		PriceTableInstance.UpdatePrice(ctx, header["name"][0], priceDownstream)
		logger("[After Resp]:	The price table is from %s\n", header["name"])
	}

	return err
}

// unaryInterceptor is an example unary interceptor.
func (PriceTableInstance *PriceTable) UnaryInterceptorEnduser(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {

	// logger("[Before Req]:	The method name for price table is ")
	// logger(method)

	// rand.Seed(time.Now().UnixNano())
	// tok := rand.Intn(30)
	// tok_string := strconv.Itoa(tok)

	var tok int64

	// Jiali: before sending. check the price, calculate the #tokens to add to request, update the total tokens
	for {
		// right now let's assume that client uses all the tokens on her next request.
		tok = PriceTableInstance.tokensLeft
		if !PriceTableInstance.rateLimiting {
			break
		}
		ratelimit := PriceTableInstance.RateLimiting(ctx, tok, "echo")
		if ratelimit == RateLimited {
			// return ratelimit
			<-PriceTableInstance.rateLimiter
		} else {
			break
		}
	}

	PriceTableInstance.tokensLeft -= tok
	tok_string := strconv.FormatInt(tok, 10)
	ctx = metadata.AppendToOutgoingContext(ctx, "tokens", tok_string, "name", PriceTableInstance.nodeName)

	var header metadata.MD // variable to store header and trailer
	err := invoker(ctx, method, req, reply, cc, grpc.Header(&header))

	// Jiali: after replied. update and store the price info for future
	if len(header["price"]) > 0 {
		priceDownstream, _ := strconv.ParseInt(header["price"][0], 10, 64)
		PriceTableInstance.UpdatePrice(ctx, header["name"][0], priceDownstream)
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

func (PriceTableInstance *PriceTable) UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// This is the server side interceptor, it should check tokens, update price, do overload handling and attach price to response
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

	if val, ok := md["tokens-"+PriceTableInstance.nodeName]; ok {
		logger("[Received Req]:	tokens for %s are %s\n", PriceTableInstance.nodeName, val)
		tok, err = strconv.ParseInt(val[0], 10, 64)
	} else {
		logger("[Received Req]:	tokens are %s\n", md["tokens"])
		tok, err = strconv.ParseInt(md["tokens"][0], 10, 64)
	}

	// overload handler:
	tokenleft, err := PriceTableInstance.LoadShading(ctx, tok, "echo")
	if err == InsufficientTokens {
		price_string, _ := PriceTableInstance.RetrieveTotalPrice(ctx, "echo")
		header := metadata.Pairs("price", price_string, "name", PriceTableInstance.nodeName)
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
	downstreamTokens, _ := PriceTableInstance.SplitTokens(ctx, tokenleft, "echo")
	ctx = metadata.AppendToOutgoingContext(ctx, downstreamTokens...)

	// ctx = metadata.NewOutgoingContext(ctx, md)

	start := time.Now()

	m, err := handler(ctx, req)

	// Attach the price info to response before sending
	// right now let's just propagate the corresponding price of the RPC method rather than a whole pricetable.
	// totalPrice_string, _ := PriceTableInstance.ptmap.Load("totalprice")
	price_string, _ := PriceTableInstance.RetrieveTotalPrice(ctx, "echo")

	header := metadata.Pairs("price", price_string, "name", PriceTableInstance.nodeName)
	logger("[Preparing Resp]:	Total price is %s\n", price_string)
	grpc.SendHeader(ctx, header)

	duration := time.Since(start).Milliseconds()
	logger("[Server-side Timer] Processing Duration is: %.2d milliseconds\n", duration)

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
