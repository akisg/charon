package charon

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"strconv"
	"sync"

	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// func StringLength(s string) int {
// 	return len(s)
// }

var (
	// ErrLimitExhausted is returned by the Limiter in case the number of requests overflows the capacity of a Limiter.
	ErrLimitExhausted = errors.New("requests limit exhausted")
)

// PriceTable implements the Charon price table
type PriceTable struct {
	// locker  DistLocker
	// The following lockfree hashmap should contain total price, selfprice and downstream price
	// initprice is the price table's initprice.
	initprice int64
	cmap      sync.Map
	ptmap     sync.Map
	// clock   Clock
	// logger  Logger
	// updateRate is the rate at which price should be updated at least once.
	// updateRate time.Duration
	// mu        sync.Mutex
}

// NewPriceTable creates a new instance of PriceTable.
// func NewPriceTable(initprice int64, callmap sync.Map, pricetable sync.Map) *PriceTable {
func NewPriceTable(initprice int64, callmap sync.Map) *PriceTable {
	return &PriceTable{
		initprice: initprice,
		cmap:      callmap,
		ptmap:     sync.Map{},
		// locker:     locker,
		// clock:      clock,
		// logger:     logger,
		// updateRate: updateRate,
	}
}

// Limit takes tokens from the request according to the price table,
// then it updates the price table according to the tokens on the req.
// It returns #token left, total price, and a nil error if the request has sufficient amount of tokens.
// It returns ErrLimitExhausted if the amount of available tokens is less than requested.
func (t *PriceTable) Limit(ctx context.Context, tokens int64) (int64, int64, error) {

	ownPrice_string, _ := t.ptmap.LoadOrStore("ownprice", t.initprice)
	ownPrice := ownPrice_string.(int64)
	downstreamPrice_string, _ := t.ptmap.LoadOrStore("/greeting.v3.GreetingService/Greeting", t.initprice)
	downstreamPrice := downstreamPrice_string.(int64)
	totalPrice_string, _ := t.ptmap.LoadOrStore("totalprice", t.initprice)
	totalPrice := totalPrice_string.(int64)
	var extratoken int64
	extratoken = tokens - totalPrice

	logger("[Received Req]:	Total price is %d, ownPrice is %d downstream price is %d\n", totalPrice, ownPrice, downstreamPrice)

	if extratoken < 0 {
		logger("[Received Req]: Request rejected for lack of tokens. ownPrice is %d downstream price is %d\n", ownPrice, downstreamPrice)
		if ownPrice > 0 {
			ownPrice -= 1
		}
		t.ptmap.Store("ownprice", ownPrice)
		totalPrice = ownPrice + downstreamPrice
		return 0, totalPrice, ErrLimitExhausted
	}

	// Take the tokens from the req.
	var tokenleft int64
	tokenleft = tokens - ownPrice

	ownPrice += 1
	if ownPrice > 3 {
		ownPrice -= 3
	}
	logger("[Received Req]:	Own price updated to %d\n", ownPrice)

	t.ptmap.Store("ownprice", ownPrice)
	totalPrice = ownPrice + downstreamPrice
	return tokenleft, totalPrice, nil
}

// Include incorperates (add to own price, so far) the downstream price table to its own price table.
func (t *PriceTable) Include(ctx context.Context, method string, downstreamPrice int64) (int64, error) {

	// Update the downstream price.
	t.ptmap.Store(method, downstreamPrice)
	logger("[Received Resp]:	Downstream price updated to %d\n", downstreamPrice)

	var totalprice int64
	ownprice, _ := t.ptmap.LoadOrStore("ownprice", t.initprice)
	totalprice = ownprice.(int64) + downstreamPrice
	t.ptmap.Store("totalprice", totalprice)
	logger("[Received Resp]:	Total price updated to %d\n", totalprice)
	return totalprice, nil
}

// unaryInterceptor is an example unary interceptor.
func (PriceTableInstance *PriceTable) UnaryInterceptorClient(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	// var credsConfigured bool
	// for _, o := range opts {
	// 	_, ok := o.(grpc.PerRPCCredsCallOption)
	// 	if ok {
	// 		// credsConfigured = true
	// 		break
	// 	}
	// }

	// Jiali: the following line print the method name of the req/response, will be used to update the
	logger("[Before Req]:	The method name for price table is ")
	logger(method)
	// Jiali: before sending. check the price, calculate the #tokens to add to request, update the total tokens
	var header metadata.MD // variable to store header and trailer
	err := invoker(ctx, method, req, reply, cc, grpc.Header(&header))
	if err != nil {
		// The request failed. This error should be logged and examined.
		// log.Println(err)
		return err
	}
	// err := invoker(ctx, method, req, reply, cc, opts...)
	// log.Println(err)
	// Jiali: after replied. update and store the price info for future
	// fmt.Println("Price from downstream: ", header["price"])
	if len(header["price"]) > 0 {
		priceDownstream, _ := strconv.ParseInt(header["price"][0], 10, 64)
		PriceTableInstance.Include(ctx, method, priceDownstream)
		// logger("[Received Resp]:	Total price is %d\n", totalPrice)
	}
	// end := time.Now()
	// logger("RPC: %s, start time: %s, end time: %s, err: %v", method, start.Format("Basic"), end.Format(time.RFC3339), err)
	return err
}

// unaryInterceptor is an example unary interceptor.
func (PriceTableInstance *PriceTable) UnaryInterceptorEnduser(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	// start := time.Now()

	logger("[Before Req]:	The method name for price table is ")
	logger(method)
	// Jiali: before sending. check the price, calculate the #tokens to add to request, update the total tokens

	rand.Seed(time.Now().UnixNano())
	tok := rand.Intn(10) + 100
	tok_string := strconv.Itoa(tok)
	ctx = metadata.AppendToOutgoingContext(ctx, "tokens", tok_string)

	// check price table before sending a request, if price > tokens, enqueue the request and return
	// enqueue

	var header metadata.MD // variable to store header and trailer
	err := invoker(ctx, method, req, reply, cc, grpc.Header(&header))
	if err != nil {
		// The request failed. This error should be logged and examined.
		// log.Println(err)
		return err
	}
	// Jiali: after replied. update and store the price info for future
	if len(header["price"]) > 0 {
		priceDownstream, _ := strconv.ParseInt(header["price"][0], 10, 64)
		PriceTableInstance.Include(ctx, method, priceDownstream)
		// logger("[Received Resp]:	Total price is %d\n", totalPrice)
	}
	// check again the token and price and queue, to dequeue
	// req = dequeue
	// invo

	// err := invoker(ctx, method, req, reply, cc, opts...)
	// Jiali: after replied. update and store the price info for future

	// end := time.Now()
	// logger("RPC: %s, start time: %s, end time: %s, err: %v", method, start.Format("Basic"), end.Format(time.RFC3339), err)
	return err
}

var (
	errMissingMetadata = status.Errorf(codes.InvalidArgument, "missing metadata")
)

// logger is to mock a sophisticated logging system. To simplify the example, we just print out the content.
func logger(format string, a ...interface{}) {
	fmt.Printf("LOG:\t"+format+"\n", a...)
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
	logger("[Received Req]:	The method name for request is ")
	logger(info.FullMethod)

	logger("[Received Req]:	tokens are %s\n", md["tokens"])
	// Jiali: overload handler, do AQM, deduct the tokens on the request, update price info

	tok, err := strconv.ParseInt(md["tokens"][0], 10, 64)

	// overload handler:
	tokenleft, totalprice, err := PriceTableInstance.Limit(ctx, tok)
	if err == ErrLimitExhausted {
		return nil, status.Errorf(codes.ResourceExhausted, "try again later")
	} else if err != nil {
		// The limiter failed. This error should be logged and examined.
		log.Println(err)
		return nil, status.Error(codes.Internal, "internal error")
	}

	// Attach the price info to response before sending
	// right now let's just propagate the corresponding price of the RPC method rather than a whole pricetable.
	price_string := strconv.FormatInt(totalprice, 10)
	header := metadata.Pairs("price", price_string)
	logger("[Preparing Resp]:	Total price is %s\n", price_string)
	grpc.SendHeader(ctx, header)

	tok_string := strconv.FormatInt(tokenleft, 10)
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
