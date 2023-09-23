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

var (
	// InsufficientTokens is returned by the Limiter in case the number of requests overflows the capacity of a Limiter.
	InsufficientTokens = errors.New("Received insufficient tokens, trigger load shedding.")
	RateLimited        = errors.New("Insufficient tokens to send, trigger rate limit.")
	errMissingMetadata = status.Errorf(codes.InvalidArgument, "missing metadata")
)

// PriceTable implements the Charon price table
type PriceTable struct {
	// The following lockfree hashmap should contain total price, selfprice and downstream price
	// initprice is the price table's initprice.
	initprice          int64
	nodeName           string
	callMap            map[string][]string
	priceTableMap      sync.Map
	rateLimiting       bool
	rateLimitWaiting   bool
	loadShedding       bool
	pinpointThroughput bool
	pinpointLatency    bool
	pinpointQueuing    bool
	rateLimiter        chan int64
	invokeAfterRL      bool
	lazyResponse       bool
	// updateRate is the rate at which price should be updated at least once.
	tokensLeft          int64
	tokenUpdateRate     time.Duration
	lastUpdateTime      time.Time
	lastRateLimitedTime time.Time
	tokenUpdateStep     int64
	tokenRefillDist     string
	tokenStrategy       string
	priceStrategy       string
	throughputCounter   int64
	priceUpdateRate     time.Duration
	observedDelay       time.Duration
	clientTimeOut       time.Duration
	clientBackoff       time.Duration
	randomRateLimit     int64
	throughputThreshold int64
	latencyThreshold    time.Duration
	priceStep           int64
	debug               bool
	debugFreq           int64
	guidePrice          int64
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
	servicePrice, _ := pt.RetrieveDSPrice(ctx, methodName)
	extratoken := tokens - servicePrice
	pt.logger(ctx, "[Ratelimiting]: Checking Request. Token is %d, %s price is %d\n", tokens, methodName, servicePrice)

	if extratoken < 0 {
		pt.logger(ctx, "[Prepare Req]: Request blocked for lack of tokens.")
		return RateLimited
	}
	return nil
}

// LoadShedding takes tokens from the request according to the price table,
// then it updates the price table according to the tokens on the req.
// It returns #token left from ownprice, and a nil error if the request has sufficient amount of tokens.
// It returns ErrLimitExhausted if the amount of available tokens is less than requested.
func (pt *PriceTable) LoadShedding(ctx context.Context, tokens int64, methodName string) (int64, error) {
	// if pt.loadShedding is false, then return tokens and nil error
	if !pt.loadShedding {
		return tokens, nil
	}
	ownPrice_string, _ := pt.priceTableMap.LoadOrStore("ownprice", pt.initprice)
	ownPrice := ownPrice_string.(int64)
	downstreamPrice, _ := pt.RetrieveDSPrice(ctx, methodName)
	totalPrice := ownPrice + downstreamPrice
	// totalPrice, _ := pt.RetrieveTotalPrice(ctx, methodName)

	extratoken := tokens - totalPrice

	pt.logger(ctx, "[Received Req]:	Total price is %d, ownPrice is %d downstream price is %d\n", totalPrice, ownPrice, downstreamPrice)

	if extratoken < 0 {
		pt.logger(ctx, "[Received Req]: Request rejected for lack of tokens. ownPrice is %d downstream price is %d\n", ownPrice, downstreamPrice)
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

	// pt.logger(ctx, "[Received Req]:	Own price updated to %d\n", ownPrice)

	return tokenleft, nil
}

// unaryInterceptor is an example unary interceptor.
func (pt *PriceTable) UnaryInterceptorClient(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	// Jiali: the following line print the method name of the req/response, will be used to update the
	md, _ := metadata.FromOutgoingContext(ctx)
	methodName := md["method"][0]
	pt.logger(ctx, "[Before Sub Req]:	Node %s calling %s\n", pt.nodeName, methodName)
	// Jiali: before sending. check the price, calculate the #tokens to add to request, update the total tokens
	// overwrite rather than append to the header with the node name of this client
	ctx = metadata.AppendToOutgoingContext(ctx, "name", pt.nodeName)
	var header metadata.MD // variable to store header and trailer
	err := invoker(ctx, method, req, reply, cc, grpc.Header(&header))

	// run the following code asynchorously, without blocking the main thread.
	// go func() {
	// Jiali: after replied. update and store the price info for future
	if len(header["price"]) > 0 {
		priceDownstream, _ := strconv.ParseInt(header["price"][0], 10, 64)
		pt.UpdateDownstreamPrice(ctx, methodName, header["name"][0], priceDownstream)
		pt.logger(ctx, "[After Resp]:	The price table is from %s\n", header["name"])
	} else {
		pt.logger(ctx, "[After Resp]:	No price table received\n")
	}
	// }()

	return err
}

// unaryInterceptor is an example unary interceptor.
func (pt *PriceTable) UnaryInterceptorEnduser(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {

	// timer the intereceptor overhead
	interceptorStartTime := time.Now()

	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return errMissingMetadata
	}

	methodName := md["method"][0]
	pt.logger(ctx, "[Before Req]:	Node %s calling %s\n", pt.nodeName, methodName)
	// print all the k-v pairs in the metadata md
	// pt.logger(ctx, "[Received Req]:	The sender's name for request is %s\n", md["name"])
	// for k, v := range md {
	// 	pt.logger(ctx, "[Sending Req Enduser]:	The metadata for request is %s: %s\n", k, v)
	// }
	var metadataLog string
	for k, v := range md {
		metadataLog += fmt.Sprintf("%s: %s, ", k, v)
	}
	if metadataLog != "" {
		pt.logger(ctx, "[Sending Req Enduser]: The metadata for request is %s\n", metadataLog)
	}

	// if `randomRateLimit` is greater than 0, then we randomly drop requests based on the last digit of `request-id` in md
	if pt.randomRateLimit > 0 {
		// get the request-id from the metadata
		if requestIDs, found := md["request-id"]; found && len(requestIDs) > 0 {
			reqid, err := strconv.ParseInt(requestIDs[0], 10, 64)
			if err != nil {
				// Error parsing request ID, handle accordingly
				panic(err)
			}
			// take the last two digits of the requestIDs
			lastDigit := reqid % 100
			// if the last digit is smaller than the randomRateLimit, then drop the request
			if lastDigit < pt.randomRateLimit {
				pt.logger(ctx, "[Random Drop]:	The request is dropped randomly.")
				if pt.invokeAfterRL {
					opts = append(opts, grpc.MaxCallSendMsgSize(0))
					_ = invoker(ctx, method, req, reply, cc, opts...)
				}
				return status.Error(codes.ResourceExhausted, "Client is rate limited, req dropped randomly.")
			}
		}
	}

	// Check the time duration since the last RateLimited error
	if pt.clientBackoff > 0 && time.Since(pt.lastRateLimitedTime) < pt.clientBackoff {
		if !pt.rateLimitWaiting {
			pt.logger(ctx, "[Backoff Triggered]:	Client is rate limited, req dropped without waiting.")
			// the request is dropped without waiting in this scenario, but we want to return an error to the client
			// to do this, we use a fake invoker without actually sending the request to the server
			// Invoke the gRPC method with the new callOptions: MaxCallSendMsgSize as 0
			// append to opts
			if pt.invokeAfterRL {
				opts = append(opts, grpc.MaxCallSendMsgSize(0))
				_ = invoker(ctx, method, req, reply, cc, opts...)
			}
			return status.Error(codes.ResourceExhausted, "Client is rate limited, req dropped without waiting.")
		}
	}

	var tok int64
	// Set a timer for the client to timeout if it has been waiting for too long.
	startTime := time.Now()
	// Jiali: before sending. check the price, calculate the #tokens to add to request, update the total tokens
	for {
		// if waiting for longer than ClientTimeout, return error RateLimited
		if pt.rateLimiting && pt.rateLimitWaiting && time.Since(startTime) > pt.clientTimeOut {
			pt.logger(ctx, "[Client Timeout]:	Client timeout waiting for tokens.\n")
			// Invoke the gRPC method with the new callOptions: MaxCallSendMsgSize as 0
			// append to opts
			if pt.invokeAfterRL {
				opts = append(opts, grpc.MaxCallSendMsgSize(0))
				_ = invoker(ctx, method, req, reply, cc, opts...)
			}
			return status.Errorf(codes.DeadlineExceeded, "Client timeout waiting for tokens.")
		}
		// right now let's assume that client uses all the tokens on her next request.
		if pt.tokenStrategy == "all" {
			tok = pt.tokensLeft
		} else if pt.tokenStrategy == "uniform" {
			// set the tok to be a uniform random number between 0 and tokensLeft
			tok = rand.Int63n(pt.tokensLeft)
		}

		if !pt.rateLimiting {
			break
		}
		ratelimit := pt.RateLimiting(ctx, tok, methodName)
		// if clientBackoff is greater than 0, update the lastRateLimitedTime
		if pt.clientBackoff > 0 {
			if ratelimit == RateLimited && time.Since(pt.lastRateLimitedTime) > pt.clientBackoff {
				pt.logger(ctx, "[Backoff Started]:	Client has been rate limited, backoff started.\n")
				pt.lastRateLimitedTime = time.Now()
			}
		}

		if ratelimit == RateLimited {
			if !pt.rateLimitWaiting {
				pt.logger(ctx, "[Rate Limited]:	Client is rate limited, req dropped without waiting.\n")
				// the request is dropped without waiting in this scenario, but we want to return an error to the client
				// to do this, we use a fake invoker without actually sending the request to the server
				// Invoke the gRPC method with the new callOptions: MaxCallSendMsgSize as 0
				// append to opts
				if pt.invokeAfterRL {
					opts = append(opts, grpc.MaxCallSendMsgSize(0))
					_ = invoker(ctx, method, req, reply, cc, opts...)
				}
				return status.Error(codes.ResourceExhausted, "Client is rate limited, req dropped without waiting.")
			}
			<-pt.rateLimiter
			// pt.logger(ctx, "[Rate Limited]:	Client has been rate limited for %d ms, \n", time.Since(startTime).Milliseconds())
			// log the waiting time so far for client and how long until timeout
			pt.logger(ctx, "[Rate Limited]:	Client has been rate limited for %d ms, %d ms left until timeout.\n",
				time.Since(startTime).Milliseconds(), (pt.clientTimeOut - time.Since(startTime)).Milliseconds())
		} else {
			break
		}
	}

	pt.tokensLeft -= tok
	tok_string := strconv.FormatInt(tok, 10)
	ctx = metadata.AppendToOutgoingContext(ctx, "tokens", tok_string, "name", pt.nodeName)

	// check the timer and log the overhead of intercepting
	pt.logger(ctx, "[Enduser Interceptor Overhead]:	 %.2f milliseconds\n", float64(time.Since(interceptorStartTime).Microseconds())/1000)

	var header metadata.MD // variable to store header and trailer
	err := invoker(ctx, method, req, reply, cc, grpc.Header(&header))

	// run the following code asynchorously, without blocking the main thread.
	// go func() {
	// Jiali: after replied. update and store the price info for future
	if len(header["price"]) > 0 {
		priceDownstream, _ := strconv.ParseInt(header["price"][0], 10, 64)
		pt.UpdateDownstreamPrice(ctx, methodName, header["name"][0], priceDownstream)
		pt.logger(ctx, "[After Resp]:	The price table is from %s\n", header["name"])
	} else {
		pt.logger(ctx, "[After Resp]:	No price table received\n")
	}
	// }()
	return err
}

// func getMethodInfo(ctx context.Context) {
// 	methodName, _ := grpc.Method(ctx)
// 	logger(ctx, methodName)
// }

func (pt *PriceTable) UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// This is the server side interceptor, it should check tokens, update price, do overload handling and attach price to response
	startTime := time.Now()

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errMissingMetadata
	}

	// print all the k-v pairs in the metadata md
	// for k, v := range md {
	// 	pt.logger(ctx, "[Received Req]:	The metadata for request is %s: %s\n", k, v)
	// }
	var metadataLog string
	for k, v := range md {
		metadataLog += fmt.Sprintf("%s: %s, ", k, v)
	}
	if metadataLog != "" {
		pt.logger(ctx, "[Received Req]: The metadata for request is %s\n", metadataLog)
	}

	// Jiali: overload handler, do AQM, deduct the tokens on the request, update price info
	var tok int64
	var err error

	if val, ok := md["tokens-"+pt.nodeName]; ok {
		pt.logger(ctx, "[Received Req]:	tokens for %s are %s\n", pt.nodeName, val)
		// raise error if the val length is not 1
		if len(val) > 1 {
			return nil, status.Errorf(codes.InvalidArgument, "duplicated tokens")
		} else if len(val) == 0 {
			return nil, errMissingMetadata
		}
		tok, err = strconv.ParseInt(val[0], 10, 64)
	} else {
		pt.logger(ctx, "[Received Req]:	tokens are %s\n", md["tokens"])
		// raise error if the tokens length is not 1
		if len(md["tokens"]) > 1 {
			return nil, status.Errorf(codes.InvalidArgument, "duplicated tokens")
		} else if len(md["tokens"]) == 0 {
			return nil, errMissingMetadata
		}
		tok, err = strconv.ParseInt(md["tokens"][0], 10, 64)
	}

	// overload handler:
	methodName := md["method"][0]
	tokenleft, err := pt.LoadShedding(ctx, tok, methodName)
	if err == InsufficientTokens && pt.loadShedding {
		price_string, _ := pt.RetrieveTotalPrice(ctx, methodName)
		header := metadata.Pairs("price", price_string, "name", pt.nodeName)
		pt.logger(ctx, "[Sending Error Resp]:	Total price is %s\n", price_string)
		grpc.SendHeader(ctx, header)

		totalLatency := time.Since(startTime)
		pt.logger(ctx, "[Server-side Timer] Processing Duration is: %.2d milliseconds\n", totalLatency.Milliseconds())

		// if pt.pinpointLatency {
		// 	if totalLatency > pt.observedDelay {
		// 		pt.observedDelay = totalLatency // update the observed delay
		// 	}
		// }
		// return nil, status.Errorf(codes.ResourceExhausted, "req dropped, try again later")
		return nil, status.Errorf(codes.ResourceExhausted, "%s req dropped by %s. %d token for %s price. Try again later.", methodName, pt.nodeName, tok, price_string)
	}
	if err != nil && err != InsufficientTokens {
		// The limiter failed. This error should be logged and examined.
		log.Println(err)
		return nil, status.Error(codes.Internal, "internal error")
	}

	tok_string := strconv.FormatInt(tokenleft, 10)
	pt.logger(ctx, "[Preparing Sub Req]:	Token left is %s\n", tok_string)

	// [critical] Jiali: Being outgoing seems to be critical for us.
	// Jiali: we need to attach the token info to the context, so that the downstream can retrieve it.
	// ctx = metadata.AppendToOutgoingContext(ctx, "tokens", tok_string)
	// Jiali: we actually need multiple kv pairs for the token information, because one context is sent to multiple downstreams.
	downstreamTokens, _ := pt.SplitTokens(ctx, tokenleft, methodName)

	ctx = metadata.AppendToOutgoingContext(ctx, downstreamTokens...)

	// queuingDelay := time.Since(startTime)
	// pt.logger(ctx, "[Server-side Timer] Queuing delay is: %.2d milliseconds\n", queuingDelay.Milliseconds())

	// if pt.pinpointQueuing {
	// 	// increment the counter and add the queuing delay to the observed delay
	// 	pt.Increment()
	// 	pt.observedDelay += queuingDelay
	// }

	totalLatency := time.Since(startTime)
	// log the total latency in unit of millisecond, decimal precision 2
	pt.logger(ctx, "[Server-side Interceptor] Overhead is: %.2f milliseconds\n", float64(totalLatency.Microseconds())/1000)

	m, err := handler(ctx, req)

	// Attach the price info to response before sending
	// right now let's just propagate the corresponding price of the RPC method rather than a whole pricetable.
	// totalPrice_string, _ := PriceTableInstance.ptmap.Load("totalprice")

	// if not pt.lazyResponse
	if !pt.lazyResponse {
		price_string, _ := pt.RetrieveTotalPrice(ctx, methodName)
		header := metadata.Pairs("price", price_string, "name", pt.nodeName)
		pt.logger(ctx, "[Preparing Resp]:	Total price of %s is %s\n", methodName, price_string)
		grpc.SendHeader(ctx, header)
	} else {
		pt.logger(ctx, "[Preparing Resp]:	Lazy response is enabled, no price attached to response.\n")
	}

	if pt.pinpointLatency {
		// if totalLatency > pt.observedDelay {
		// 	pt.observedDelay = totalLatency // update the observed delay
		// }

		// change the observed delay to the average latency, first, sum the latency and increment the counter
		pt.Increment()
		pt.observedDelay += totalLatency
	}

	if err != nil {
		pt.logger(ctx, "RPC failed with error %v", err)
	}
	return m, err
}

/*
// wrappedStream wraps around the embedded grpc.ServerStream, and intercepts the RecvMsg and
// SendMsg method call.
type wrappedStream struct {
	grpc.ServerStream
}

func (w *wrappedStream) RecvMsg(m interface{}) error {
	logger(ctx, "Receive a message (Type: %T) at %s", m, time.Now().Format(time.RFC3339))
	return w.ServerStream.RecvMsg(m)
}

func (w *wrappedStream) SendMsg(m interface{}) error {
	logger(ctx, "Send a message (Type: %T) at %v", m, time.Now().Format(time.RFC3339))
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
		logger(ctx, "RPC failed with error %v", err)
	}
	return err
}
*/
