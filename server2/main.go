/*
 *
 * Copyright 2018 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Binary server is an example server.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"strconv"
	"sync"

	"errors"

	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
	"google.golang.org/grpc/examples/data"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	ecpb "google.golang.org/grpc/examples/features/proto/echo"
	pb "google.golang.org/grpc/examples/features/proto/echo"
)

var (
	// ErrLimitExhausted is returned by the Limiter in case the number of requests overflows the capacity of a Limiter.
	ErrLimitExhausted = errors.New("requests limit exhausted")
)

// PriceTableState represents a state of a price table.
type PriceTableState struct {
	// Last is the last time the state was updated (Unix timestamp in nanoseconds).
	Last int64
	// Price is the number of available tokens in the price table.
	Price int64
}

// isZero returns true if the price table state is zero valued.
func (s PriceTableState) isZero() bool {
	return s.Last == 0 && s.Price == 0
}

// PriceTableStateBackend interface encapsulates the logic of retrieving and persisting the state of a PriceTable.
type PriceTableStateBackend interface {
	// State gets the current state of the PriceTable.
	State(ctx context.Context) (PriceTableState, error)
	// SetState sets (persists) the current state of the PriceTable.
	SetState(ctx context.Context, state PriceTableState) error
}

// PriceTable implements the Charon price table
type PriceTable struct {
	// locker  DistLocker
	backend PriceTableStateBackend
	// clock   Clock
	// logger  Logger
	// updateRate is the rate at which price should be updated at least once.
	// updateRate time.Duration
	// initprice is the price table's initprice.
	initprice int64
	mu        sync.Mutex
}

// NewPriceTable creates a new instance of PriceTable.
func NewPriceTable(initprice int64, priceTableStateBackend PriceTableStateBackend) *PriceTable {
	return &PriceTable{
		// locker:     locker,
		backend: priceTableStateBackend,
		// clock:      clock,
		// logger:     logger,
		// updateRate: updateRate,
		initprice: initprice,
	}
}

// Take takes tokens from the price table.
// It returns #token left and a nil error if the request has sufficient amount of tokens.
// It returns ErrLimitExhausted if the amount of available tokens is less than requested.
func (t *PriceTable) Take(ctx context.Context, tokens int64) (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// if err := t.locker.Lock(ctx); err != nil {
	// 	return 0, err
	// }
	// defer func() {
	// 	if err := t.locker.Unlock(); err != nil {
	// 		t.logger.Log(err)
	// 	}
	// }()
	state, err := t.backend.State(ctx)
	if err != nil {
		return 0, err
	}
	if state.isZero() {
		// Initially the price table is initprice.
		state.Price = t.initprice
	}
	// now := t.clock.Now().UnixNano()
	// // Refill the price table.
	// tokensToAdd := (now - state.Last) / int64(t.updateRate)
	// if tokensToAdd > 0 {
	// 	state.Last = now
	// 	// if tokensToAdd+state.Price <= t.initprice {
	// 	// 	state.Price += tokensToAdd
	// 	// } else {
	// 	// 	state.Price = t.initprice
	// 	// }
	// }

	if tokens < state.Price {
		fmt.Printf("Request rejected for lack of tokens. Price is %d\n", state.Price)
		return state.Price - tokens, ErrLimitExhausted
	}

	// Take the tokens from the req.
	var tokenleft int64
	tokenleft = tokens - state.Price

	state.Price += 1
	fmt.Printf("Price updated to %d\n", state.Price)

	if err = t.backend.SetState(ctx, state); err != nil {
		return 0, err
	}
	return tokenleft, nil
}

// Limit takes x token from the req.
func (t *PriceTable) Limit(ctx context.Context, tokens int64) (int64, error) {
	return t.Take(ctx, tokens)
}

// PriceTableInMemory is an in-memory implementation of PriceTableStateBackend.
//
// The state is not shared nor persisted so it won't survive restarts or failures.
// Due to the local nature of the state the rate at which some endpoints are accessed can't be reliably predicted or
// limited.
type PriceTableInMemory struct {
	state PriceTableState
}

// NewPriceTableInMemory creates a new instance of PriceTableInMemory.
func NewPriceTableInMemory() *PriceTableInMemory {
	return &PriceTableInMemory{}
}

// State returns the current price table's state.
func (t *PriceTableInMemory) State(ctx context.Context) (PriceTableState, error) {
	return t.state, ctx.Err()
}

// SetState sets the current price table's state.
func (t *PriceTableInMemory) SetState(ctx context.Context, state PriceTableState) error {
	t.state = state
	return ctx.Err()
}

const fallbackToken = "some-secret-token"

func callUnaryEcho(ctx context.Context, client ecpb.EchoClient, message string) {
	// [critical] to pass ctx to subrequests.
	resp, err := client.UnaryEcho(ctx, &ecpb.EchoRequest{Message: message})
	// The function UnaryEcho above is the RPC stub provided by downstream nodes (server/main.go) to this service to call.
	if err != nil {
		log.Fatalf("client.UnaryEcho(_) = _, %v: ", err)
	}
	fmt.Println("UnaryEcho: ", resp.Message)
}

// unaryInterceptor is an example unary interceptor.
func (priceTable *PriceTable) unaryInterceptor_client(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	var credsConfigured bool
	for _, o := range opts {
		_, ok := o.(grpc.PerRPCCredsCallOption)
		if ok {
			credsConfigured = true
			break
		}
	}
	if !credsConfigured {
		opts = append(opts, grpc.PerRPCCredentials(oauth.NewOauthAccess(&oauth2.Token{
			AccessToken: fallbackToken,
		})))
	}
	// start := time.Now()

	// Jiali: before sending. check the price, calculate the #tokens to add to request, update the total tokens
	err := invoker(ctx, method, req, reply, cc, opts...)
	// Jiali: after replied. update and store the price info for future

	// end := time.Now()
	// logger("RPC: %s, start time: %s, end time: %s, err: %v", method, start.Format("Basic"), end.Format(time.RFC3339), err)
	return err
}

var addr = flag.String("addr", "localhost:50052", "the address to connect to")

var (
	port = flag.Int("port", 50051, "the port to serve on")

	errMissingMetadata = status.Errorf(codes.InvalidArgument, "missing metadata")
	errInvalidToken    = status.Errorf(codes.Unauthenticated, "invalid token")
)

// logger is to mock a sophisticated logging system. To simplify the example, we just print out the content.
func logger(format string, a ...interface{}) {
	fmt.Printf("LOG:\t"+format+"\n", a...)
}

type server struct {
	pb.UnimplementedEchoServer
	// This line below is the downstream server (an echo client) of the server.
	rgc ecpb.EchoClient
}

// func newServer(priceTable *PriceTable) *server {
// 	// This function creates a new server with a given pricetable, which is used later for the client interceptor
// 	creds_client, err_client := credentials.NewClientTLSFromFile(data.Path("x509/ca_cert.pem"), "x.test.example.com")
// 	if err_client != nil {
// 		log.Fatalf("failed to load credentials: %v", err_client)
// 	}

// 	// Set up a connection to the downstream server.
// 	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(creds_client),
// 		grpc.WithUnaryInterceptor(priceTable.unaryInterceptor_client))
// 	if err != nil {
// 		log.Fatalf("did not connect: %v", err)
// 	}
// 	// defer conn.Close()

// 	// Make a echo client and send RPCs.
// 	// rgc := ecpb.NewEchoClient(conn)

// 	s := &server{rgc: ecpb.NewEchoClient(conn)}
// 	return s
// }

func (s *server) UnaryEcho(ctx context.Context, in *pb.EchoRequest) (*pb.EchoResponse, error) {
	// This function is the server-side stub provided by this service to upstream nodes/clients.
	fmt.Printf("unary echoing message at server 2 %q\n", in.Message)
	// [critical] to pass ctx from upstream to downstream
	// This function is called when the middle tier service behave as a client and dials the downstream nodes.
	callUnaryEcho(ctx, s.rgc, in.Message)
	return &pb.EchoResponse{Message: in.Message}, nil
}

func (s *server) BidirectionalStreamingEcho(stream pb.Echo_BidirectionalStreamingEchoServer) error {
	for {
		in, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			fmt.Printf("server: error receiving from stream: %v\n", err)
			return err
		}
		fmt.Printf("bidi echoing message %q\n", in.Message)
		stream.Send(&pb.EchoResponse{Message: in.Message})
	}
}

// valid validates the authorization.
func valid(authorization []string) bool {
	if len(authorization) < 1 {
		return false
	}
	token := strings.TrimPrefix(authorization[0], "Bearer ")
	// Perform the token validation here. For the sake of this example, the code
	// here forgoes any of the usual OAuth2 token validation and instead checks
	// for a token matching an arbitrary string.
	return token == "some-secret-token"
}

func (priceTable *PriceTable) unaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// authentication (token verification)
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errMissingMetadata
	}
	if !valid(md["authorization"]) {
		return nil, errInvalidToken
	}

	logger("tokens are %s\n", md["tokens"])
	// Jiali: overload handler, do AQM, deduct the tokens on the request, update price info

	tok, err := strconv.ParseInt(md["tokens"][0], 10, 64)

	tokenleft, err := priceTable.Limit(ctx, tok)
	if err == ErrLimitExhausted {
		return nil, status.Errorf(codes.ResourceExhausted, "try again later")
	} else if err != nil {
		// The limiter failed. This error should be logged and examined.
		log.Println(err)
		return nil, status.Error(codes.Internal, "internal error")
	}

	tok_string := strconv.FormatInt(tokenleft, 10)
	// [critical] Jiali: Being outgoing seems to be critical for us.
	ctx = metadata.AppendToOutgoingContext(ctx, "tokens", tok_string)
	// ctx = metadata.NewOutgoingContext(ctx, md)

	m, err := handler(ctx, req)
	// Attach the price info to response before sending

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

func streamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	// authentication (token verification)
	md, ok := metadata.FromIncomingContext(ss.Context())
	if !ok {
		return errMissingMetadata
	}
	if !valid(md["authorization"]) {
		return errInvalidToken
	}

	err := handler(srv, newWrappedStream(ss))
	if err != nil {
		logger("RPC failed with error %v", err)
	}
	return err
}

func main() {
	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Create tls based credential.
	creds, err := credentials.NewServerTLSFromFile(data.Path("x509/server_cert.pem"), data.Path("x509/server_key.pem"))
	if err != nil {
		log.Fatalf("failed to create credentials: %v", err)
	}

	const initialPrice = 2
	priceTable := NewPriceTable(
		initialPrice,
		NewPriceTableInMemory(),
	)
	s := grpc.NewServer(grpc.Creds(creds), grpc.UnaryInterceptor(priceTable.unaryInterceptor), grpc.StreamInterceptor(streamInterceptor))

	creds_client, err_client := credentials.NewClientTLSFromFile(data.Path("x509/ca_cert.pem"), "x.test.example.com")
	if err_client != nil {
		log.Fatalf("failed to load credentials: %v", err_client)
	}

	// Set up a connection to the downstream server, but with the client-side interceptor on top of priceTable.
	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(creds_client),
		grpc.WithUnaryInterceptor(priceTable.unaryInterceptor_client))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	// Make a echo client rgc and send RPCs.
	// Register EchoServer on the server.
	pb.RegisterEchoServer(s, &server{rgc: ecpb.NewEchoClient(conn)})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}
