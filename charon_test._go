package charon

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"testing"
	"time"

	pb "github.com/Jiali-Xing/protobuf"
	"github.com/google/uuid"
	"github.com/tgiannoukos/charon"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var (
	// errMissingMetadata = status.Errorf(codes.InvalidArgument, "missing metadata")
	greetings []*pb.Greeting
)

// mockUnaryInvoker is used to mock grpc.UnaryInvoker
func mockUnaryInvoker(_ context.Context, _ string, _, _ interface{}, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
	return nil // Return nil error to simulate a successful invocation
}

// func DummyClient() takes input mockInvoker and interceptor and returns a dummy client pb.GreetingServiceClient
func setupDummyClient(mock bool, interceptor bool) pb.GreetingServiceClient {
	// Initialize parameters
	ctx := context.Background()
	// method := "/service.method"
	// req := "fake request"
	// reply := "reply"
	// cc := &grpc.ClientConn{}
	// info := &grpc.UnaryClientInfo{}

	// handler := func(ctx context.Context, req interface{}) (interface{}, error) {
	// 	return "fake response", nil
	// }
	callGraph := make(map[string][]string)

	charonOptions := map[string]interface{}{
		// "rateLimitWaiting": true,
		"rateLimiting": true,
		"debug":        true,
		"debugFreq":    int64(1000),
		"tokensLeft":   int64(0),
		"initprice":    int64(10),
		// "clientTimeOut":   time.Millisecond * time.Duration(arg),
		// "clientTimeOut":   time.Duration(0),
		"tokenUpdateStep": int64(10),
		"tokenUpdateRate": time.Millisecond * 10,
		// "randomRateLimit": int64(35),
		// "invokeAfterRL":   true,
		// "clientBackoff": time.Millisecond * 0,
		"tokenRefillDist": "poisson",
		"tokenStrategy":   "uniform",
		// "latencyThreshold":   time.Millisecond * 7,
	}

	// Create a new PriceTable instance
	priceTable := charon.NewCharon(
		"client",
		callGraph,
		charonOptions,
	)
	// Initialize necessary fields
	var opts []grpc.DialOption
	if mock && interceptor {
		opts = append(opts,
			grpc.WithInsecure(),
			grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				return priceTable.UnaryInterceptorEnduser(ctx, method, req, reply, cc, mockUnaryInvoker, opts...)
			}),
		)
	} else if !mock && interceptor {
		opts = append(opts,
			grpc.WithInsecure(),
			grpc.WithUnaryInterceptor(priceTable.UnaryInterceptorEnduser),
		)
	} else if !mock && !interceptor {
		opts = append(opts,
			grpc.WithInsecure(),
		)
	} else {
		// raise error
	}

	// Set up a connection to the server.
	conn, err := grpc.DialContext(ctx, "localhost:50051", opts...)

	if err != nil {
		log.Fatal(err)
		return nil
	}

	// create a server instance to listen on port 50051
	// go SimpleServer()

	client := pb.NewGreetingServiceClient(conn)
	return client
}

func TestUnaryInterceptorClient(t *testing.T) {
	requestGreeting := pb.Greeting{
		Created: time.Now().Local().String(),
	}
	client := setupDummyClient(true, true)

	// ctx has no metadata
	ctx := context.Background()
	resp, err := client.Greeting(ctx, &pb.GreetingRequest{Greeting: &requestGreeting})

	// Call your UnaryInterceptorClient

	// Assertions
	// expected := errMissingMetadata
	if !errors.Is(err, errMissingMetadata) {
		t.Errorf("Expected %v, got %v", errMissingMetadata, err)
	}

	if resp != nil {
		t.Errorf("Expected 'fake response', got %v", resp)
	}

	// add metadata to ctx
	ctx = metadata.AppendToOutgoingContext(ctx, "request-id", "12345")
	resp, err = client.Greeting(ctx, &pb.GreetingRequest{Greeting: &requestGreeting})
	// err should be nil
	if !errors.Is(err, nil) {
		t.Errorf("Expected %v, got %v", nil, err)
	}
	// resp should be "fake response"
	if resp == nil {
		t.Errorf("Expected 'fake response', got %v", resp)
	}
}

func runServer(port string) error {
	// port := ":50051"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal(err)
	}
	server := pb.UnimplementedGreetingServiceServer{}
	var grpcServer *grpc.Server
	// intercept := false
	// if intercept {
	// 	grpcServer = grpc.NewServer(grpc.UnaryInterceptor(priceTable.UnaryInterceptor))
	// } else {
	grpcServer = grpc.NewServer()
	// }
	pb.RegisterGreetingServiceServer(grpcServer, server)

	return grpcServer.Serve(lis)
}

func SimpleServer(port string) {
	if err := runServer(port); err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}

type greetingServiceServer struct {
	pb.UnimplementedGreetingServiceServer
	pt          *charon.PriceTable
	callGraph   map[string][]string
	downstreams []string
	// downstreamURLs []string
	clients map[string]pb.GreetingServiceClient
	conns   map[string]*grpc.ClientConn
}

func serverCompute(duration int) {
	// Jiali: the following block implements the fake computation
	quit := make(chan bool)
	busyChan := make(chan int)
	go busyLoop(busyChan, quit)
	select {
	case busyResult := <-busyChan:
		log.Println(busyResult)
	case <-time.After(time.Duration(duration) * time.Millisecond):
		// log.Println("timed out")
	}
	quit <- true
	return
}

func (s *greetingServiceServer) InitializeDownstreamClients() error {
	// Initialize the clients and connections for downstream services
	ctx := context.Background()
	// for service, addresses := range s.callGraph {
	// for _, address := range addresses {
	for _, downstream := range s.downstreams {
		// address is based on the downstream service name
		address := getURL(downstream)
		conn, err := s.createGRPCConn(ctx, address)
		if err != nil {
			log.Errorf("Failed to create gRPC connection for %s: %v", downstream, err)
			return err
		}
		client := pb.NewGreetingServiceClient(conn)

		s.clients[downstream] = client
		s.conns[downstream] = conn
		// }
	}
	return nil
}

func (s *greetingServiceServer) Greeting(ctx context.Context, req *pb.GreetingRequest) (*pb.GreetingResponse, error) {
	requestGreeting := pb.Greeting{
		Id:      uuid.New().String(),
		Created: time.Now().Local().String(),
	}
	greetings = append(greetings, &requestGreeting)

	computation(2)
	/*
		addresses := s.callGraph["echo"]
		for _, address := range addresses {
			log.Debugf("Calling downstream service %s at %s", "echo", address)
			s.callGrpcService(ctx, &requestGreeting, address)
		}
		// Jiali: use callGrpcService to call all downstream services in loops
	*/
	// read the method from the request metadata
	md, _ := metadata.FromIncomingContext(ctx)
	method := md["method"][0]

	responseGreetings, err := s.client.Greeting(ctx, &pb.GreetingRequest{Greeting: requestGreeting})

	greetings = append(greetings)

	return &pb.GreetingResponse{
		Greeting: greetings,
	}, nil
}

func (s *greetingServiceServer) createGRPCConn(ctx context.Context, addr string, intercept bool) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption

	if intercept {
		opts = append(opts,
			grpc.WithUnaryInterceptor(s.pt.UnaryInterceptorClient),
			grpc.WithInsecure(),
			grpc.WithBlock())
	} else {
		opts = append(opts,
			grpc.WithInsecure(),
			grpc.WithBlock())
	}
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	return conn, nil
}

func TestSimpleServer(t *testing.T) {
	go SimpleServer()
	time.Sleep(1 * time.Second)
}

func TestSimpleServerClient(t *testing.T) {
	// this test is to test if client can call server without interceptor
	// setup server and client
	go SimpleServer()
	client := setupDummyClient(false, false)
	requestGreeting := pb.Greeting{
		Created: time.Now().Local().String(),
	}
	ctx := context.Background()
	resp, err := client.Greeting(ctx, &pb.GreetingRequest{Greeting: &requestGreeting})
	// err should be nil
	if !errors.Is(err, nil) {
		t.Errorf("Expected %v, got %v", nil, err)
	}

}
