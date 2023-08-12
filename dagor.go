package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	B_MAX = 5 	 // max admission level of business priority
	U_MAX = 3 	 // max admission level of user priority
	alpha = 0.05 // value for alpha as percentage
	beta  = 0.01 // value for beta as percentage
)

type AdmissionControl struct {
	Bmax                 int
	Umax                 int
	C                    [][]int
	N                    int
	clientBackoff        time.Duration
	clientTimeOut        time.Duration
	randomRateLimit      int
	rateLimiting         bool
	rateLimitWaiting     bool
	invokeAfterRL        bool
	tokenStrategy        string
	rateLimiter          <-chan time.Time
	lastRateLimitedTime  time.Time
}

func NewAdmissionControl() *AdmissionControl {
	return &AdmissionControl{
		Bmax:         Bmax,
		Umax:         Umax,
		C:            make([][]int, Bmax+1),
		rateLimiter:  time.Tick(time.Second), // Initialize with a channel
	}
}

func (ac *AdmissionControl) ResetHistogram() {
	ac.N = 0
	for b := 0; b <= ac.Bmax; b++ {
		ac.C[b] = make([]int, ac.Umax+1)
	}
}

func (ac *AdmissionControl) UpdateHistogram(b, u int, admitted bool) {
	ac.N++
	if admitted && b >= 0 && b <= ac.Bmax && u >= 0 && u <= ac.Umax {
		ac.C[b][u]++
	}
}

func (ac *AdmissionControl) CalculateAdmissionLevel(foverload bool) (int, int) {
	Nexp := ac.N
	if foverload {
		Nexp = int((1 - alpha) * float64(Nexp))
	} else {
		Nexp = int((1 + beta) * float64(Nexp))
	}

	var Bstar, Ustar int
	Nprefix := 0
	for B := 0; B <= ac.Bmax; B++ {
		for U := 0; U <= ac.Umax; U++ {
			Nprefix += ac.C[B][U]
			if Nprefix > Nexp {
				return Bstar, Ustar
			} else {
				Bstar, Ustar = B, U
			}
		}
	}

	return Bstar, Ustar
}

// pre process request before sending and post-processing response after hear back
func (ac *AdmissionControl) UnaryInterceptorClient(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	
	fmt.Printf("[Before Sub Req]: Node %s calling %s\n", ac.nodeName, method)

	// Calculate adaptive admission level using the provided algorithm
	Bstar, Ustar := ac.CalculateAdmissionLevel(false) // Assuming foverload = false

	// Update metadata with adaptive admission levels
	md := metadata.New(map[string]string{
		"business-priority": fmt.Sprintf("%d", Bstar),
		"user-priority":     fmt.Sprintf("%d", Ustar),
		"name":              ac.nodeName,
	})

	// Create a new context with the metadata attached
	ctx = metadata.NewOutgoingContext(ctx, md)

	// Call the gRPC method with the updated context
	err := invoker(ctx, method, req, reply, cc, opts...)

	// Implement response processing here

	return err
}

// pre process request (more in comparison) before sending and post-processing response after hear back
func (ac *AdmissionControl) UnaryInterceptorEnduser (ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {

	// Simulated user and business priority values
	userPriority := rand.Intn(ac.Umax + 1)
	businessPriority := rand.Intn(ac.Bmax + 1)

	// Create metadata with user and business priorities
	md := metadata.New(map[string]string{
		"user-priority":     fmt.Sprintf("%d", userPriority),
		"business-priority": fmt.Sprintf("%d", businessPriority),
	})

	// Create a new context with the metadata attached
	ctx = metadata.NewOutgoingContext(ctx, md)

	// Call the gRPC method with the updated context
	err := invoker(ctx, method, req, reply, cc, opts...)

	// Implement response processing here

	return err
}

// server–intercepting user (still pre-process, but after receive reqest, before handling, need to modify response before sending back)
func (ac *AdmissionControl) UnaryInterceptor (ctx context.Context, method string, req, reply interface{}, cc *ClientConn, invoker UnaryInvoker, opts ...CallOption) error {

	// Calculate adaptive admission level using the provided algorithm
	Bstar, Ustar := ac.CalculateAdmissionLevel(true) // Assuming foverload = true

	// Update metadata with adaptive admission levels
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errMissingMetadata
	}

	mdCopy := md.Copy()
	mdCopy["business-priority"] = []string{fmt.Sprintf("%d", Bstar)}
	mdCopy["user-priority"] = []string{fmt.Sprintf("%d", Ustar)}

	// Create a new context with the updated metadata
	ctx = metadata.NewIncomingContext(ctx, mdCopy)

	// Call the actual gRPC handler with the updated context
	resp, err := handler(ctx, req)

	// Implement additional processing here

	return resp, err
}
