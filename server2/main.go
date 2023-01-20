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
	"flag"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/examples/data"

	ecpb "google.golang.org/grpc/examples/features/proto/echo"
	pb "google.golang.org/grpc/examples/features/proto/echo"

	"charon/interceptor"
)

func main() {

	greeting := fmt.Sprintf("Hello, %s", "James")
	fmt.Printf("Length of greeting is %d\n", interceptor.StringLength(greeting))

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
	priceTable := interceptor.NewPriceTable(
		initialPrice,
		interceptor.NewPriceTableInMemory(),
	)
	s := grpc.NewServer(grpc.Creds(creds), grpc.UnaryInterceptor(priceTable.UnaryInterceptor), grpc.StreamInterceptor(interceptor.StreamInterceptor))

	creds_client, err_client := credentials.NewClientTLSFromFile(data.Path("x509/ca_cert.pem"), "x.test.example.com")
	if err_client != nil {
		log.Fatalf("failed to load credentials: %v", err_client)
	}

	// Set up a connection to the downstream server, but with the client-side interceptor on top of priceTable.
	conn, err := grpc.Dial(*interceptor.Addr, grpc.WithTransportCredentials(creds_client),
		grpc.WithUnaryInterceptor(priceTable.unaryInterceptor_client))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	// Make a echo client rgc and send RPCs.
	// Register EchoServer on the server.
	pb.RegisterEchoServer(s, &interceptor.Server{rgc: ecpb.NewEchoClient(conn)})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}
