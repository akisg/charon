package charon

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"google.golang.org/grpc/metadata"
)

// logger is to mock a sophisticated logging system. To simplify the example, we just print out the content.
func (pt *PriceTable) logger(ctx context.Context, format string, a ...interface{}) {
	if pt.debug {
		reqid := int64(-1)
		var err error

		// Check incoming context for "request-id" metadata
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if requestIDs, found := md["request-id"]; found && len(requestIDs) > 0 {
				reqid, err = strconv.ParseInt(requestIDs[0], 10, 64)
				if err != nil {
					// Error parsing request ID, handle accordingly
					panic(err)
				}
			}
		}

		// Check outgoing context for "request-id" metadata only if it wasn't found in the incoming context
		if reqid == -1 {
			if md, ok := metadata.FromOutgoingContext(ctx); ok {
				if requestIDs, found := md["request-id"]; found && len(requestIDs) > 0 {
					reqid, err = strconv.ParseInt(requestIDs[0], 10, 64)
					if err != nil {
						// Error parsing request ID, handle accordingly
						panic(err)
					}
				}
			}
		}

		if reqid%pt.debugFreq == 0 {
			timestamp := time.Now().Format("2006-01-02T15:04:05.999999999-07:00")
			fmt.Printf("LOG: "+timestamp+"|\t"+format+"\n", a...)
		}
	}
}
