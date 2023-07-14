package charon

import (
	"context"
	"runtime/metrics"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/metadata"
)

func (pt *PriceTable) Increment() {
	atomic.AddInt64(&pt.throughputCounter, 1)
}

func (pt *PriceTable) Decrement(step int64) {
	atomic.AddInt64(&pt.throughputCounter, -step)
}

func (pt *PriceTable) GetCount() int64 {
	// return atomic.LoadInt64(&cc.throughtputCounter)
	return atomic.SwapInt64(&pt.throughputCounter, 0)
}

func (pt *PriceTable) latencyCheck() {
	for range time.Tick(pt.priceUpdateRate) {
		// create a new incoming context with the "request-id" as "0"
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("request-id", "0"))

		// change to using the average latency
		pt.UpdateOwnPrice(ctx, pt.observedDelay.Milliseconds() > pt.latencyThreshold.Milliseconds()*pt.GetCount())
		pt.observedDelay = time.Duration(0)
	}
}

// queuingCheck checks if the queuing delay of go routine is greater than the latency SLO.
func (pt *PriceTable) queuingCheck() {
	// init a null histogram
	var prevHist *metrics.Float64Histogram
	for range time.Tick(pt.priceUpdateRate) {
		// get the current histogram
		currHist := readHistogram()
		/*
			// calculate the differernce between the two histograms prevHist and currHist
			diff := metrics.Float64Histogram{}
			// if preHist is empty pointer, return currHist
			if prevHist == nil {
				diff = *currHist
			} else {
				diff = GetHistogramDifference(*prevHist, *currHist)
			}
			// maxLatency is the max of the histogram in milliseconds.
			gapLatency := maximumBucket(&diff)
		*/
		if prevHist == nil {
			// directly go to next iteration
			prevHist = currHist
			continue
		}
		gapLatency := maximumQueuingDelayms(prevHist, currHist)
		// medianLatency := medianBucket(&diff)
		// gapLatency := percentileBucket(&diff, 90)

		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("request-id", "0"))

		// ToDo: move the print of the histogram to a file
		/*
			cumulativeLat := medianBucket(currHist)
			// printHistogram(currHist)
			pt.logger(ctx, "[Cumulative Waiting Time Median]:	%f ms.\n", cumulativeLat)
			// printHistogram(&diff)
			pt.logger(ctx, "[Incremental Waiting Time 90-tile]:	%f ms.\n", percentileBucket(&diff, 90))
			pt.logger(ctx, "[Incremental Waiting Time Median]:	%f ms.\n", medianBucket(&diff))
			pt.logger(ctx, "[Incremental Waiting Time Maximum]:	%f ms.\n", maximumBucket(&diff))
		*/
		pt.logger(ctx, "[Incremental Waiting Time Maximum]:	%f ms.\n", gapLatency)
		// store the gapLatency in the context ctx
		ctx = context.WithValue(ctx, "gapLatency", gapLatency)

		if pt.priceStrategy == "step" {
			pt.UpdateOwnPrice(ctx, pt.overloadDetection(ctx))
		} else if pt.priceStrategy == "proportional" {
			pt.UpdatePricebyQueueDelay(ctx)
		} else if pt.priceStrategy == "exponential" {
			pt.UpdatePricebyQueueDelayExp(ctx)
		}
		// copy the content of current histogram to the previous histogram
		prevHist = currHist
	}
}

// throughputCheck decrements the counter by 2x every x milliseconds.
func (pt *PriceTable) throughputCheck() {
	for range time.Tick(pt.priceUpdateRate) {
		// pt.Decrement(pt.throughputThreshold)
		// Create an empty context
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("request-id", "0"))
		pt.logger(ctx, "[Throughput Counter]:	The throughtput counter is %d\n", pt.throughputCounter)
		// pt.UpdateOwnPrice(ctx, pt.GetCount() > 0)
		// update own price if getCounter is greater than the threshold
		pt.UpdateOwnPrice(ctx, pt.overloadDetection(ctx))
	}
}

// checkBoth checks both throughput and latency.
func (pt *PriceTable) checkBoth() {
	var prevHist *metrics.Float64Histogram
	for range time.Tick(pt.priceUpdateRate) {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("request-id", "0"))
		pt.logger(ctx, "[Throughput Counter]:	The throughtput counter is %d\n", pt.throughputCounter)

		// get the current histogram
		currHist := readHistogram()

		// calculate the differernce between the two histograms prevHist and currHist
		diff := metrics.Float64Histogram{}
		// if preHist is empty pointer, return currHist
		if prevHist == nil {
			diff = *currHist
		} else {
			diff = GetHistogramDifference(*prevHist, *currHist)
		}
		// maxLatency is the max of the histogram in milliseconds.
		gapLatency := maximumBucket(&diff)
		// medianLatency := medianBucket(&diff)
		// gapLatency := percentileBucket(&diff, 90)

		cumulativeLat := medianBucket(currHist)
		// printHistogram(currHist)
		pt.logger(ctx, "[Cumulative Waiting Time Median]:	%f ms.\n", cumulativeLat)
		// printHistogram(&diff)
		pt.logger(ctx, "[Incremental Waiting Time 90-tile]:	%f ms.\n", percentileBucket(&diff, 90))
		pt.logger(ctx, "[Incremental Waiting Time Median]:	%f ms.\n", medianBucket(&diff))
		pt.logger(ctx, "[Incremental Waiting Time Maximum]:	%f ms.\n", maximumBucket(&diff))

		pt.UpdateOwnPrice(ctx, pt.GetCount() > pt.throughputThreshold && int64(gapLatency*1000) > pt.latencyThreshold.Microseconds())
		// copy the content of current histogram to the previous histogram
		prevHist = currHist
	}
}

// overloadDetection takes signals as input, (either pinpointLatency or throughputCounter)
// and compares them with the threshold. If the signal is greater than the threshold,
// then the overload flag is set to true. If the signal is less than the threshold,
// then the overload flag is set to false. The overload flag is then used to update
// the price table.
func (pt *PriceTable) overloadDetection(ctx context.Context) bool {
	if pt.pinpointThroughput {
		if pt.GetCount() > pt.throughputThreshold {
			return true
		}
	} else if pt.pinpointQueuing {
		// read the gapLatency from context ctx
		gapLatency := ctx.Value("gapLatency").(float64)

		if int64(gapLatency*1000) > pt.latencyThreshold.Microseconds() {
			return true
		}
	}
	return false
}
