package charon

import (
	"fmt"
	"log"
	"math"
	"runtime/metrics"
	"time"
)

func medianBucket(h *metrics.Float64Histogram) float64 {
	total := uint64(0)
	for _, count := range h.Counts {
		total += count
	}

	// round up thresh if total is odd
	thresh := (total + 1) / 2
	total = 0
	// if count is 0, return the 2nd bucket
	// for i, count := range h.Counts, but skip the first bucket
	for i := 1; i < len(h.Counts); i++ {
		total += h.Counts[i]
		if total >= thresh {
			return h.Buckets[i] * 1000
		}
	}
	panic("should not happen")
}

func percentileBucket(h *metrics.Float64Histogram, percentile float64) float64 {
	total := uint64(0)
	for _, count := range h.Counts {
		total += count
	}

	thresh := uint64(math.Ceil(float64(total) * (percentile / 100.0)))
	total = 0

	// Iterate through the histogram counts, starting from the second bucket
	// and find the bucket that surpasses the threshold count.
	for i := 1; i < len(h.Counts); i++ {
		total += h.Counts[i]
		if total >= thresh {
			return h.Buckets[i] * 1000 // Convert to milliseconds
		}
	}

	panic("should not happen")
}

// similarly, maximumBucket returns the maximum bucket
func maximumBucket(h *metrics.Float64Histogram) float64 {
	for i := len(h.Counts) - 1; i >= 0; i-- {
		if h.Counts[i] != 0 {
			return h.Buckets[i] * 1000
		}
	}
	return 0
}

// To extract the difference between two Float64Histogram distributions, and return a new Float64Histogram
// you can subtract the corresponding bucket counts of the two histograms.
// If the earlier histogram is from an empty pointer, return the later histogram
// Ensure the two histograms have the same number of buckets
func GetHistogramDifference(earlier, later metrics.Float64Histogram) metrics.Float64Histogram {
	// if the earlier histogram isfrom an empty pointer, return the later histogram
	if len(earlier.Counts) == 0 {
		return later
	}

	// Ensure the two histograms have the same number of buckets
	if len(earlier.Counts) != len(later.Counts) {
		panic("histograms have different number of buckets")
	}

	// if either the earlier or later histogram is empty, panic
	if len(earlier.Counts) == 0 || len(later.Counts) == 0 {
		panic("histogram has no buckets")
		// return &metrics.Float64Histogram{}
	}

	// Calculate the difference between the bucket counts and return the gap histogram
	// diff := metrics.Float64Histogram{}

	// Create a new histogram for the difference
	diff := metrics.Float64Histogram{
		Counts:  make([]uint64, len(earlier.Counts)),
		Buckets: earlier.Buckets, // Assuming Buckets are the same for both histograms
	}

	for i := range earlier.Counts {
		diff.Counts[i] = later.Counts[i] - earlier.Counts[i]
	}
	return diff
}

func busyLoop(c chan<- int, quit chan bool) {
	for {
		if <-quit {
			return
		}
	}
}

func computation(duration int) {
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

// this function reads the currHist from metrics
func readHistogram() *metrics.Float64Histogram {
	const queueingDelay = "/sched/latencies:seconds"

	// Create a sample for the metric.
	sample := make([]metrics.Sample, 1)
	sample[0].Name = queueingDelay

	// Sample the metric.
	metrics.Read(sample)

	// Check if the metric is actually supported.
	// If it's not, the resulting value will always have
	// kind KindBad.
	if sample[0].Value.Kind() == metrics.KindBad {
		panic(fmt.Sprintf("metric %q no longer supported", queueingDelay))
	}

	// get the current histogram
	currHist := sample[0].Value.Float64Histogram()

	return currHist
}

/*
// queuingCheck checks if the queuing delay of go routine is greater than the latency SLO.
func main() {
	// init a null histogram
	var prevHist *metrics.Float64Histogram

	// run computation with 100 go routines, each 10000ms
	for i := 0; i < 10000; i++ {
		go computation(50000)
	}

	for range time.Tick(time.Second) {
		// create a new incoming context with the "request-id" as "0"
		const queueingDelay = "/sched/latencies:seconds"

		// Create a sample for the metric.
		sample := make([]metrics.Sample, 1)
		sample[0].Name = queueingDelay

		// Sample the metric.
		metrics.Read(sample)

		// Check if the metric is actually supported.
		// If it's not, the resulting value will always have
		// kind KindBad.
		if sample[0].Value.Kind() == metrics.KindBad {
			panic(fmt.Sprintf("metric %q no longer supported", queueingDelay))
		}

		// get the current histogram
		currHist := sample[0].Value.Float64Histogram()
		// calculate the differernce between the two histograms prevHist and currHist
		diff := metrics.Float64Histogram{}
		// if preHist is empty pointer, return currHist
		if prevHist == nil {
			diff = *currHist
		} else {
			diff = GetHistogramDifference(*prevHist, *currHist)
		}
		// printHistogram(&diff)
		printHistogram(currHist)
		// medianLatency is the median of the histogram in milliseconds.
		medianLatency := medianBucket(&diff)
		cmedianLatency := medianBucket(currHist)

		fmt.Printf("[Sampled Cumulative Waiting Time]:	%f ms.\n", cmedianLatency)
		fmt.Printf("[Sampled Difference Waiting Time]:	%f ms.\n", medianLatency)

		// copy the content of current histogram to the previous histogram
		prevHist = currHist
	}
}
*/

// func printHistogram(h *metrics.Float64Histogram) prints the content of histogram h
func printHistogram(h *metrics.Float64Histogram) {
	// fmt.Printf("Histogram: %v\n", h)
	fmt.Printf("Buckets: %v\n", h.Buckets)
	fmt.Printf("Counts: %v\n", h.Counts)
}
