package charon

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SplitTokens splits the tokens left on the request to the downstream services.
// It returns a map, with the downstream service names as keys, and tokens left for them as values.
func (pt *PriceTable) SplitTokens(ctx context.Context, tokenleft int64, methodName string) ([]string, error) {
	downstreamNames := pt.callMap[methodName]
	size := len(downstreamNames)
	if size == 0 {
		return nil, nil
	}

	downstreamTokens := []string{}
	downstreamPriceSum, _ := pt.RetrieveDSPrice(ctx, methodName)
	tokenleftPerDownstream := (tokenleft - downstreamPriceSum) / int64(size)

	logger("[Split tokens]: downstream total price is %d, from %d downstream services for %s, extra token left for each ds is %d\n",
		downstreamPriceSum, size, pt.nodeName, tokenleftPerDownstream)

	for _, downstreamName := range downstreamNames {
		// concatenate the method name with node name to distinguish different downstream services calls.
		downstreamPriceString, _ := pt.priceTableMap.Load(methodName + "-" + downstreamName)
		downstreamPrice := downstreamPriceString.(int64)
		downstreamToken := tokenleftPerDownstream + downstreamPrice
		downstreamTokens = append(downstreamTokens, "tokens-"+downstreamName, strconv.FormatInt(downstreamToken, 10))
		// logger("[Split tokens]:	token for %s is %d + %d\n", downstreamName, tokenleftPerDownstream, downstreamPrice)
	}
	return downstreamTokens, nil
}

func (pt *PriceTable) RetrieveDSPrice(ctx context.Context, methodName string) (int64, error) {
	if len(pt.callMap[methodName]) == 0 {
		logger("[Retrieve DS Price]:	No downstream service for %s\n", methodName)
		return 0, nil
	}
	// load the downstream price from the price table with method name as key.
	downstreamPrice_string, ok := pt.priceTableMap.Load(methodName)
	if !ok || downstreamPrice_string == nil {
		return 0, errors.New("[retrieve ds price] downstream price not found")
	}

	downstreamPrice, ok := downstreamPrice_string.(int64)
	if !ok {
		return 0, errors.New("[retrieve ds price] downstream price wrong type")
	}
	logger("[Retrieve DS Price]:	Downstream price of %s is %d\n", methodName, downstreamPrice)
	return downstreamPrice, nil
}

func (pt *PriceTable) RetrieveTotalPrice(ctx context.Context, methodName string) (string, error) {
	ownPrice_string, _ := pt.priceTableMap.Load("ownprice")
	ownPrice := ownPrice_string.(int64)
	downstreamPrice, _ := pt.RetrieveDSPrice(ctx, methodName)
	var totalPrice int64
	if pt.priceAggregation == "maximal" {
		// totalPrice is the max of ownPrice and downstreamPrice
		if ownPrice > downstreamPrice {
			totalPrice = ownPrice
		} else {
			totalPrice = downstreamPrice
		}
	} else if pt.priceAggregation == "additive" {
		totalPrice = ownPrice + downstreamPrice
	} else if pt.priceAggregation == "mean" {
		totalPrice = (ownPrice + downstreamPrice) / 2
	}
	price_string := strconv.FormatInt(totalPrice, 10)
	logger("[Retrieve Total Price]:	Downstream price of %s is now %d, total price %d \n", methodName, downstreamPrice, totalPrice)
	return price_string, nil
}

// Assume that own price is per microservice and it does not change across different types of requests/interfaces.
func (pt *PriceTable) UpdateOwnPrice(congestion bool) error {

	ownPrice_string, _ := pt.priceTableMap.Load("ownprice")
	ownPrice := ownPrice_string.(int64)
	// The following code has been moved to decrementCounter() for pinpointThroughput.
	logger("[Update OwnPrice]:	congestion is %t, own price %d, step %d\n", congestion, ownPrice, pt.priceStep)
	if congestion {
		ownPrice += pt.priceStep
		if pt.guidePrice > -1 {
			ownPrice = pt.guidePrice
		}
	} else if ownPrice > 0 {
		ownPrice -= 1
	}
	pt.priceTableMap.Store("ownprice", ownPrice)
	logger("[Update OwnPrice]:	Own price updated to %d\n", ownPrice)
	return nil
}

func (pt *PriceTable) calculatePriceAdjustment(diff int64) int64 {
	if diff > 0 {
		// Use a non-linear adjustment: larger adjustment for larger differences
		adjustment := int64(diff * pt.priceStep / 10000)
		return adjustment
	} else if 2*diff < -pt.latencyThreshold.Microseconds() {
		return -1
	} else {
		return 0
	}
}

// The following functions are used to update the price table based on the queue delay, linearly and return adjustment any way.
func (pt *PriceTable) calculatePriceAdjustmentLinear(diff int64) int64 {
	adjustment := int64(diff * pt.priceStep / 10000)
	return adjustment
}

// UpdatePricebyQueueDelay incorperates the queue delay to its own price steps. Thus, the price step is not linear.
func (pt *PriceTable) UpdatePricebyQueueDelay(ctx context.Context) error {
	ownPrice_string, _ := pt.priceTableMap.Load("ownprice")
	ownPrice := ownPrice_string.(int64)

	// read the gapLatency from context ctx
	gapLatency := ctx.Value("gapLatency").(float64)
	// Calculate the priceStep as a fraction of the difference between gapLatency and latencyThreshold
	diff := int64(gapLatency*1000) - pt.latencyThreshold.Microseconds()
	adjustment := pt.calculatePriceAdjustment(diff)

	logger("[Update Price by Queue Delay]: Own price %d, step %d\n", ownPrice, adjustment)

	ownPrice += adjustment
	// Set reservePrice to the larger of pt.guidePrice and 0
	reservePrice := int64(math.Max(float64(pt.guidePrice), 0))

	if ownPrice <= reservePrice {
		ownPrice = reservePrice
	}

	pt.priceTableMap.Store("ownprice", ownPrice)
	// run the following code every 200 milliseconds
	if pt.lastUpdateTime.Add(200 * time.Millisecond).Before(time.Now()) {
		recordPrice("[Update Price by Queue Delay]: Own price updated to %d\n", ownPrice)
		recordPrice("[Incremental Waiting Time Maximum]:	%f ms.\n", gapLatency)
		pt.lastUpdateTime = time.Now()
	}

	return nil
}

// UpdatePricebyQueueDelay incorperates the queue delay to its own price steps. Thus, the price step is not linear.
func (pt *PriceTable) UpdatePricebyQueueDelayLinear(ctx context.Context) error {
	ownPrice_string, _ := pt.priceTableMap.Load("ownprice")
	ownPrice := ownPrice_string.(int64)

	// read the gapLatency from context ctx
	gapLatency := ctx.Value("gapLatency").(float64)
	// Calculate the priceStep as a fraction of the difference between gapLatency and latencyThreshold
	diff := int64(gapLatency*1000) - pt.latencyThreshold.Microseconds()
	adjustment := pt.calculatePriceAdjustmentLinear(diff)

	logger("[Update Price by Queue Delay]: Own price %d, step %d\n", ownPrice, adjustment)

	ownPrice += adjustment
	// Set reservePrice to the larger of pt.guidePrice and 0
	reservePrice := int64(math.Max(float64(pt.guidePrice), 0))

	if ownPrice <= reservePrice {
		ownPrice = reservePrice
	}

	pt.priceTableMap.Store("ownprice", ownPrice)
	// run the following code every 200 milliseconds
	if pt.lastUpdateTime.Add(200 * time.Millisecond).Before(time.Now()) {
		recordPrice("[Update Price by Queue Delay]: Own price updated to %d\n", ownPrice)
		recordPrice("[Incremental Waiting Time Maximum]:	%f ms.\n", gapLatency)
		pt.lastUpdateTime = time.Now()
	}

	return nil
}

// UpdatePricebyQueueDelayExp uses exponential function to adjust the price step.
func (pt *PriceTable) UpdatePricebyQueueDelayExp(ctx context.Context) error {
	ownPrice_string, _ := pt.priceTableMap.Load("ownprice")
	ownPrice := ownPrice_string.(int64)

	// read the gapLatency from context ctx
	gapLatency := ctx.Value("gapLatency").(float64)
	// Calculate the priceStep as a fraction of the difference between gapLatency and latencyThreshold

	diff := int64(gapLatency*1000) - pt.latencyThreshold.Microseconds()
	// adjustment is exponential function of diff/1000
	adjustment := int64(math.Exp(float64(diff*pt.priceStep/10000))) - 1

	logger("[Update Price by Queue Delay]: own price %d, step %d\n", ownPrice, adjustment)

	ownPrice += adjustment

	// Set reservePrice to the larger of pt.guidePrice and 0
	reservePrice := int64(math.Max(float64(pt.guidePrice), 0))

	if ownPrice <= reservePrice {
		ownPrice = reservePrice
	}

	pt.priceTableMap.Store("ownprice", ownPrice)
	logger("[Update Price by Queue Delay]: Own price updated to %d\n", ownPrice)

	return nil
}

// UpdatePricebyQueueDelayExp uses exponential function to adjust the price step.
func (pt *PriceTable) UpdatePricebyQueueDelayLog(ctx context.Context) error {
	ownPrice_string, _ := pt.priceTableMap.Load("ownprice")
	ownPrice := ownPrice_string.(int64)

	// read the gapLatency from context ctx
	gapLatency := ctx.Value("gapLatency").(float64)
	// Calculate the priceStep as a fraction of the difference between gapLatency and latencyThreshold

	diff := int64(gapLatency*1000) - pt.latencyThreshold.Microseconds()
	// adjustment is a logarithmic function of diff*priceStep/1000 if diff > 0
	adjustment := int64(0)
	if diff > 0 {
		adjustment = int64(math.Log(float64(diff*pt.priceStep/10000) + 1))
	} else if 2*diff < -pt.latencyThreshold.Microseconds() {
		adjustment = -1
	}

	logger("[Update Price by Queue Delay]: own price %d, step %d\n", ownPrice, adjustment)

	ownPrice += adjustment
	// Set reservePrice to the larger of pt.guidePrice and 0
	reservePrice := int64(math.Max(float64(pt.guidePrice), 0))

	if ownPrice <= reservePrice {
		ownPrice = reservePrice
	}

	pt.priceTableMap.Store("ownprice", ownPrice)
	logger("[Update Price by Queue Delay]: Own price %d\n", ownPrice)

	return nil
}

// UpdateDownstreamPrice incorperates the downstream price table to its own price table.
func (pt *PriceTable) UpdateDownstreamPrice(ctx context.Context, method string, nodeName string, downstreamPrice int64) (int64, error) {
	if pt.priceAggregation == "maximal" || pt.priceAggregation == "mean" {
		// Update the downstream price, but concatenate the method name with node name to distinguish different downstream services calls.
		pt.priceTableMap.Store(method+"-"+nodeName, downstreamPrice)
		logger("[Received Resp]:	Downstream price of %s updated to %d\n", method+"-"+nodeName, downstreamPrice)
		// if the downstream price is greater than the current downstream price, update the downstream price.
		downstreamPrice_old, loaded := pt.priceTableMap.Load(method)
		if !loaded {
			// raise an error if the downstream price is not loaded.
			logger("[Error]:	Cannot find the previous downstream price of %s\n", method)
			// return 0, status.Errorf(codes.Aborted, fmt.Sprintf("Downstream price of %s is not loaded", method+"-"+nodeName))
			downstreamPrice_old = int64(0)
		} else {
			logger("[Previous DS Price]:	The DS price of %s was %d before the update.\n", method, downstreamPrice_old)
		}
		if downstreamPrice > downstreamPrice_old.(int64) {
			// update the downstream price
			pt.priceTableMap.Store(method, downstreamPrice)
			logger("[Updating DS Price]:	Downstream price of %s updated to %d\n", method, downstreamPrice)
			return downstreamPrice, nil
		}
		// if the downstream price is not greater than the current downstream price, store it and calculate the max
		// find the maximum downstream price of the request.

		maxPrice := int64(math.MinInt64) // Smallest int64 value

		pt.priceTableMap.Range(func(key, value interface{}) bool {
			if k, ok := key.(string); ok && strings.HasPrefix(k, method+"-") {
				if v, valid := value.(int64); valid && v > maxPrice {
					maxPrice = v
				}
			}
			return true
		})
		logger("[Updated DS Price]:	The price of %s is now %d\n", method, maxPrice)
		// update the downstream price only for the method involved in the request.
		pt.priceTableMap.Store(method, maxPrice)

	} else if pt.priceAggregation == "additive" {
		// load the downstream price from the price table with method + node name as key.
		downstreamPrice_old, loaded := pt.priceTableMap.Swap(method+"-"+nodeName, downstreamPrice)
		if !loaded {
			// raise an error if the downstream price is not loaded.
			return 0, status.Errorf(codes.Aborted, fmt.Sprintf("Downstream price of %s is not loaded", method+"-"+nodeName))
		}
		// calculate the new diff between the old downstream price and the new downstream price.
		diff := downstreamPrice - downstreamPrice_old.(int64)
		// if the diff is 0, return 0
		if diff == 0 {
			return 0, nil
		}
		// apply the diff to the all methods' whenever the downstream price is part of the method call.
		for methodName, downstreamNames := range pt.callMap {
			for _, downstreamName := range downstreamNames {
				if downstreamName == nodeName {
					// increase the downstream price of the method by the diff.
					// load the downstream price from the price table with method name as key. then update the downstream price. then save it back to the price table.
					methodPrice, _ := pt.priceTableMap.Load(methodName)
					methodPrice = methodPrice.(int64) + diff
					pt.priceTableMap.Store(methodName, methodPrice)
					// log the downstream price of the request.
					logger("[Updated DS Price]:	The price of %s is now %d\n", methodName, methodPrice)
				}
			}
		}

		// Update the downstream price, but concatenate the method name with node name to distinguish different downstream services calls.
		pt.priceTableMap.Store(method+"-"+nodeName, downstreamPrice)
		logger("[Received Resp]:	Downstream price of %s updated to %d\n", method+"-"+nodeName, downstreamPrice)
		// pt.SaveDSPrice(ctx, method)
	}
	return downstreamPrice, nil
}
