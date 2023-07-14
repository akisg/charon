package charon

import (
	"context"
	"math"
	"strconv"
)

// SplitTokens splits the tokens left on the request to the downstream services.
// It returns a map, with the downstream service names as keys, and tokens left for them as values.
func (pt *PriceTable) SplitTokens(ctx context.Context, tokenleft int64, methodName string) ([]string, error) {
	downstreamNames, _ := pt.callMap[methodName]
	size := len(downstreamNames)
	if size == 0 {
		return nil, nil
	}

	downstreamTokens := []string{}
	downstreamPriceSum, _ := pt.RetrieveDSPrice(ctx, methodName)
	pt.logger(ctx, "[Split tokens]:	downstream total price is %d\n", downstreamPriceSum)

	pt.logger(ctx, "[Split tokens]:	%d downstream services for %s \n", size, pt.nodeName)
	tokenleftPerDownstream := (tokenleft - downstreamPriceSum) / int64(size)
	pt.logger(ctx, "[Split tokens]:	extra token left for each ds is %d\n", tokenleftPerDownstream)
	for _, downstreamName := range downstreamNames {
		downstreamPriceString, _ := pt.priceTableMap.LoadOrStore(downstreamName, int64(0))
		downstreamPrice := downstreamPriceString.(int64)
		downstreamToken := tokenleftPerDownstream + downstreamPrice
		downstreamTokens = append(downstreamTokens, "tokens-"+downstreamName, strconv.FormatInt(downstreamToken, 10))
		pt.logger(ctx, "[Split tokens]:	token for %s is %d + %d\n", downstreamName, tokenleftPerDownstream, downstreamPrice)
	}
	return downstreamTokens, nil
}

func (pt *PriceTable) RetrieveDSPrice(ctx context.Context, methodName string) (int64, error) {
	// retrive downstream node name involved in the request from callmap.
	downstreamNames, _ := pt.callMap[methodName]
	var downstreamPriceSum int64
	for _, downstreamName := range downstreamNames {
		downstreamPriceString, _ := pt.priceTableMap.LoadOrStore(downstreamName, pt.initprice)
		downstreamPrice := downstreamPriceString.(int64)
		downstreamPriceSum += downstreamPrice
	}
	// fmt.Println("Total Price:", downstreamPriceSum)
	return downstreamPriceSum, nil
}

func (pt *PriceTable) RetrieveTotalPrice(ctx context.Context, methodName string) (string, error) {
	ownPrice_string, _ := pt.priceTableMap.LoadOrStore("ownprice", pt.initprice)
	ownPrice := ownPrice_string.(int64)
	downstreamPrice, _ := pt.RetrieveDSPrice(ctx, methodName)
	totalPrice := ownPrice + downstreamPrice
	price_string := strconv.FormatInt(totalPrice, 10)
	return price_string, nil
}

// Assume that own price is per microservice and it does not change across different types of requests/interfaces.
func (pt *PriceTable) UpdateOwnPrice(ctx context.Context, congestion bool) error {

	ownPrice_string, _ := pt.priceTableMap.LoadOrStore("ownprice", pt.initprice)
	ownPrice := ownPrice_string.(int64)
	// The following code has been moved to decrementCounter() for pinpointThroughput.
	pt.logger(ctx, "[Update OwnPrice]:	congestion is %t, own price %d, step %d\n", congestion, ownPrice, pt.priceStep)
	if congestion {
		ownPrice += pt.priceStep
		if pt.guidePrice > -1 {
			ownPrice = pt.guidePrice
		}
	} else if ownPrice > 0 {
		ownPrice -= pt.priceStep
	}
	pt.priceTableMap.Store("ownprice", ownPrice)
	pt.logger(ctx, "[Update OwnPrice]:	Own price updated to %d\n", ownPrice)
	return nil
}

func (pt *PriceTable) calculatePriceAdjustment(diff int64) int64 {
	if diff > 0 {
		// Use a non-linear adjustment: larger adjustment for larger differences
		adjustment := int64(diff / pt.priceStep)
		// if adjustment > pt.priceStep {
		// 	return pt.priceStep
		// }
		return adjustment
	} else if diff < -1000 {
		return -1
	} else {
		return 0
	}
}

// UpdatePricebyQueueDelay incorperates the queue delay to its own price steps. Thus, the price step is not linear.
func (pt *PriceTable) UpdatePricebyQueueDelay(ctx context.Context) error {
	ownPrice_string, _ := pt.priceTableMap.LoadOrStore("ownprice", pt.initprice)
	ownPrice := ownPrice_string.(int64)

	// read the gapLatency from context ctx
	gapLatency := ctx.Value("gapLatency").(float64)
	// Calculate the priceStep as a fraction of the difference between gapLatency and latencyThreshold

	diff := int64(gapLatency*1000) - pt.latencyThreshold.Microseconds()
	adjustment := pt.calculatePriceAdjustment(diff)

	pt.logger(ctx, "[Update Price by Queue Delay]: own price %d, step %d\n", ownPrice, adjustment)

	ownPrice += adjustment
	// Set reservePrice to the larger of pt.guidePrice and 0
	reservePrice := int64(math.Max(float64(pt.guidePrice), 0))

	if ownPrice <= reservePrice {
		ownPrice = reservePrice
	}

	pt.priceTableMap.Store("ownprice", ownPrice)
	pt.logger(ctx, "[Update Price by Queue Delay]: Own price updated to %d\n", ownPrice)

	return nil
}

// UpdatePricebyQueueDelayExp uses exponential function to adjust the price step.
func (pt *PriceTable) UpdatePricebyQueueDelayExp(ctx context.Context) error {
	ownPrice_string, _ := pt.priceTableMap.LoadOrStore("ownprice", pt.initprice)
	ownPrice := ownPrice_string.(int64)

	// read the gapLatency from context ctx
	gapLatency := ctx.Value("gapLatency").(float64)
	// Calculate the priceStep as a fraction of the difference between gapLatency and latencyThreshold

	diff := int64(gapLatency*1000) - pt.latencyThreshold.Microseconds()
	// adjustment is exponential function of diff/1000
	adjustment := int64(math.Exp(float64(diff) / 1000))

	pt.logger(ctx, "[Update Price by Queue Delay]: own price %d, step %d\n", ownPrice, adjustment)

	ownPrice += adjustment
	// Set reservePrice to the larger of pt.guidePrice and 0
	reservePrice := int64(math.Max(float64(pt.guidePrice), 0))

	if ownPrice <= reservePrice {
		ownPrice = reservePrice
	}

	pt.priceTableMap.Store("ownprice", ownPrice)
	pt.logger(ctx, "[Update Price by Queue Delay]: Own price updated to %d\n", ownPrice)

	return nil
}

// UpdateDownstreamPrice incorperates the downstream price table to its own price table.
func (pt *PriceTable) UpdateDownstreamPrice(ctx context.Context, method string, downstreamPrice int64) (int64, error) {

	// Update the downstream price.
	pt.priceTableMap.Store(method, downstreamPrice)
	pt.logger(ctx, "[Received Resp]:	Downstream price of %s updated to %d\n", method, downstreamPrice)

	// var totalPrice int64
	// ownPrice, _ := t.ptmap.LoadOrStore("ownprice", t.initprice)
	// totalPrice = ownPrice.(int64) + downstreamPrice
	// t.ptmap.Store("totalprice", totalPrice)
	// pt.logger(ctx, "[Received Resp]:	Total price updated to %d\n", totalPrice)
	return downstreamPrice, nil
}
