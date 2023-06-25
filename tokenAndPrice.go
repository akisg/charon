package charon

import (
	"context"
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
