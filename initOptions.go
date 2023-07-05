package charon

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc/metadata"
)

// NewPriceTable creates a new instance of PriceTable.
func NewPriceTable(initprice int64, nodeName string, callmap map[string][]string) (priceTable *PriceTable) {
	priceTable = &PriceTable{
		initprice:          initprice,
		nodeName:           nodeName,
		callMap:            callmap,
		priceTableMap:      sync.Map{},
		rateLimiting:       false,
		loadShedding:       false,
		pinpointThroughput: false,
		pinpointLatency:    false,
		pinpointQueuing:    false,
		rateLimiter:        make(chan int64, 1),
		tokensLeft:         10,
		tokenUpdateRate:    time.Millisecond * 10,
		lastUpdateTime:     time.Now(),
		tokenUpdateStep:    1,
		tokenStrategy:      "all",
		throughputCounter:  0,
		priceUpdateRate:    time.Millisecond * 10,
		observedDelay:      time.Duration(0),
		clientTimeOut:      time.Millisecond * 5,
		priceStep:          1,
		debug:              false,
		debugFreq:          4000,
	}
	// priceTable.rateLimiter <- 1
	// Only refill the tokens when the interceptor is for enduser.
	if priceTable.nodeName == "client" {
		go priceTable.tokenRefill()
	} else if priceTable.pinpointThroughput {
		go priceTable.throughputCheck()
	} else if priceTable.pinpointLatency {
		go priceTable.latencyCheck()
	}

	return priceTable
}

func NewCharon(nodeName string, callmap map[string][]string, options map[string]interface{}) *PriceTable {
	priceTable := &PriceTable{
		initprice:           0,
		nodeName:            nodeName,
		callMap:             callmap,
		priceTableMap:       sync.Map{},
		rateLimiting:        false,
		rateLimitWaiting:    false,
		loadShedding:        false,
		pinpointThroughput:  false,
		pinpointLatency:     false,
		pinpointQueuing:     false,
		rateLimiter:         make(chan int64, 1),
		tokensLeft:          10,
		tokenUpdateRate:     time.Millisecond * 10,
		lastUpdateTime:      time.Now(),
		tokenUpdateStep:     1,
		tokenRefillDist:     "fixed",
		tokenStrategy:       "all",
		throughputCounter:   0,
		priceUpdateRate:     time.Millisecond * 10,
		observedDelay:       time.Duration(0),
		clientTimeOut:       time.Duration(0),
		throughputThreshold: 0,
		latencyThreshold:    time.Duration(0),
		priceStep:           1,
		debug:               false,
		debugFreq:           4000,
		guidePrice:          -1,
	}

	// create a new incoming context with the "request-id" as "0"
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("request-id", "0"))

	if debug, ok := options["debug"].(bool); ok {
		priceTable.debug = debug
	}

	if debugFreq, ok := options["debugFreq"].(int64); ok {
		priceTable.debugFreq = debugFreq
		// print the debug and debugFreq of the node if the name is not client
		priceTable.logger(ctx, "debug and debugFreq of %s set to %v and %v\n", nodeName, priceTable.debug, debugFreq)
	}

	if initprice, ok := options["initprice"].(int64); ok {
		priceTable.initprice = initprice
		// print the initprice of the node if the name is not client
		priceTable.logger(ctx, "initprice of %s set to %d\n", nodeName, priceTable.initprice)
	}

	if rateLimiting, ok := options["rateLimiting"].(bool); ok {
		priceTable.rateLimiting = rateLimiting
		priceTable.logger(ctx, "rateLimiting 		of %s set to %v\n", nodeName, rateLimiting)
	}

	if loadShedding, ok := options["loadShedding"].(bool); ok {
		priceTable.loadShedding = loadShedding
		priceTable.logger(ctx, "loadShedding 		of %s set to %v\n", nodeName, loadShedding)
	}

	if pinpointThroughput, ok := options["pinpointThroughput"].(bool); ok {
		priceTable.pinpointThroughput = pinpointThroughput
		priceTable.logger(ctx, "pinpointThroughput	of %s set to %v\n", nodeName, pinpointThroughput)
	}

	if pinpointLatency, ok := options["pinpointLatency"].(bool); ok {
		priceTable.pinpointLatency = pinpointLatency
		priceTable.logger(ctx, "pinpointLatency		of %s set to %v\n", nodeName, pinpointLatency)
	}

	if pinpointQueuing, ok := options["pinpointQueuing"].(bool); ok {
		priceTable.pinpointQueuing = pinpointQueuing
		priceTable.logger(ctx, "pinpointQueuing		of %s set to %v\n", nodeName, pinpointQueuing)
	}

	if tokensLeft, ok := options["tokensLeft"].(int64); ok {
		priceTable.tokensLeft = tokensLeft
		priceTable.logger(ctx, "tokensLeft		of %s set to %v\n", nodeName, tokensLeft)
	}

	if tokenUpdateRate, ok := options["tokenUpdateRate"].(time.Duration); ok {
		priceTable.tokenUpdateRate = tokenUpdateRate
		priceTable.logger(ctx, "tokenUpdateRate		of %s set to %v\n", nodeName, tokenUpdateRate)
	}

	if tokenUpdateStep, ok := options["tokenUpdateStep"].(int64); ok {
		priceTable.tokenUpdateStep = tokenUpdateStep
		priceTable.logger(ctx, "tokenUpdateStep		of %s set to %v\n", nodeName, tokenUpdateStep)
	}

	if tokenRefillDist, ok := options["tokenRefillDist"].(string); ok {
		// if the tokenRefillDist is not "fixed" or "uniform", then set it to be "fixed"
		if tokenRefillDist != "fixed" && tokenRefillDist != "uniform" {
			tokenRefillDist = "fixed"
		}
		priceTable.tokenRefillDist = tokenRefillDist
		priceTable.logger(ctx, "tokenRefillDist		of %s set to %v\n", nodeName, tokenRefillDist)
	}

	if tokenStrategy, ok := options["tokenStrategy"].(string); ok {
		// if the tokenStrategy is not "all" or "uniform", then set it to be "all"
		if tokenStrategy != "all" && tokenStrategy != "uniform" {
			tokenStrategy = "all"
		}
		priceTable.tokenStrategy = tokenStrategy
		priceTable.logger(ctx, "tokenStrategy		of %s set to %v\n", nodeName, tokenStrategy)
	}

	if priceUpdateRate, ok := options["priceUpdateRate"].(time.Duration); ok {
		priceTable.priceUpdateRate = priceUpdateRate
		priceTable.logger(ctx, "priceUpdateRate		of %s set to %v\n", nodeName, priceUpdateRate)
	}

	if clientTimeOut, ok := options["clientTimeOut"].(time.Duration); ok {
		priceTable.clientTimeOut = clientTimeOut
		priceTable.logger(ctx, "clientTimeout		of %s set to %v\n", nodeName, clientTimeOut)
	}

	// priceTable.rateLimitWaiting = true if and only if the clientTimeOut is set to be greater than 0 duration
	if priceTable.clientTimeOut > 0 {
		priceTable.rateLimitWaiting = true
	} else {
		priceTable.rateLimitWaiting = false
	}
	priceTable.logger(ctx, "rateLimitWaiting 	of %s set to %v\n", nodeName, priceTable.rateLimitWaiting)

	if throughputThreshold, ok := options["throughputThreshold"].(int64); ok {
		priceTable.throughputThreshold = throughputThreshold
		priceTable.logger(ctx, "throughputThreshold	of %s set to %v\n", nodeName, throughputThreshold)
	}

	if latencyThreshold, ok := options["latencyThreshold"].(time.Duration); ok {
		priceTable.latencyThreshold = latencyThreshold
		priceTable.logger(ctx, "latencyThreshold	of %s set to %v\n", nodeName, latencyThreshold)
	}

	if priceStep, ok := options["priceStep"].(int64); ok {
		priceTable.priceStep = priceStep
		priceTable.logger(ctx, "priceStep		of %s set to %v\n", nodeName, priceStep)
	}

	if guidePrice, ok := options["guidePrice"].(int64); ok {
		priceTable.guidePrice = guidePrice
		priceTable.logger(ctx, "guidePrice		of %s set to %v\n", nodeName, guidePrice)
	}

	// Rest of the code remains the same
	if priceTable.nodeName == "client" {
		go priceTable.tokenRefill()
	} else {
		if priceTable.pinpointQueuing && priceTable.pinpointThroughput {
			go priceTable.checkBoth()
		} else if priceTable.pinpointThroughput {
			go priceTable.throughputCheck()
		} else if priceTable.pinpointLatency {
			go priceTable.latencyCheck()
		} else if priceTable.pinpointQueuing {
			go priceTable.queuingCheck()
		}
	}

	return priceTable
}

// tokenRefill is a goroutine that refills the tokens in the price table.
func (pt *PriceTable) tokenRefill() {
	for range time.Tick(pt.tokenUpdateRate) {
		// add tokens to the client deterministically or randomly, depending on the tokenRefillDist
		if pt.tokenRefillDist == "fixed" {
			pt.tokensLeft += pt.tokenUpdateStep
		} else if pt.tokenRefillDist == "uniform" {
			pt.tokensLeft += rand.Int63n(pt.tokenUpdateStep)
		}

		pt.lastUpdateTime = time.Now()
		pt.unblockRateLimiter()
		// ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("request-id", "0"))
		// pt.logger(ctx, "[TokenRefill]: Tokens refilled. Tokens left: %d\n", pt.tokensLeft)
	}
}
