package charon

import (
	"math/rand"
	"sync"
	"time"
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
		// debug:              false,
		// debugFreq:          4000,
	}
	// priceTable.rateLimiter <- 1
	// Only refill the tokens when the interceptor is for enduser.
	if priceTable.nodeName == "client" {
		go priceTable.tokenRefill(priceTable.tokenRefillDist, priceTable.tokenUpdateStep, priceTable.tokenUpdateRate)
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
		invokeAfterRL:       false,
		lazyResponse:        false,
		tokensLeft:          10,
		tokenUpdateRate:     time.Millisecond * 10,
		lastUpdateTime:      time.Now(),
		lastRateLimitedTime: time.Now().Add(-time.Second),
		tokenUpdateStep:     1,
		tokenRefillDist:     "fixed",
		tokenStrategy:       "all",
		priceStrategy:       "step",
		throughputCounter:   0,
		priceUpdateRate:     time.Millisecond * 10,
		observedDelay:       time.Duration(0),
		clientTimeOut:       time.Duration(0),
		clientBackoff:       time.Duration(0),
		randomRateLimit:     -1,
		throughputThreshold: 0,
		latencyThreshold:    time.Duration(0),
		priceStep:           1,
		priceAggregation:    "maximal",
		guidePrice:          -1,
	}

	// create a new incoming context with the "request-id" as "0"
	// ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("request-id", "0"))

	if debugOpt, ok := options["debug"].(bool); ok {
		debug = debugOpt
	}

	// if debugFreq, ok := options["debugFreq"].(int64); ok {
	// 	priceTable.debugFreq = debugFreq
	// 	// print the debug and debugFreq of the node if the name is not client
	// 	logger("debug and debugFreq of %s set to %v and %v\n", nodeName, priceTable.debug, )
	// }

	if initprice, ok := options["initprice"].(int64); ok {
		priceTable.initprice = initprice
		// print the initprice of the node if the name is not client
		logger("initprice of %s set to %d\n", nodeName, priceTable.initprice)
	}

	if rateLimiting, ok := options["rateLimiting"].(bool); ok {
		priceTable.rateLimiting = rateLimiting
		logger("rateLimiting 		of %s set to %v\n", nodeName, rateLimiting)
	}

	if loadShedding, ok := options["loadShedding"].(bool); ok {
		priceTable.loadShedding = loadShedding
		logger("loadShedding 		of %s set to %v\n", nodeName, loadShedding)
	}

	if pinpointThroughput, ok := options["pinpointThroughput"].(bool); ok {
		priceTable.pinpointThroughput = pinpointThroughput
		logger("pinpointThroughput	of %s set to %v\n", nodeName, pinpointThroughput)
	}

	if pinpointLatency, ok := options["pinpointLatency"].(bool); ok {
		priceTable.pinpointLatency = pinpointLatency
		logger("pinpointLatency		of %s set to %v\n", nodeName, pinpointLatency)
	}

	if pinpointQueuing, ok := options["pinpointQueuing"].(bool); ok {
		priceTable.pinpointQueuing = pinpointQueuing
		logger("pinpointQueuing		of %s set to %v\n", nodeName, pinpointQueuing)
	}

	if invokeAfterRL, ok := options["invokeAfterRL"].(bool); ok {
		priceTable.invokeAfterRL = invokeAfterRL
		logger("invokeAfterRL		of %s set to %v\n", nodeName, invokeAfterRL)
	}

	if lazyResponse, ok := options["lazyResponse"].(bool); ok {
		priceTable.lazyResponse = lazyResponse
		logger("lazyResponse		of %s set to %v\n", nodeName, lazyResponse)
	}

	if tokensLeft, ok := options["tokensLeft"].(int64); ok {
		priceTable.tokensLeft = tokensLeft
		logger("tokensLeft		of %s set to %v\n", nodeName, tokensLeft)
	}

	if tokenUpdateRate, ok := options["tokenUpdateRate"].(time.Duration); ok {
		priceTable.tokenUpdateRate = tokenUpdateRate
		logger("tokenUpdateRate		of %s set to %v\n", nodeName, tokenUpdateRate)
	}

	if tokenUpdateStep, ok := options["tokenUpdateStep"].(int64); ok {
		priceTable.tokenUpdateStep = tokenUpdateStep
		logger("tokenUpdateStep		of %s set to %v\n", nodeName, tokenUpdateStep)
	}

	if tokenRefillDist, ok := options["tokenRefillDist"].(string); ok {
		// if the tokenRefillDist is not "fixed" or "uniform", then set it to be "fixed"
		if tokenRefillDist != "fixed" && tokenRefillDist != "uniform" && tokenRefillDist != "poisson" {
			tokenRefillDist = "fixed"
		}
		priceTable.tokenRefillDist = tokenRefillDist
		logger("tokenRefillDist		of %s set to %v\n", nodeName, tokenRefillDist)
	}

	if tokenStrategy, ok := options["tokenStrategy"].(string); ok {
		// if the tokenStrategy is not "all" or "uniform", then set it to be "all"
		if tokenStrategy != "all" && tokenStrategy != "uniform" {
			tokenStrategy = "all"
		}
		priceTable.tokenStrategy = tokenStrategy
		logger("tokenStrategy		of %s set to %v\n", nodeName, tokenStrategy)
	}

	if priceStrategy, ok := options["priceStrategy"].(string); ok {
		// if the priceStrategy is not "step" or "proportional", then set it to be "step"
		priceTable.priceStrategy = priceStrategy
		logger("priceStrategy		of %s set to %v\n", nodeName, priceStrategy)
	}

	if priceUpdateRate, ok := options["priceUpdateRate"].(time.Duration); ok {
		priceTable.priceUpdateRate = priceUpdateRate
		logger("priceUpdateRate		of %s set to %v\n", nodeName, priceUpdateRate)
	}

	if clientTimeOut, ok := options["clientTimeOut"].(time.Duration); ok {
		priceTable.clientTimeOut = clientTimeOut
		logger("clientTimeout		of %s set to %v\n", nodeName, clientTimeOut)
	}

	if clientBackoff, ok := options["clientBackoff"].(time.Duration); ok {
		priceTable.clientBackoff = clientBackoff
		logger("clientBackoff		of %s set to %v\n", nodeName, clientBackoff)
	}

	if randomRateLimit, ok := options["randomRateLimit"].(int64); ok {
		priceTable.randomRateLimit = randomRateLimit
		logger("randomRateLimit		of %s set to %v\n", nodeName, randomRateLimit)
	}

	// priceTable.rateLimitWaiting = true if and only if the clientTimeOut is set to be greater than 0 duration
	if priceTable.clientTimeOut > 0 {
		priceTable.rateLimitWaiting = true
	} else {
		priceTable.rateLimitWaiting = false
	}
	logger("rateLimitWaiting 	of %s set to %v\n", nodeName, priceTable.rateLimitWaiting)

	if throughputThreshold, ok := options["throughputThreshold"].(int64); ok {
		priceTable.throughputThreshold = throughputThreshold
		logger("throughputThreshold	of %s set to %v\n", nodeName, throughputThreshold)
	}

	if latencyThreshold, ok := options["latencyThreshold"].(time.Duration); ok {
		priceTable.latencyThreshold = latencyThreshold
		logger("latencyThreshold	of %s set to %v\n", nodeName, latencyThreshold)
	}

	if priceStep, ok := options["priceStep"].(int64); ok {
		priceTable.priceStep = priceStep
		logger("priceStep		of %s set to %v\n", nodeName, priceStep)
	}

	if priceAggregation, ok := options["priceAggregation"].(string); ok {
		// if the priceAggregation is not "maximal" or "additive", then set it to be "maximal"
		if priceAggregation != "maximal" && priceAggregation != "additive" {
			priceAggregation = "maximal"
		}
		priceTable.priceAggregation = priceAggregation
		logger("priceAggregation	of %s set to %v\n", nodeName, priceAggregation)
	}

	if guidePrice, ok := options["guidePrice"].(int64); ok {
		priceTable.guidePrice = guidePrice
		logger("guidePrice		of %s set to %v\n", nodeName, guidePrice)
	}

	// Rest of the code remains the same
	if priceTable.nodeName == "client" {
		go priceTable.tokenRefill(priceTable.tokenRefillDist, priceTable.tokenUpdateStep, priceTable.tokenUpdateRate)
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

	// initialize the price table: Store the initprice to the priceTableMap with key "ownprice", and all method
	// and all method-nodeName pairs to the priceTableMap with key "method-nodeName"
	priceTable.priceTableMap.Store("ownprice", priceTable.initprice)
	for method, _ := range priceTable.callMap {
		priceTable.priceTableMap.Store(method, priceTable.initprice)
		for _, node := range priceTable.callMap[method] {
			priceTable.priceTableMap.Store(method+"-"+node, priceTable.initprice)
		}
	}
	return priceTable
}

// tokenRefill is a goroutine that refills the tokens in the price table.
func (pt *PriceTable) tokenRefill(tokenRefillDist string, tokenUpdateStep int64, tokenUpdateRate time.Duration) {
	if tokenRefillDist == "poisson" {
		// Create a ticker with an initial tick duration
		ticker := time.NewTicker(pt.initialTokenUpdateInterval())
		defer ticker.Stop()
		// lambda is 1 over pt.tokenUpdateRate.Milliseconds(), but make lambda a float64
		lambda := float64(1) / float64(tokenUpdateRate.Milliseconds())

		for range ticker.C {
			// Add tokens to the client deterministically or randomly, depending on the tokenRefillDist
			// if pt.tokenRefillDist == "fixed" {
			pt.tokensLeft += tokenUpdateStep
			// }

			pt.lastUpdateTime = time.Now()
			pt.unblockRateLimiter()

			// Adjust the tick duration based on the exponential distribution
			ticker.Reset(pt.nextTokenUpdateInterval(lambda))
		}
	} else {
		for range time.Tick(tokenUpdateRate) {
			// add tokens to the client deterministically or randomly, depending on the tokenRefillDist
			if tokenRefillDist == "fixed" {
				pt.tokensLeft += tokenUpdateStep
			} else if tokenRefillDist == "uniform" {
				pt.tokensLeft += rand.Int63n(tokenUpdateStep * 2)
			}

			pt.lastUpdateTime = time.Now()
			pt.unblockRateLimiter()
			// ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("request-id", "0"))
			// logger("[TokenRefill]: Tokens refilled. Tokens left: %d\n", pt.tokensLeft)
		}
	}
}

// initialTokenUpdateInterval returns the initial tick duration for the tokenRefill.
func (pt *PriceTable) initialTokenUpdateInterval() time.Duration {
	// Return the desired initial tick duration
	return pt.tokenUpdateRate
}

// nextTokenUpdateInterval returns the next tick duration for the tokenRefill based on the exponential distribution.
func (pt *PriceTable) nextTokenUpdateInterval(lambda float64) time.Duration {
	// Calculate the next tick duration based on the exponential distribution
	// For example, you can use a lambda value of 0.5 for the exponential distribution
	// lambda := 0.5
	nextTickDuration := time.Duration(rand.ExpFloat64()/lambda) * time.Millisecond
	// ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("request-id", "0"))
	// logger("[TokenRefill]: Next tick duration: %v\n", nextTickDuration)

	if nextTickDuration <= 0 {
		// Handle the case when nextTickDuration is non-positive
		nextTickDuration = time.Millisecond // Set a default positive duration
	}
	// Return the next tick duration
	return time.Duration(nextTickDuration)
}
