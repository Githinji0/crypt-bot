package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Tick struct {
	Symbol    string
	Price     float64
	Timestamp time.Time
}
type Signal string

const (
	SignalBuy  Signal = "BUY"
	SignalSell Signal = "SELL"
	SignalHold Signal = "HOLD"
)

type TradeOrder struct {
	Symbol string
	Side   Signal
	Price  float64
	Volume float64
}

// Simulates a live exchange WebSocket stream
func marketDataStream(symbol string, tickChan chan<- Tick, wg *sync.WaitGroup, stopChan <-chan struct{}) {
	defer wg.Done()
	fmt.Printf("Market Stream Started %s.......\n", symbol)

	currPrice := 65000.0

	for {
		select {
		case <-stopChan:
			fmt.Printf("Market Stream Stopped %s\n", symbol)
			return
			// Simulating an update every second
		case <-time.After(1 * time.Second):
			change := currPrice * (rand.Float64() - 0.5) * 0.01
			currPrice += change
			tickChan <- Tick{
				Symbol:    symbol,
				Price:     currPrice,
				Timestamp: time.Now(),
			}

		}
	}

}


// Tracks a rolling window of prices to calculate a simple moving average

func runStrategyEngine (
	tickChan <-chan Tick, orderChan chan<- TradeOrder, wg *sync.WaitGroup, stopChan <-chan struct{},
){
	defer wg.Done()
	fmt.Println("[Strategy] Engine running...")

	var priceHistory []float64
	windowSize := 5


	for {
		select{
			case <-stopChan:
				fmt.Println("[Strategy] Engine stopped")
				return

			case tick:= <-tickChan:
				priceHistory = append(priceHistory, tick.Price)
				if len(priceHistory) > windowSize{
					priceHistory = priceHistory[1:]
				}
				if len(priceHistory) == windowSize{
					fmt.Printf("[Strategy] Gathering data... (%d/%d)\n", len(priceHistory), windowSize)
				continue
					
				}


				sum := 0.0
			for _, p := range priceHistory {
				sum += p
			}
			sma := sum / float64(windowSize)

			fmt.Printf("[Strategy] Price: $%.2f | %d-Tick SMA: $%.2f\n", tick.Price, windowSize, sma)

			// Simple Momentum Rules:
			// If price crosses above SMA by 0.1%, Buy. If it falls below by 0.1%, Sell.
			if tick.Price > sma*1.001 {
				orderChan <- TradeOrder{Symbol: tick.Symbol, Side: SignalBuy, Price: tick.Price, Volume: 0.05}
			} else if tick.Price < sma*0.999 {
				orderChan <- TradeOrder{Symbol: tick.Symbol, Side: SignalSell, Price: tick.Price, Volume: 0.05}
			}
			}
		}
	}


}

// Validates risk rules and signs simulated order execution logs
func runExecutionEngine(orderChan <-chan TradeOrder, wg *sync.WaitGroup, stopChan <-chan struct{}) {
	defer wg.Done()
	fmt.Println("[Execution] Engine listening for trading signals...")

	// Initial virtual capital balance
	balanceUSD := 10000.0
	positionCrypto := 0.0

	for {
		select {
		case <-stopChan:
			fmt.Println("[Execution] Stopping engine. Final Portfolio Value calculated locally.")
			return
		case order := <-orderChan:
			cost := order.Price * order.Volume

			// Basic Risk Management Validation Check
			if order.Side == SignalBuy {
				if balanceUSD >= cost {
					balanceUSD -= cost
					positionCrypto += order.Volume
					fmt.Printf("🟩 [EXECUTION] BOUGHT %.4f %s at $%.2f | Balance: $%.2f | Position: %.4f\n", 
						order.Volume, order.Symbol, order.Price, balanceUSD, positionCrypto)
				} else {
					fmt.Println("⚠️ [RISK REJECTION] Insufficient funds to execute BUY order.")
				}
			} else if order.Side == SignalSell {
				if positionCrypto >= order.Volume {
					positionCrypto -= order.Volume
					balanceUSD += cost
					fmt.Printf("🟥 [EXECUTION] SOLD %.4f %s at $%.2f | Balance: $%.2f | Position: %.4f\n", 
						order.Volume, order.Symbol, order.Price, balanceUSD, positionCrypto)
				} else {
					fmt.Println("⚠️ [RISK REJECTION] Insufficient crypto asset balance to execute SELL order.")
				}
			}
		}
	}
}

// --- 5. SYSTEM COORDINATOR ---
func main() {
	rand.Seed(time.Now().UnixNano())

	// Create communication channels
	tickChannel := make(chan Tick, 100)
	orderChannel := make(chan TradeOrder, 100)
	stopChannel := make(chan struct{})

	var wg sync.WaitGroup

	// Spin up pipeline components concurrently using goroutines
	wg.Add(3)
	go streamMarketData("BTCUSD", tickChannel, &wg, stopChannel)
	go runStrategyEngine(tickChannel, orderChannel, &wg, stopChannel)
	go runExecutionEngine(orderChannel, &wg, stopChannel)

	// Clean Shutdown Setup (Listen for Ctrl+C)
	shutdownSig := make(chan os.Signal, 1)
	signal.Notify(shutdownSig, syscall.SIGINT, syscall.SIGTERM)

	<-shutdownSig
	fmt.Println("\n[System] Shutdown signal received. Cleaning pipelines gracefully...")
	
	close(stopChannel) // Notifies all routines to stop processing loops safely
	wg.Wait()          // Wait until all processes safely finish closing out
	fmt.Println("[System] Core engine shutdown clean. Safe to terminate.")
}
