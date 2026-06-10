package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
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

// BinanceTickerEvent maps to the Binance public WebSocket ticker stream schema
type BinanceTickerEvent struct {
	Symbol      string      `json:"s"`
	LastPrice   interface{} `json:"c"`
	IgnoreClose interface{} `json:"C"` // Explicitly handle C to prevent overwriting c
}

// streamLiveMarketData connects to real exchange ticker socket
func streamLiveMarketData(symbol string, tickChan chan<- Tick, wg *sync.WaitGroup, stopChan <-chan struct{}) {
	defer wg.Done()

	// Ensure the symbol is lowercase for connection path
	streamSymbol := strings.ToLower(symbol)
	urlStr := "wss://stream.binance.com:9443/ws/" + streamSymbol + "@ticker"

	dialer := websocket.DefaultDialer
	conn, _, err := dialer.Dial(urlStr, nil)
	if err != nil {
		log.Fatalf("[Ingestion Error] Failed to connect to exchange: %v", err)
	}
	defer conn.Close()

	log.Printf("[Ingestion] Connected live to stream endpoint: %s\n", urlStr)

	// Create channel to handle WebSocket reader failures
	errChan := make(chan error, 1)

	// Read loop
	go func() {
		log.Println("[Ingestion] Reader loop started")
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("[Ingestion Error] ReadMessage failed: %v\n", err)
				errChan <- err
				return
			}
			log.Printf("[Ingestion] Received message of length %d\n", len(message))

			var event BinanceTickerEvent
			if err := json.Unmarshal(message, &event); err != nil {
				log.Printf("[Ingestion Error] Unmarshal failed: %v\n", err)
				continue
			}

			var price float64
			switch val := event.LastPrice.(type) {
			case string:
				var err error
				price, err = strconv.ParseFloat(val, 64)
				if err != nil {
					log.Printf("[Ingestion Error] ParseFloat failed for %q: %v\n", val, err)
					continue
				}
			case float64:
				price = val
			case json.Number:
				var err error
				price, err = val.Float64()
				if err != nil {
					log.Printf("[Ingestion Error] json.Number Float64 failed: %v\n", err)
					continue
				}
			default:
				log.Printf("[Ingestion Error] Unexpected type for c: %T\n", val)
				continue
			}

			// Push clean typed data directly into Strategy Engine
			// Keep symbol in uppercase for uniform downstream usage
			tickChan <- Tick{
				Symbol:    strings.ToUpper(event.Symbol),
				Price:     price,
				Timestamp: time.Now(),
			}
		}
	}()

	// Wait for stop signal or WS read error
	select {
	case <-stopChan:
		log.Printf("[Ingestion] Market Stream Stop Signal Received for %s\n", symbol)
		// Send a close frame before disconnecting
		_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	case err := <-errChan:
		log.Printf("[Ingestion Warning] Read disconnect error: %v\n", err)
	}
}

// runStrategyEngine tracks rolling prices to calculate simple moving average and trigger orders
func runStrategyEngine(
	tickChan <-chan Tick, orderChan chan<- TradeOrder, wg *sync.WaitGroup, stopChan <-chan struct{},
) {
	defer wg.Done()
	log.Println("[Strategy] Engine running...")

	var priceHistory []float64
	windowSize := 5

	for {
		select {
		case <-stopChan:
			log.Println("[Strategy] Engine stopped")
			return

		case tick := <-tickChan:
			priceHistory = append(priceHistory, tick.Price)
			if len(priceHistory) > windowSize {
				priceHistory = priceHistory[1:]
			}
			if len(priceHistory) < windowSize {
				log.Printf("[Strategy] Gathering data... (%d/%d)\n", len(priceHistory), windowSize)
				continue
			}

			sum := 0.0
			for _, p := range priceHistory {
				sum += p
			}
			sma := sum / float64(windowSize)

			log.Printf("[Strategy] Price: $%.2f | %d-Tick SMA: $%.2f\n", tick.Price, windowSize, sma)

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

// GenerateSignature signs API parameters with your Secret Key using HMAC-SHA256
func GenerateSignature(payload, secretKey string) string {
	mac := hmac.New(sha256.New, []byte(secretKey))
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

// sendLiveMarketOrder signs API request parameters and executes a live order
func sendLiveMarketOrder(symbol, side, quantity string, apiKey, secretKey string) {
	baseURL := "https://api.binance.com/api/v3/order"
	timestamp := time.Now().UnixNano() / int64(time.Millisecond)

	// Format parameters exactly as required by the endpoint protocol
	params := url.Values{}
	params.Add("symbol", strings.ToUpper(symbol))
	params.Add("side", string(side))
	params.Add("type", "MARKET")
	params.Add("quantity", quantity)
	params.Add("timestamp", strconv.FormatInt(timestamp, 10))

	// Generate signature from query payload
	payloadQueryString := params.Encode()
	signature := GenerateSignature(payloadQueryString, secretKey)

	// Form final complete authenticated URL destination
	finalURL := fmt.Sprintf("%s?%s&signature=%s", baseURL, payloadQueryString, signature)

	req, err := http.NewRequest("POST", finalURL, nil)
	if err != nil {
		log.Printf("[Execution Critical Error] Failed to create order request: %v\n", err)
		return
	}
	req.Header.Set("X-MBX-APIKEY", apiKey) // Authenticate headers

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[Execution Critical Error] Order route failure: %v\n", err)
		return
	}
	defer resp.Body.Close()

	log.Printf("[Execution Status] Server Response Network Status Received: %s\n", resp.Status)
}

// runExecutionEngine routes orders according to paper or live configurations
func runExecutionEngine(orderChan <-chan TradeOrder, wg *sync.WaitGroup, stopChan <-chan struct{}) {
	defer wg.Done()

	// Parse trading mode from environment
	tradingMode := strings.ToLower(os.Getenv("TRADING_MODE"))
	if tradingMode == "" {
		tradingMode = "paper" // Default trading mode
	}

	log.Printf("[Execution] Engine running in %s mode...\n", strings.ToUpper(tradingMode))

	var apiKey, secretKey string
	if tradingMode == "live" {
		apiKey = os.Getenv("BINANCE_API_KEY")
		secretKey = os.Getenv("BINANCE_SECRET_KEY")
		if apiKey == "" || secretKey == "" {
			log.Fatalf("[Execution Fatal] API credentials missing for live execution (BINANCE_API_KEY / BINANCE_SECRET_KEY)")
		}
	}

	// Open paper trades log if in paper mode
	var paperLogFile *os.File
	var err error
	if tradingMode == "paper" {
		paperLogFile, err = os.OpenFile("paper_trades.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Printf("[Execution Warning] Could not open paper_trades.log: %v\n", err)
		} else {
			defer paperLogFile.Close()
		}
	}

	// Initial virtual capital balance (for paper trading simulation)
	balanceUSD := 10000.0
	positionCrypto := 0.0

	for {
		select {
		case <-stopChan:
			log.Printf("[Execution] Stopping engine. Final Portfolio Balance: $%.2f | Position: %.4f\n", balanceUSD, positionCrypto)
			return
		case order := <-orderChan:
			if tradingMode == "live" {
				// Live trading path
				log.Printf("[Execution] Routing live order to API for %s %s...\n", order.Side, order.Symbol)
				qtyStr := strconv.FormatFloat(order.Volume, 'f', 4, 64)
				go sendLiveMarketOrder(order.Symbol, string(order.Side), qtyStr, apiKey, secretKey)
			} else {
				// Paper trading simulation path
				cost := order.Price * order.Volume
				timestampStr := time.Now().Format(time.RFC3339)

				if order.Side == SignalBuy {
					if balanceUSD >= cost {
						balanceUSD -= cost
						positionCrypto += order.Volume
						logMsg := fmt.Sprintf("%s | 🟩 [PAPER EXECUTION] BOUGHT %.4f %s at $%.2f | Balance: $%.2f | Position: %.4f\n",
							timestampStr, order.Volume, order.Symbol, order.Price, balanceUSD, positionCrypto)
						log.Print(logMsg)

						if paperLogFile != nil {
							_, _ = paperLogFile.WriteString(logMsg)
						}
					} else {
						log.Println("⚠️ [RISK REJECTION] Insufficient funds to execute BUY paper order.")
					}
				} else if order.Side == SignalSell {
					if positionCrypto >= order.Volume {
						positionCrypto -= order.Volume
						balanceUSD += cost
						logMsg := fmt.Sprintf("%s | 🟥 [PAPER EXECUTION] SOLD %.4f %s at $%.2f | Balance: $%.2f | Position: %.4f\n",
							timestampStr, order.Volume, order.Symbol, order.Price, balanceUSD, positionCrypto)
						log.Print(logMsg)

						if paperLogFile != nil {
							_, _ = paperLogFile.WriteString(logMsg)
						}
					} else {
						log.Println("⚠️ [RISK REJECTION] Insufficient crypto asset balance to execute SELL paper order.")
					}
				}
			}
		}
	}
}

// --- 5. SYSTEM COORDINATOR ---
func main() {
	// Configure logging format
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	rand.Seed(time.Now().UnixNano())

	// Create communication channels
	tickChannel := make(chan Tick, 100)
	orderChannel := make(chan TradeOrder, 100)
	stopChannel := make(chan struct{})

	var wg sync.WaitGroup

	// Binance WebSocket ticker stream symbol (e.g., "BTCUSDT")
	symbol := "BTCUSDT"

	// Spin up pipeline components concurrently using goroutines
	wg.Add(3)
	go streamLiveMarketData(symbol, tickChannel, &wg, stopChannel)
	go runStrategyEngine(tickChannel, orderChannel, &wg, stopChannel)
	go runExecutionEngine(orderChannel, &wg, stopChannel)

	// Clean Shutdown Setup (Listen for Ctrl+C / kill signals)
	shutdownSig := make(chan os.Signal, 1)
	signal.Notify(shutdownSig, syscall.SIGINT, syscall.SIGTERM)

	<-shutdownSig
	log.Println("\n[System] Shutdown signal received. Cleaning pipelines gracefully...")

	close(stopChannel) // Notifies all routines to stop processing loops safely
	wg.Wait()          // Wait until all processes safely finish closing out
	log.Println("[System] Core engine shutdown clean. Safe to terminate.")
}
