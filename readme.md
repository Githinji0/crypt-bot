# CryptBot

An event-driven, concurrent, high-frequency cryptocurrency trading bot written in Go. The bot ingests live market data directly from exchange WebSocket streams, processes it through a moving average momentum strategy engine, and executes trades via simulated (Paper Trading) or live secure API routes.

---

## 🛠 Architecture & Data Pipeline

The bot is designed with a concurrent, thread-safe pipeline utilizing Go's lightweight goroutines and channels to ensure minimal processing latency:

```
                  +----------------------------------+
                  |  Exchange WebSocket Feed (Live)  |
                  +-----------------+----------------+
                                    | Ticker Stream (BTCUSDT)
                                    v
                  +-----------------+----------------+
                  |    streamLiveMarketData (Go)    |
                  +-----------------+----------------+
                                    | channel (Tick)
                                    v
                  +-----------------+----------------+
                  |      runStrategyEngine (SMA)     |
                  +-----------------+----------------+
                                    | channel (TradeOrder)
                                    v
                  +-----------------+----------------+
                  |       runExecutionEngine         |
                  +-----------------+----------------+
                                    |
            +-----------------------+-----------------------+
            | (Trading Mode = paper)                        | (Trading Mode = live)
            v                                               v
+-----------+------------+                      +-----------+------------+
| Paper Execution Engine |                      | Live Execution Engine  |
| - Local Balance/Pos    |                      | - HMAC-SHA256 Signing  |
| - paper_trades.log     |                      | - POST Order to API    |
+------------------------+                      +------------------------+
```

1. **Ingestion Engine (`streamLiveMarketData`):** Manages a persistent WebSocket connection to the Binance stream. Incoming JSON frames are parsed, validated, and normalized before being pushed onto the `Tick` channel.
2. **Strategy Engine (`runStrategyEngine`):** Subscribes to the `Tick` channel, maintains a rolling price window, calculates the Simple Moving Average (SMA), and fires buying or selling signals into the `TradeOrder` channel when price thresholds are crossed.
3. **Execution Engine (`runExecutionEngine`):** Coordinates order processing based on the configured mode. It either tracks simulated balances and writes history to `paper_trades.log`, or signs and transmits signed orders directly to the exchange API.

---

## 📈 Trading Strategy & Theory

### Simple Moving Average (SMA) Momentum
The strategy calculates a rolling average of tick prices over a defined window size ($N = 5$):

$$\text{SMA} = \frac{1}{N} \sum_{i=1}^{N} P_i$$

Where $P_i$ is the asset price at tick $i$.

### Entry and Exit Signals
Signals are triggered when the asset price diverges from the SMA by a configurable momentum threshold (defaulted to 0.1%):

*   **BUY Signal:** Triggered when the current price crosses above the SMA by $0.1\%$:
    $$P_{\text{current}} > \text{SMA} \times 1.001$$
*   **SELL Signal:** Triggered when the current price drops below the SMA by $0.1\%$:
    $$P_{\text{current}} < \text{SMA} \times 0.999$$

*Note: In paper trading mode, virtual transactions execution balances are validated locally to check risk rules before logging the trade.*

---

## ⚡ Execution Modes

### 1. Paper Trading (Dry Run) - Default
Used to evaluate strategy performance and latency without financial risk.
*   The bot connects to the live WebSocket feed for accurate, real-time market action.
*   Trade executions are simulated locally against a virtual capital balance (starting at $\$10,000$).
*   Every mock trade is logged instantly to the console and appended to a persistent [paper_trades.log]\

### 2. Live Trading
Performs actual order placement on the Binance API.
*   Order parameters (`symbol`, `side`, `quantity`, `timestamp`) are signed via **HMAC-SHA256** using your private API Secret Key.
*   Requests are sent as authenticated POST requests with the API Key supplied in the header (`X-MBX-APIKEY`).

---

## 🚀 Setup & Usage Instructions

### Prerequisites
*   Go (version 1.20+)
*   API credentials from your exchange (optional, only required for Live Trading)

### Installation
Clone the repository and install the WebSocket library dependency:
```bash
go get github.com/gorilla/websocket
```

### Running the Bot (Paper Mode)
Start the bot in the default paper trading configuration:
```bash
go run main.go
```

### Running the Bot (Live Mode)
To activate live order routing, supply your API credentials and set the trading mode environment variables before starting:

**On PowerShell (Windows):**
```powershell
$env:TRADING_MODE="live"
$env:BINANCE_API_KEY="your_api_key"
$env:BINANCE_SECRET_KEY="your_secret_key"
go run main.go
```

**On Bash (Linux/macOS):**
```bash
export TRADING_MODE="live"
export BINANCE_API_KEY="your_api_key"
export BINANCE_SECRET_KEY="your_secret_key"
go run main.go
```

---

## ⏱ Performance & Production Optimization

