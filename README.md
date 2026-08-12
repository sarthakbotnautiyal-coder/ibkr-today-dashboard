# IBKR Trading Dashboard

A real-time Streamlit dashboard that displays live trading data from the IBKR trading engine. Monitor your open positions, closed trades, P&L metrics, and trading analytics — all with a clean, dark-themed interface.

## Features

### Live Dashboard
- **P&L Summary** — Today's realized P&L, unrealized P&L, and total P&L at a glance
- **Open Positions** — Current positions with strike prices, contracts, exit votes, and unrealized P&L
- **Closed Today** — Trades closed in the current session with final P&L
- **Today's Activity** — Quick metrics: open positions, closed trades, skipped signals
- Auto-refreshes every 5 seconds

### Trade History
- **Daily & Monthly Analytics** — Group your trading data by day or month
- **P&L Charts** — Visual breakdown of profit/loss with a clear zero baseline
- **Trade Volume** — See how many trades executed per day or month
- **Win Rate** — Track your win percentage and best trading day/month
- **Detailed Breakdown** — Sortable table with dates, trade counts, wins, and P&L

All charts are static (non-interactive) and designed for quick visual scanning.

## Quick Start

### Prerequisites
- Python 3.8 or later
- The IBKR trading engine running and generating trade data
- Access to the engine's `positions.db` database and log files

### Installation

1. Clone or download this repository:
```bash
git clone <repository-url>
cd ibkr-today-dashboard
```

2. Create a Python virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

### Running the Dashboard

#### Option 1: Default (Engine in Standard Location)
If your IBKR engine is at `/Users/ubexbot/.openclaw/workspace-venkat/ibkr_trader_engine`:
```bash
./venv/bin/streamlit run dashboard.py --server.port 8501
```

Then open your browser to **http://localhost:8501**

#### Option 2: Custom Engine Location
If your engine is elsewhere, set the environment variable:
```bash
export IBKR_ENGINE_DIR=/path/to/your/ibkr_trader_engine
./venv/bin/streamlit run dashboard.py --server.port 8501
```

#### Option 2b: Windows

Use PowerShell — set the variable on its own line, then run:

```powershell
$env:IBKR_ENGINE_DIR = "C:\Users\you\ibkr_trader_engine"
streamlit run dashboard.py --server.port 8501
```

> **Avoid `set VAR=value && streamlit ...` in `cmd.exe`.** That form assigns
> everything up to the `&&`, including the space before it, so the engine
> directory silently gains a trailing space and every derived path misses.
> The dashboard strips it defensively, but PowerShell above is the safer form.

Engine health detection uses `psutil` on Windows (`pgrep`/`lsof` are
Unix-only), so make sure `pip install -r requirements.txt` has been run. If
`psutil` is missing the dashboard still runs — the health panel just reports
the engine as stopped.

#### Option 3: Headless Mode (Server/Background)
```bash
./venv/bin/streamlit run dashboard.py --server.port 8501 --server.headless true
```

### Stopping the Dashboard
Press `Ctrl+C` in the terminal where Streamlit is running.

## Usage

### Live Dashboard Tab
- **Refreshes automatically** every 5 seconds with the latest data
- Shows only today's activity
- Click on any row to see more details (future feature)

### Trade History Tab
- **Toggle between Daily and Monthly** views at the top
- Displays historical P&L trends and trade statistics
- Use the table to find your best and worst trading periods
- Page isolation: **Live Dashboard is hidden** when viewing Trade History, so you can scroll without interruption

## Data Sources

The dashboard reads from your IBKR trading engine:

| Source | Purpose | Read-Only |
|--------|---------|-----------|
| `positions.db` | Live positions and trade history | ✓ Yes |
| `engine_YYYY-MM-DD.log` | Real-time event stream | ✓ Yes |

**Default engine location:** `/Users/ubexbot/.openclaw/workspace-venkat/ibkr_trader_engine`

All database reads are read-only — the dashboard will never modify your trading data.

## Troubleshooting

### "Database not found"
- Verify your engine is running and creating `positions.db`
- Check that `IBKR_ENGINE_DIR` points to the correct folder
- Ensure you have read permissions on the database

### Dashboard not updating
- Confirm the engine is active and logging trades
- Check that the auto-refresh interval is set to 5 seconds (default)
- Restart the dashboard: `Ctrl+C` and rerun the command above

### "No historical data" in Trade History
- Trade History shows only closed trades from the `positions` table
- If you have no closed trades yet, this is expected
- After your first few trades are closed, they'll appear here

### Port already in use
If port 8501 is in use, specify a different one:
```bash
./venv/bin/streamlit run dashboard.py --server.port 9999
```
Then visit **http://localhost:9999**

## Technical Stack

- **Streamlit** — Web UI framework for rapid dashboards
- **Pandas** — Data manipulation and analysis
- **Altair** — Static, non-interactive charting
- **streamlit-autorefresh** — 5-second polling for live updates
- **SQLite** — Read-only database access

## Configuration

Edit `dashboard.py` to customize:
- Refresh interval (line: `REFRESH_MS = 5000`)
- Default port (`.claude/launch.json`)
- Color scheme (CSS variables in `CUSTOM_CSS`)

## Performance Notes

- Dashboard queries are lightweight and don't block the trading engine
- Live refreshes pause when viewing Trade History (historical data doesn't change every 5s)
- Charts are pre-rendered and update only on new data
- Typically uses <50MB RAM

## Support

For issues or feature requests, check your engine's logs:
```bash
tail -f $IBKR_ENGINE_DIR/logs/engine_*.log
```

## License

Internal use only.