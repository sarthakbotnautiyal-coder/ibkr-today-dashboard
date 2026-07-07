# ibkr-today-dashboard

Read-only real-time Streamlit dashboard for **[sarthakbotnautiyal-coder/ibkr_trader_engine](https://github.com/sarthakbotnautiyal-coder/ibkr_trader_engine)**.

Visualizes today's:
- Open positions with live uPnL
- Closed trades with final P/L
- Live signal-intent loop (what the bot is *trying* to do)
- Rejection funnel (why entries don't fill)
- Exit-decision voting (L1/L2/L3 transitions)
- Engine health (PID, uptime, log activity)

**Status:** Phase 0 wireframe spike in progress. See [TASK-2026-327](https://github.com/sarthakbotnautiyal-coder/ibkr_trader_engine) in the agent vault for full design.

## Stack
- Streamlit (port 5558)
- streamlit-autorefresh (5s polling)
- pandas
- Read-only SQLite reader (positions.db)

## Run (Phase 0 dev)
```bash
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
./venv/bin/streamlit run dashboard.py --server.port 5558 --server.headless true
```

Open http://localhost:5558.

## Data Sources
- `$IBKR_ENGINE_DIR/data/positions.db` — `positions` and `signals` tables (RO)
- `$IBKR_ENGINE_DIR/logs/engine_YYYY-MM-DD.log` — live decision stream

Default: `/Users/ubexbot/.openclaw/workspace-venkat/ibkr_trader_engine`.

## License
Internal use only.