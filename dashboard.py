"""
ibkr_today — Phase 0 Wireframe Spike
====================================
Single-file Streamlit dashboard rendering all 8 panels with representative
DUMMY data shaped to today's actual (2026-07-07) engine activity.

Metadata (for Obsidian / repo tracking — not interpreted by Python):
---
task_id: TASK-2026-327
master_task: "[[Tasks/Master/TASK-2026-327-ibkr-today-dashboard-design]]"
phase: 0-wireframe
---

GATE: Awaiting Sarthak wireframe approval before Phase 1 (real data wiring).

Data sources for Phase 1 (not wired here):
- $IBKR_ENGINE_DIR/data/positions.db (SQLite, mode=ro, uri=True)
- $IBKR_ENGINE_DIR/logs/engine_YYYY-MM-DD.log (offset-cached tail)

For Phase 0 Panel 8, we read the last 20 lines of the real log as a sample
to demonstrate stream rendering. No live updates in Phase 0.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ENGINE_DIR = Path("/Users/ubexbot/.openclaw/workspace-venkat/ibkr_trader_engine")
LOG_PATH = ENGINE_DIR / "logs" / "engine_2026-07-07.log"
LOG_SAMPLE_LINES = 20

PAGE_TITLE = "ibkr_today"
PAGE_ICON = "📈"

# ---------------------------------------------------------------------------
# Page config + theme (must be first Streamlit call)
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title=PAGE_TITLE,
    page_icon=PAGE_ICON,
    layout="wide",
    initial_sidebar_state="collapsed",
)

CUSTOM_CSS = """
<style>
  /* Streamlit default #0e1117 background */
  .stApp { background-color: #0e1117; }
  section.main > div { padding-top: 1rem; }

  /* Panel cards */
  .panel {
    background: #161b22;
    border: 1px solid #21262d;
    border-radius: 8px;
    padding: 10px 12px;
    margin-bottom: 8px;
  }
  .panel h3 {
    margin: 0 0 10px 0;
    font-size: 12px;
    font-weight: 600;
    color: #8b949e;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin: 0 0 6px 0;
  }

  /* Health strip metrics */
  .metric-row { display: flex; gap: 24px; flex-wrap: wrap; align-items: center; }
  .metric { display: flex; flex-direction: column; gap: 2px; }
  .metric .label { font-size: 11px; color: #8b949e; text-transform: uppercase; }
  .metric .value { font-size: 18px; color: #e6edf3; font-weight: 600; }
  .metric .value.mono { font-family: 'SF Mono', Menlo, Consolas, monospace; font-size: 13px; }

  /* Status pill */
  .pill {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 12px;
    font-size: 12px;
    font-weight: 600;
    text-transform: uppercase;
  }
  .pill-green { background: #0d4429; color: #00d97e; border: 1px solid #00d97e; }
  .pill-red { background: #4d1417; color: #ff4b4b; border: 1px solid #ff4b4b; }
  .pill-amber { background: #4d3a14; color: #f0a020; border: 1px solid #f0a020; }
  .pill-blue { background: #14294d; color: #58a6ff; border: 1px solid #58a6ff; }

  /* Big number for P&L */
  .big-number {
    font-size: 40px;
    font-weight: 700;
    line-height: 1;
    margin: 4px 0;
    font-feature-settings: 'tnum';
  }
  .big-number.pos { color: #00d97e; }
  .big-number.neg { color: #ff4b4b; }
  .big-number.zero { color: #8b949e; }

  /* Sub counts grid for P&L panel */
  .counts-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; margin-top: 6px; }
  .count-cell { background: #0d1117; padding: 4px 8px; border-radius: 6px; text-align: center; }
  .count-cell .c-label { font-size: 9px; color: #8b949e; text-transform: uppercase; }
  .count-cell .c-value { font-size: 18px; font-weight: 600; color: #e6edf3; margin-top: 1px; }

  /* Mono log */
  .log-line { font-family: 'SF Mono', Menlo, Consolas, monospace; font-size: 12px; padding: 2px 6px; color: #c9d1d9; }
  .log-line.INFO { color: #c9d1d9; }
  .log-line.WARN { color: #f0a020; }
  .log-line.ERROR { color: #ff4b4b; }
  .log-line .tag { color: #58a6ff; font-weight: 600; }
  .log-line .ts { color: #6e7681; }

  /* Dataframe tweaks */
  .stDataFrame { font-size: 12px; }

  /* Donut */
  .donut-wrap { display: flex; align-items: center; gap: 16px; }
  .donut {
    width: 110px; height: 110px; border-radius: 50%;
    background: conic-gradient(
      #6e7681 0% 40%,
      #58a6ff 40% 70%,
      #00d97e 70% 100%
    );
    position: relative;
    flex-shrink: 0;
  }
  .donut::after {
    content: '';
    position: absolute;
    inset: 18px;
    border-radius: 50%;
    background: #161b22;
  }
  .donut-legend { display: flex; flex-direction: column; gap: 4px; font-size: 12px; }
  .donut-legend .item { display: flex; align-items: center; gap: 6px; }
  .donut-legend .sw { width: 10px; height: 10px; border-radius: 2px; }

  /* Closed table footer */
  .table-footer {
    display: flex; gap: 24px; margin-top: 8px; padding-top: 8px;
    border-top: 1px solid #21262d; font-size: 13px; color: #8b949e;
  }
  .table-footer .fv { color: #e6edf3; font-weight: 600; }
  .table-footer .fv.green { color: #00d97e; }

  /* Open positions empty state */
  .empty-state {
    text-align: center;
    padding: 24px 16px;
    color: #6e7681;
    border: 1px dashed #21262d;
    border-radius: 6px;
    background: #0d1117;
  }
  .empty-state .big { font-size: 28px; margin-bottom: 6px; color: #8b949e; }

  /* Entries list */
  .entry-row {
    display: grid;
    grid-template-columns: 60px 140px 1fr 80px 70px;
    gap: 10px;
    padding: 3px 8px;
    font-size: 12px;
    font-family: 'SF Mono', Menlo, Consolas, monospace;
    border-bottom: 1px solid #21262d;
  }
  .entry-row.header { color: #8b949e; font-family: inherit; text-transform: uppercase; font-size: 10px; }
  .entry-row .pnl.pos { color: #00d97e; }
  .entry-row .pnl.neg { color: #ff4b4b; }

  /* Header bar */
  .top-bar {
    display: flex; justify-content: space-between; align-items: baseline;
    margin-bottom: 6px;
  }
  .top-bar h1 { margin: 0; font-size: 18px; color: #e6edf3; }
  .top-bar .clock { font-family: 'SF Mono', Menlo, Consolas, monospace; color: #8b949e; font-size: 13px; }
</style>
"""

st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Auto-refresh — every 5s, unlimited
# ---------------------------------------------------------------------------

st_autorefresh(interval=5000, limit=None, key="refresh")

# ---------------------------------------------------------------------------
# Dummy data — shaped to 2026-07-07 actual
# ---------------------------------------------------------------------------

NOW = datetime.now()  # ticks on every 5s autorefresh (Phase 0 demo)
TODAY = NOW.date()


def _fmt_money(value: float) -> str:
    sign = "+" if value >= 0 else "-"
    return f"{sign}${abs(value):,.2f}"


# Panel 1 — Engine Health
HEALTH = {
    "pid": 12345,
    "uptime": "06:35",  # 09:30 → 16:05
    "status": "stopped",  # engine stopped at 16:05
    "last_log": "TICK SPX=7503.35 | EM=3.02 | GEX=1 | regime=neutral",
    "last_log_age_s": 32,
    "log_size": "1.1 MB",
    "warn_count": 185,
    "err_count": 0,
    "started_at": NOW.replace(hour=9, minute=30, second=0),
    "db_mtime": NOW - timedelta(seconds=45),
}

# Panel 2 — P&L Summary
PNL = {
    "realized": 116.00,
    "unrealized": 0.00,
    "counts": {"open": 0, "closed": 5, "signals": 12, "rejected": 8},
}

# Panel 3 — Live Signal Intent
TICK = {
    "spx": "7503.35",
    "em": "3.02",
    "gex": "1",
    "regime": "neutral",
    "rsi": "49.1",
    "gex_regime": "dealer_long",
    "ts": "15:59:52 ET",
}
LAYER = "L2"
RECENT_ENTRIES = [
    {"pos_id": 33, "side": "CALL", "strike": "7555/7575", "credit": 0.21, "fill_status": "filled", "fill_latency_s": 314},
    {"pos_id": 32, "side": "PUT", "strike": "7405/7385", "credit": 0.35, "fill_status": "filled", "fill_latency_s": 31},
    {"pos_id": 31, "side": "PUT", "strike": "7415/7395", "credit": 0.24, "fill_status": "filled", "fill_latency_s": 73},
    {"pos_id": 30, "side": "PUT", "strike": "7420/7400", "credit": 0.22, "fill_status": "filled", "fill_latency_s": 64},
    {"pos_id": 29, "side": "PUT", "strike": "7425/7415", "credit": 0.21, "fill_status": "filled", "fill_latency_s": 269},
]
SKIP_REASONS = {
    "premium_failed": 23,
    "overlap_detected": 16,
    "spot_distance_failed": 7,
}

# Panel 4 — Open Positions (0 today)
OPEN_POSITIONS: list[dict] = []

# Panel 5 — Closed Today
CLOSED_TODAY = [
    {"pos_id": 29, "side": "PUT", "strike": "7425/7415", "entry_time": "09:34", "exit_time": "16:00", "hold": "6h26m", "credit": 0.20, "exit": 0.00, "pnl": 20.00, "reason": "EOD_EXPIRE"},
    {"pos_id": 30, "side": "PUT", "strike": "7420/7400", "entry_time": "09:46", "exit_time": "16:00", "hold": "6h14m", "credit": 0.20, "exit": 0.00, "pnl": 20.00, "reason": "EOD_EXPIRE"},
    {"pos_id": 31, "side": "PUT", "strike": "7415/7395", "entry_time": "10:12", "exit_time": "16:00", "hold": "5h48m", "credit": 0.20, "exit": 0.00, "pnl": 20.00, "reason": "EOD_EXPIRE"},
    {"pos_id": 32, "side": "PUT", "strike": "7405/7385", "entry_time": "10:15", "exit_time": "16:00", "hold": "5h45m", "credit": 0.35, "exit": 0.00, "pnl": 35.00, "reason": "EOD_EXPIRE"},
    {"pos_id": 33, "side": "CALL", "strike": "7555/7575", "entry_time": "12:01", "exit_time": "16:00", "hold": "3h59m", "credit": 0.20, "exit": 0.00, "pnl": 21.00, "reason": "EOD_EXPIRE"},
]

# Panel 6 — Rejection Funnel
REJECTION_COUNTS = {
    "premium_failed": 4,
    "overlap_detected": 3,
    "max_positions": 1,
    "day_gate": 0,
}

# Panel 7 — Exit Vote Tally (last [EXIT CHECK] aggregate)
VOTE_TALLY = {
    "votes=0 [STAY]": 18,   # dominant — neutral mean-reversion regime
    "votes=1 [STAY]": 6,
    "votes=2 [EXIT]": 1,
}


# ---------------------------------------------------------------------------
# Panel 8 — Engine Decision Stream (sample from real log)
# ---------------------------------------------------------------------------

TAG_RE = re.compile(r"\[(INFO|WARN|ERROR)\]")
TAGNAME_RE = re.compile(r"\[(TICK|ENTRY|ENTRY_PENDING|FILLED_CONFIRMED|EXIT CHECK|EOD_EXPIRE|ENTRY_TIMEOUT|SKIP|COMBO_MKTDATA_CANCEL)\]")


def _parse_log_sample(path: Path, n: int) -> list[dict]:
    """Read last n lines of log file; return parsed rows. Phase-0 sample only."""
    if not path.exists():
        return []
    try:
        with path.open("r") as f:
            lines = f.readlines()
    except OSError:
        return []
    sample = lines[-n:]
    rows: list[dict] = []
    for line in sample:
        line = line.rstrip("\n")
        m_level = TAG_RE.search(line)
        m_tag = TAGNAME_RE.search(line)
        rows.append({
            "ts": line.split(" ET ", 1)[0] if " ET " in line else line[:20],
            "level": m_level.group(1) if m_level else "?",
            "tag": m_tag.group(1) if m_tag else "-",
            "msg": line,
        })
    return rows


LOG_SAMPLE = _parse_log_sample(LOG_PATH, LOG_SAMPLE_LINES)


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------

# Top bar
st.markdown(
    f"""
    <div class="top-bar">
      <h1>📈 ibkr_today — {TODAY.isoformat()}</h1>
      <div class="clock">render {NOW.strftime('%H:%M:%S ET')} · auto-refresh 5s</div>
    </div>
    """,
    unsafe_allow_html=True,
)


# ---- Panel 1 — Engine Health Strip ---------------------------------------

status_pill_class = (
    "pill-green" if HEALTH["status"] == "running" else "pill-red" if HEALTH["status"] == "stopped" else "pill-amber"
)
health_cols = st.columns([1, 1, 1, 1, 1, 1, 1])

with health_cols[0]:
    st.markdown(
        f'<div class="metric"><span class="label">PID</span>'
        f'<span class="value mono">{HEALTH["pid"]}</span></div>',
        unsafe_allow_html=True,
    )
with health_cols[1]:
    st.markdown(
        f'<div class="metric"><span class="label">Uptime</span>'
        f'<span class="value mono">{HEALTH["uptime"]}</span></div>',
        unsafe_allow_html=True,
    )
with health_cols[2]:
    st.markdown(
        f'<div class="metric"><span class="label">Status</span>'
        f'<span class="pill {status_pill_class}">{HEALTH["status"]}</span></div>',
        unsafe_allow_html=True,
    )
with health_cols[3]:
    st.markdown(
        f'<div class="metric"><span class="label">Last log</span>'
        f'<span class="value mono" style="font-size:13px">{HEALTH["last_log"][:38]}…</span>'
        f'<span style="font-size:11px;color:#6e7681">{HEALTH["last_log_age_s"]}s ago</span></div>',
        unsafe_allow_html=True,
    )
with health_cols[4]:
    st.markdown(
        f'<div class="metric"><span class="label">Log size</span>'
        f'<span class="value">{HEALTH["log_size"]}</span></div>',
        unsafe_allow_html=True,
    )
with health_cols[5]:
    st.markdown(
        f'<div class="metric"><span class="label">WARN</span>'
        f'<span class="value mono" style="color:#f0a020">{HEALTH["warn_count"]}</span></div>',
        unsafe_allow_html=True,
    )
with health_cols[6]:
    st.markdown(
        f'<div class="metric"><span class="label">ERROR</span>'
        f'<span class="value mono" style="color:#ff4b4b">{HEALTH["err_count"]}</span></div>',
        unsafe_allow_html=True,
    )

st.markdown(
    f'<div style="font-size:11px;color:#6e7681;margin-top:4px">'
    f'DB mtime: {HEALTH["db_mtime"].strftime("%H:%M:%S")} · '
    f'Started: {HEALTH["started_at"].strftime("%H:%M:%S")} · '
    f'Phase 0 — dummy data</div>',
    unsafe_allow_html=True,
)


# ---- Row 1: P&L (left wide) | Signal Intent (right narrow) --------------

row1_l, row1_r = st.columns([3, 2])

with row1_l:
    st.markdown('<div class="panel"><h3>Panel 2 — P&amp;L Summary</h3>', unsafe_allow_html=True)
    realized_class = "pos" if PNL["realized"] > 0 else "neg" if PNL["realized"] < 0 else "zero"
    unreal_class = "pos" if PNL["unrealized"] > 0 else "neg" if PNL["unrealized"] < 0 else "zero"
    st.markdown(
        f'<div style="display:flex;gap:24px;align-items:flex-end">'
        f'  <div>'
        f'    <div style="font-size:11px;color:#8b949e;text-transform:uppercase">Realized today</div>'
        f'    <div class="big-number {realized_class}">{_fmt_money(PNL["realized"])}</div>'
        f'  </div>'
        f'  <div style="margin-bottom:4px">'
        f'    <div style="font-size:11px;color:#8b949e;text-transform:uppercase">Unrealized</div>'
        f'    <div class="big-number {unreal_class}" style="font-size:32px">{_fmt_money(PNL["unrealized"])}</div>'
        f'  </div>'
        f'</div>',
        unsafe_allow_html=True,
    )
    counts_html = "".join(
        f'<div class="count-cell"><div class="c-label">{k}</div>'
        f'<div class="c-value">{v}</div></div>'
        for k, v in PNL["counts"].items()
    )
    st.markdown(f'<div class="counts-grid">{counts_html}</div>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

with row1_r:
    st.markdown('<div class="panel"><h3>Panel 3 — Live Signal Intent</h3>', unsafe_allow_html=True)
    layer_pill = "pill-blue" if LAYER == "L1" else "pill-amber" if LAYER == "L2" else "pill-green"
    st.markdown(
        f'<div style="font-family:SF Mono,Menlo,Consolas,monospace;font-size:13px;color:#c9d1d9">'
        f'<span style="color:#58a6ff">[TICK]</span> '
        f'<span style="color:#6e7681">{TICK["ts"]}</span><br>'
        f'SPX=<b>{TICK["spx"]}</b> &nbsp; EM=<b>{TICK["em"]}</b> &nbsp; GEX=<b>{TICK["gex"]}</b><br>'
        f'regime=<b>{TICK["regime"]}</b> &nbsp; RSI=<b>{TICK["rsi"]}</b><br>'
        f'GEX_regime=<b>{TICK["gex_regime"]}</b></div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<div style="margin-top:8px;display:flex;align-items:center;gap:8px">'
        f'<span style="font-size:11px;color:#8b949e;text-transform:uppercase">Layer</span>'
        f'<span class="pill {layer_pill}">{LAYER}</span>'
        f'<span style="font-size:11px;color:#6e7681">neutral mean-reversion regime</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        '<div style="font-size:11px;color:#8b949e;text-transform:uppercase;margin-top:12px">'
        'Last 5 [ENTRY] attempts</div>',
        unsafe_allow_html=True,
    )
    entry_header = (
        '<div class="entry-row header">'
        '<div>Pos</div><div>Side·Strike</div><div>Status</div>'
        '<div>Credit</div><div>Latency</div></div>'
    )
    entry_rows = "".join(
        f'<div class="entry-row">'
        f'<div>#{e["pos_id"]}</div>'
        f'<div>{e["side"]} {e["strike"]}</div>'
        f'<div style="color:{"#00d97e" if e["fill_status"]=="filled" else "#f0a020"}">{e["fill_status"]}</div>'
        f'<div>${e["credit"]:.2f}</div>'
        f'<div style="color:#8b949e">{e["fill_latency_s"]}s</div>'
        f'</div>'
        for e in RECENT_ENTRIES
    )
    st.markdown(entry_header + entry_rows, unsafe_allow_html=True)

    st.markdown(
        '<div style="font-size:11px;color:#8b949e;text-transform:uppercase;margin-top:12px">'
        'Skip reasons today</div>',
        unsafe_allow_html=True,
    )
    skip_total = sum(SKIP_REASONS.values()) or 1
    skip_bars = "".join(
        f'<div style="display:flex;align-items:center;gap:8px;font-size:12px;margin:3px 0">'
        f'<div style="width:160px;color:#c9d1d9">{r}</div>'
        f'<div style="flex:1;background:#21262d;border-radius:3px;height:11px;overflow:hidden">'
        f'<div style="width:{100 * v / skip_total:.0f}%;height:100%;background:#ff4b4b;opacity:0.7"></div>'
        f'</div>'
        f'<div style="width:36px;text-align:right;color:#8b949e;font-family:SF Mono,Menlo,Consolas,monospace">{v}</div>'
        f'</div>'
        for r, v in SKIP_REASONS.items()
    )
    st.markdown(skip_bars, unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)


# ---- Row 2: Closed Today (left) | Open Pos + Exit Vote + Rejection (right) -

row2_l, row2_r = st.columns([3, 2])

with row2_l:
    st.markdown(
        '<div class="panel"><h3>Panel 4 — Open Positions</h3>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div style="padding:8px 12px;background:#0d1117;border:1px dashed #21262d;border-radius:6px;font-size:12px;color:#8b949e">∅  No open positions — all 5 closed via EOD_EXPIRE at 16:00</div>',
        unsafe_allow_html=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)

    # ---- Panel 5 — Closed Today (nested under same column body)
    st.markdown('<div class="panel"><h3>Panel 5 — Closed Today</h3>', unsafe_allow_html=True)
    closed_df = pd.DataFrame(CLOSED_TODAY)
    closed_df = closed_df.rename(
        columns={
            "pos_id": "Pos #",
            "side": "Side",
            "strike": "Strike",
            "entry_time": "Entry",
            "exit_time": "Exit",
            "hold": "Hold",
            "credit": "Credit",
            "exit": "Exit $",
            "pnl": "PnL",
            "reason": "Reason",
        }
    )
    styled = (
        closed_df.style.format({"Credit": "${:.2f}", "Exit $": "${:.2f}", "PnL": "${:+.2f}"})
        .map(lambda v: "color: #00d97e" if isinstance(v, (int, float)) and v > 0 else "", subset=["PnL"])
    )
    st.dataframe(styled, width='stretch', hide_index=True, height=108)

    total_pnl = sum(p["pnl"] for p in CLOSED_TODAY)
    wins = sum(1 for p in CLOSED_TODAY if p["pnl"] > 0)
    win_rate = 100 * wins / len(CLOSED_TODAY) if CLOSED_TODAY else 0
    avg_hold_min = sum(
        int(p["hold"].replace("h", "").replace("m", "").split("h")[0]) * 60
        + int(p["hold"].split("h")[1].replace("m", ""))
        for p in CLOSED_TODAY
    ) // len(CLOSED_TODAY) if CLOSED_TODAY else 0

    st.markdown(
        f'<div class="table-footer">'
        f'<div>count <span class="fv">{len(CLOSED_TODAY)}</span></div>'
        f'<div>total P/L <span class="fv green">${total_pnl:+.2f}</span></div>'
        f'<div>win rate <span class="fv">{win_rate:.0f}%</span></div>'
        f'<div>avg hold <span class="fv">{avg_hold_min // 60}h{avg_hold_min % 60:02d}m</span></div>'
        f'</div>',
        unsafe_allow_html=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)

with row2_r:
    st.markdown('<div class="panel"><h3>Panel 7 — Exit Vote Tally</h3>', unsafe_allow_html=True)
    total_votes = sum(VOTE_TALLY.values()) or 1

    v0 = VOTE_TALLY["votes=0 [STAY]"]
    v1 = VOTE_TALLY["votes=1 [STAY]"]
    v2 = VOTE_TALLY["votes=2 [EXIT]"]
    p0 = 100 * v0 / total_votes
    p1 = 100 * v1 / total_votes
    p2 = 100 * v2 / total_votes
    c0_end = p0
    c1_end = p0 + p1
    c2_end = p0 + p1 + p2
    donut_grad = (
        f"#6e7681 0% {c0_end:.1f}%, "
        f"#58a6ff {c0_end:.1f}% {c1_end:.1f}%, "
        f"#00d97e {c1_end:.1f}% {c2_end:.1f}%"
    )

    st.markdown(
        f'<div class="donut-wrap">'
        f'<div class="donut" style="background: conic-gradient({donut_grad})"></div>'
        f'<div class="donut-legend">'
        f'<div class="item"><div class="sw" style="background:#6e7681"></div>votes=0 [STAY] <span style="color:#8b949e;margin-left:auto">{v0}</span></div>'
        f'<div class="item"><div class="sw" style="background:#58a6ff"></div>votes=1 [STAY] <span style="color:#8b949e;margin-left:auto">{v1}</span></div>'
        f'<div class="item"><div class="sw" style="background:#00d97e"></div>votes=2 [EXIT] <span style="color:#8b949e;margin-left:auto">{v2}</span></div>'
        f'<div style="margin-top:6px;color:#8b949e;font-size:11px">last 25 [EXIT CHECK] reads</div>'
        f'</div>'
        f'</div>',
        unsafe_allow_html=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)

    # ---- Panel 6 — Rejection Funnel (in right column, under Panel 7)
    st.markdown('<div class="panel"><h3>Panel 6 — Rejection Funnel</h3>', unsafe_allow_html=True)
    rej_df = pd.DataFrame(
        sorted(REJECTION_COUNTS.items(), key=lambda kv: -kv[1]),
        columns=["reason", "count"],
    ).set_index("reason")
    st.bar_chart(rej_df, height=180, width='stretch', color='#ff4b4b')
    st.markdown("</div>", unsafe_allow_html=True)
st.markdown("</div>", unsafe_allow_html=True)


# ---- Panel 8 — Engine Decision Stream (compact strip + expander) ----

if LOG_SAMPLE:
    last3 = LOG_SAMPLE[-2:]
    rows_html = "".join(
        f'<div class="log-line {r["level"]}">'
        f'<span class="ts">{r["ts"]}</span> '
        f'<span style="color:#8b949e">[{r["level"]}]</span> '
        f'<span class="tag">[{r["tag"]}]</span> '
        f'{r["msg"].split(r["tag"] + "] ", 1)[-1][:140]}'
        f'</div>'
        for r in last3
    )
    st.markdown(
        '<div class="panel">'
        '<h3 style="display:flex;justify-content:space-between;align-items:center">'
        '<span>Panel 8 — Engine Decision Stream</span>'
        f'<span style="font-size:10px;color:#6e7681;text-transform:none;letter-spacing:0">'
        f'last 3 of {len(LOG_SAMPLE)} lines · from {LOG_PATH.name}'
        '</span>'
        '</h3>'
        f'<div style="background:#0d1117;padding:6px 10px;border-radius:6px;'
        f'border:1px solid #21262d">{rows_html}</div>'
        '</div>',
        unsafe_allow_html=True,
    )
    with st.expander(f"View full {len(LOG_SAMPLE)}-line stream", expanded=False):
        st.markdown(
            '<div style="background:#0d1117;padding:8px 12px;border-radius:6px;'
            'border:1px solid #21262d;max-height:300px;overflow-y:auto">',
            unsafe_allow_html=True,
        )
        for row in LOG_SAMPLE:
            level_cls = row["level"]
            st.markdown(
                f'<div class="log-line {level_cls}">'
                f'<span class="ts">{row["ts"]}</span> '
                f'<span style="color:#8b949e">[{row["level"]}]</span> '
                f'<span class="tag">[{row["tag"]}]</span> '
                f'{row["msg"].split(row["tag"] + "] ", 1)[-1][:160]}'
                f'</div>',
                unsafe_allow_html=True,
            )
        st.markdown("</div>", unsafe_allow_html=True)
        st.caption(
            f"Phase 0 sample: last {len(LOG_SAMPLE)} lines from `{LOG_PATH.name}`. "
            f"Phase 1 will wire a real offset-cached tail reader (5s autorefresh). "
            f"Tag filter chips (TICK / ENTRY / EXIT / ORDER / WARN / ERROR) deferred to Phase 1."
        )
else:
    st.markdown(
        '<div class="panel"><h3>Panel 8 — Engine Decision Stream</h3>'
        f'<div style="font-size:12px;color:#8b949e;padding:8px 0">'
        f'Log not found at `{LOG_PATH}`. (Read-only — engine log dir preserved as-is.)'
        '</div></div>',
        unsafe_allow_html=True,
    )

st.caption(
    "Phase 0 wireframe · dummy data shaped to 2026-07-07 actual · "
    "GATE: awaiting Sarthak approval before Phase 1 (real data wiring)."
)
