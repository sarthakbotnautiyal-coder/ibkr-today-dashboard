"""
ibkr_today — Phase 1 (Real Data Wiring)
=======================================
Single-file Streamlit dashboard for IBKR engine live visualization.

Wires 8 panels to real engine sources:
- $IBKR_ENGINE_DIR/data/positions.db    (SQLite, mode=ro, uri=True, query_only)
- $IBKR_ENGINE_DIR/logs/engine_YYYY-MM-DD.log (offset-cached tail)

Architecture:
- SQLite RO via `file:...?mode=ro` + PRAGMA query_only=ON (per TASK-2026-326)
- WAL retry pattern (Layer-1) from KB SQLite-WAL-Resilience.md
- Atomic offset cache via tmpfile + replace (no partial writes)
- Process liveness via pgrep — pattern matches run.py invocation
- Market-hours gate via SharedResources/Scripts/is_market_open.py

Predecessor: TASK-2026-327 (Phase 0 wireframe, commit 3a88e9d)

---
task_id: TASK-2026-328
master_task: "[[Tasks/Master/TASK-2026-328-ibkr-today-dashboard-phase1-deploy]]"
parent: "[[TASK-2026-327-ibkr-today-dashboard-design]]"
phase: 1-real-data
---
"""

from __future__ import annotations

import os
import re
import sqlite3
import subprocess
import tempfile
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import altair as alt
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# ---------------------------------------------------------------------------
# Config layer
# ---------------------------------------------------------------------------

def _load_dotenv(env_path: Path) -> None:
    """Tiny .env loader: KEY=VALUE per line. No python-dotenv dep."""
    if not env_path.exists():
        return
    try:
        text = env_path.read_text()
    except OSError:
        return
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k:
            os.environ.setdefault(k, v)


_load_dotenv(Path(__file__).parent / ".env")

DEFAULT_ENGINE_DIR = "/Users/ubexbot/.openclaw/workspace-venkat/ibkr_trader_engine"
ENGINE_DIR = Path(os.environ.get("IBKR_ENGINE_DIR", DEFAULT_ENGINE_DIR))
DB_PATH = ENGINE_DIR / "data" / "positions.db"
ET = ZoneInfo("America/New_York")

CACHE_DIR = Path.home() / ".cache" / "ibkr_today"
LOG_OFFSET_PATH = CACHE_DIR / "log_offset"
# Marker substring that identifies the engine's cwd. The engine process's
# argv[0] shows only 'run.py' (the shell wrapper sets cwd but not argv[0]),
# so pgrep alone cannot distinguish it from the 5+ sibling run.py processes
# (spx_dashboard, 5556_dashboard, wc2026_dashboard, premium_extractor,
# gex_extractor). We pgrep all run.py candidates then filter by cwd.
ENGINE_PGREP_PATTERN = r"ibkr_trader_engine.*run\.py"  # legacy, unused (kept for grep back-compat)
IBKR_ENGINE_CWD_MARKER = "/ibkr_trader_engine"  # suffix-match cwd path
IS_MARKET_OPEN_SCRIPT = "/Users/ubexbot/.openclaw/vault/vault/SharedResources/Scripts/is_market_open.py"

PAGE_TITLE = "ibkr_today"
PAGE_ICON = "📈"
REFRESH_MS = 5000
DECISION_STREAM_LINES = 50
EXIT_CHECK_SAMPLE_N = 50

# Resolved today's log path (frozen at startup; rolls on day change at next refresh)
TODAY: date = datetime.now(ET).date()
LOG_PATH = ENGINE_DIR / "logs" / f"engine_{TODAY.isoformat()}.log"


# ---------------------------------------------------------------------------
# Page config + theme
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title=PAGE_TITLE,
    page_icon=PAGE_ICON,
    layout="wide",
    initial_sidebar_state="expanded",
)

CUSTOM_CSS = """
<style>
  :root {
    --bg-primary: #0d1117;
    --bg-secondary: #161b22;
    --bg-tertiary: #21262d;
    --text-primary: #e6edf3;
    --text-secondary: #8b949e;
    --text-muted: #6e7681;
    --border: #21262d;
    --accent-green: #00d97e;
    --accent-red: #ff4b4b;
    --accent-blue: #58a6ff;
    --accent-amber: #f0a020;
  }

  * { box-sizing: border-box; }

  .stApp {
    background-color: var(--bg-primary);
  }

  section.main > div {
    padding: 1.5rem 1.2rem;
    max-width: 1400px;
    margin: 0 auto;
  }

  /* Header section */
  .dashboard-header {
    margin-bottom: 2rem;
    border-bottom: 2px solid var(--bg-tertiary);
    padding-bottom: 1.5rem;
  }

  .dashboard-header h1 {
    margin: 0 0 0.5rem 0;
    font-size: 28px;
    font-weight: 700;
    color: var(--text-primary);
    letter-spacing: -0.5px;
  }

  .header-meta {
    display: flex;
    gap: 2rem;
    font-size: 13px;
    color: var(--text-muted);
    font-family: 'SF Mono', monospace;
    flex-wrap: wrap;
  }

  .header-meta > div {
    display: flex;
    gap: 0.5rem;
    align-items: center;
  }

  /* Panels */
  .panel {
    background: var(--bg-secondary);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 1rem;
    margin-bottom: 1rem;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.12);
  }

  .panel h3 {
    margin: 0 0 0.75rem 0;
    font-size: 12px;
    font-weight: 600;
    color: var(--text-secondary);
    text-transform: uppercase;
    letter-spacing: 0.1em;
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  .panel-icon {
    font-size: 16px;
  }

  /* Metrics */
  .metric-row {
    display: flex;
    gap: 2rem;
    flex-wrap: wrap;
    align-items: flex-start;
  }

  .metric {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }

  .metric .label {
    font-size: 12px;
    color: var(--text-secondary);
    text-transform: uppercase;
    letter-spacing: 0.05em;
    font-weight: 600;
  }

  .metric .value {
    font-size: 24px;
    color: var(--text-primary);
    font-weight: 700;
    letter-spacing: -0.5px;
  }

  .metric .value.mono {
    font-family: 'SF Mono', monospace;
    font-size: 16px;
  }

  /* Status pills */
  .pill {
    display: inline-flex;
    align-items: center;
    padding: 6px 12px;
    border-radius: 20px;
    font-size: 12px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    gap: 0.5rem;
  }

  .pill-green {
    background: rgba(0, 217, 126, 0.1);
    color: var(--accent-green);
    border: 1px solid rgba(0, 217, 126, 0.3);
  }

  .pill-red {
    background: rgba(255, 75, 75, 0.1);
    color: var(--accent-red);
    border: 1px solid rgba(255, 75, 75, 0.3);
  }

  .pill-amber {
    background: rgba(240, 160, 32, 0.1);
    color: var(--accent-amber);
    border: 1px solid rgba(240, 160, 32, 0.3);
  }

  .pill-blue {
    background: rgba(88, 166, 255, 0.1);
    color: var(--accent-blue);
    border: 1px solid rgba(88, 166, 255, 0.3);
  }

  .pill-grey {
    background: var(--bg-tertiary);
    color: var(--text-secondary);
    border: 1px solid rgba(139, 148, 158, 0.2);
  }

  /* Big numbers */
  .big-number {
    font-size: 48px;
    font-weight: 700;
    line-height: 1.1;
    margin: 0.5rem 0;
    font-feature-settings: 'tnum';
    letter-spacing: -1px;
  }

  .big-number.pos {
    color: var(--accent-green);
  }

  .big-number.neg {
    color: var(--accent-red);
  }

  .big-number.zero {
    color: var(--text-secondary);
  }

  /* Counts grid */
  .counts-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(100px, 1fr));
    gap: 12px;
    margin-top: 1.5rem;
  }

  .count-cell {
    background: var(--bg-primary);
    padding: 12px;
    border-radius: 8px;
    text-align: center;
    border: 1px solid var(--border);
    transition: all 0.2s ease;
  }

  .count-cell:hover {
    border-color: var(--text-secondary);
  }

  .count-cell .c-label {
    font-size: 10px;
    color: var(--text-secondary);
    text-transform: uppercase;
    letter-spacing: 0.05em;
    font-weight: 600;
    margin-bottom: 0.5rem;
  }

  .count-cell .c-value {
    font-size: 24px;
    font-weight: 700;
    color: var(--text-primary);
  }

  /* Logs */
  .log-line {
    font-family: 'SF Mono', monospace;
    font-size: 13px;
    padding: 8px 10px;
    color: var(--text-primary);
    line-height: 1.5;
    border-bottom: 1px solid rgba(33, 38, 45, 0.5);
  }

  .log-line.INFO {
    color: var(--text-primary);
  }

  .log-line.WARNING {
    color: var(--accent-amber);
    background: rgba(240, 160, 32, 0.05);
  }

  .log-line.ERROR {
    color: var(--accent-red);
    background: rgba(255, 75, 75, 0.05);
  }

  .log-line .tag {
    color: var(--accent-blue);
    font-weight: 600;
  }

  .log-line .ts {
    color: var(--text-muted);
  }

  /* Tables */
  .stDataFrame {
    font-size: 13px;
  }

  .stDataFrame th {
    background-color: var(--bg-tertiary) !important;
    color: var(--text-secondary) !important;
    font-weight: 600 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.05em !important;
    font-size: 11px !important;
  }

  .stDataFrame td {
    padding: 10px 12px !important;
    border-bottom: 1px solid var(--border) !important;
  }

  /* Charts */
  .donut-wrap {
    display: flex;
    align-items: center;
    gap: 2rem;
    padding: 1rem 0;
  }

  .donut {
    width: 140px;
    height: 140px;
    border-radius: 50%;
    position: relative;
    flex-shrink: 0;
  }

  .donut::after {
    content: '';
    position: absolute;
    inset: 28px;
    border-radius: 50%;
    background: var(--bg-secondary);
  }

  .donut-legend {
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
    font-size: 13px;
  }

  .donut-legend .item {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .donut-legend .sw {
    width: 12px;
    height: 12px;
    border-radius: 3px;
    flex-shrink: 0;
  }

  /* Table footer */
  .table-footer {
    display: flex;
    gap: 2rem;
    margin-top: 1rem;
    padding-top: 1rem;
    border-top: 1px solid var(--border);
    font-size: 13px;
    color: var(--text-secondary);
    flex-wrap: wrap;
  }

  .table-footer .fv {
    color: var(--text-primary);
    font-weight: 600;
    font-family: 'SF Mono', monospace;
  }

  .table-footer .fv.green {
    color: var(--accent-green);
  }

  /* Empty states */
  .empty-state {
    text-align: center;
    padding: 2rem;
    color: var(--text-muted);
    border: 2px dashed var(--border);
    border-radius: 8px;
    background: var(--bg-primary);
  }

  .empty-state .big {
    font-size: 32px;
    margin-bottom: 0.5rem;
    color: var(--text-secondary);
  }

  /* Entry rows */
  .entry-row {
    display: grid;
    grid-template-columns: 70px 150px 1fr 90px 80px;
    gap: 12px;
    padding: 10px;
    font-size: 13px;
    font-family: 'SF Mono', monospace;
    border-bottom: 1px solid var(--border);
    align-items: center;
  }

  .entry-row.header {
    color: var(--text-secondary);
    font-family: inherit;
    text-transform: uppercase;
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.05em;
    background: var(--bg-tertiary);
    padding: 8px 10px;
  }

  .entry-row .pnl.pos {
    color: var(--accent-green);
    font-weight: 600;
  }

  .entry-row .pnl.neg {
    color: var(--accent-red);
    font-weight: 600;
  }

  /* Responsive */
  @media (max-width: 768px) {
    section.main > div {
      padding: 1rem;
    }

    .metric-row {
      gap: 1rem;
    }

    .counts-grid {
      grid-template-columns: repeat(2, 1fr);
    }

    .entry-row {
      grid-template-columns: 1fr;
      gap: 4px;
      padding: 8px;
    }

    .entry-row > div {
      display: flex;
      justify-content: space-between;
    }
  }
</style>
"""

st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Auto-refresh (5s)
# ---------------------------------------------------------------------------

PAGE_LIVE = "📊 Live Dashboard"
PAGE_HISTORY = "📈 Trade History"

# The nav widget is rendered further down, but its value is already in
# session_state on every rerun after the first — read it here so the refresh
# timer can be gated before any rendering happens.
ACTIVE_PAGE = st.session_state.get("nav_page") or PAGE_LIVE

# Only poll on the live page. Trade History is historical data that does not
# change every 5s, and a rerun there would fight the user's scroll position.
if ACTIVE_PAGE == PAGE_LIVE:
    st_autorefresh(interval=REFRESH_MS, limit=None, key="refresh")

NOW = datetime.now(ET)


# ---------------------------------------------------------------------------
# SQLite RO reader + retry (Layer-1 WAL resilience)
# ---------------------------------------------------------------------------

def open_ro(db_path: Path, retries: int = 3) -> sqlite3.Connection:
    """Open SQLite in read-only mode with brief WAL-contention retry.

    Hard rules per TASK-2026-326:
        uri=True + mode=ro + PRAGMA query_only=ON
    """
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")
    uri = f"file:{db_path}?mode=ro"
    last_err: sqlite3.OperationalError | None = None
    for i in range(retries):
        try:
            conn = sqlite3.connect(uri, uri=True, timeout=2.0)
            conn.execute("PRAGMA query_only=ON")
            return conn
        except sqlite3.OperationalError as e:
            last_err = e
            if i < retries - 1:
                time.sleep(0.05 * (i + 1))  # 50/100/150ms
    raise last_err if last_err else RuntimeError("open_ro failed without exception")


def safe_query(sql: str, params: tuple = ()) -> tuple[pd.DataFrame | None, str | None]:
    """Run SQL; return (df, None) on success or (None, error_msg) on persistent failure.

    Wrapper used by all panel fetchers — single point of failure handling.
    """
    try:
        with open_ro(DB_PATH) as conn:
            return pd.read_sql_query(sql, conn, params=params), None
    except FileNotFoundError:
        return None, f"DB missing at {DB_PATH}"
    except sqlite3.OperationalError as e:
        return None, f"DB locked after 3 retries: {e}"


# ---------------------------------------------------------------------------
# Log tail reader with atomic offset cache
# ---------------------------------------------------------------------------

TICK_RE = re.compile(
    r"\[TICK\]\s+SPX=(?P<spx>[\d.]+)\s*\|\s*EM=(?P<em>[\d.]+)\s*\|\s*GEX=(?P<gex>[\d-]+)\s*\|\s*regime=(?P<regime>\S+)\s*\|\s*RSI=(?P<rsi>[\d.]+)\s*\|\s*GEX_regime=(?P<gex_regime>\S+)(?:\s*\|\s*VIX=(?P<vix>[\d.]+))?"
)
ENTRY_RE = re.compile(
    r"\[ENTRY\]\s+(?P<side>PUT|CALL)\s*\|\s*strike=(?P<strike>\S+)\s*\|\s*credit=\$(?P<credit>[\d.]+)\s*\|\s*layer=(?P<layer>\d+)"
)
SKIP_RE = re.compile(r"\[SKIP\]\s+(?P<side>PUT|CALL)\s*\|\s*reason=(?P<reason>[^\s|]+)")
EXIT_CHECK_RE = re.compile(r"\[EXIT CHECK\]\s+pos_id=(?P<pos_id>\d+)")
EXIT_VOTES_RE = re.compile(r"votes=(?P<v_n>\d+)/(?P<v_m>\d+)")
LEVEL_RE = re.compile(r"\[(INFO|WARNING|ERROR)\]")
TAG_RE = re.compile(r"\[(TICK|ENTRY|ENTRY_PENDING|FILLED_CONFIRMED|EXIT CHECK|EOD_EXPIRE|ENTRY_TIMEOUT|SKIP|OPENED|CLOSED_CONFIRMED|DAY_GATE|STALE|STARTUP|ORDER_[A-Z_]+|COMBO_[A-Z_]+|IBKR [A-Z]+|STREAMING|EXIT)\]")


def _atomic_write_text(path: Path, content: str) -> None:
    """Atomic write: tmpfile + os.replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as f:
            f.write(content)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def read_log_tail_incremental() -> list[str]:
    """Read log from cached byte offset to EOF; persist new offset.

    Returns the newly-read lines only (delta). Caller decides whether to
    accumulate across renders. On first run, offset is 0 → returns all lines.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    offset = 0
    if LOG_OFFSET_PATH.exists():
        try:
            offset = int((LOG_OFFSET_PATH.read_text().strip() or "0"))
        except ValueError:
            offset = 0

    if not LOG_PATH.exists():
        return []

    try:
        file_size = LOG_PATH.stat().st_size
        if offset > file_size:
            offset = 0  # rotated/truncated

        with LOG_PATH.open("rb") as f:
            f.seek(offset)
            raw = f.read()

        new_offset = offset + len(raw)
        _atomic_write_text(LOG_OFFSET_PATH, str(new_offset))

        if not raw:
            return []
        return raw.decode("utf-8", errors="replace").splitlines()
    except OSError:
        return []


# Accumulator across script reloads: persists as session_state of the Streamlit run.
# Streamlit re-runs the script on autorefresh, but session_state survives within the
# Streamlit server's session lifetime (~5s of polling keeps the session alive).
if "log_lines" not in st.session_state:
    st.session_state["log_lines"] = []
if "log_loaded_mtime" not in st.session_state:
    st.session_state["log_loaded_mtime"] = 0.0

new_lines = read_log_tail_incremental()
if new_lines:
    st.session_state["log_lines"].extend(new_lines)
    # Cap memory
    if len(st.session_state["log_lines"]) > 20000:
        st.session_state["log_lines"] = st.session_state["log_lines"][-20000:]
LOG_LINES: list[str] = st.session_state["log_lines"]


def parse_log_rows(lines: list[str]) -> list[dict]:
    """Parse log lines into typed rows: ts, level, tag, msg."""
    rows: list[dict] = []
    for line in lines:
        if not line:
            continue
        m_lvl = LEVEL_RE.search(line)
        m_tag = TAG_RE.search(line)
        ts_match = re.search(r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) ET", line)
        rows.append({
            "ts": ts_match.group("ts") if ts_match else "",
            "level": m_lvl.group(1) if m_lvl else "?",
            "tag": m_tag.group(1) if m_tag else "",
            "msg": line,
        })
    return rows


def parse_tick(rows: list[dict]) -> dict | None:
    """Get most recent [TICK] row → parsed dict."""
    for r in reversed(rows):
        if r["tag"] == "TICK":
            m = TICK_RE.search(r["msg"])
            if m:
                return {"ts": r["ts"], **m.groupdict()}
    return None


def parse_entries(rows: list[dict], n: int = 5) -> list[dict]:
    """Get most recent N [ENTRY] rows."""
    out = []
    for r in reversed(rows):
        if r["tag"] == "ENTRY":
            m = ENTRY_RE.search(r["msg"])
            if m:
                out.append({"ts": r["ts"], **m.groupdict()})
                if len(out) >= n:
                    break
    return out


def parse_skips(rows: list[dict], n: int = 5) -> tuple[list[dict], dict]:
    """Return (recent_n_skips, reason_counts_full_day)."""
    recent: list[dict] = []
    counts: dict[str, int] = {}
    for r in rows:
        if r["tag"] == "SKIP":
            m = SKIP_RE.search(r["msg"])
            if m:
                reason = m.group("reason")
                counts[reason] = counts.get(reason, 0) + 1
                if len(recent) < n:
                    recent.append({"ts": r["ts"], **m.groupdict()})
    recent.reverse()
    counts_full = dict(sorted(counts.items(), key=lambda kv: -kv[1]))
    return recent, counts_full


def parse_exit_votes(rows: list[dict], n: int = EXIT_CHECK_SAMPLE_N) -> dict:
    """Count last N [EXIT CHECK] by vote bucket (votes=N/M)."""
    tally = {"votes=0 [STAY]": 0, "votes=1 [STAY]": 0, "votes=2+ [EXIT]": 0}
    sample = []
    for r in reversed(rows):
        if r["tag"] == "EXIT CHECK":
            sample.append(r["msg"])
            m = EXIT_VOTES_RE.search(r["msg"])
            if m:
                v_n = int(m.group("v_n"))
                v_m = int(m.group("v_m"))
                if v_m == 0:
                    continue
                if v_n >= v_m:
                    tally["votes=2+ [EXIT]"] += 1
                elif v_n == 1:
                    tally["votes=1 [STAY]"] += 1
                else:
                    tally["votes=0 [STAY]"] += 1
            if len(sample) >= n:
                break
    return tally


# ---------------------------------------------------------------------------
# Process + market status
# ---------------------------------------------------------------------------

def engine_pid() -> int | None:
    """Find ibkr engine PID via cwd-based detection.

    The engine process's argv shows just "run.py" (the shell wrapper sets
    cwd but not argv[0]), so the previous pattern "ibkr_trader_engine.*run\\.py"
    never matched and Panel 1 always reported RED STOPPED while the engine
    was alive (TASK-2026-329).

    Approach: pgrep all "run.py" candidates (returns 5+ sibling processes
    today: spx_dashboard, 5556_dashboard, wc2026_dashboard, premium_extractor,
    gex_extractor, plus the engine), then filter by cwd using lsof.

    Note: `lsof -p PID -d cwd` without -a OR-combines the filters on macOS
    and lists every process's cwd. Adding -a forces AND so we get ONLY the
    cwd of the given PID (verified 2026-07-08 10:20 ET with all six siblings).
    """
    try:
        out = subprocess.run(
            ["pgrep", "-f", r"run\.py"],
            capture_output=True, text=True, timeout=2.0,
        )
        if out.returncode != 0 or not out.stdout.strip():
            return None
        for p_str in out.stdout.split():
            pid = int(p_str.strip())
            lsof_out = subprocess.run(
                ["lsof", "-a", "-p", str(pid), "-d", "cwd", "-F", "n"],
                capture_output=True, text=True, timeout=2.0,
            )
            # -F n output: one or more `n<path>` lines; pick the cwd line
            for line in lsof_out.stdout.splitlines():
                if line.startswith("n") and IBKR_ENGINE_CWD_MARKER in line:
                    return pid
        return None
    except (subprocess.TimeoutExpired, subprocess.SubprocessError, ValueError):
        return None


def engine_uptime(pid: int) -> timedelta | None:
    """Process uptime via ps -o etime=."""
    try:
        out = subprocess.run(
            ["ps", "-p", str(pid), "-o", "etime="],
            capture_output=True, text=True, timeout=2.0,
        )
        s = out.stdout.strip()
        if not s:
            return None
        # etime format: [[DD-]HH:]MM:SS
        days = 0
        if "-" in s:
            d, s = s.split("-", 1)
            days = int(d)
        h, m, sec = s.split(":")
        return timedelta(days=days, hours=int(h), minutes=int(m), seconds=int(sec))
    except (subprocess.SubprocessError, ValueError):
        return None


def market_open_today() -> bool | None:
    """True if NYSE open today, False if closed, None on script error."""
    try:
        out = subprocess.run(
            ["python3", IS_MARKET_OPEN_SCRIPT],
            capture_output=True, text=True, timeout=5.0,
        )
        if out.returncode == 0 and "OPEN" in out.stdout:
            return True
        if out.returncode == 1 and "CLOSED" in out.stdout:
            return False
        return None
    except (subprocess.TimeoutExpired, subprocess.SubprocessError, FileNotFoundError):
        return None


# ---------------------------------------------------------------------------
# Panel 1 — Engine Health
# ---------------------------------------------------------------------------

def panel1_health() -> dict:
    pid = engine_pid()
    db_mtime_age_s: float | None = None
    if DB_PATH.exists():
        db_mtime_age_s = (time.time() - DB_PATH.stat().st_mtime)

    log_size_b = LOG_PATH.stat().st_size if LOG_PATH.exists() else 0

    last_log_line = ""
    log_mtime_age_s: float | None = None
    if LOG_PATH.exists():
        try:
            log_size = LOG_PATH.stat().st_size
            with LOG_PATH.open("rb") as f:
                f.seek(max(0, log_size - 4096))
                tail_b = f.read()
            tail = tail_b.decode("utf-8", errors="replace").splitlines()
            # last non-empty
            for ln in reversed(tail):
                if ln.strip():
                    last_log_line = ln.strip()
                    break
            log_mtime_age_s = time.time() - LOG_PATH.stat().st_mtime
        except OSError:
            pass

    warn_count = sum(1 for ln in LOG_LINES if "[WARNING]" in ln)
    err_count = sum(1 for ln in LOG_LINES if "[ERROR]" in ln)

    uptime = engine_uptime(pid) if pid else None

    status = "stopped" if pid is None else "running"

    return {
        "pid": pid,
        "status": status,
        "uptime_s": int(uptime.total_seconds()) if uptime else None,
        "last_log": last_log_line[:200],
        "last_log_age_s": int(log_mtime_age_s) if log_mtime_age_s is not None else None,
        "log_size_b": log_size_b,
        "warn_count": warn_count,
        "err_count": err_count,
        "db_age_s": int(db_mtime_age_s) if db_mtime_age_s is not None else None,
    }


# ---------------------------------------------------------------------------
# Panel 2 — P&L
# ---------------------------------------------------------------------------

def panel2_pnl() -> dict:
    df, err = safe_query(
        "SELECT SUM(pnl) AS realized, COUNT(*) AS n "
        "FROM positions "
        "WHERE status IN ('closed', 'expired') AND DATE(close_time) = DATE('now', '-4 hours')",
    )
    # NB: positions.close_time is "YYYY-MM-DDTHH:MM:SS-04:00" (ISO with offset) so DATE() works on local SQLite.
    realized = float(df["realized"].iloc[0]) if df is not None and df["realized"].iloc[0] is not None else 0.0
    n_closed = int(df["n"].iloc[0]) if df is not None and df["n"].iloc[0] is not None else 0

    df_open, _ = safe_query(
        "SELECT COUNT(*) AS n FROM positions WHERE status = 'open' AND DATE(open_time) <= DATE('now', '-4 hours')"
    )
    n_open = int(df_open["n"].iloc[0]) if df_open is not None and df_open["n"].iloc[0] is not None else 0

    df_sig, _ = safe_query("SELECT COUNT(*) AS n FROM signals WHERE DATE(timestamp) = DATE('now', '-4 hours')")
    n_signals = int(df_sig["n"].iloc[0]) if df_sig is not None and df_sig["n"].iloc[0] is not None else 0

    # Unrealized: parse latest uPnL per open position from log
    unrealized = 0.0
    if df_open is not None and n_open > 0:
        df_open_pos, _ = safe_query(
            "SELECT id, ticker, short_strike, long_strike FROM positions "
            "WHERE status = 'open' AND DATE(open_time) <= DATE('now', '-4 hours')"
        )
        if df_open_pos is not None:
            pos_ids = set(int(x) for x in df_open_pos["id"].tolist())
            latest_per_pos: dict[int, float] = {}
            upnl_re = re.compile(r"\[EXIT CHECK\]\s+pos_id=(?P<pid>\d+).*?uPnL=\$?(?P<sign>[+\-]?)(?P<val>[\d.]+)")
            for ln in reversed(LOG_LINES):
                m = upnl_re.search(ln)
                if m:
                    pid = int(m.group("pid"))
                    if pid in pos_ids and pid not in latest_per_pos:
                        v = float(m.group("val"))
                        latest_per_pos[pid] = v * (1 if m.group("sign") != "-" else -1)
                    if len(latest_per_pos) == len(pos_ids):
                        break
            unrealized = sum(latest_per_pos.values())

    return {
        "realized": realized,
        "unrealized": unrealized,
        "counts": {
            "open": n_open,
            "closed_today": n_closed,
            "signals_today": n_signals,
            "skips_today": sum(1 for ln in LOG_LINES if "[SKIP]" in ln),
        },
        "error": err,
    }


# ---------------------------------------------------------------------------
# Panel 3 — Signal Intent
# ---------------------------------------------------------------------------

def panel3_signal_intent() -> dict:
    rows = parse_log_rows(LOG_LINES)
    tick = parse_tick(rows)
    entries = parse_entries(rows, n=5)
    _, skip_counts = parse_skips(rows, n=5)

    return {
        "tick": tick,
        "entries": entries,
        "skip_counts": skip_counts,
    }


# ---------------------------------------------------------------------------
# Panel 4 — Open Positions
# ---------------------------------------------------------------------------

def panel4_open_positions() -> tuple[pd.DataFrame, str | None]:
    df, err = safe_query(
        "SELECT id, ticker, side, short_strike, long_strike, credit, num_contracts "
        "FROM positions "
        "WHERE status = 'open' AND DATE(open_time) <= DATE('now', '-4 hours') "
        "ORDER BY id DESC"
    )
    if df is None or df.empty:
        return pd.DataFrame(), err
    # Compute uPnL from latest EXIT CHECK per pos
    pos_ids = [int(x) for x in df["id"].tolist()]
    latest_upnl: dict[int, float] = {pid: 0.0 for pid in pos_ids}
    exit_votes: dict[int, str] = {pid: "—" for pid in pos_ids}

    upnl_re = re.compile(r"\[EXIT CHECK\]\s+pos_id=(?P<pid>\d+).*?uPnL=\$?(?P<sign>[+\-]?)(?P<val>[\d.]+)")
    votes_re = re.compile(r"\[EXIT CHECK\]\s+pos_id=(?P<pid>\d+).*?votes=(?P<v_n>\d+)/(?P<v_m>\d+)")

    for ln in reversed(LOG_LINES):
        # Get uPnL
        m = upnl_re.search(ln)
        if m:
            pid = int(m.group("pid"))
            if pid in pos_ids and pid in latest_upnl:
                v = float(m.group("val"))
                latest_upnl[pid] = v * (1 if m.group("sign") != "-" else -1)

        # Get exit votes (latest only)
        m_votes = votes_re.search(ln)
        if m_votes:
            pid = int(m_votes.group("pid"))
            if pid in pos_ids and exit_votes[pid] == "—":
                v_n = m_votes.group("v_n")
                v_m = m_votes.group("v_m")
                exit_votes[pid] = f"{v_n}/{v_m}"

    df["upnl"] = df["id"].map(lambda p: latest_upnl.get(int(p), 0.0))
    df["exit_votes"] = df["id"].map(lambda p: exit_votes.get(int(p), "—"))
    return df, err


# ---------------------------------------------------------------------------
# Panel 5 — Closed Today
# ---------------------------------------------------------------------------

def panel5_closed_today() -> tuple[pd.DataFrame, str | None]:
    df, err = safe_query(
        "SELECT id, ticker, side, short_strike, long_strike, open_time, close_time, "
        "       credit, pnl, status, exit_regime, fill_time, fill_price "
        "FROM positions "
        "WHERE status IN ('closed', 'expired') AND DATE(close_time) = DATE('now', '-4 hours') "
        "ORDER BY close_time DESC"
    )
    if df is None or df.empty:
        return pd.DataFrame(), err
    return df, err


# ---------------------------------------------------------------------------
# Panel 6 — Rejection Funnel
# ---------------------------------------------------------------------------

def panel6_rejections() -> dict:
    # DB signals (post-decision fills=0)
    df, _ = safe_query(
        "SELECT blocked_reason AS reason, COUNT(*) AS n FROM signals "
        "WHERE DATE(timestamp) = DATE('now', '-4 hours') AND filled = 0 AND blocked_reason IS NOT NULL "
        "GROUP BY blocked_reason"
    )
    db_counts: dict[str, int] = {}
    if df is not None and not df.empty:
        for _, r in df.iterrows():
            db_counts[r["reason"]] = int(r["n"])

    # Log SKIPs (broader — pre-signal-gate rejections)
    _, log_counts = parse_skips(parse_log_rows(LOG_LINES), n=0)
    return {"db_counts": db_counts, "log_counts": log_counts}


# ---------------------------------------------------------------------------
# Panel 7 — Exit Vote Tally
# ---------------------------------------------------------------------------

def panel7_exit_votes() -> dict:
    return parse_exit_votes(parse_log_rows(LOG_LINES))


# ---------------------------------------------------------------------------
# Panel 8 — Decision Stream
# ---------------------------------------------------------------------------

def panel8_decision_stream(n: int = DECISION_STREAM_LINES) -> list[dict]:
    rows = parse_log_rows(LOG_LINES)
    return rows[-n:]


# ---------------------------------------------------------------------------
# Historical Analysis Functions
# ---------------------------------------------------------------------------

def get_historical_pnl_by_day() -> pd.DataFrame:
    """Per-day rollup of every closed position: P&L, trade count, wins."""
    df, _ = safe_query(
        "SELECT DATE(close_time) AS period, COUNT(*) AS trades, "
        "       SUM(pnl) AS pnl, "
        "       SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins "
        "FROM positions "
        "WHERE status IN ('closed', 'expired') AND close_time IS NOT NULL "
        "GROUP BY DATE(close_time) "
        "ORDER BY period"
    )
    if df is None or df.empty:
        return pd.DataFrame()
    return df


def get_historical_pnl_by_month() -> pd.DataFrame:
    """Per-month rollup of every closed position: P&L, trade count, wins."""
    df, _ = safe_query(
        "SELECT strftime('%Y-%m', close_time) AS period, COUNT(*) AS trades, "
        "       SUM(pnl) AS pnl, "
        "       SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins "
        "FROM positions "
        "WHERE status IN ('closed', 'expired') AND close_time IS NOT NULL "
        "GROUP BY strftime('%Y-%m', close_time) "
        "ORDER BY period"
    )
    if df is None or df.empty:
        return pd.DataFrame()
    return df


def static_bar(df: pd.DataFrame, x_col: str, y_col: str, x_title: str, y_title: str,
               color: str = "#00d97e", diverging: bool = False) -> alt.Chart:
    """Build a non-interactive Altair bar chart (no pan, no zoom, no selection).

    st.bar_chart ships with pan/zoom bound in; constructing the chart directly
    and never calling .interactive() leaves it static.
    """
    y_enc = alt.Y(f"{y_col}:Q", title=y_title,
                  axis=alt.Axis(grid=True, gridColor="#21262d", gridOpacity=0.6))
    if diverging:
        mark_color = alt.condition(
            alt.datum[y_col] >= 0,
            alt.value("#00d97e"),
            alt.value("#ff4b4b"),
        )
    else:
        mark_color = alt.value(color)

    x_enc = alt.X(f"{x_col}:N", title=x_title, axis=alt.Axis(labelAngle=-45, labelLimit=90))

    bars = (
        alt.Chart(df)
        .mark_bar(size=14, cornerRadiusTopLeft=2, cornerRadiusTopRight=2)
        .encode(
            x=x_enc,
            y=y_enc,
            color=mark_color,
            tooltip=[alt.Tooltip(f"{x_col}:N", title=x_title),
                     alt.Tooltip(f"{y_col}:Q", title=y_title, format=",.2f")],
        )
    )

    # Explicit baseline. The default y-grid line at 0 is the same weight as
    # every other gridline, so on a diverging chart you cannot see where
    # profit turns into loss.
    zero_line = (
        alt.Chart(pd.DataFrame({"y": [0]}))
        .mark_rule(color="#8b949e", strokeWidth=2, opacity=0.9)
        .encode(y=alt.Y("y:Q"))
    )

    return (
        alt.layer(zero_line, bars)
        .properties(height=280)
        .configure_axis(labelColor="#8b949e", titleColor="#8b949e", domainColor="#21262d",
                        tickColor="#21262d", labelFontSize=11, titleFontSize=11)
        .configure_view(strokeWidth=0, fill="transparent")
        .configure(background="transparent")
    )


# ---------------------------------------------------------------------------
# Fetch all (cached for 5s via Streamlit's autorefresh + our own state)
# ---------------------------------------------------------------------------

HEALTH = panel1_health()
PNL = panel2_pnl()
SIGNAL_INTENT = panel3_signal_intent()
OPEN_POS_DF, OPEN_POS_ERR = panel4_open_positions()
CLOSED_DF, CLOSED_ERR = panel5_closed_today()
REJECTIONS = panel6_rejections()
VOTE_TALLY = panel7_exit_votes()
DECISION_STREAM = panel8_decision_stream()

MARKET_OPEN = market_open_today()

# Historical data
DAILY_PNL_DF = get_historical_pnl_by_day()
MONTHLY_PNL_DF = get_historical_pnl_by_month()


# ---------------------------------------------------------------------------
# Status pill logic
# ---------------------------------------------------------------------------

def status_pill_state() -> tuple[str, str]:
    """Return (status_text, pill_class)."""
    if HEALTH["pid"] is None:
        return "STOPPED", "pill-red"
    # Process alive: running or paused based on market
    if MARKET_OPEN is True:
        return "RUNNING", "pill-green"
    if MARKET_OPEN is False:
        return "PAUSED", "pill-amber"
    return "RUNNING", "pill-green"  # market check failed; process is alive


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_money(value: float, signed: bool = True) -> str:
    if signed:
        sign = "+" if value >= 0 else "-"
        return f"{sign}${abs(value):,.2f}"
    return f"${value:,.2f}"


def _fmt_uptime(secs: int | None) -> str:
    if secs is None:
        return "—"
    h, rem = divmod(secs, 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def _fmt_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n / (1024 * 1024):.1f} MB"


def section_header(icon: str, label: str, note: str = "") -> None:
    """Render a compact section label.

    Deliberately NOT wrapped in `<div class="panel">`: Streamlit closes every
    st.markdown block's tags, so an opening wrapper div renders as an empty
    box with the widgets stranded outside it.
    """
    note_html = (
        f'<span style="margin-left:auto;font-size:10px;color:var(--text-muted);'
        f'letter-spacing:0;text-transform:none">{note}</span>' if note else ""
    )
    st.markdown(
        f'<div style="display:flex;align-items:center;gap:0.4rem;margin:0.25rem 0 0.4rem;'
        f'font-size:10px;font-weight:600;color:var(--text-secondary);'
        f'text-transform:uppercase;letter-spacing:0.12em">'
        f'<span style="font-size:12px">{icon}</span>{label}{note_html}</div>',
        unsafe_allow_html=True,
    )


def _parse_hhmm(iso_ts: str) -> str:
    """Extract HH:MM from '2026-07-07T09:34:37-04:00' → '09:34'."""
    if not iso_ts:
        return ""
    try:
        return datetime.fromisoformat(iso_ts).strftime("%H:%M")
    except (ValueError, TypeError):
        return ""


def _parse_hold(open_iso: str, close_iso: str) -> str:
    """Compute hold duration HH:MM → '05h45m'."""
    if not open_iso or not close_iso:
        return ""
    try:
        o = datetime.fromisoformat(open_iso)
        c = datetime.fromisoformat(close_iso)
        secs = int((c - o).total_seconds())
        if secs < 0:
            return ""
        h, rem = divmod(secs, 3600)
        m = rem // 60
        return f"{h}h{m:02d}m"
    except (ValueError, TypeError):
        return ""


# ---------------------------------------------------------------------------
# Render — Header
# ---------------------------------------------------------------------------

st.markdown(
    f"""
    <div class="dashboard-header">
      <h1>📈 IBKR Trading Dashboard</h1>
      <div class="header-meta">
        <div><span style="color:var(--text-secondary)">Date:</span> {TODAY.isoformat()}</div>
        <div><span style="color:var(--text-secondary)">Time:</span> {NOW.strftime('%H:%M:%S')} ET</div>
        <div><span style="color:var(--text-secondary)">Refresh:</span> every {REFRESH_MS // 1000}s</div>
        <div><span style="color:var(--text-secondary)">Port:</span> 5558</div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# Page selector. A segmented_control (not st.tabs) because its value lives in
# session_state and therefore survives the autorefresh rerun — st.tabs would
# snap back to the first tab every 5 seconds.
st.segmented_control(
    "Page", [PAGE_LIVE, PAGE_HISTORY],
    key="nav_page", default=PAGE_LIVE, label_visibility="collapsed",
)
ACTIVE_PAGE = st.session_state.get("nav_page") or PAGE_LIVE


# ---------------------------------------------------------------------------
# Render — Live Dashboard page
#
# Everything below must stay indented inside its page branch — any st.* call
# left at module level renders on BOTH pages.
# ---------------------------------------------------------------------------

if ACTIVE_PAGE == PAGE_LIVE:
    section_header("💰", "P&amp;L Summary")
    realized_class = "pos" if PNL["realized"] > 0 else "neg" if PNL["realized"] < 0 else "zero"
    unreal_class = "pos" if PNL["unrealized"] > 0 else "neg" if PNL["unrealized"] < 0 else "zero"

    total_pnl = PNL["realized"] + PNL["unrealized"]
    total_class = "pos" if total_pnl > 0 else "neg" if total_pnl < 0 else "zero"

    # Single markdown block: all tags open and close together, so the panel
    # actually wraps its contents instead of rendering as an empty box.
    counts_to_show = {k: v for k, v in PNL["counts"].items() if k != "signals_today"}
    counts_html = "".join(
        f'<div class="count-cell"><div class="c-label">{k.replace("_", " ")}</div>'
        f'<div class="c-value">{v}</div></div>'
        for k, v in counts_to_show.items()
    )
    st.markdown(
        f'<div class="panel">'
        f'<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:1.5rem;align-items:flex-start">'
        f'  <div>'
        f'    <div style="font-size:11px;color:var(--text-secondary);text-transform:uppercase;margin-bottom:0.25rem;font-weight:600">Realized Today</div>'
        f'    <div class="big-number {realized_class}">{_fmt_money(PNL["realized"])}</div>'
        f'  </div>'
        f'  <div>'
        f'    <div style="font-size:11px;color:var(--text-secondary);text-transform:uppercase;margin-bottom:0.25rem;font-weight:600">Unrealized</div>'
        f'    <div class="big-number {unreal_class}">{_fmt_money(PNL["unrealized"])}</div>'
        f'  </div>'
        f'  <div style="background:var(--bg-primary);padding:0.75rem 1rem;border-radius:8px;border:1px solid var(--border)">'
        f'    <div style="font-size:11px;color:var(--text-secondary);text-transform:uppercase;margin-bottom:0.25rem;font-weight:600">Total P&amp;L</div>'
        f'    <div class="big-number {total_class}" style="font-size:34px">{_fmt_money(total_pnl)}</div>'
        f'  </div>'
        f'</div>'
        f'<div style="margin-top:1rem;padding-top:1rem;border-top:1px solid var(--border)">'
        f'<div style="font-size:10px;color:var(--text-secondary);text-transform:uppercase;margin-bottom:0.6rem;font-weight:600;letter-spacing:0.12em">Today\'s Activity</div>'
        f'<div class="counts-grid" style="margin-top:0">{counts_html}</div>'
        f'</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    section_header("📍", "Open Positions")
    if OPEN_POS_DF.empty:
        st.markdown(
            '<div class="empty-state"><div class="big">∅</div>'
            'No open positions right now.</div>',
            unsafe_allow_html=True,
        )
    else:
        view_df = OPEN_POS_DF[["id", "side", "short_strike", "long_strike", "credit", "num_contracts", "upnl", "exit_votes"]].copy()
        view_df = view_df.dropna(how='all')
        view_df["Strike"] = view_df.apply(
            lambda r: f"{int(r['short_strike'])}/{int(r['long_strike']) if pd.notna(r['long_strike']) else '?'}", axis=1
        )
        view_df = view_df.rename(columns={
            "id": "Pos #",
            "side": "Side",
            "credit": "Credit Received",
            "num_contracts": "Contracts",
            "upnl": "Unrealized P&L",
            "exit_votes": "Exit Votes",
        })
        view_df = view_df[["Pos #", "Side", "Strike", "Credit Received", "Contracts", "Exit Votes", "Unrealized P&L"]]
        st.dataframe(
            view_df.style
            .format({"Credit Received": "${:.2f}", "Unrealized P&L": "${:+.2f}"})
            .map(lambda v: "color: #00d97e" if isinstance(v, (int, float)) and v > 0 else "color: #ff4b4b" if isinstance(v, (int, float)) and v < 0 else "", subset=["Unrealized P&L"]),
            use_container_width=True, hide_index=True,
        )

    section_header("✅", "Closed Today")
    if CLOSED_DF.empty:
        st.markdown(
            '<div class="empty-state"><div class="big">∅</div>'
            'No closed positions today yet.</div>',
            unsafe_allow_html=True,
        )
    else:
        view_df = CLOSED_DF.copy()
        view_df = view_df.dropna(how='all')
        view_df["Strike"] = view_df.apply(
            lambda r: f"{int(r['short_strike'])}/{int(r['long_strike']) if pd.notna(r['long_strike']) else '?'}", axis=1
        )
        view_df = view_df.rename(columns={
            "id": "Pos #",
            "side": "Side",
            "credit": "Credit Received",
            "pnl": "P&L"
        })
        view_df = view_df[["Pos #", "Side", "Strike", "Credit Received", "P&L"]]
        st.dataframe(
            view_df.style
            .format({"Credit Received": "${:.2f}", "P&L": "${:+.2f}"})
            .map(lambda v: "color: #00d97e" if isinstance(v, (int, float)) and v > 0 else "color: #ff4b4b" if isinstance(v, (int, float)) and v < 0 else "", subset=["P&L"]),
            use_container_width=True, hide_index=True,
        )

        total_pnl = float(CLOSED_DF["pnl"].sum())
        wins = int((CLOSED_DF["pnl"] > 0).sum())
        n = len(CLOSED_DF)
        win_rate = (100 * wins / n) if n else 0

        pnl_class = "green" if total_pnl > 0 else ""
        st.markdown(
            f'<div class="table-footer">'
            f'<div>count <span class="fv">{n}</span></div>'
            f'<div>total P/L <span class="fv {pnl_class}">${total_pnl:+.2f}</span></div>'
            f'<div>win rate <span class="fv">{win_rate:.0f}%</span></div>'
            f'</div>',
            unsafe_allow_html=True,
        )

# ---------------------------------------------------------------------------
# Render — Trade History page
# ---------------------------------------------------------------------------

else:
    view_type = st.radio(
        "Group by", ["Daily", "Monthly"],
        key="history_view", horizontal=True, label_visibility="collapsed",
    )

    # Both rollups share the same schema (period / trades / pnl / wins), so
    # every metric below works unchanged for either grouping.
    hist_df = DAILY_PNL_DF if view_type == "Daily" else MONTHLY_PNL_DF
    period_label = "Date" if view_type == "Daily" else "Month"

    if hist_df.empty:
        st.markdown(
            '<div class="empty-state"><div class="big">∅</div>'
            'No closed trades on record yet.</div>',
            unsafe_allow_html=True,
        )
    else:
        total_trades = int(hist_df["trades"].sum())
        total_pnl = float(hist_df["pnl"].sum())
        total_wins = int(hist_df["wins"].sum())
        win_rate = (100 * total_wins / total_trades) if total_trades else 0.0
        best = hist_df.loc[hist_df["pnl"].idxmax()]

        m1, m2, m3, m4 = st.columns(4)
        m1.metric(f"Total Trades ({len(hist_df)} {'days' if view_type == 'Daily' else 'months'})", total_trades)
        m2.metric("Total P&L", f"${total_pnl:+,.2f}")
        m3.metric("Win Rate", f"{win_rate:.0f}%", f"{total_wins} wins")
        m4.metric(f"Best {period_label}", f"${float(best['pnl']):+,.2f}", str(best["period"]))

        section_header("📊", f"P&amp;L by {period_label}", "green = profit · red = loss")
        st.altair_chart(
            static_bar(hist_df, "period", "pnl", period_label, "P&L ($)", diverging=True),
            use_container_width=True,
        )

        section_header("🔢", f"Trades per {period_label}")
        st.altair_chart(
            static_bar(hist_df, "period", "trades", period_label, "Trades", color="#58a6ff"),
            use_container_width=True,
        )

        section_header("📋", f"{period_label} Breakdown")
        summary_df = hist_df.copy()
        summary_df["win_rate"] = (summary_df["wins"] / summary_df["trades"] * 100).round(0).astype(int)
        summary_df = summary_df.rename(columns={
            "period": period_label,
            "trades": "Trades",
            "wins": "Wins",
            "pnl": "P&L",
            "win_rate": "Win %",
        })
        summary_df = summary_df[[period_label, "Trades", "Wins", "Win %", "P&L"]]
        st.dataframe(
            summary_df.sort_values(period_label, ascending=False).style
            .format({"P&L": "${:+.2f}", "Win %": "{:.0f}%"})
            .map(lambda v: "color: #00d97e" if isinstance(v, (int, float)) and v > 0
                 else "color: #ff4b4b" if isinstance(v, (int, float)) and v < 0 else "",
                 subset=["P&L"]),
            width='stretch', hide_index=True, height=320,
        )

