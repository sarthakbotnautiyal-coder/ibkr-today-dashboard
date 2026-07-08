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
ENGINE_PGREP_PATTERN = r"ibkr_trader_engine.*run\.py"
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
    initial_sidebar_state="collapsed",
)

CUSTOM_CSS = """
<style>
  .stApp { background-color: #0e1117; }
  section.main > div { padding-top: 1rem; }

  .panel { background: #161b22; border: 1px solid #21262d; border-radius: 8px; padding: 10px 12px; margin-bottom: 8px; }
  .panel h3 { margin: 0 0 10px 0; font-size: 12px; font-weight: 600; color: #8b949e; text-transform: uppercase; letter-spacing: 0.05em; }

  .metric-row { display: flex; gap: 24px; flex-wrap: wrap; align-items: center; }
  .metric { display: flex; flex-direction: column; gap: 2px; }
  .metric .label { font-size: 11px; color: #8b949e; text-transform: uppercase; }
  .metric .value { font-size: 18px; color: #e6edf3; font-weight: 600; }
  .metric .value.mono { font-family: 'SF Mono', Menlo, Consolas, monospace; font-size: 13px; }

  .pill { display: inline-block; padding: 3px 10px; border-radius: 12px; font-size: 12px; font-weight: 600; text-transform: uppercase; }
  .pill-green { background: #0d4429; color: #00d97e; border: 1px solid #00d97e; }
  .pill-red { background: #4d1417; color: #ff4b4b; border: 1px solid #ff4b4b; }
  .pill-amber { background: #4d3a14; color: #f0a020; border: 1px solid #f0a020; }
  .pill-blue { background: #14294d; color: #58a6ff; border: 1px solid #58a6ff; }
  .pill-grey { background: #21262d; color: #8b949e; border: 1px solid #6e7681; }

  .big-number { font-size: 40px; font-weight: 700; line-height: 1; margin: 4px 0; font-feature-settings: 'tnum'; }
  .big-number.pos { color: #00d97e; }
  .big-number.neg { color: #ff4b4b; }
  .big-number.zero { color: #8b949e; }

  .counts-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; margin-top: 6px; }
  .count-cell { background: #0d1117; padding: 4px 8px; border-radius: 6px; text-align: center; }
  .count-cell .c-label { font-size: 9px; color: #8b949e; text-transform: uppercase; }
  .count-cell .c-value { font-size: 18px; font-weight: 600; color: #e6edf3; margin-top: 1px; }

  .log-line { font-family: 'SF Mono', Menlo, Consolas, monospace; font-size: 12px; padding: 2px 6px; color: #c9d1d9; }
  .log-line.INFO { color: #c9d1d9; }
  .log-line.WARNING { color: #f0a020; }
  .log-line.ERROR { color: #ff4b4b; }
  .log-line .tag { color: #58a6ff; font-weight: 600; }
  .log-line .ts { color: #6e7681; }

  .stDataFrame { font-size: 12px; }

  .donut-wrap { display: flex; align-items: center; gap: 16px; }
  .donut { width: 110px; height: 110px; border-radius: 50%; position: relative; flex-shrink: 0; }
  .donut::after { content: ''; position: absolute; inset: 18px; border-radius: 50%; background: #161b22; }
  .donut-legend { display: flex; flex-direction: column; gap: 4px; font-size: 12px; }
  .donut-legend .item { display: flex; align-items: center; gap: 6px; }
  .donut-legend .sw { width: 10px; height: 10px; border-radius: 2px; }

  .table-footer { display: flex; gap: 24px; margin-top: 8px; padding-top: 8px; border-top: 1px solid #21262d; font-size: 13px; color: #8b949e; }
  .table-footer .fv { color: #e6edf3; font-weight: 600; }
  .table-footer .fv.green { color: #00d97e; }

  .empty-state { text-align: center; padding: 24px 16px; color: #6e7681; border: 1px dashed #21262d; border-radius: 6px; background: #0d1117; }
  .empty-state .big { font-size: 28px; margin-bottom: 6px; color: #8b949e; }

  .entry-row { display: grid; grid-template-columns: 60px 140px 1fr 80px 70px; gap: 10px; padding: 3px 8px; font-size: 12px; font-family: 'SF Mono', Menlo, Consolas, monospace; border-bottom: 1px solid #21262d; }
  .entry-row.header { color: #8b949e; font-family: inherit; text-transform: uppercase; font-size: 10px; }
  .entry-row .pnl.pos { color: #00d97e; }
  .entry-row .pnl.neg { color: #ff4b4b; }

  .top-bar { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 6px; }
  .top-bar h1 { margin: 0; font-size: 18px; color: #e6edf3; }
  .top-bar .clock { font-family: 'SF Mono', Menlo, Consolas, monospace; color: #8b949e; font-size: 13px; }
</style>
"""

st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Auto-refresh (5s)
# ---------------------------------------------------------------------------

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
    r"\[TICK\]\s+SPX=(?P<spx>[\d.]+)\s*\|\s*EM=(?P<em>[\d.]+)\s*\|\s*GEX=(?P<gex>[\d-]+)\s*\|\s*regime=(?P<regime>\S+)\s*\|\s*RSI=(?P<rsi>[\d.]+)\s*\|\s*GEX_regime=(?P<gex_regime>\S+)"
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
    """Find ibkr engine PID via pgrep, or None."""
    try:
        out = subprocess.run(
            ["pgrep", "-f", ENGINE_PGREP_PATTERN],
            capture_output=True, text=True, timeout=2.0,
        )
        pids = [int(p) for p in out.stdout.split() if p.strip().isdigit()]
        return pids[0] if pids else None
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
        "SELECT id, ticker, side, short_strike, long_strike, open_time, status, layer, "
        "       entry_regime, entry_spx_spot, credit, fill_price, fill_time "
        "FROM positions "
        "WHERE status = 'open' AND DATE(open_time) <= DATE('now', '-4 hours') "
        "ORDER BY open_time"
    )
    if df is None or df.empty:
        return pd.DataFrame(), err
    # Compute uPnL from latest EXIT CHECK per pos
    pos_ids = [int(x) for x in df["id"].tolist()]
    latest_upnl: dict[int, float] = {pid: 0.0 for pid in pos_ids}
    upnl_re = re.compile(r"\[EXIT CHECK\]\s+pos_id=(?P<pid>\d+).*?uPnL=\$?(?P<sign>[+\-]?)(?P<val>[\d.]+)")
    for ln in reversed(LOG_LINES):
        m = upnl_re.search(ln)
        if m:
            pid = int(m.group("pid"))
            if pid in pos_ids and pid in latest_upnl:
                v = float(m.group("val"))
                latest_upnl[pid] = v * (1 if m.group("sign") != "-" else -1)
    df["upnl"] = df["id"].map(lambda p: latest_upnl.get(int(p), 0.0))
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
# Render — Top Bar
# ---------------------------------------------------------------------------

st.markdown(
    f"""
    <div class="top-bar">
      <h1>📈 ibkr_today — {TODAY.isoformat()}</h1>
      <div class="clock">render {NOW.strftime('%H:%M:%S ET')} · auto-refresh {REFRESH_MS // 1000}s · port 5558</div>
    </div>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Render — Panel 1 Health (full width above main grid)
# ---------------------------------------------------------------------------

status_text, pill_class = status_pill_state()
health_cols = st.columns([1, 1, 1, 1.6, 1, 1, 1])

with health_cols[0]:
    pid_text = str(HEALTH["pid"]) if HEALTH["pid"] else "—"
    st.markdown(
        f'<div class="metric"><span class="label">PID</span>'
        f'<span class="value mono">{pid_text}</span></div>',
        unsafe_allow_html=True,
    )
with health_cols[1]:
    st.markdown(
        f'<div class="metric"><span class="label">Uptime</span>'
        f'<span class="value mono">{_fmt_uptime(HEALTH["uptime_s"])}</span></div>',
        unsafe_allow_html=True,
    )
with health_cols[2]:
    st.markdown(
        f'<div class="metric"><span class="label">Status</span>'
        f'<span class="pill {pill_class}">{status_text}</span></div>',
        unsafe_allow_html=True,
    )
with health_cols[3]:
    last_log_disp = (HEALTH["last_log"] or "(no log)")[:60]
    age = f'{HEALTH["last_log_age_s"]}s ago' if HEALTH["last_log_age_s"] is not None else "—"
    st.markdown(
        f'<div class="metric"><span class="label">Last log</span>'
        f'<span class="value mono" style="font-size:13px" title="{HEALTH["last_log"]}">{last_log_disp}…</span>'
        f'<span style="font-size:11px;color:#6e7681">{age}</span></div>',
        unsafe_allow_html=True,
    )
with health_cols[4]:
    st.markdown(
        f'<div class="metric"><span class="label">Log size</span>'
        f'<span class="value">{_fmt_bytes(HEALTH["log_size_b"])}</span></div>',
        unsafe_allow_html=True,
    )
with health_cols[5]:
    st.markdown(
        f'<div class="metric"><span class="label">WARN</span>'
        f'<span class="value mono" style="color:#f0a020">{HEALTH["warn_count"]}</span></div>',
        unsafe_allow_html=True,
    )
with health_cols[6]:
    err_color = "#ff4b4b" if HEALTH["err_count"] > 0 else "#8b949e"
    st.markdown(
        f'<div class="metric"><span class="label">ERROR</span>'
        f'<span class="value mono" style="color:{err_color}">{HEALTH["err_count"]}</span></div>',
        unsafe_allow_html=True,
    )

sub_meta = []
sub_meta.append(f'DB age: {HEALTH["db_age_s"]}s' if HEALTH["db_age_s"] is not None else 'DB missing')
sub_meta.append(f'Market: {"OPEN" if MARKET_OPEN is True else "CLOSED" if MARKET_OPEN is False else "?"}')
if PNL.get("error"):
    sub_meta.append(f'⚠️ DB: {PNL["error"]}')
st.markdown(
    f'<div style="font-size:11px;color:#6e7681;margin-top:4px">'
    f'{" · ".join(sub_meta)} · '
    f'Engine: {ENGINE_DIR} · '
    f'Phase 1 — real data wired</div>',
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Render — Row 1: P&L (left wide) | Signal Intent (right narrow)
# ---------------------------------------------------------------------------

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
    tick = SIGNAL_INTENT["tick"]
    if tick:
        st.markdown(
            f'<div style="font-family:SF Mono,Menlo,Consolas,monospace;font-size:13px;color:#c9d1d9">'
            f'<span style="color:#58a6ff">[TICK]</span> '
            f'<span style="color:#6e7681">{tick["ts"]}</span><br>'
            f'SPX=<b>{tick["spx"]}</b> &nbsp; EM=<b>{tick["em"]}</b> &nbsp; GEX=<b>{tick["gex"]}</b><br>'
            f'regime=<b>{tick["regime"]}</b> &nbsp; RSI=<b>{tick["rsi"]}</b><br>'
            f'GEX_regime=<b>{tick["gex_regime"]}</b></div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<div style="font-size:12px;color:#8b949e;padding:8px 0">'
            'no [TICK] in log yet today</div>',
            unsafe_allow_html=True,
        )

    layer = SIGNAL_INTENT["entries"][0]["layer"] if SIGNAL_INTENT["entries"] else "?"
    layer_pill = "pill-blue" if layer == "1" else "pill-amber" if layer == "2" else "pill-green"
    st.markdown(
        f'<div style="margin-top:8px;display:flex;align-items:center;gap:8px">'
        f'<span style="font-size:11px;color:#8b949e;text-transform:uppercase">Most recent layer</span>'
        f'<span class="pill {layer_pill}">L{layer}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        '<div style="font-size:11px;color:#8b949e;text-transform:uppercase;margin-top:12px">'
        'Last 5 [ENTRY]</div>',
        unsafe_allow_html=True,
    )
    entry_header = (
        '<div class="entry-row header">'
        '<div>Time</div><div>Side·Strike</div><div>Layer</div>'
        '<div>Credit</div><div></div></div>'
    )
    if SIGNAL_INTENT["entries"]:
        entry_rows = "".join(
            f'<div class="entry-row">'
            f'<div style="color:#8b949e">{e["ts"].split(" ")[-1] if e["ts"] else ""}</div>'
            f'<div>{e["side"]} {e["strike"]}</div>'
            f'<div>L{e["layer"]}</div>'
            f'<div>${float(e["credit"]):.2f}</div>'
            f'<div></div>'
            f'</div>'
            for e in SIGNAL_INTENT["entries"]
        )
    else:
        entry_rows = '<div style="color:#6e7681;font-size:12px;padding:8px">no [ENTRY] today</div>'
    st.markdown(entry_header + entry_rows, unsafe_allow_html=True)

    st.markdown(
        '<div style="font-size:11px;color:#8b949e;text-transform:uppercase;margin-top:12px">'
        'Skip reasons (log-derived, today)</div>',
        unsafe_allow_html=True,
    )
    if SIGNAL_INTENT["skip_counts"]:
        max_v = max(SIGNAL_INTENT["skip_counts"].values())
        skip_bars = "".join(
            f'<div style="display:flex;align-items:center;gap:8px;font-size:12px;margin:3px 0">'
            f'<div style="width:160px;color:#c9d1d9">{r}</div>'
            f'<div style="flex:1;background:#21262d;border-radius:3px;height:11px;overflow:hidden">'
            f'<div style="width:{100 * v / max_v:.0f}%;height:100%;background:#ff4b4b;opacity:0.7"></div>'
            f'</div>'
            f'<div style="width:36px;text-align:right;color:#8b949e;font-family:SF Mono,Menlo,Consolas,monospace">{v}</div>'
            f'</div>'
            for r, v in SIGNAL_INTENT["skip_counts"].items()
        )
        st.markdown(skip_bars, unsafe_allow_html=True)
    else:
        st.markdown('<div style="color:#6e7681;font-size:12px;padding:8px">no [SKIP] today</div>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Render — Row 2: Open Pos (left) | Closed Today (nested under) | Votes + Rejection (right)
# ---------------------------------------------------------------------------

row2_l, row2_r = st.columns([3, 2])

with row2_l:
    st.markdown('<div class="panel"><h3>Panel 4 — Open Positions</h3>', unsafe_allow_html=True)
    if OPEN_POS_DF.empty:
        st.markdown(
            '<div style="padding:8px 12px;background:#0d1117;border:1px dashed #21262d;border-radius:6px;font-size:12px;color:#8b949e">'
            '∅  No open positions right now.</div>',
            unsafe_allow_html=True,
        )
    else:
        view_df = OPEN_POS_DF[["id", "side", "short_strike", "long_strike", "open_time", "layer", "entry_regime", "upnl"]].copy()
        view_df["strike"] = view_df.apply(
            lambda r: f"{int(r['short_strike'])}/{int(r['long_strike']) if pd.notna(r['long_strike']) else '?'}", axis=1
        )
        view_df["open_time"] = view_df["open_time"].apply(_parse_hhmm)
        view_df = view_df.rename(columns={
            "id": "Pos #", "side": "Side", "open_time": "Opened",
            "layer": "L", "entry_regime": "Regime", "upnl": "uPnL",
        })
        view_df = view_df[["Pos #", "Side", "strike", "Opened", "L", "Regime", "uPnL"]]
        st.dataframe(
            view_df.style.format({"uPnL": "${:+.2f}"}),
            width='stretch', hide_index=True, height=108,
        )
    st.markdown("</div>", unsafe_allow_html=True)

    # Panel 5 — Closed Today
    st.markdown('<div class="panel"><h3>Panel 5 — Closed Today</h3>', unsafe_allow_html=True)
    if CLOSED_DF.empty:
        st.markdown(
            '<div class="empty-state"><div class="big">∅</div>'
            'No closed positions today yet.</div>',
            unsafe_allow_html=True,
        )
    else:
        view_df = CLOSED_DF.copy()
        view_df["strike"] = view_df.apply(
            lambda r: f"{int(r['short_strike'])}/{int(r['long_strike']) if pd.notna(r['long_strike']) else '?'}", axis=1
        )
        view_df["Entry"] = view_df["open_time"].apply(_parse_hhmm)
        view_df["Exit"] = view_df["close_time"].apply(_parse_hhmm)
        view_df["Hold"] = view_df.apply(lambda r: _parse_hold(r["open_time"], r["close_time"]), axis=1)
        view_df["Reason"] = view_df.apply(
            lambda r: "EOD_EXPIRE" if r["status"] == "expired" else r["exit_regime"].upper()
                if pd.notna(r.get("exit_regime")) else "—",
            axis=1,
        )
        view_df = view_df.rename(columns={"id": "Pos #", "side": "Side", "credit": "Credit", "pnl": "PnL"})
        view_df = view_df[["Pos #", "Side", "strike", "Entry", "Exit", "Hold", "Credit", "PnL", "Reason"]]
        st.dataframe(
            view_df.style
            .format({"Credit": "${:.2f}", "PnL": "${:+.2f}"})
            .map(lambda v: "color: #00d97e" if isinstance(v, (int, float)) and v > 0 else "", subset=["PnL"]),
            width='stretch', hide_index=True, height=108,
        )

        total_pnl = float(CLOSED_DF["pnl"].sum())
        wins = int((CLOSED_DF["pnl"] > 0).sum())
        n = len(CLOSED_DF)
        win_rate = (100 * wins / n) if n else 0
        # avg hold via min
        holds_secs = []
        for _, r in CLOSED_DF.iterrows():
            try:
                o = datetime.fromisoformat(r["open_time"])
                c = datetime.fromisoformat(r["close_time"])
                holds_secs.append((c - o).total_seconds())
            except Exception:
                pass
        avg_hold_min = int(sum(holds_secs) / len(holds_secs) / 60) if holds_secs else 0

        pnl_class = "green" if total_pnl > 0 else ""
        st.markdown(
            f'<div class="table-footer">'
            f'<div>count <span class="fv">{n}</span></div>'
            f'<div>total P/L <span class="fv {pnl_class}">${total_pnl:+.2f}</span></div>'
            f'<div>win rate <span class="fv">{win_rate:.0f}%</span></div>'
            f'<div>avg hold <span class="fv">{avg_hold_min // 60}h{avg_hold_min % 60:02d}m</span></div>'
            f'</div>',
            unsafe_allow_html=True,
        )
    st.markdown("</div>", unsafe_allow_html=True)

with row2_r:
    # Panel 7 — Exit Vote Tally
    st.markdown('<div class="panel"><h3>Panel 7 — Exit Vote Tally</h3>', unsafe_allow_html=True)
    total_votes = sum(VOTE_TALLY.values()) or 1
    v0 = VOTE_TALLY["votes=0 [STAY]"]
    v1 = VOTE_TALLY["votes=1 [STAY]"]
    v2 = VOTE_TALLY["votes=2+ [EXIT]"]
    p0 = 100 * v0 / total_votes
    p1 = 100 * v1 / total_votes
    p2 = 100 * v2 / total_votes
    c0_end, c1_end, c2_end = p0, p0 + p1, p0 + p1 + p2
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
        f'<div class="item"><div class="sw" style="background:#00d97e"></div>votes=2+ [EXIT] <span style="color:#8b949e;margin-left:auto">{v2}</span></div>'
        f'<div style="margin-top:6px;color:#8b949e;font-size:11px">last {EXIT_CHECK_SAMPLE_N} [EXIT CHECK] reads</div>'
        f'</div>'
        f'</div>',
        unsafe_allow_html=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)

    # Panel 6 — Rejection Funnel
    st.markdown('<div class="panel"><h3>Panel 6 — Rejection Funnel</h3>', unsafe_allow_html=True)
    # Prefer log-derived counts (broader), fallback to DB
    counts = REJECTIONS["log_counts"] or REJECTIONS["db_counts"]
    if counts:
        rej_df = pd.DataFrame(
            sorted(counts.items(), key=lambda kv: -kv[1]),
            columns=["reason", "count"],
        ).set_index("reason")
        st.bar_chart(rej_df, height=180, width='stretch', color='#ff4b4b')
        src = "log" if REJECTIONS["log_counts"] else "db"
        st.markdown(
            f'<div style="font-size:10px;color:#6e7681;margin-top:4px">'
            f'source: <b>{src}</b>'
            f'{" · DB-side " + str(len(REJECTIONS["db_counts"])) + " reasons additionally tracked" if REJECTIONS["db_counts"] and REJECTIONS["log_counts"] else ""}'
            f'</div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<div style="font-size:12px;color:#8b949e;padding:8px 0">'
            'no rejections today</div>',
            unsafe_allow_html=True,
        )
    st.markdown("</div>", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Render — Panel 8 Decision Stream
# ---------------------------------------------------------------------------

if DECISION_STREAM:
    last3 = DECISION_STREAM[-2:]
    rows_html = "".join(
        f'<div class="log-line {r["level"]}">'
        f'<span class="ts">{r["ts"]}</span> '
        f'<span style="color:#8b949e">[{r["level"]}]</span> '
        f'<span class="tag">[{r["tag"]}]</span> '
        f'{r["msg"][:160]}'
        f'</div>'
        for r in last3
    )
    st.markdown(
        '<div class="panel">'
        '<h3 style="display:flex;justify-content:space-between;align-items:center">'
        '<span>Panel 8 — Engine Decision Stream</span>'
        f'<span style="font-size:10px;color:#6e7681;text-transform:none;letter-spacing:0">'
        f'last 2 of {len(DECISION_STREAM)} buffered · from {LOG_PATH.name}'
        '</span>'
        '</h3>'
        f'<div style="background:#0d1117;padding:6px 10px;border-radius:6px;'
        f'border:1px solid #21262d">{rows_html}</div>'
        '</div>',
        unsafe_allow_html=True,
    )
    with st.expander(f"View full {len(DECISION_STREAM)}-line stream", expanded=False):
        st.markdown(
            '<div style="background:#0d1117;padding:8px 12px;border-radius:6px;'
            'border:1px solid #21262d;max-height:360px;overflow-y:auto">',
            unsafe_allow_html=True,
        )
        for row in DECISION_STREAM:
            st.markdown(
                f'<div class="log-line {row["level"]}">'
                f'<span class="ts">{row["ts"]}</span> '
                f'<span style="color:#8b949e">[{row["level"]}]</span> '
                f'<span class="tag">[{row["tag"]}]</span> '
                f'{row["msg"][:200]}'
                f'</div>',
                unsafe_allow_html=True,
            )
        st.markdown("</div>", unsafe_allow_html=True)
else:
    st.markdown(
        '<div class="panel"><h3>Panel 8 — Engine Decision Stream</h3>'
        f'<div style="font-size:12px;color:#8b949e;padding:8px 0">'
        f'Log not found at `{LOG_PATH}` (read-only — engine log dir preserved as-is).</div></div>',
        unsafe_allow_html=True,
    )

# Footer
err_count_in_footer = sum(1 for line in LOG_LINES if "[ERROR]" in line)
market_disp_map = {True: "🟢 OPEN", False: "🟡 CLOSED", None: "⚪ ?"}; market_disp = market_disp_map[MARKET_OPEN]
st.caption(
    f"Phase 1 · {len(LOG_LINES)} log lines buffered · "
    f"engine: {status_text} · market: {market_disp} · "
    f"phase 0 dummy data replaced; all 8 panels live · {err_count_in_footer} log errors today"
)
