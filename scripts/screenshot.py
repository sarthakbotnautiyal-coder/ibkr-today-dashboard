#!/usr/bin/env python3
"""
Phase 0 wireframe screenshot capture.

Loads the running Streamlit dashboard on http://127.0.0.1:5558 in headless
Chromium, waits for full render, and captures:
  - 01: 1920x1200 viewport (above-the-fold)
  - 02: 1920x1200 viewport scrolled to bottom
  - 03: 1920x1400 viewport (all 8 panels visible without scroll)
  - 04: Panel 8 expander open, full_page (all 20 log lines)
  - 05-panel{NN}-{slug}.png: per-panel close-ups

This is dev-only tooling — not in requirements.txt, not used at runtime.
The user runs:
  $ ./venv/bin/streamlit run dashboard.py --server.port 5558 --server.headless true --server.fileWatcherType=none &
  $ ./venv/bin/python scripts/screenshot.py

Outputs PNGs to ../screenshots/ (gitignored).
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

URL = "http://127.0.0.1:5558"
OUT_DIR = Path(__file__).resolve().parent.parent / "screenshots"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _panel_clip_boxes(page):
    """Walk the DOM to compute visual clip boxes for each panel.

    Each Streamlit .panel div is just the h3 header — the actual content
    (dataframe, bar_chart, etc.) lives in a SIBLING stElementContainer.
    So we walk the column stVerticalBlock children: for each h3 panel, the
    visual extent runs from that h3's stElementContainer top through all
    subsequent stElementContainers until the next h3 panel or column end.
    """
    return page.evaluate("""() => {
        function findColumn(h3) {
            let el = h3.closest('div.panel');
            while (el) {
                el = el.parentElement;
                if (el && el.matches('[data-testid="stVerticalBlock"]') && el.children.length > 1) return el;
            }
            return null;
        }
        const h3s = Array.from(document.querySelectorAll('h3'));
        const panels = [];
        for (const h3 of h3s) {
            const col = findColumn(h3);
            if (!col) continue;
            const cr = col.getBoundingClientRect();
            let el = h3;
            while (el && el.parentElement !== col) el = el.parentElement;
            if (!el) continue;
            const idx = Array.from(col.children).indexOf(el);
            const er = el.getBoundingClientRect();
            const myNum = parseInt(h3.textContent.match(/Panel (\\d+)/)?.[1] || '0');
            let botY = er.bottom;
            for (let i = idx + 1; i < col.children.length; i++) {
                const child = col.children[i];
                const childH3 = child.querySelector('h3');
                if (childH3 && childH3.textContent.includes('Panel')) break;
                const childR = child.getBoundingClientRect();
                if (childR.height > 0) {
                    let cb = childR.bottom;
                    for (const d of child.querySelectorAll('*')) {
                        const dr = d.getBoundingClientRect();
                        if (dr.height > 0 && dr.bottom > cb) cb = dr.bottom;
                    }
                    if (cb > botY) botY = cb;
                }
            }
            panels.push({n: myNum, heading: h3.textContent.trim().substring(0, 30),
                         x: cr.x, y: er.y, w: cr.width, h: botY - er.y});
        }
        // Panel 1: the metric row (first horizontal block)
        const metricCols = document.querySelectorAll('.metric');
        if (metricCols.length > 0) {
            let hb = metricCols[0].parentElement;
            while (hb && !hb.matches('[data-testid="stHorizontalBlock"]')) hb = hb.parentElement;
            if (hb) {
                const r = hb.getBoundingClientRect();
                panels.push({n: 1, heading: 'Panel 1 — Health Strip', x: r.x, y: r.y, w: r.width, h: r.height});
            }
        }
        panels.sort((a, b) => a.n - b.n);
        return panels;
    }""")


def main() -> int:
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--no-sandbox"])

        console_msgs: list[str] = []

        # 1) 1920x1200 viewport — above-the-fold
        ctx = browser.new_context(viewport={"width": 1920, "height": 1200}, device_scale_factor=1)
        page = ctx.new_page()
        page.on("console", lambda m: console_msgs.append(f"[{m.type}] {m.text}"))
        page.on("pageerror", lambda e: console_msgs.append(f"[pageerror] {e}"))
        print(f"→ navigating to {URL}")
        page.goto(URL, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_selector("text=Panel 2", timeout=30000)
        page.wait_for_selector("text=Panel 8", timeout=30000)
        time.sleep(2.5)  # let charts settle

        out1 = OUT_DIR / "01-1920x1200-above-fold.png"
        page.screenshot(path=str(out1))
        print(f"  ✓ 01 → {out1.name} ({out1.stat().st_size // 1024} KB)")

        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(0.8)
        out2 = OUT_DIR / "02-1920x1200-bottom-fold.png"
        page.screenshot(path=str(out2))
        print(f"  ✓ 02 → {out2.name} ({out2.stat().st_size // 1024} KB)")

        # per-panel close-ups (clipped from the 1920x1400 viewport, larger so all panels fit)
        ctx2 = browser.new_context(viewport={"width": 1920, "height": 1400}, device_scale_factor=1)
        page2 = ctx2.new_page()
        page2.on("console", lambda m: console_msgs.append(f"[{m.type}] {m.text}"))
        page2.on("pageerror", lambda e: console_msgs.append(f"[pageerror] {e}"))
        page2.goto(URL, wait_until="domcontentloaded", timeout=30000)
        page2.wait_for_selector("text=Panel 2", timeout=30000)
        page2.wait_for_selector("text=Panel 8", timeout=30000)
        time.sleep(2.5)

        out3 = OUT_DIR / "03-1920x1400-all-panels.png"
        page2.screenshot(path=str(out3))
        print(f"  ✓ 03 → {out3.name} ({out3.stat().st_size // 1024} KB)")

        # per-panel clips
        panels = _panel_clip_boxes(page2)
        print(f"  found {len(panels)} panel regions")
        for p in panels:
            slug = p["heading"].split("—")[0].strip().lower().replace(" ", "-")
            clip = {
                "x": max(0, p["x"] - 8),
                "y": max(0, p["y"] - 8),
                "width": min(1920, p["w"] + 16),
                "height": min(1400, p["h"] + 16),
            }
            out = OUT_DIR / f"05-panel{p['n']:02d}-{slug}.png"
            page2.screenshot(path=str(out), clip=clip)
            print(f"  ✓ {p['heading']:36s} → {out.name} ({out.stat().st_size // 1024} KB)")

        # 04: Panel 8 expander open, full page
        try:
            page2.click("text=View full 20-line stream", timeout=5000)
            time.sleep(1.0)
        except Exception as exc:  # noqa: BLE001
            print(f"  ! expander click failed: {exc}")
        out4 = OUT_DIR / "04-panel8-expanded.png"
        page2.screenshot(path=str(out4), full_page=True)
        print(f"  ✓ 04 → {out4.name} ({out4.stat().st_size // 1024} KB)")

        errs = [m for m in console_msgs if "[error]" in m or "[pageerror]" in m]
        if errs:
            print("\nConsole / page errors:")
            for e in errs:
                print(f"  {e}")
        else:
            print(f"\nNo console errors. ({len(console_msgs)} total console messages captured)")

        browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
