"""
build_neetcode_map.py

Scrape NeetCode "NeetCode All" practice list and generate:
  neetcode_all_map.json -> { "two-sum": ["Arrays & Hashing"], ... }

This version is hardened for modern SPA DOMs:
- does NOT assume <a href="..."> links exist
- extracts "/problems/<slug>" from ANY attribute (href, data-href, onclick, etc.)
- dumps debug artifacts (HTML + screenshot) when extraction fails

Env:
  NEETCODE_DEBUG=1  -> always dump debug artifacts
"""

import asyncio
import json
import os
import re
import sys
from pathlib import Path

from playwright.async_api import async_playwright

NEETCODE_URL = "https://neetcode.io/practice/practice/allNC"
OUTPUT_FILE = "neetcode_all_map.json"

CANONICAL_CATEGORIES = [
    "Arrays & Hashing",
    "Two Pointers",
    "Sliding Window",
    "Stack",
    "Binary Search",
    "Linked List",
    "Trees",
    "Heap / Priority Queue",
    "Backtracking",
    "Tries",
    "Graphs",
    "Advanced Graphs",
    "1-D Dynamic Programming",
    "2-D Dynamic Programming",
    "Greedy",
    "Intervals",
    "Math & Geometry",
    "Bit Manipulation",
    "JavaScript",
]

DEBUG = os.getenv("NEETCODE_DEBUG", "0") == "1"


def _dump_path(name: str) -> Path:
    # Dump into repo workspace (GitHub Actions) or cwd (local)
    base = Path(os.getenv("GITHUB_WORKSPACE", "."))
    return base / name


async def dump_debug(page) -> None:
    """Write screenshot + HTML so we can inspect what the runner actually got."""
    try:
        await page.screenshot(path=str(_dump_path("neetcode_debug.png")), full_page=True)
    except Exception as e:
        print(f"[debug] screenshot failed: {e}")

    try:
        html = await page.content()
        _dump_path("neetcode_debug.html").write_text(html, encoding="utf-8")
    except Exception as e:
        print(f"[debug] html dump failed: {e}")

    try:
        print(f"[debug] page.url={page.url}")
        print(f"[debug] page.title={await page.title()}")
    except Exception:
        pass


async def best_effort_click(page, label: str) -> bool:
    """Click a control by exact visible text, but never fail the run if missing."""
    try:
        loc = page.get_by_text(label, exact=True).first
        await loc.click(timeout=5_000)
        await page.wait_for_timeout(750)
        return True
    except Exception:
        return False


async def expand_every_category(page) -> None:
    """
    Try to expand the list. NeetCode uses collapsible sections.
    We do:
      - click "Expand" once if present
      - click each category heading (best-effort)
    """
    await best_effort_click(page, "Expand")

    for cat in CANONICAL_CATEGORIES:
        try:
            await page.get_by_text(cat, exact=True).first.click(timeout=1_500)
            await page.wait_for_timeout(200)
        except Exception:
            # Not fatal—DOM may differ.
            pass


async def scroll_page(page, rounds: int = 25) -> None:
    # Many SPAs render more items only after scrolling.
    for _ in range(rounds):
        await page.mouse.wheel(0, 3000)
        await page.wait_for_timeout(250)
    # Also try End/Home (sometimes the scroll container listens to keys)
    try:
        await page.keyboard.press("End")
        await page.wait_for_timeout(500)
        await page.keyboard.press("Home")
        await page.wait_for_timeout(500)
        await page.keyboard.press("End")
        await page.wait_for_timeout(500)
    except Exception:
        pass


async def scrape() -> dict[str, list[str]]:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )

        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id="UTC",
            viewport={"width": 1400, "height": 900},
        )

        # Stealth-ish: hide navigator.webdriver
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )

        page = await context.new_page()

        if DEBUG:
            page.on("console", lambda msg: print(f"[browser console] {msg.type}: {msg.text}"))
            page.on("pageerror", lambda err: print(f"[browser pageerror] {err}"))

        print(f"[build_neetcode_map] Navigating to {NEETCODE_URL} ...")
        await page.goto(NEETCODE_URL, wait_until="networkidle", timeout=120_000)
        await page.wait_for_timeout(3_000)

        # Expand/click categories and scroll to force rendering
        await expand_every_category(page)
        await scroll_page(page)

        # Extract categories + problem slugs by scanning attributes of all elements
        raw_map = await page.evaluate(
            f"""() => {{
  const CANON = {json.dumps(CANONICAL_CATEGORIES)};

  function norm(s) {{
    return (s || "").replace(/\\s+/g, " ").trim();
  }}

  function slugFromAnyValue(v) {{
    if (!v) return null;
    const m = String(v).match(/problems\\/([^\\/\\?#]+)/);
    return m ? m[1] : null;
  }}

  // Locate category headings by text match and record vertical position.
  // We look at lots of elements because headings might be <div>/<button>/<h3>, etc.
  const els = Array.from(document.querySelectorAll("body *"));

  const headings = [];
  for (const el of els) {{
    const t = norm(el.textContent);
    if (!t) continue;
    const hit = CANON.find(c => t === c);
    if (!hit) continue;

    const rect = el.getBoundingClientRect();
    headings.push({{
      category: hit,
      top: rect.top + window.scrollY
    }});
  }}
  headings.sort((a,b) => a.top - b.top);

  // Scan ALL elements for ANY attribute containing "problems/<slug>"
  const problems = [];
  for (const el of els) {{
    const rect = el.getBoundingClientRect();
    const top = rect.top + window.scrollY;

    // Standard attributes
    for (const attr of el.getAttributeNames ? el.getAttributeNames() : []) {{
      const val = el.getAttribute(attr);
      const slug = slugFromAnyValue(val);
      if (slug) problems.push({{ slug, top }});
    }}

    // Sometimes frameworks store navigation in properties:
    // try common ones very carefully
    const any = el;
    const candidates = [any.href, any.to, any.pathname];
    for (const v of candidates) {{
      const slug = slugFromAnyValue(v);
      if (slug) problems.push({{ slug, top }});
    }}
  }}

  // Build mapping: assign each problem to nearest heading above it
  const result = {{}};
  for (const p of problems) {{
    let cat = null;
    for (let i = headings.length - 1; i >= 0; i--) {{
      if (headings[i].top <= p.top) {{
        cat = headings[i].category;
        break;
      }}
    }}
    if (!cat) cat = "Uncategorized";
    if (!result[p.slug]) result[p.slug] = [];
    if (!result[p.slug].includes(cat)) result[p.slug].push(cat);
  }}

  return {{
    headingsCount: headings.length,
    problemsFound: problems.length,
    map: result
  }};
}}"""
        )

        mapping: dict[str, list[str]] = raw_map["map"]
        slugs = list(mapping.keys())

        print(
            f"[build_neetcode_map] headings={raw_map['headingsCount']} "
            f"problemElements={raw_map['problemsFound']} uniqueSlugs={len(slugs)}"
        )

        if DEBUG or len(slugs) == 0:
            await dump_debug(page)

        await browser.close()

        return mapping


def main() -> None:
    mapping = asyncio.run(scrape())

    if not mapping:
        print("[build_neetcode_map] ERROR: extracted 0 problems. Aborting.")
        sys.exit(1)

    Path(OUTPUT_FILE).write_text(
        json.dumps(mapping, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    print(f"[build_neetcode_map] Wrote {len(mapping)} slugs to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()