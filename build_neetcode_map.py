"""
build_neetcode_map.py

Builds neetcode_all_map.json:
  { "<leetcode_slug>": ["Arrays & Hashing", ...], ... }

Source:
  https://neetcode.io/practice/practice/allNC

Approach:
- Load the page with Playwright
- Capture JSON network responses + common SPA globals
- Walk JSON to associate:
    slug -> NeetCode category (canonical list)
- Write neetcode_all_map.json

Debug artifacts (on failure or NEETCODE_DEBUG=1):
  neetcode_debug.html
  neetcode_debug.png
"""

import asyncio
import json
import os
import re
from pathlib import Path
from typing import Any

from playwright.async_api import async_playwright

NEETCODE_URL = "https://neetcode.io/practice/practice/allNC"
OUTPUT_FILE = "neetcode_all_map.json"
DEBUG = os.getenv("NEETCODE_DEBUG", "0") == "1"

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


def wpath(name: str) -> Path:
    return Path(os.getenv("GITHUB_WORKSPACE", ".")) / name


def normalize_category(s: str) -> str | None:
    if not isinstance(s, str):
        return None
    s2 = " ".join(s.strip().split())
    # exact canonical
    if s2 in CANONICAL_CATEGORIES:
        return s2
    # case-insensitive match
    for c in CANONICAL_CATEGORIES:
        if s2.lower() == c.lower():
            return c
    # common variations
    aliases = {
        "Heaps / Priority Queue": "Heap / Priority Queue",
        "Heap/Priority Queue": "Heap / Priority Queue",
        "1D Dynamic Programming": "1-D Dynamic Programming",
        "2D Dynamic Programming": "2-D Dynamic Programming",
    }
    if s2 in aliases:
        return aliases[s2]
    return None


def extract_slug(val: str) -> str | None:
    if not val:
        return None

    # leetcode URL
    m = re.search(r"leetcode\.com/problems/([^/\s?#]+)", val)
    if m:
        return m.group(1)

    # neetcode internal URL
    m = re.search(r"/problems/([^/\s?#]+)", val)
    if m:
        return m.group(1)

    # already a slug
    if re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", val):
        return val

    return None


def walk(node: Any, mapping: dict[str, set[str]], cat_ctx: str | None = None, key_ctx: str = "") -> None:
    if isinstance(node, dict):
        local_cat = cat_ctx

        # category context
        for k, v in node.items():
            if isinstance(v, str):
                c = normalize_category(v)
                if c:
                    local_cat = c
                if k.lower() in {"category", "section", "group", "topic", "name", "title"}:
                    c2 = normalize_category(v)
                    if c2:
                        local_cat = c2

        # slug from common keys
        for k in ("titleSlug", "title_slug", "leetcodeSlug", "problemSlug", "slug", "href", "url", "link", "path", "to"):
            v = node.get(k)
            if isinstance(v, str):
                slug = extract_slug(v)
                if slug:
                    mapping.setdefault(slug, set()).add(local_cat or "Uncategorized")

        for k, v in node.items():
            walk(v, mapping, local_cat, k)

    elif isinstance(node, list):
        for item in node:
            walk(item, mapping, cat_ctx, key_ctx)

    elif isinstance(node, str):
        slug = extract_slug(node)
        if slug and ("leetcode.com/problems/" in node or "/problems/" in node):
            mapping.setdefault(slug, set()).add(cat_ctx or "Uncategorized")


async def dump_debug(page) -> None:
    try:
        await page.screenshot(path=str(wpath("neetcode_debug.png")), full_page=True)
    except Exception:
        pass
    try:
        wpath("neetcode_debug.html").write_text(await page.content(), encoding="utf-8")
    except Exception:
        pass


async def scrape() -> dict[str, list[str]]:
    captured: list[dict[str, Any]] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1400, "height": 900},
            locale="en-US",
        )
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        page = await context.new_page()

        async def on_response(resp):
            try:
                ct = (resp.headers.get("content-type") or "").lower()
                if "application/json" not in ct and "application/ld+json" not in ct and "text/json" not in ct:
                    return
                data = await resp.json()
                captured.append({"url": resp.url, "json": data})
            except Exception:
                return

        page.on("response", on_response)

        print(f"[build_neetcode_map] Navigating to {NEETCODE_URL} ...")
        await page.goto(NEETCODE_URL, wait_until="networkidle", timeout=120_000)
        await page.wait_for_timeout(2500)

        # Click Expand if present
        try:
            await page.get_by_text("Expand", exact=True).click(timeout=3000)
            await page.wait_for_timeout(800)
        except Exception:
            pass

        # Scroll a bit to trigger any lazy loads
        for _ in range(20):
            await page.mouse.wheel(0, 3000)
            await page.wait_for_timeout(200)

        globals_blob = await page.evaluate(
            """() => ({
              __NEXT_DATA__: (typeof window.__NEXT_DATA__ !== 'undefined') ? window.__NEXT_DATA__ : null,
              __NUXT__: (typeof window.__NUXT__ !== 'undefined') ? window.__NUXT__ : null,
              __APOLLO_STATE__: (typeof window.__APOLLO_STATE__ !== 'undefined') ? window.__APOLLO_STATE__ : null,
              __INITIAL_STATE__: (typeof window.__INITIAL_STATE__ !== 'undefined') ? window.__INITIAL_STATE__ : null,
            })"""
        )

        sources: list[Any] = []
        sources.append({"url": "globals", "json": globals_blob})
        sources.extend(captured)

        print(f"[build_neetcode_map] json_sources={len(captured)}")

        mapping_sets: dict[str, set[str]] = {}
        for src in sources:
            walk(src.get("json"), mapping_sets)

        mapping: dict[str, list[str]] = {}
        for slug, cats in mapping_sets.items():
            # keep only canonical-ish categories; drop Uncategorized if we have better
            cats2 = sorted(set([c for c in cats if c and c != "Uncategorized"]))
            mapping[slug] = cats2 if cats2 else ["Uncategorized"]

        if DEBUG or len(mapping) == 0:
            await dump_debug(page)

        await browser.close()
        return mapping


def main() -> None:
    mapping = asyncio.run(scrape())
    if not mapping or all(v == ["Uncategorized"] for v in mapping.values()):
        print("[build_neetcode_map] ERROR: extracted no usable categories/slugs.")
        raise SystemExit(1)

    wpath(OUTPUT_FILE).write_text(json.dumps(mapping, indent=2, sort_keys=True), encoding="utf-8")
    print(f"[build_neetcode_map] Wrote {len(mapping)} slugs → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()