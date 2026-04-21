from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Iterable

import requests
from playwright.sync_api import sync_playwright

OUT = Path("maps/tuf.json")
ART = Path("artifacts")
ART.mkdir(exist_ok=True)

SCRAPE_URLS = [
    "https://takeuforward.org/dsa/strivers-a2z-sheet-learn-dsa-a-to-z",
]

A2Z_JSON_URL = os.getenv(
    "A2Z_JSON_URL",
    "https://raw.githubusercontent.com/hitarth-gg/CP/refs/heads/main/striver-a2z.json",
)

MIN_SCRAPE_SLUGS = int(os.getenv("TUF_MIN_SCRAPE_SLUGS", "100"))

_lc_slug_re = re.compile(r"leetcode\.com/problems/([^/\s?#]+)")


def slug_from_lc_link(s: Any) -> str | None:
    if not isinstance(s, str) or not s:
        return None
    m = _lc_slug_re.search(s)
    return m.group(1) if m else None


def parse_ques_topic(qt: Any) -> list[str]:
    if qt is None:
        return []
    if isinstance(qt, list):
        return [x.get("label") for x in qt if isinstance(x, dict) and x.get("label")]
    if isinstance(qt, str) and qt.strip().startswith("["):
        try:
            arr = json.loads(qt)
            if isinstance(arr, list):
                return [x.get("label") for x in arr if isinstance(x, dict) and x.get("label")]
        except Exception:
            pass
    return []


def items_from_a2z_json(data: Any) -> Iterable[dict]:
    if isinstance(data, dict) and isinstance(data.get("topics"), list):
        return (x for x in data["topics"] if isinstance(x, dict))
    if isinstance(data, list):
        return (x for x in data if isinstance(x, dict))
    return []


def build_map_from_items(items: Iterable[dict]) -> dict[str, list[str]]:
    out: dict[str, set[str]] = {}
    for it in items:
        slug = slug_from_lc_link(it.get("lc_link"))
        if not slug:
            continue
        tags = [t for t in parse_ques_topic(it.get("ques_topic")) if t and isinstance(t, str)]
        if not tags:
            continue
        out.setdefault(slug, set()).update(tags)
    return {k: sorted(v) for k, v in out.items()}


def try_scrape_tuf() -> dict[str, list[str]]:
    captured:       list[Any] = []
    captured_urls:  list[str] = []
    cookie_header = os.getenv("TUF_COOKIE", "").strip()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            ),
            extra_http_headers={"cookie": cookie_header} if cookie_header else None,
        )
        page = ctx.new_page()

        def on_response(resp):
            try:
                if resp.request.resource_type not in ("xhr", "fetch"):
                    return
                captured_urls.append(resp.url)
                txt = resp.text()
                t   = txt.lstrip()
                if not (t.startswith("{") or t.startswith("[")):
                    return
                if len(t) > 6_000_000:
                    return
                captured.append(json.loads(t))
            except Exception:
                return

        page.on("response", on_response)

        for url in SCRAPE_URLS:
            page.goto(url, wait_until="domcontentloaded", timeout=120_000)
            page.wait_for_timeout(2500)
            for _ in range(18):
                page.mouse.wheel(0, 3000)
                page.wait_for_timeout(150)

        try:
            el = page.locator("script#__NEXT_DATA__")
            if el.count() > 0:
                txt = el.first.text_content()
                if txt:
                    captured.append(json.loads(txt))
        except Exception:
            pass

        try:
            (ART / "tuf_page.html").write_text(page.content(), encoding="utf-8")
            page.screenshot(path=str(ART / "tuf_page.png"), full_page=True)
        except Exception:
            pass

        (ART / "tuf_capture_urls.json").write_text(
            json.dumps(captured_urls, indent=2), encoding="utf-8"
        )
        browser.close()

    def walk(node: Any, found: list[dict]) -> None:
        if isinstance(node, dict):
            if "lc_link" in node:
                found.append(node)
            for v in node.values():
                walk(v, found)
        elif isinstance(node, list):
            for x in node:
                walk(x, found)

    found: list[dict] = []
    for blob in captured:
        walk(blob, found)

    return build_map_from_items(found)


def fallback_a2z() -> dict[str, list[str]]:
    r = requests.get(A2Z_JSON_URL, timeout=90)
    r.raise_for_status()
    return build_map_from_items(items_from_a2z_json(r.json()))


def main() -> None:
    OUT.parent.mkdir(exist_ok=True)

    tuf_map: dict[str, list[str]] = {}
    source = "none"

    try:
        tuf_map = try_scrape_tuf()
        source  = "scrape"
    except Exception as e:
        (ART / "tuf_scrape_error.txt").write_text(str(e), encoding="utf-8")

    if len(tuf_map) < MIN_SCRAPE_SLUGS:
        try:
            tuf_map = fallback_a2z()
            source  = "a2z_fallback"
        except Exception as e:
            (ART / "tuf_fallback_error.txt").write_text(str(e), encoding="utf-8")
            if not OUT.exists():
                OUT.write_text("{}", encoding="utf-8")
            print("[tuf] WARNING: scrape + fallback both failed. Keeping existing maps/tuf.json.")
            return

    OUT.write_text(json.dumps(tuf_map, indent=2, sort_keys=True), encoding="utf-8")
    (ART / "tuf_source.txt").write_text(source, encoding="utf-8")
    print(f"[tuf] wrote {len(tuf_map)} slugs -> {OUT}  (source={source})")


if __name__ == "__main__":
    main()