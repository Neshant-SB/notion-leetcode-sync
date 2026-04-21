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

# scrape target (best-effort)
SCRAPE_URLS = [
    "https://takeuforward.org/dsa/strivers-a2z-sheet-learn-dsa-a-to-z",
]

# fallback dataset (this is the one you debugged)
A2Z_JSON_URL = os.getenv(
    "A2Z_JSON_URL",
    "https://raw.githubusercontent.com/hitarth-gg/CP/refs/heads/main/striver-a2z.json",
)

# If scrape yields fewer than this, fallback to A2Z JSON
MIN_SCRAPE_SLUGS = int(os.getenv("TUF_MIN_SCRAPE_SLUGS", "100"))

LC_SLUG_RE = re.compile(r"leetcode\.com/problems/([^/\s?#\"']+)")


def slug_from_lc_link(s: Any) -> str | None:
    if not isinstance(s, str) or not s:
        return None
    m = LC_SLUG_RE.search(s)
    return m.group(1) if m else None


def parse_ques_topic(qt: Any) -> list[str]:
    """
    In the A2Z dataset you showed, ques_topic is a JSON-encoded string like:
      '[{"value":"basics","label":"Introduction to DSA"}]'
    """
    if qt is None:
        return []

    if isinstance(qt, list):
        out: list[str] = []
        for x in qt:
            if isinstance(x, dict) and x.get("label"):
                out.append(str(x["label"]))
        return out

    if isinstance(qt, str) and qt.strip():
        s = qt.strip()
        if s.startswith("[") or s.startswith("{"):
            try:
                return parse_ques_topic(json.loads(s))
            except Exception:
                return []
        return [s]

    return []


def iter_topics_from_a2z(data: Any) -> Iterable[dict]:
    """
    Supports the structure you printed:
      data: list[step]
      step: { step_no, step_title, sub_steps: [...] }
      sub_step: { sub_step_no, sub_step_title, topics: [...] }
      topic: { lc_link, ques_topic, ... }

    Also supports a few alternate shapes just in case the file changes.
    """
    if isinstance(data, list):
        for step in data:
            if not isinstance(step, dict):
                continue
            sub_steps = step.get("sub_steps")
            if isinstance(sub_steps, list):
                for ss in sub_steps:
                    if not isinstance(ss, dict):
                        continue
                    topics = ss.get("topics")
                    if isinstance(topics, list):
                        for t in topics:
                            if isinstance(t, dict):
                                yield t

            # fallback: sometimes steps might directly contain "topics"
            topics = step.get("topics")
            if isinstance(topics, list):
                for t in topics:
                    if isinstance(t, dict):
                        yield t

    elif isinstance(data, dict):
        # fallback: if some future version uses {"steps": [...]}
        for k in ("steps", "data", "topics"):
            v = data.get(k)
            if v is not None:
                yield from iter_topics_from_a2z(v)


def build_map_from_topics(topics: Iterable[dict]) -> dict[str, list[str]]:
    out: dict[str, set[str]] = {}
    total = 0
    lc_count = 0

    for t in topics:
        total += 1
        slug = slug_from_lc_link(t.get("lc_link"))
        if not slug:
            continue
        lc_count += 1

        tags = parse_ques_topic(t.get("ques_topic"))
        tags = [x for x in tags if x and isinstance(x, str)]
        if not tags:
            continue

        out.setdefault(slug, set()).update(tags)

    # debug stats
    (ART / "tuf_a2z_stats.json").write_text(
        json.dumps(
            {
                "topics_total": total,
                "topics_with_lc_link": lc_count,
                "slugs_with_tags": len(out),
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    return {slug: sorted(tags) for slug, tags in out.items()}


def try_scrape_tuf() -> dict[str, list[str]]:
    """
    Best-effort scrape. Might fail / return too little. That's OK; we fallback.
    """
    captured: list[Any] = []
    captured_urls: list[str] = []
    cookie_header = os.getenv("TUF_COOKIE", "").strip()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
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
                t = txt.lstrip()
                if not (t.startswith("{") or t.startswith("[")):
                    return
                if len(t) > 8_000_000:
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

        # Try __NEXT_DATA__
        try:
            el = page.locator("script#__NEXT_DATA__")
            if el.count() > 0:
                txt = el.first.text_content()
                if txt:
                    captured.append(json.loads(txt))
        except Exception:
            pass

        # Debug dumps
        try:
            (ART / "tuf_page.html").write_text(page.content(), encoding="utf-8")
            page.screenshot(path=str(ART / "tuf_page.png"), full_page=True)
        except Exception:
            pass

        (ART / "tuf_capture_urls.json").write_text(json.dumps(captured_urls, indent=2), encoding="utf-8")
        browser.close()

    # Find nested "topics" arrays that look like your A2Z format
    found_topics: list[dict] = []

    def walk(node: Any):
        if isinstance(node, dict):
            # if it looks like a topic object
            if "lc_link" in node and "ques_topic" in node:
                found_topics.append(node)
                return
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for x in node:
                walk(x)

    for blob in captured:
        walk(blob)

    return build_map_from_topics(found_topics)


def fallback_a2z() -> dict[str, list[str]]:
    r = requests.get(A2Z_JSON_URL, timeout=90)
    r.raise_for_status()
    data = r.json()

    # Save small structure hint
    (ART / "tuf_a2z_structure.json").write_text(
        json.dumps(
            {
                "type": str(type(data)),
                "top_len": len(data) if isinstance(data, list) else None,
                "top_keys": list(data.keys())[:30] if isinstance(data, dict) else None,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    topics = iter_topics_from_a2z(data)
    return build_map_from_topics(topics)


def main() -> None:
    OUT.parent.mkdir(exist_ok=True)

    tuf_map: dict[str, list[str]] = {}
    source = "none"

    # 1) scrape
    try:
        tuf_map = try_scrape_tuf()
        source = "scrape"
        print(f"[tuf] scrape result: {len(tuf_map)} slugs")
    except Exception as e:
        (ART / "tuf_scrape_error.txt").write_text(str(e), encoding="utf-8")
        tuf_map = {}

    # 2) fallback
    if len(tuf_map) < MIN_SCRAPE_SLUGS:
        tuf_map = fallback_a2z()
        source = "a2z_fallback"
        print(f"[tuf] a2z fallback result: {len(tuf_map)} slugs")

    OUT.write_text(json.dumps(tuf_map, indent=2, sort_keys=True), encoding="utf-8")
    (ART / "tuf_source.txt").write_text(source, encoding="utf-8")
    print(f"[tuf] wrote {len(tuf_map)} slugs -> {OUT} (source={source})")


if __name__ == "__main__":
    main()