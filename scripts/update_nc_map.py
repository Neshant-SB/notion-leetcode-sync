from __future__ import annotations

import json
import re
import time
from html.parser import HTMLParser
from pathlib import Path
from typing import Optional

import requests

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

BASE = "https://neetcode.io/solutions/"
OUT  = Path("maps/nc_allnc.json")
ART  = Path("artifacts")
ART.mkdir(exist_ok=True)

CATEGORIES = [
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

# One representative slug per category area.
# We do NOT assign these to a category; the parser detects it from page text.
SEEDS = [
    "two-sum",
    "valid-palindrome",
    "minimum-window-substring",
    "valid-parentheses",
    "binary-search",
    "reverse-linked-list",
    "invert-binary-tree",
    "kth-largest-element-in-an-array",
    "subsets",
    "implement-trie-prefix-tree",
    "number-of-islands",
    "min-cost-to-connect-all-points",
    "climbing-stairs",
    "longest-common-subsequence",
    "jump-game",
    "merge-intervals",
    "rotate-image",
    "single-number",
    "chunk-array",
]

_slug_re = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")


def slug_from_href(href: str) -> Optional[str]:
    if not href or "/solutions/" not in href:
        return None
    slug = href.split("/solutions/", 1)[1].split("?")[0].split("#")[0].strip("/")
    return slug if _slug_re.match(slug) else None


class SidebarParser(HTMLParser):
    """
    Streaming HTML parser.
    Tracks the nearest category heading seen in text,
    then maps every <a href="/solutions/<slug>"> to it.
    """
    def __init__(self) -> None:
        super().__init__()
        self.current_category: Optional[str] = None
        self.categories_seen:  set[str]      = set()
        self.in_a = False
        self.a_href: Optional[str] = None
        self.mapping: dict[str, set[str]] = {}

    def handle_starttag(self, tag, attrs):
        if tag != "a":
            return
        self.in_a   = True
        self.a_href = None
        for k, v in attrs:
            if k == "href":
                self.a_href = v
                break

    def handle_endtag(self, tag):
        if tag == "a":
            self.in_a   = False
            self.a_href = None

    def handle_data(self, data):
        if not data:
            return
        t = " ".join(data.split()).strip()
        if not t:
            return

        if t in CATEGORIES:
            self.current_category = t
            self.categories_seen.add(t)

        if self.in_a and self.a_href and self.current_category:
            slug = slug_from_href(self.a_href)
            if slug:
                self.mapping.setdefault(slug, set()).add(self.current_category)


def fetch_html(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=60)
        return r.text if r.status_code == 200 else None
    except Exception:
        return None


def main() -> None:
    OUT.parent.mkdir(exist_ok=True)

    final:     dict[str, set[str]] = {}
    cats_seen: set[str]            = set()
    debug_saved                    = False

    for seed in SEEDS:
        html = fetch_html(f"{BASE}{seed}")
        if not html:
            continue

        if not debug_saved:
            (ART / "nc_seed_page.html").write_text(html, encoding="utf-8")
            debug_saved = True

        p = SidebarParser()
        p.feed(html)

        for slug, cats in p.mapping.items():
            final.setdefault(slug, set()).update(cats)
        cats_seen.update(p.categories_seen)

        time.sleep(0.2)

        if len(cats_seen) == len(CATEGORIES):
            break  # got everything

    if not final:
        print("[nc] WARNING: 0 slugs extracted. Keeping existing maps/nc_allnc.json.")
        if not OUT.exists():
            OUT.write_text("{}", encoding="utf-8")
        return

    out = {slug: sorted(cats) for slug, cats in sorted(final.items())}
    OUT.write_text(json.dumps(out, indent=2, sort_keys=True), encoding="utf-8")
    print(f"[nc] wrote {len(out)} slugs -> {OUT}  (categories={len(cats_seen)}/{len(CATEGORIES)})")


if __name__ == "__main__":
    main()