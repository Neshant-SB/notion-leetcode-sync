"""
build_neetcode_map.py
Scrapes NeetCode All (neetcode.io/practice) and produces:
  neetcode_all_map.json  ->  { "two-sum": ["Arrays & Hashing"], ... }
Run: python build_neetcode_map.py
"""

import asyncio
import json
import re
import sys

from playwright.async_api import async_playwright

NEETCODE_URL = "https://neetcode.io/practice"
OUTPUT_FILE  = "neetcode_all_map.json"

# Canonical NeetCode category names (used as fallback / normalisation)
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


def normalise_category(raw: str) -> str:
    """Best-effort match raw DOM text to a canonical category name."""
    raw_clean = raw.strip()
    for c in CANONICAL_CATEGORIES:
        if c.lower() in raw_clean.lower() or raw_clean.lower() in c.lower():
            return c
    return raw_clean  # keep as-is if no match found


async def scrape() -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page    = await browser.new_page(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            )
        )

        print(f"[build_neetcode_map] Navigating to {NEETCODE_URL} ...")
        await page.goto(NEETCODE_URL, wait_until="networkidle", timeout=90_000)

        # Give Angular/React a moment to finish rendering
        await page.wait_for_timeout(4_000)

        # Wait until at least one LeetCode problem link is visible
        try:
            await page.wait_for_selector(
                "a[href*='leetcode.com/problems']", timeout=30_000
            )
        except Exception:
            print("[build_neetcode_map] WARNING: no LeetCode links found after 30s")

        # ------------------------------------------------------------------ #
        # Core extraction via JavaScript running inside the browser context.  #
        # Strategy:                                                            #
        #   1. Collect all category headings (h2 / h3 / elements whose        #
        #      text matches a known category).                                 #
        #   2. For each LeetCode problem link, walk up the DOM until we find  #
        #      the closest category container.                                 #
        # ------------------------------------------------------------------ #
        raw_map: dict = await page.evaluate(
            """() => {
            const result = {};

            // Helper: extract slug from a leetcode URL
            function getSlug(href) {
                const m = href.match(/leetcode\\.com\\/problems\\/([^\\/\\?#]+)/);
                return m ? m[1] : null;
            }

            // Collect ALL elements that look like category headings
            // NeetCode uses h3 tags with the category name, sometimes wrapped
            // in a div/td with a class like "group-name" or similar.
            const headingSelectors = [
                'h1', 'h2', 'h3', 'h4',
                '[class*="group"]',
                '[class*="category"]',
                '[class*="section-title"]',
                '[class*="table-title"]',
            ];
            const allHeadings = Array.from(
                document.querySelectorAll(headingSelectors.join(','))
            ).filter(el => el.textContent.trim().length > 0);

            // Build an ordered list of {el, text, top} for fast lookup
            const headings = allHeadings.map(el => ({
                el,
                text: el.textContent.trim(),
                top: el.getBoundingClientRect().top + window.scrollY,
            }));

            // Sort headings top-to-bottom
            headings.sort((a, b) => a.top - b.top);

            // For each problem link, find the heading immediately above it
            const links = Array.from(
                document.querySelectorAll('a[href*="leetcode.com/problems"]')
            );

            links.forEach(link => {
                const href  = link.getAttribute('href') || '';
                const slug  = getSlug(href);
                if (!slug) return;

                const linkTop = link.getBoundingClientRect().top + window.scrollY;

                // Find the last heading whose top <= linkTop
                let category = null;
                for (let i = headings.length - 1; i >= 0; i--) {
                    if (headings[i].top <= linkTop) {
                        category = headings[i].text;
                        break;
                    }
                }
                if (!category) category = 'Uncategorized';

                if (!result[slug]) result[slug] = [];
                if (!result[slug].includes(category)) result[slug].push(category);
            });

            return result;
        }"""
        )

        await browser.close()

    # Normalise category names
    normalised: dict[str, list[str]] = {}
    for slug, cats in raw_map.items():
        normalised[slug] = [normalise_category(c) for c in cats]

    return normalised


def main() -> None:
    mapping = asyncio.run(scrape())

    if not mapping:
        print("[build_neetcode_map] ERROR: extracted 0 problems. Aborting.")
        sys.exit(1)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as fh:
        json.dump(mapping, fh, indent=2, sort_keys=True)

    total_problems  = len(mapping)
    total_category_assignments = sum(len(v) for v in mapping.values())
    print(
        f"[build_neetcode_map] Done. "
        f"{total_problems} slugs, "
        f"{total_category_assignments} category assignments → {OUTPUT_FILE}"
    )


if __name__ == "__main__":
    main()