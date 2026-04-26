"""
sync.py  –  LeetCode ➜ Notion tracker
- Sync tracker properties (ID, difficulty, topics, tags, completed date, etc.)
- Maintain pivot Stats DB row (streak, totals)
- Store FULL problem statement in the Notion page body (directly, no toggle)
- Store latest accepted solution code ONLY if it can be fetched WITHOUT cookies
- Overwrite only a managed "synced section" each time (keeps your notes untouched)

Commands
--------
python sync.py sync      --recent-limit 20
python sync.py backfill  --create-missing --fill-dates
python sync.py diagnose
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag
from dateutil import tz

# ──────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────
NOTION_VERSION = "2026-03-11"
NOTION_API = "https://api.notion.com/v1"
LC_GRAPHQL = "https://leetcode.com/graphql"

NC_MAP_FILE = "maps/nc_allnc.json"
TUF_MAP_FILE = "maps/tuf.json"

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

# ── Tracker DB property names — must match your Notion DB exactly ──
P_NAME = "Name"
P_ID = "Problem ID"
P_DIFFICULTY = "Difficulty"
P_STATUS = "Status"
P_COMPLETED = "Completed date"
P_CATEGORY = "Category"
P_TOPICS = "Topics"
P_NC_TAGS = "NC Tags"
P_TUF_TAGS = "TUF Tags"
P_SLUG = "Slug"
P_URL = "LeetCode URL"

# Optional but recommended to avoid rewriting the same pages every schedule run
P_LAST_AC_ID = "Last AC Submission ID"  # Number

# ── Stats pivot DB property names — must match your Stats DB exactly ──
SP_NAME = "Name"  # Title
SP_CURRENT_STREAK = "Current Streak"  # Number
SP_LONGEST_STREAK = "Longest Streak"  # Number
SP_TOTAL_SOLVED = "Total Solved"      # Number
SP_THIS_WEEK = "Solved This Week"     # Number
SP_THIS_MONTH = "Solved This Month"   # Number
SP_UPDATED = "Last Updated"           # Date
STATS_ROW_NAME = "Stats"

# ── Page body managed section (directly on page, no toggle) ──
SYNC_SECTION_TITLE = "LeetCode Question"
SYNC_SECTION_ICON = "📌"   # visible but small; change if you want

# ──────────────────────────────────────────────
# Notion: Data source resolver (newer Notion API model)
# ──────────────────────────────────────────────
_DS_CACHE: dict[str, str] = {}
_TRACKER_PROP_NAMES: set[str] = set()
_STATS_PROP_NAMES: set[str] = set()


def mustenv(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        print(f"[sync] ERROR: missing env var '{name}'", file=sys.stderr)
        sys.exit(1)
    return v


def optenv(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def notion_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def notion_resolve_data_source_id(notion_token: str, database_or_data_source_id: str) -> str:
    """
    Accepts either a database_id (container) OR a data_source_id and returns a data_source_id.
    Caches results so we don't re-resolve every call.
    """
    key = database_or_data_source_id.replace("-", "")
    if key in _DS_CACHE:
        return _DS_CACHE[key]

    headers = notion_headers(notion_token)

    # 1) Try treating it as a data_source_id
    r1 = requests.get(
        f"{NOTION_API}/data_sources/{database_or_data_source_id}",
        headers=headers,
        timeout=60,
    )
    if r1.ok:
        ds_id = r1.json()["id"]
        _DS_CACHE[key] = ds_id
        return ds_id

    # 2) Try treating it as a database_id (container) and pick first data source
    r2 = requests.get(
        f"{NOTION_API}/databases/{database_or_data_source_id}",
        headers=headers,
        timeout=60,
    )
    if not r2.ok:
        print(
            "[notion_resolve_data_source_id] Could not resolve ID as data_source or database.\n"
            f"  as data_source: HTTP {r1.status_code}: {r1.text}\n"
            f"  as database    : HTTP {r2.status_code}: {r2.text}",
            file=sys.stderr,
        )
        r2.raise_for_status()

    db = r2.json()
    data_sources = db.get("data_sources") or []
    if not data_sources:
        raise RuntimeError("Database has no data_sources array (unexpected).")

    ds_id = data_sources[0]["id"]
    _DS_CACHE[key] = ds_id
    return ds_id


def notion_get_data_source_schema(notion_token: str, database_or_data_source_id: str) -> dict:
    ds_id = notion_resolve_data_source_id(notion_token, database_or_data_source_id)
    r = requests.get(
        f"{NOTION_API}/data_sources/{ds_id}",
        headers=notion_headers(notion_token),
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


def init_prop_names(notion_token: str, tracker_db_id: str, stats_db_id: str | None) -> None:
    global _TRACKER_PROP_NAMES, _STATS_PROP_NAMES
    tracker_schema = notion_get_data_source_schema(notion_token, tracker_db_id)
    _TRACKER_PROP_NAMES = set((tracker_schema.get("properties") or {}).keys())

    if stats_db_id:
        stats_schema = notion_get_data_source_schema(notion_token, stats_db_id)
        _STATS_PROP_NAMES = set((stats_schema.get("properties") or {}).keys())


# ──────────────────────────────────────────────
# Generic helpers
# ──────────────────────────────────────────────
def unix_to_iso(ts: int | str) -> str:
    dt = datetime.fromtimestamp(int(ts), tz=tz.UTC).astimezone(tz.tzlocal())
    return dt.date().isoformat()


def load_json_map(path: str) -> dict[str, list[str]]:
    p = Path(path)
    if not p.exists():
        print(f"[sync] WARNING: {path} not found – tags from this map will be empty.")
        return {}
    with p.open(encoding="utf-8") as fh:
        return json.load(fh)


def chunk_str(s: str, n: int = 1800) -> list[str]:
    s = (s or "").strip()
    out: list[str] = []
    while len(s) > n:
        cut = s.rfind(" ", 0, n)
        if cut < 400:
            cut = n
        out.append(s[:cut].strip())
        s = s[cut:].strip()
    if s:
        out.append(s)
    return out


def _number_value(page: dict, prop: str) -> int | None:
    p = page.get("properties", {}).get(prop)
    if not p:
        return None
    return p.get("number")


# ──────────────────────────────────────────────
# Notion: query + upsert tracker pages
# ──────────────────────────────────────────────
def notion_query_all(token: str, database_or_data_source_id: str) -> list[dict]:
    ds_id = notion_resolve_data_source_id(token, database_or_data_source_id)

    url = f"{NOTION_API}/data_sources/{ds_id}/query"
    headers = notion_headers(token)

    pages: list[dict] = []
    payload: dict = {"page_size": 100}

    while True:
        r = requests.post(url, headers=headers, json=payload, timeout=60)
        if not r.ok:
            print(
                "[notion_query_all] HTTP error querying data source.\n"
                f"  ds_id : {ds_id}\n"
                f"  HTTP  : {r.status_code}\n"
                f"  body  : {r.text}",
                file=sys.stderr,
            )
            r.raise_for_status()

        data = r.json()
        pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        payload["start_cursor"] = data["next_cursor"]

    return pages


def _rich_text_value(page: dict, prop: str) -> str:
    rts = page.get("properties", {}).get(prop, {}).get("rich_text", [])
    return "".join(rt.get("plain_text", "") for rt in rts).strip()


def _title_value(page: dict, prop: str) -> str:
    rts = page.get("properties", {}).get(prop, {}).get("title", [])
    return "".join(rt.get("plain_text", "") for rt in rts).strip()


def _date_value(page: dict, prop: str) -> str | None:
    d = page.get("properties", {}).get(prop, {}).get("date")
    return d.get("start") if d else None


def notion_index_pages(pages: list[dict]) -> dict[str, dict]:
    """Returns {slug: {page_id, completed, last_ac_id}}"""
    index: dict[str, dict] = {}
    for p in pages:
        slug = _rich_text_value(p, P_SLUG)
        if slug:
            index[slug] = {
                "page_id": p["id"],
                "completed": _date_value(p, P_COMPLETED),
                "last_ac_id": _number_value(p, P_LAST_AC_ID),
            }
    return index


def notion_upsert(token: str, db_id: str, page_id: str | None, props: dict) -> dict:
    headers = notion_headers(token)

    if page_id is None:
        ds_id = notion_resolve_data_source_id(token, db_id)
        payload: dict = {"parent": {"data_source_id": ds_id}, "properties": props}
        r = requests.post(f"{NOTION_API}/pages", headers=headers, json=payload, timeout=60)
    else:
        r = requests.patch(
            f"{NOTION_API}/pages/{page_id}",
            headers=headers,
            json={"properties": props},
            timeout=60,
        )

    if not r.ok:
        print(
            f"[notion_upsert] HTTP {r.status_code} error.\n"
            f"  page_id     : {page_id or '(new page)'}\n"
            f"  Notion says : {r.text}\n"
            f"  Props sent  : {json.dumps(list(props.keys()), indent=2)}",
            file=sys.stderr,
        )
        r.raise_for_status()

    return r.json()


# ──────────────────────────────────────────────
# Notion blocks (page body): managed section is a Callout block
# ──────────────────────────────────────────────
def notion_list_children(notion_token: str, block_id: str) -> list[dict]:
    out: list[dict] = []
    cursor = None
    while True:
        params = {"page_size": 100}
        if cursor:
            params["start_cursor"] = cursor

        r = requests.get(
            f"{NOTION_API}/blocks/{block_id}/children",
            headers=notion_headers(notion_token),
            params=params,
            timeout=60,
        )
        r.raise_for_status()
        data = r.json()
        out.extend(data.get("results", []))
        if not data.get("has_more"):
            return out
        cursor = data.get("next_cursor")


def notion_delete_block(notion_token: str, block_id: str) -> None:
    r = requests.delete(
        f"{NOTION_API}/blocks/{block_id}",
        headers=notion_headers(notion_token),
        timeout=60,
    )
    r.raise_for_status()


def notion_append_children(notion_token: str, block_id: str, children: list[dict], after: str | None = None) -> dict:
    payload: dict = {"children": children}
    if after:
        payload["after"] = after

    r = requests.patch(
        f"{NOTION_API}/blocks/{block_id}/children",
        headers=notion_headers(notion_token),
        json=payload,
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


def rich_text_plain(rt_list: list[dict]) -> str:
    return "".join(x.get("plain_text", "") for x in rt_list or []).strip()


def find_sync_callout_block(notion_token: str, page_id: str) -> dict | None:
    for b in notion_list_children(notion_token, page_id):
        if b.get("type") == "callout":
            title = rich_text_plain((b.get("callout") or {}).get("rich_text", []))
            if title == SYNC_SECTION_TITLE:
                return b
    return None


def clear_block_children(notion_token: str, block_id: str) -> None:
    kids = notion_list_children(notion_token, block_id)
    for k in kids:
        notion_delete_block(notion_token, k["id"])
        time.sleep(0.12)


def append_children_in_chunks(notion_token: str, block_id: str, children: list[dict]) -> None:
    # keep margin under 100
    CHUNK = 90
    for i in range(0, len(children), CHUNK):
        notion_append_children(notion_token, block_id, children[i : i + CHUNK])
        time.sleep(0.2)


def ensure_sync_callout(notion_token: str, page_id: str) -> dict:
    callout = find_sync_callout_block(notion_token, page_id)
    if callout:
        return callout

    resp = notion_append_children(
        notion_token,
        page_id,
        [
            {
                "object": "block",
                "type": "callout",
                "callout": {
                    "rich_text": [{"type": "text", "text": {"content": SYNC_SECTION_TITLE}}],
                    "icon": {"type": "emoji", "emoji": SYNC_SECTION_ICON},
                    "color": "gray_background",
                    "children": [],
                },
            }
        ],
    )
    created = (resp.get("results") or [None])[0]
    if not created:
        raise RuntimeError("Failed to create synced callout block.")
    return created


# ──────────────────────────────────────────────
# LeetCode GraphQL
# ──────────────────────────────────────────────
def lc_post(query: str, variables: dict, session: str | None = None, csrf: str | None = None) -> dict:
    headers: dict = {"Content-Type": "application/json", "User-Agent": UA}
    cookies = None
    if session and csrf:
        headers["x-csrftoken"] = csrf
        headers["Referer"] = "https://leetcode.com/"
        cookies = {"LEETCODE_SESSION": session, "csrftoken": csrf}

    r = requests.post(
        LC_GRAPHQL,
        headers=headers,
        cookies=cookies,
        json={"query": query, "variables": variables},
        timeout=60,
    )
    r.raise_for_status()
    data = r.json()
    if "errors" in data:
        raise RuntimeError(f"LeetCode GraphQL errors: {data['errors']}")
    return data["data"]


_Q_RECENT_AC = """
query recentAcSubmissions($username: String!, $limit: Int!) {
  recentAcSubmissionList(username: $username, limit: $limit) {
    id
    title
    titleSlug
    timestamp
  }
}
"""

_Q_DETAIL = """
query questionData($titleSlug: String!) {
  question(titleSlug: $titleSlug) {
    categoryTitle
    title
    titleSlug
    questionFrontendId
    difficulty
    topicTags { name slug }
  }
}
"""

_Q_LIST = """
query problemsetQuestionList(
    $categorySlug: String, $limit: Int,
    $skip: Int, $filters: QuestionListFilterInput
) {
  problemsetQuestionList: questionList(
    categorySlug: $categorySlug
    limit: $limit
    skip: $skip
    filters: $filters
  ) {
    total: totalNum
    questions: data {
      frontendQuestionId: questionFrontendId
      difficulty
      status
      title
      titleSlug
      topicTags { name slug }
    }
  }
}
"""

_Q_QUESTION_CONTENT = """
query questionContent($titleSlug: String!) {
  question(titleSlug: $titleSlug) {
    content
  }
}
"""

# Best-effort ONLY (no cookies). If it requires auth, we return None and store question only.
_Q_SUBMISSION_DETAILS = """
query submissionDetails($submissionId: Int!) {
  submissionDetails(submissionId: $submissionId) {
    code
    runtimeDisplay
    memoryDisplay
    timestamp
    statusCode
    lang { name verboseName }
  }
}
"""


def fetch_recent_ac(username: str, limit: int) -> list[dict]:
    data = lc_post(_Q_RECENT_AC, {"username": username, "limit": limit})
    return data.get("recentAcSubmissionList") or []


def fetch_problem_detail(slug: str) -> dict:
    data = lc_post(_Q_DETAIL, {"titleSlug": slug})
    q = data.get("question")
    if not q:
        raise RuntimeError(f"No question returned for slug '{slug}'")
    return {
        "frontendQuestionId": int(q["questionFrontendId"]),
        "difficulty": q["difficulty"],
        "title": q["title"],
        "titleSlug": q["titleSlug"],
        "topicTags": q.get("topicTags") or [],
        "categoryTitle": q.get("categoryTitle"),
    }


def fetch_all_solved_slugs(session: str, csrf: str) -> list[str]:
    solved: list[str] = []
    skip = 0
    limit = 100
    total: int | None = None

    while total is None or skip < total:
        data = lc_post(
            _Q_LIST,
            {"categorySlug": "", "skip": skip, "limit": limit, "filters": {}},
            session=session,
            csrf=csrf,
        )
        lst = data["problemsetQuestionList"]
        total = int(lst["total"])
        for q in lst["questions"]:
            if q.get("status") == "ac" and q.get("titleSlug"):
                solved.append(q["titleSlug"])
        skip += limit
        time.sleep(0.25)

    return solved


def fetch_earliest_ac_date(slug: str, session: str, csrf: str) -> str | None:
    url = f"https://leetcode.com/api/submissions/{slug}"
    r = requests.get(
        url,
        headers={
            "User-Agent": UA,
            "Referer": f"https://leetcode.com/problems/{slug}/submissions/",
            "x-csrftoken": csrf,
        },
        cookies={"LEETCODE_SESSION": session, "csrftoken": csrf},
        timeout=60,
    )
    r.raise_for_status()
    subs = r.json().get("submissions_dump") or []

    accepted_ts = []
    for s in subs:
        if s.get("status_display") == "Accepted":
            ts = s.get("timestamp") or s.get("time")
            if ts is not None:
                try:
                    accepted_ts.append(int(ts))
                except Exception:
                    pass

    if not accepted_ts:
        return None
    return unix_to_iso(min(accepted_ts))


def fetch_question_statement_html(slug: str) -> str | None:
    data = lc_post(_Q_QUESTION_CONTENT, {"titleSlug": slug})
    q = data.get("question")
    return q.get("content") if q else None


def try_fetch_submission_details_public(submission_id: int) -> dict | None:
    try:
        data = lc_post(_Q_SUBMISSION_DETAILS, {"submissionId": int(submission_id)})
        subm = data.get("submissionDetails")
        # Require code to consider it useful
        if subm and (subm.get("code") or "").strip():
            return subm
        return None
    except Exception:
        return None


# ──────────────────────────────────────────────
# LeetCode HTML → Notion blocks (fixes the formatting issues you showed)
# ──────────────────────────────────────────────
def _norm_ws(s: str) -> str:
    # Keep newlines; normalize other whitespace to single spaces
    s = s.replace("\u00a0", " ")
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t\f\v]+", " ", s)
    return s


def _rt(
    text: str,
    *,
    bold: bool = False,
    italic: bool = False,
    code: bool = False,
    link: str | None = None,
) -> dict:
    text = _norm_ws(text)
    obj = {
        "type": "text",
        "text": {"content": text},
        "annotations": {
            "bold": bool(bold),
            "italic": bool(italic),
            "strikethrough": False,
            "underline": False,
            "code": bool(code),
            "color": "default",
        },
    }
    if link:
        obj["text"]["link"] = {"url": link}
    return obj


def _split_rich_text_item(item: dict, limit: int = 1800) -> list[dict]:
    content = (item.get("text") or {}).get("content") or ""
    if len(content) <= limit:
        return [item]
    parts = chunk_str(content, limit)
    out = []
    for p in parts:
        x = json.loads(json.dumps(item))  # deep copy
        x["text"]["content"] = p
        out.append(x)
    return out


def _merge_rich_text(parts: list[dict]) -> list[dict]:
    out: list[dict] = []
    for p in parts:
        if not (p.get("text") or {}).get("content"):
            continue
        if not out:
            out.append(p)
            continue
        prev = out[-1]
        if (
            prev.get("annotations") == p.get("annotations")
            and (prev.get("text") or {}).get("link") == (p.get("text") or {}).get("link")
        ):
            prev["text"]["content"] += p["text"]["content"]
        else:
            out.append(p)

    # Split overly long items
    final: list[dict] = []
    for it in out:
        final.extend(_split_rich_text_item(it))
    return final


def _inline_rich_text(node: Tag, *, bold=False, italic=False, code=False, link: str | None = None) -> list[dict]:
    parts: list[dict] = []
    for ch in node.children:
        if isinstance(ch, NavigableString):
            parts.append(_rt(str(ch), bold=bold, italic=italic, code=code, link=link))
        elif isinstance(ch, Tag):
            name = ch.name.lower()
            if name in ("strong", "b"):
                parts.extend(_inline_rich_text(ch, bold=True or bold, italic=italic, code=code, link=link))
            elif name in ("em", "i"):
                parts.extend(_inline_rich_text(ch, bold=bold, italic=True or italic, code=code, link=link))
            elif name == "code":
                parts.extend(_inline_rich_text(ch, bold=bold, italic=italic, code=True, link=link))
            elif name == "a":
                href = ch.get("href")
                parts.extend(_inline_rich_text(ch, bold=bold, italic=italic, code=code, link=href))
            elif name == "br":
                parts.append(_rt("\n", bold=bold, italic=italic, code=code, link=link))
            else:
                parts.extend(_inline_rich_text(ch, bold=bold, italic=italic, code=code, link=link))
    return _merge_rich_text(parts)


def _normalize_lc_html(html: str) -> BeautifulSoup:
    soup = BeautifulSoup(html or "", "html.parser")

    for t in soup(["script", "style"]):
        t.decompose()

    # Replace superscripts/subscripts with ^ / _ so Notion doesn't break them into new lines
    for sup in soup.find_all("sup"):
        sup.replace_with(f"^{sup.get_text(strip=True)}")
    for sub in soup.find_all("sub"):
        sub.replace_with(f"_{sub.get_text(strip=True)}")

    return soup


def lc_html_to_notion_blocks(html: str) -> list[dict]:
    """
    Converts LeetCode statement HTML into Notion blocks preserving inline code/bold/italic and exponents.
    """
    soup = _normalize_lc_html(html)
    blocks: list[dict] = []

    def add_paragraph(tag: Tag) -> None:
        rts = _inline_rich_text(tag)
        plain = "".join((x.get("text") or {}).get("content", "") for x in rts).strip()
        if not plain:
            return

        # Chunk if huge: convert to multiple paragraph blocks (loses some inline formatting across chunk boundaries,
        # but avoids Notion limits). Usually not needed for LC statements.
        if len(plain) > 1900:
            for part in chunk_str(plain, 1800):
                blocks.append({"object": "block", "type": "paragraph", "paragraph": {"rich_text": [_rt(part)]}})
            return

        blocks.append({"object": "block", "type": "paragraph", "paragraph": {"rich_text": rts}})

    def add_heading(tag: Tag, level: int) -> None:
        rts = _inline_rich_text(tag)
        text = "".join((x.get("text") or {}).get("content", "") for x in rts).strip()
        if not text:
            return
        t = "heading_2" if level == 2 else "heading_3"
        blocks.append({"object": "block", "type": t, t: {"rich_text": [_rt(text, bold=True)]}})

    def add_pre(tag: Tag) -> None:
        txt = tag.get_text("\n")
        txt = txt.replace("\u00a0", " ").strip()
        if not txt:
            return
        for part in chunk_str(txt, 1800):
            blocks.append(
                {
                    "object": "block",
                    "type": "code",
                    "code": {"rich_text": [_rt(part)], "language": "plain text"},
                }
            )

    def add_list(tag: Tag, ordered: bool) -> None:
        block_type = "numbered_list_item" if ordered else "bulleted_list_item"
        for li in tag.find_all("li", recursive=False):
            rts = _inline_rich_text(li)
            plain = "".join((x.get("text") or {}).get("content", "") for x in rts).strip()
            if not plain:
                continue
            blocks.append({"object": "block", "type": block_type, block_type: {"rich_text": rts}})

    def walk(node: Tag) -> None:
        for ch in node.children:
            if isinstance(ch, Tag):
                name = ch.name.lower()
                if name == "p":
                    add_paragraph(ch)
                elif name == "pre":
                    add_pre(ch)
                elif name == "ul":
                    add_list(ch, ordered=False)
                elif name == "ol":
                    add_list(ch, ordered=True)
                elif name in ("h1", "h2"):
                    add_heading(ch, level=2)
                elif name in ("h3", "h4"):
                    add_heading(ch, level=3)
                elif name == "hr":
                    blocks.append({"object": "block", "type": "divider", "divider": {}})
                else:
                    # descend until we find block-level tags
                    walk(ch)

            # safety
            if len(blocks) > 350:
                blocks.append(
                    {
                        "object": "block",
                        "type": "paragraph",
                        "paragraph": {"rich_text": [_rt("(Truncated: statement too long for sync limits.)")]},
                    }
                )
                return

    walk(soup)
    return blocks


# ──────────────────────────────────────────────
# Page body sync (direct, no toggle): overwrite callout children each time
# ──────────────────────────────────────────────
def build_synced_section_children(slug: str, statement_html: str | None, subm: dict | None) -> list[dict]:
    kids: list[dict] = []

    # Link
    kids.append({"object": "block", "type": "bookmark", "bookmark": {"url": f"https://leetcode.com/problems/{slug}/"}})

    # Statement header + content
    kids.append({"object": "block", "type": "heading_2", "heading_2": {"rich_text": [_rt("Statement", bold=True)]}})

    if statement_html:
        kids.extend(lc_html_to_notion_blocks(statement_html))
    else:
        kids.append(
            {"object": "block", "type": "paragraph", "paragraph": {"rich_text": [_rt("Statement not available.")]}}
        )

    # Latest Accepted (public only)
    if subm and (subm.get("code") or "").strip():
        kids.append(
            {
                "object": "block",
                "type": "heading_2",
                "heading_2": {"rich_text": [_rt("Latest Accepted Solution (public)", bold=True)]},
            }
        )
        lang = (subm.get("lang") or {}).get("verboseName") or (subm.get("lang") or {}).get("name") or "unknown"
        meta = f"lang={lang} | runtime={subm.get('runtimeDisplay')} | memory={subm.get('memoryDisplay')}"
        kids.append({"object": "block", "type": "paragraph", "paragraph": {"rich_text": [_rt(meta)]}})

        code = subm.get("code") or ""
        for part in chunk_str(code, 1800)[:80]:
            kids.append({"object": "block", "type": "code", "code": {"rich_text": [_rt(part)], "language": "plain text"}})

    return kids


def overwrite_synced_section(
    notion_token: str,
    page_id: str,
    slug: str,
    statement_html: str | None,
    subm: dict | None,
) -> None:
    callout = ensure_sync_callout(notion_token, page_id)
    callout_id = callout["id"]

    clear_block_children(notion_token, callout_id)

    kids = build_synced_section_children(slug, statement_html, subm)
    append_children_in_chunks(notion_token, callout_id, kids)


# ──────────────────────────────────────────────
# Build Notion properties
# ──────────────────────────────────────────────
def build_props(problem: dict, nc_map: dict[str, list[str]], tuf_map: dict[str, list[str]], completed_date: str | None, ac_submission_id: int | None) -> dict:
    slug = problem["titleSlug"]

    topics = [{"name": t["name"]} for t in (problem.get("topicTags") or []) if t.get("name")]
    nc_tags = [{"name": c} for c in nc_map.get(slug, [])]
    tuf_tags = [{"name": c} for c in tuf_map.get(slug, [])]

    props: dict = {
        P_NAME: {"title": [{"text": {"content": problem["title"]}}]},
        P_ID: {"number": int(problem["frontendQuestionId"])},
        P_DIFFICULTY: {"select": {"name": problem["difficulty"]}},
        P_SLUG: {"rich_text": [{"text": {"content": slug}}]},
        P_URL: {"url": f"https://leetcode.com/problems/{slug}/"},
        P_TOPICS: {"multi_select": topics},
        P_NC_TAGS: {"multi_select": nc_tags},
        P_TUF_TAGS: {"multi_select": tuf_tags},
    }

    if problem.get("categoryTitle"):
        props[P_CATEGORY] = {"select": {"name": problem["categoryTitle"]}}

    if completed_date:
        props[P_COMPLETED] = {"date": {"start": completed_date}}
        props[P_STATUS] = {"status": {"name": "Done"}}

    # only set if the property exists in your DB schema
    if ac_submission_id is not None and P_LAST_AC_ID in _TRACKER_PROP_NAMES:
        props[P_LAST_AC_ID] = {"number": int(ac_submission_id)}

    return props


# ──────────────────────────────────────────────
# Stats (pivot DB)
# ──────────────────────────────────────────────
def compute_stats(notion_index: dict[str, dict]) -> dict[str, int]:
    today = date.today()
    this_week_start = today - timedelta(days=today.weekday())
    this_month_start = today.replace(day=1)

    solved_dates: set[date] = set()
    week_count = 0
    month_count = 0

    for info in notion_index.values():
        raw = info.get("completed")
        if not raw:
            continue
        try:
            d = date.fromisoformat(raw[:10])
        except ValueError:
            continue
        solved_dates.add(d)
        if d >= this_week_start:
            week_count += 1
        if d >= this_month_start:
            month_count += 1

    total_solved = len(notion_index)

    current_streak = 0
    anchor = today if today in solved_dates else today - timedelta(days=1)
    if anchor in solved_dates:
        cursor = anchor
        while cursor in solved_dates:
            current_streak += 1
            cursor -= timedelta(days=1)

    longest_streak = 0
    if solved_dates:
        sorted_dates = sorted(solved_dates)
        run = 1
        for i in range(1, len(sorted_dates)):
            if sorted_dates[i] - sorted_dates[i - 1] == timedelta(days=1):
                run += 1
            else:
                longest_streak = max(longest_streak, run)
                run = 1
        longest_streak = max(longest_streak, run)

    return {
        SP_CURRENT_STREAK: current_streak,
        SP_LONGEST_STREAK: longest_streak,
        SP_TOTAL_SOLVED: total_solved,
        SP_THIS_WEEK: week_count,
        SP_THIS_MONTH: month_count,
    }


def push_stats(notion_token: str, stats_db_id: str, stats: dict[str, int]) -> None:
    if not stats_db_id:
        return

    if not _STATS_PROP_NAMES:
        # if init_prop_names wasn't called with stats DB, this remains empty
        pass

    today_iso = date.today().isoformat()

    pages = notion_query_all(notion_token, stats_db_id)
    page_id = None
    for p in pages:
        if _title_value(p, SP_NAME) == STATS_ROW_NAME:
            page_id = p["id"]
            break

    if not page_id:
        print(f"[stats] ERROR: Could not find Stats DB row with Name='{STATS_ROW_NAME}'.", file=sys.stderr)
        return

    props = {
        SP_CURRENT_STREAK: {"number": stats[SP_CURRENT_STREAK]},
        SP_LONGEST_STREAK: {"number": stats[SP_LONGEST_STREAK]},
        SP_TOTAL_SOLVED: {"number": stats[SP_TOTAL_SOLVED]},
        SP_THIS_WEEK: {"number": stats[SP_THIS_WEEK]},
        SP_THIS_MONTH: {"number": stats[SP_THIS_MONTH]},
        SP_UPDATED: {"date": {"start": today_iso}},
    }

    r = requests.patch(
        f"{NOTION_API}/pages/{page_id}",
        headers=notion_headers(notion_token),
        json={"properties": props},
        timeout=60,
    )
    if not r.ok:
        print(f"[stats] HTTP {r.status_code}: {r.text}", file=sys.stderr)
        r.raise_for_status()

    print("[stats] Pivot stats updated.")


# ──────────────────────────────────────────────
# Upsert + body sync
# ──────────────────────────────────────────────
def upsert(
    slug: str,
    completed_date: str | None,
    accepted_submission_id: int | None,
    nc_map: dict[str, list[str]],
    tuf_map: dict[str, list[str]],
    notion_token: str,
    notion_db: str,
    notion_index: dict[str, dict],
    force_body_sync: bool = False,
) -> None:
    try:
        problem = fetch_problem_detail(slug)
    except Exception as exc:
        print(f"[sync] SKIP {slug}: could not fetch detail – {exc}")
        return

    existing = notion_index.get(slug)
    page_id = existing["page_id"] if existing else None
    existing_completed = existing.get("completed") if existing else None
    existing_last_ac = existing.get("last_ac_id") if existing else None

    props = build_props(problem, nc_map, tuf_map, completed_date, accepted_submission_id)

    # Never overwrite an existing completed date
    if existing_completed and completed_date:
        props.pop(P_COMPLETED, None)
        props.pop(P_STATUS, None)

    result = notion_upsert(notion_token, notion_db, page_id, props)

    # Update in-memory index for stats + AC-id comparisons
    if slug not in notion_index:
        notion_index[slug] = {"page_id": result["id"], "completed": None, "last_ac_id": None}
    notion_index[slug]["page_id"] = result["id"]
    if completed_date and not notion_index[slug].get("completed"):
        notion_index[slug]["completed"] = completed_date
    if accepted_submission_id is not None:
        notion_index[slug]["last_ac_id"] = accepted_submission_id

    # Decide whether to overwrite synced section
    # - backfill forces it for every solved problem
    # - sync runs: update when AC submission id changed (if we can track it), otherwise still update
    #   for recently-seen slugs (best effort)
    should_sync_body = (
        force_body_sync
        or (page_id is None)
        or (completed_date is not None and not existing_completed)
        or (accepted_submission_id is not None and (existing_last_ac is None or accepted_submission_id != existing_last_ac))
    )

    if should_sync_body:
        try:
            statement_html = fetch_question_statement_html(slug)

            # IMPORTANT: per your NOTE, we do NOT send cookies. This is best-effort.
            subm = try_fetch_submission_details_public(accepted_submission_id) if accepted_submission_id else None

            overwrite_synced_section(notion_token, result["id"], slug, statement_html, subm)
        except Exception as e:
            print(f"[page] WARNING: failed to overwrite synced section for {slug}: {e}", file=sys.stderr)

    action = "updated" if page_id else "created"
    print(f"[sync] {action}: {problem['title']} ({slug})")


# ──────────────────────────────────────────────
# Commands
# ──────────────────────────────────────────────
def cmd_sync(args: argparse.Namespace) -> None:
    notion_token = mustenv("NOTION_TOKEN")
    notion_db = mustenv("NOTION_DATABASE_ID").replace("-", "")
    stats_db_id = optenv("NOTION_STATS_DATABASE_ID").replace("-", "") or None
    username = mustenv("LEETCODE_USERNAME")

    init_prop_names(notion_token, notion_db, stats_db_id)

    nc_map = load_json_map(NC_MAP_FILE)
    tuf_map = load_json_map(TUF_MAP_FILE)

    print("[sync] Querying Notion tracker index …")
    pages = notion_query_all(notion_token, notion_db)
    notion_index = notion_index_pages(pages)
    print(f"[sync] {len(notion_index)} existing pages indexed.")

    print(f"[sync] Fetching last {args.recent_limit} accepted submissions for '{username}' …")
    recent = fetch_recent_ac(username, args.recent_limit)
    print(f"[sync] {len(recent)} recent AC submissions found.")

    # Deduplicate slugs in the same run
    seen = set()
    for item in recent:
        slug = item["titleSlug"]
        if slug in seen:
            continue
        seen.add(slug)

        completed = unix_to_iso(item["timestamp"])
        sub_id = None
        try:
            sub_id = int(item["id"])
        except Exception:
            sub_id = None

        upsert(slug, completed, sub_id, nc_map, tuf_map, notion_token, notion_db, notion_index, force_body_sync=False)
        time.sleep(0.25)

    if stats_db_id:
        print("[sync] Computing stats …")
        try:
            push_stats(notion_token, stats_db_id, compute_stats(notion_index))
        except Exception as e:
            print(f"[stats] WARNING: stats update failed (tracker sync succeeded): {e}", file=sys.stderr)

    print("[sync] Done.")


def cmd_backfill(args: argparse.Namespace) -> None:
    notion_token = mustenv("NOTION_TOKEN")
    notion_db = mustenv("NOTION_DATABASE_ID").replace("-", "")
    stats_db_id = optenv("NOTION_STATS_DATABASE_ID").replace("-", "") or None

    session = mustenv("LEETCODE_SESSION")
    csrf = mustenv("LEETCODE_CSRF")

    init_prop_names(notion_token, notion_db, stats_db_id)

    nc_map = load_json_map(NC_MAP_FILE)
    tuf_map = load_json_map(TUF_MAP_FILE)

    print("[backfill] Querying Notion tracker index …")
    pages = notion_query_all(notion_token, notion_db)
    notion_index = notion_index_pages(pages)
    print(f"[backfill] {len(notion_index)} existing pages indexed.")

    print("[backfill] Fetching all solved slugs from LeetCode (authenticated) …")
    solved_slugs = fetch_all_solved_slugs(session, csrf)
    print(f"[backfill] {len(solved_slugs)} solved problems found.")

    for slug in solved_slugs:
        existing = notion_index.get(slug)
        existing_completed = existing.get("completed") if existing else None

        if not args.create_missing and existing is None:
            continue

        completed: str | None = None
        if args.fill_dates and not existing_completed:
            try:
                completed = fetch_earliest_ac_date(slug, session, csrf)
            except Exception as exc:
                print(f"[backfill] WARNING: could not get date for {slug}: {exc}")
            time.sleep(0.35)

        # backfill has no submission id in this flow -> question only, but still hydrates page bodies (force_body_sync=True)
        upsert(slug, completed, None, nc_map, tuf_map, notion_token, notion_db, notion_index, force_body_sync=True)
        time.sleep(0.25)

    if stats_db_id:
        print("[backfill] Computing stats …")
        try:
            push_stats(notion_token, stats_db_id, compute_stats(notion_index))
        except Exception as e:
            print(f"[stats] WARNING: stats update failed (backfill succeeded): {e}", file=sys.stderr)

    print("[backfill] Done.")


def cmd_diagnose(args: argparse.Namespace) -> None:
    notion_token = mustenv("NOTION_TOKEN")
    tracker_db = mustenv("NOTION_DATABASE_ID").replace("-", "")
    stats_db = optenv("NOTION_STATS_DATABASE_ID").replace("-", "") or None

    # Show data source ids
    tracker_ds = notion_resolve_data_source_id(notion_token, tracker_db)
    stats_ds = notion_resolve_data_source_id(notion_token, stats_db) if stats_db else None

    print("\n[diagnose] IDs")
    print(f"  tracker input id : {tracker_db}")
    print(f"  tracker ds id    : {tracker_ds}")
    if stats_db:
        print(f"  stats input id   : {stats_db}")
        print(f"  stats ds id      : {stats_ds}")

    # Show tracker properties from data source schema
    t_schema = notion_get_data_source_schema(notion_token, tracker_db)
    print("\n[diagnose] TRACKER data source properties")
    for name, prop in sorted((t_schema.get("properties") or {}).items()):
        print(f"  {name:<30} {prop.get('type')}")

    if stats_db:
        s_schema = notion_get_data_source_schema(notion_token, stats_db)
        print("\n[diagnose] STATS data source properties")
        for name, prop in sorted((s_schema.get("properties") or {}).items()):
            print(f"  {name:<30} {prop.get('type')}")

    print("\n[diagnose] Done.")


def main() -> None:
    ap = argparse.ArgumentParser(description="LeetCode → Notion tracker")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_sync = sub.add_parser("sync", help="Cookie-free incremental sync (recent ACs)")
    p_sync.add_argument("--recent-limit", type=int, default=20)

    p_back = sub.add_parser("backfill", help="Manual history backfill (needs LeetCode cookies)")
    p_back.add_argument("--create-missing", action="store_true", default=False)
    p_back.add_argument("--fill-dates", action="store_true", default=False)

    sub.add_parser("diagnose", help="Print Notion schema info")

    args = ap.parse_args()

    if args.cmd == "sync":
        cmd_sync(args)
    elif args.cmd == "backfill":
        cmd_backfill(args)
    elif args.cmd == "diagnose":
        cmd_diagnose(args)


if __name__ == "__main__":
    main()