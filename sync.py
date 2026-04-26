"""
sync.py  –  LeetCode  ➜  Notion tracker  (tags + pivot stats + page body sync)

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

import requests
from bs4 import BeautifulSoup
from dateutil import tz

# ──────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────
# Use a modern Notion version (matches current docs examples for page-content workflows)
NOTION_VERSION = "2026-03-11"
NOTION_API = "https://api.notion.com/v1"
LC_GRAPHQL = "https://leetcode.com/graphql"

# Tag maps in your repo
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

# ── Stats pivot DB property names (must match your Stats DB) ──
SP_NAME = "Name"  # Title
SP_CURRENT_STREAK = "Current Streak"
SP_LONGEST_STREAK = "Longest Streak"
SP_TOTAL_SOLVED = "Total Solved"
SP_THIS_WEEK = "Solved This Week"
SP_THIS_MONTH = "Solved This Month"
SP_UPDATED = "Last Updated"

STATS_ROW_NAME = "Stats"  # the single row title in the pivot Stats DB

# ── Page body synced section ──
SYNC_TOGGLE_TITLE = "🔁 LeetCode Sync"

# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
_DS_CACHE: dict[str, str] = {}

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

def mustenv(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        print(f"[sync] ERROR: missing env var '{name}'", file=sys.stderr)
        sys.exit(1)
    return v


def optenv(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


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


def load_nc_map() -> dict[str, list[str]]:
    return load_json_map(NC_MAP_FILE)


def load_tuf_map() -> dict[str, list[str]]:
    return load_json_map(TUF_MAP_FILE)


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


# ──────────────────────────────────────────────
# Notion client
# ──────────────────────────────────────────────
def notion_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


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
    """Returns {slug: {page_id, completed}}"""
    index: dict[str, dict] = {}
    for p in pages:
        slug = _rich_text_value(p, P_SLUG)
        if slug:
            index[slug] = {
                "page_id": p["id"],
                "completed": _date_value(p, P_COMPLETED),
            }
    return index


def notion_upsert(token: str, db_id: str, page_id: str | None, props: dict) -> dict:
    headers = notion_headers(token)

    if page_id is None:
        ds_id = notion_resolve_data_source_id(token, db_id)
        payload = {"parent": {"data_source_id": ds_id}, "properties": props}
        r = requests.post(f"{NOTION_API}/pages", headers=headers, json=payload, timeout=60)
    else:
        r = requests.patch(f"{NOTION_API}/pages/{page_id}", headers=headers, json={"properties": props}, timeout=60)

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
# Notion blocks (page body)
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


def notion_append_children(
    notion_token: str,
    block_id: str,
    children: list[dict],
    after: str | None = None,
) -> dict:
    """
    Append block children. Limit: 100 blocks/request. <!--citation:1-->
    """
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


def find_sync_toggle_block(notion_token: str, page_id: str) -> dict | None:
    for b in notion_list_children(notion_token, page_id):
        if b.get("type") == "toggle":
            title = rich_text_plain((b.get("toggle") or {}).get("rich_text", []))
            if title == SYNC_TOGGLE_TITLE:
                return b
    return None


def clear_block_children(notion_token: str, block_id: str) -> None:
    kids = notion_list_children(notion_token, block_id)
    for k in kids:
        # Delete can produce 409 if hammered; small delay is safer.
        notion_delete_block(notion_token, k["id"])
        time.sleep(0.12)


def append_children_in_chunks(notion_token: str, block_id: str, children: list[dict]) -> None:
    CHUNK = 90
    for i in range(0, len(children), CHUNK):
        notion_append_children(notion_token, block_id, children[i : i + CHUNK])
        time.sleep(0.2)


# ──────────────────────────────────────────────
# LeetCode GraphQL
# ──────────────────────────────────────────────
def lc_post(
    query: str,
    variables: dict,
    session: str | None = None,
    csrf: str | None = None,
) -> dict:
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

# Best-effort: We call this WITHOUT cookies.
# If LeetCode requires auth for code, it will error and we will store only the question.
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
    """
    IMPORTANT: Called WITHOUT cookies.
    If LeetCode requires auth for code, this returns None (and we store question only).
    """
    try:
        data = lc_post(_Q_SUBMISSION_DETAILS, {"submissionId": int(submission_id)})
        return data.get("submissionDetails")
    except Exception:
        return None


# ──────────────────────────────────────────────
# Page-body content: statement + (optional) latest accepted code if public
# ──────────────────────────────────────────────
def statement_html_to_text(html: str) -> str:
    soup = BeautifulSoup(html or "", "html.parser")
    for t in soup(["script", "style"]):
        t.decompose()

    text = soup.get_text("\n")
    text = text.replace("\u00a0", " ")
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text


def build_sync_toggle_children(slug: str, statement_html: str | None, subm: dict | None) -> list[dict]:
    children: list[dict] = []

    # Problem link (bookmark)
    children.append(
        {
            "object": "block",
            "type": "bookmark",
            "bookmark": {"url": f"https://leetcode.com/problems/{slug}/"},
        }
    )

    # Statement as code blocks (keeps block count low and preserves some formatting)
    children.append(
        {
            "object": "block",
            "type": "heading_3",
            "heading_3": {"rich_text": [{"type": "text", "text": {"content": "Statement"}}]},
        }
    )

    if statement_html:
        text = statement_html_to_text(statement_html)
        # chunk into multiple code blocks if needed
        for part in chunk_str(text, 1800)[:60]:
            children.append(
                {
                    "object": "block",
                    "type": "code",
                    "code": {
                        "rich_text": [{"type": "text", "text": {"content": part}}],
                        "language": "plain text",
                    },
                }
            )
    else:
        children.append(
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": "Statement not available via public API (premium/blocked/temporary failure)."
                            },
                        }
                    ]
                },
            }
        )

    # Latest accepted solution code: only if public and code exists
    if subm and (subm.get("code") or "").strip():
        children.append(
            {
                "object": "block",
                "type": "heading_3",
                "heading_3": {
                    "rich_text": [
                        {"type": "text", "text": {"content": "Latest Accepted Solution (public)"}}
                    ]
                },
            }
        )

        lang = (subm.get("lang") or {}).get("verboseName") or (subm.get("lang") or {}).get("name") or "unknown"
        meta = f"lang={lang} | runtime={subm.get('runtimeDisplay')} | memory={subm.get('memoryDisplay')}"
        children.append(
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": meta}}]},
            }
        )

        code = subm.get("code") or ""
        for part in chunk_str(code, 1800)[:80]:
            children.append(
                {
                    "object": "block",
                    "type": "code",
                    "code": {
                        "rich_text": [{"type": "text", "text": {"content": part}}],
                        "language": "plain text",
                    },
                }
            )

    return children


def sync_page_body_overwrite(
    notion_token: str,
    page_id: str,
    slug: str,
    statement_html: str | None,
    subm: dict | None,
) -> None:
    """
    Overwrites the children of the SYNC_TOGGLE_TITLE toggle.
    If the toggle doesn't exist, creates it (at the end of the page).
    """
    toggle = find_sync_toggle_block(notion_token, page_id)

    if toggle is None:
        # Create toggle block with children in one append call (will be appended at end).
        toggle_block = {
            "object": "block",
            "type": "toggle",
            "toggle": {
                "rich_text": [{"type": "text", "text": {"content": SYNC_TOGGLE_TITLE}}],
                "children": build_sync_toggle_children(slug, statement_html, subm),
            },
        }
        notion_append_children(notion_token, page_id, [toggle_block])
        return

    toggle_id = toggle["id"]

    # Clear existing children and repopulate (keeps toggle position stable)
    clear_block_children(notion_token, toggle_id)

    new_children = build_sync_toggle_children(slug, statement_html, subm)
    append_children_in_chunks(notion_token, toggle_id, new_children)


# ──────────────────────────────────────────────
# Notion property builder (tracker DB)
# ──────────────────────────────────────────────
def build_props(problem: dict, nc_map: dict[str, list[str]], tuf_map: dict[str, list[str]], completed_date: str | None) -> dict:
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

    return props


# ──────────────────────────────────────────────
# Core upsert
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
    force_body_sync: bool = False,   # ← NEW
) -> None:
    try:
        problem = fetch_problem_detail(slug)
    except Exception as exc:
        print(f"[sync] SKIP {slug}: could not fetch detail – {exc}")
        return

    existing = notion_index.get(slug)
    page_id = existing["page_id"] if existing else None
    existing_completed = existing.get("completed") if existing else None

    props = build_props(problem, nc_map, tuf_map, completed_date)

    # Never overwrite an existing completed date
    if existing_completed and completed_date:
        props.pop(P_COMPLETED, None)
        props.pop(P_STATUS, None)

    # Decide whether to sync page body now.
    # To keep API usage sane, we do it when:
    # - page is new, OR
    # - completed date is newly filled in this run
    # (You can always force re-sync by deleting the toggle block manually.)
    should_sync_body = (
        force_body_sync
        or (page_id is None)
        or (completed_date is not None and not existing_completed)
    )
    statement_html = None
    subm_details = None

    # If creating a new page and syncing body, include toggle children at creation time.
    children_for_create = None
    if page_id is None and should_sync_body:
        try:
            statement_html = fetch_question_statement_html(slug)
        except Exception:
            statement_html = None

        if accepted_submission_id is not None:
            subm_details = try_fetch_submission_details_public(accepted_submission_id)

        toggle_block = {
            "object": "block",
            "type": "toggle",
            "toggle": {
                "rich_text": [{"type": "text", "text": {"content": SYNC_TOGGLE_TITLE}}],
                "children": build_sync_toggle_children(slug, statement_html, subm_details),
            },
        }
        children_for_create = [toggle_block]

    result = notion_upsert(notion_token, notion_db, page_id, props, children=children_for_create)

    # Update local index so stats reflect current run
    if slug not in notion_index:
        notion_index[slug] = {"page_id": result["id"], "completed": None}
    notion_index[slug]["page_id"] = result["id"]
    if completed_date and not notion_index[slug].get("completed"):
        notion_index[slug]["completed"] = completed_date

    # If it's an existing page and we decided to sync body, overwrite the synced toggle section now.
    if page_id is not None and should_sync_body:
        try:
            if statement_html is None:
                statement_html = fetch_question_statement_html(slug)
        except Exception:
            statement_html = None

        if accepted_submission_id is not None and subm_details is None:
            subm_details = try_fetch_submission_details_public(accepted_submission_id)

        try:
            sync_page_body_overwrite(notion_token, result["id"], slug, statement_html, subm_details)
        except Exception as e:
            print(f"[page] WARNING: failed to update page body for {slug}: {e}", file=sys.stderr)

    action = "updated" if page_id else "created"
    print(f"[sync] {action}: {problem['title']} ({slug})")


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
# Commands
# ──────────────────────────────────────────────
def cmd_sync(args: argparse.Namespace) -> None:
    notion_token = mustenv("NOTION_TOKEN")
    notion_db = mustenv("NOTION_DATABASE_ID").replace("-", "")
    stats_db_id = optenv("NOTION_STATS_DATABASE_ID").replace("-", "")
    username = mustenv("LEETCODE_USERNAME")

    nc_map = load_nc_map()
    tuf_map = load_tuf_map()

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

        upsert(slug, completed, sub_id, nc_map, tuf_map, notion_token, notion_db, notion_index)
        time.sleep(0.25)

    if stats_db_id:
        print("[sync] Computing stats …")
        try:
            push_stats(notion_token, stats_db_id, compute_stats(notion_index))
        except Exception as e:
            print(f"[stats] WARNING: stats update failed (tracker sync succeeded): {e}", file=sys.stderr)
    else:
        print("[sync] NOTION_STATS_DATABASE_ID not set – skipping stats push.")

    print("[sync] Done.")


def cmd_backfill(args: argparse.Namespace) -> None:
    notion_token = mustenv("NOTION_TOKEN")
    notion_db = mustenv("NOTION_DATABASE_ID").replace("-", "")
    stats_db_id = optenv("NOTION_STATS_DATABASE_ID").replace("-", "")

    session = mustenv("LEETCODE_SESSION")
    csrf = mustenv("LEETCODE_CSRF")

    nc_map = load_nc_map()
    tuf_map = load_tuf_map()

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

        # If --create-missing not passed, skip entirely missing pages
        if not args.create_missing and existing is None:
            continue

        completed: str | None = None
        if args.fill_dates:
            if not existing_completed:
                try:
                    completed = fetch_earliest_ac_date(slug, session, csrf)
                except Exception as exc:
                    print(f"[backfill] WARNING: could not get date for {slug}: {exc}")
                time.sleep(0.35)

        # No submission id here -> body sync will store statement only (no code)
        upsert(slug, completed, None, nc_map, tuf_map, notion_token, notion_db, notion_index, force_body_sync=True)
        time.sleep(0.2)

    if stats_db_id:
        print("[backfill] Computing stats …")
        try:
            push_stats(notion_token, stats_db_id, compute_stats(notion_index))
        except Exception as e:
            print(f"[stats] WARNING: stats update failed (backfill succeeded): {e}", file=sys.stderr)
    else:
        print("[backfill] NOTION_STATS_DATABASE_ID not set – skipping stats push.")

    print("[backfill] Done.")


def cmd_diagnose(args: argparse.Namespace) -> None:
    notion_token = mustenv("NOTION_TOKEN")

    def print_db_schema(label: str, db_id: str) -> None:
        if not db_id:
            print(f"\n[diagnose] {label}: ID not set, skipping.")
            return
        r = requests.get(
            f"{NOTION_API}/databases/{db_id}",
            headers=notion_headers(notion_token),
            timeout=60,
        )
        r.raise_for_status()
        data = r.json()
        title = data["title"][0]["plain_text"] if data.get("title") else "(unknown)"
        print(f"\n[diagnose] ── {label} ──")
        print(f"           DB title : {title}")
        print(f"           DB id    : {db_id}")
        print(f"           {'Property name':<35} {'Type'}")
        print(f"           {'-'*35} {'-'*20}")
        for name, prop in sorted(data["properties"].items()):
            print(f"           {name:<35} {prop['type']}")

    tracker_db = mustenv("NOTION_DATABASE_ID").replace("-", "")
    stats_db = optenv("NOTION_STATS_DATABASE_ID").replace("-", "")

    print_db_schema("TRACKER DB  (NOTION_DATABASE_ID)", tracker_db)
    print_db_schema("STATS DB    (NOTION_STATS_DATABASE_ID)", stats_db)

    print("\n[diagnose] ── Expected property name constants in sync.py ──")
    constants = {
        "Tracker DB": {
            "P_NAME": P_NAME,
            "P_ID": P_ID,
            "P_DIFFICULTY": P_DIFFICULTY,
            "P_STATUS": P_STATUS,
            "P_COMPLETED": P_COMPLETED,
            "P_CATEGORY": P_CATEGORY,
            "P_TOPICS": P_TOPICS,
            "P_NC_TAGS": P_NC_TAGS,
            "P_TUF_TAGS": P_TUF_TAGS,
            "P_SLUG": P_SLUG,
            "P_URL": P_URL,
        },
        "Stats DB": {
            "SP_NAME": SP_NAME,
            "SP_CURRENT_STREAK": SP_CURRENT_STREAK,
            "SP_LONGEST_STREAK": SP_LONGEST_STREAK,
            "SP_TOTAL_SOLVED": SP_TOTAL_SOLVED,
            "SP_THIS_WEEK": SP_THIS_WEEK,
            "SP_THIS_MONTH": SP_THIS_MONTH,
            "SP_UPDATED": SP_UPDATED,
            "STATS_ROW_NAME": STATS_ROW_NAME,
        },
        "Files": {
            "NC_MAP_FILE": NC_MAP_FILE,
            "TUF_MAP_FILE": TUF_MAP_FILE,
        },
        "Page Body": {
            "SYNC_TOGGLE_TITLE": SYNC_TOGGLE_TITLE,
        },
    }

    for group, props in constants.items():
        print(f"\n           {group}:")
        for k, v in props.items():
            print(f"             {k:<20} = '{v}'")

    print("\n[diagnose] Done.")


# ──────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser(description="LeetCode → Notion tracker")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_sync = sub.add_parser("sync", help="Cookie-free incremental sync (recent ACs)")
    p_sync.add_argument("--recent-limit", type=int, default=20)

    p_back = sub.add_parser("backfill", help="Manual history backfill (needs LeetCode cookies)")
    p_back.add_argument("--create-missing", action="store_true", default=False)
    p_back.add_argument("--fill-dates", action="store_true", default=False)

    sub.add_parser("diagnose", help="Print Notion DB schemas + expected constants")

    args = ap.parse_args()

    if args.cmd == "sync":
        cmd_sync(args)
    elif args.cmd == "backfill":
        cmd_backfill(args)
    elif args.cmd == "diagnose":
        cmd_diagnose(args)


if __name__ == "__main__":
    main()