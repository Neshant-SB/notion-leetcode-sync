"""
sync.py  –  LeetCode  ➜  Notion tracker

Commands
--------
python sync.py sync      --recent-limit 20
    Cookie-free. Pulls recent Accepted submissions, upserts Notion rows.

python sync.py backfill  --create-missing --fill-dates
    Cookie-required (one-time). Discovers full solve history, sets
    earliest-AC Completed date for every solved problem.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from dateutil import tz

# ──────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────
NOTION_VERSION  = "2022-06-28"
NOTION_API      = "https://api.notion.com/v1"
LC_GRAPHQL      = "https://leetcode.com/graphql"
NC_MAP_FILE     = "neetcode_all_map.json"

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

# ── Notion property names (must match your DB exactly) ──
P_NAME       = "Name"
P_ID         = "Problem ID"
P_DIFFICULTY = "Difficulty"
P_STATUS     = "Status"
P_COMPLETED  = "Completed date"
P_CATEGORY   = "Category"
P_TOPICS     = "Topics"
P_TAGS       = "Tags"
P_SLUG       = "Slug"
P_URL        = "LeetCode URL"


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
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


def load_nc_map() -> dict[str, list[str]]:
    p = Path(NC_MAP_FILE)
    if not p.exists():
        print(f"[sync] WARNING: {NC_MAP_FILE} not found – Tags will be empty.")
        return {}
    with p.open(encoding="utf-8") as fh:
        return json.load(fh)


# ──────────────────────────────────────────────
# Notion client
# ──────────────────────────────────────────────
def notion_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def notion_query_all(token: str, db_id: str) -> list[dict]:
    url     = f"{NOTION_API}/databases/{db_id}/query"
    headers = notion_headers(token)
    pages   = []
    payload: dict = {"page_size": 100}
    while True:
        r = requests.post(url, headers=headers, json=payload, timeout=60)
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
                "page_id":   p["id"],
                "completed": _date_value(p, P_COMPLETED),
            }
    return index


def notion_upsert(token: str, db_id: str, page_id: str | None, props: dict) -> dict:
    headers = notion_headers(token)
    if page_id is None:
        r = requests.post(
            f"{NOTION_API}/pages",
            headers=headers,
            json={"parent": {"database_id": db_id}, "properties": props},
            timeout=60,
        )
    else:
        r = requests.patch(
            f"{NOTION_API}/pages/{page_id}",
            headers=headers,
            json={"properties": props},
            timeout=60,
        )
    r.raise_for_status()
    return r.json()


# ──────────────────────────────────────────────
# LeetCode GraphQL
# ──────────────────────────────────────────────
def lc_post(query: str, variables: dict,
            session: str | None = None,
            csrf: str | None = None) -> dict:
    headers: dict = {"Content-Type": "application/json", "User-Agent": UA}
    cookies = None
    if session and csrf:
        headers["x-csrftoken"] = csrf
        headers["Referer"]     = "https://leetcode.com/"
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


def fetch_recent_ac(username: str, limit: int) -> list[dict]:
    data = lc_post(_Q_RECENT_AC, {"username": username, "limit": limit})
    return data.get("recentAcSubmissionList") or []


def fetch_problem_detail(slug: str) -> dict:
    data = lc_post(_Q_DETAIL, {"titleSlug": slug})
    q    = data.get("question")
    if not q:
        raise RuntimeError(f"No question returned for slug '{slug}'")
    return {
        "frontendQuestionId": int(q["questionFrontendId"]),
        "difficulty":         q["difficulty"],
        "title":              q["title"],
        "titleSlug":          q["titleSlug"],
        "topicTags":          q.get("topicTags") or [],
        "categoryTitle":      q.get("categoryTitle"),
    }


def fetch_all_solved_slugs(session: str, csrf: str) -> list[str]:
    """
    Authenticated scan of the full problem list.
    Returns slugs where status == 'ac' (Accepted / solved by the logged-in user).
    """
    solved: list[str] = []
    skip   = 0
    limit  = 100
    total: int | None = None

    while total is None or skip < total:
        data  = lc_post(_Q_LIST, {
            "categorySlug": "",
            "skip":   skip,
            "limit":  limit,
            "filters": {},
        }, session=session, csrf=csrf)
        lst   = data["problemsetQuestionList"]
        total = int(lst["total"])
        for q in lst["questions"]:
            if q.get("status") == "ac" and q.get("titleSlug"):
                solved.append(q["titleSlug"])
        skip += limit
        time.sleep(0.25)

    return solved


def fetch_earliest_ac_date(slug: str, session: str, csrf: str) -> str | None:
    """
    Best-effort: hit /api/submissions/<slug> and find the earliest
    Accepted submission timestamp.
    """
    url = f"https://leetcode.com/api/submissions/{slug}"
    r   = requests.get(
        url,
        headers={
            "User-Agent": UA,
            "Referer":    f"https://leetcode.com/problems/{slug}/submissions/",
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


# ──────────────────────────────────────────────
# Notion property builder
# ──────────────────────────────────────────────
def build_props(
    problem:        dict,
    nc_map:         dict[str, list[str]],
    completed_date: str | None,
) -> dict:
    topics = [
        {"name": t["name"]}
        for t in (problem.get("topicTags") or [])
        if t.get("name")
    ]

    # Tags = NeetCode All categories for this problem
    nc_cats = nc_map.get(problem["titleSlug"], [])
    tags    = [{"name": c} for c in nc_cats]

    props: dict = {
        P_NAME:       {"title": [{"text": {"content": problem["title"]}}]},
        P_ID:         {"number": int(problem["frontendQuestionId"])},
        P_DIFFICULTY: {"select": {"name": problem["difficulty"]}},
        P_SLUG:       {"rich_text": [{"text": {"content": problem["titleSlug"]}}]},
        P_URL:        {"url": f"https://leetcode.com/problems/{problem['titleSlug']}/"},
        P_TOPICS:     {"multi_select": topics},
        P_TAGS:       {"multi_select": tags},
    }

    if problem.get("categoryTitle"):
        props[P_CATEGORY] = {"select": {"name": problem["categoryTitle"]}}

    if completed_date:
        props[P_COMPLETED] = {"date": {"start": completed_date}}
        props[P_STATUS]    = {"status": {"name": "Done"}}

    return props


# ──────────────────────────────────────────────
# Core upsert helper
# ──────────────────────────────────────────────
def upsert(
    slug:           str,
    completed_date: str | None,
    nc_map:         dict[str, list[str]],
    notion_token:   str,
    notion_db:      str,
    notion_index:   dict[str, dict],
) -> None:
    try:
        problem = fetch_problem_detail(slug)
    except Exception as exc:
        print(f"[sync] SKIP {slug}: could not fetch detail – {exc}")
        return

    existing  = notion_index.get(slug)
    page_id   = existing["page_id"] if existing else None
    props     = build_props(problem, nc_map, completed_date)

    # Never overwrite an existing completed date
    if existing and existing.get("completed") and completed_date:
        props.pop(P_COMPLETED, None)
        props.pop(P_STATUS,    None)

    result = notion_upsert(notion_token, notion_db, page_id, props)

    # Update local index so duplicate slugs in the same run don't create twice
    if slug not in notion_index:
        notion_index[slug] = {
            "page_id":   result["id"],
            "completed": completed_date,
        }

    action = "updated" if page_id else "created"
    print(f"[sync] {action}: {problem['title']} ({slug})")


# ──────────────────────────────────────────────
# Commands
# ──────────────────────────────────────────────
def cmd_sync(args: argparse.Namespace) -> None:
    notion_token = mustenv("NOTION_TOKEN")
    notion_db    = mustenv("NOTION_DATABASE_ID").replace("-", "")
    username     = mustenv("LEETCODE_USERNAME")
    nc_map       = load_nc_map()

    print("[sync] Querying Notion index …")
    pages        = notion_query_all(notion_token, notion_db)
    notion_index = notion_index_pages(pages)
    print(f"[sync] {len(notion_index)} existing pages indexed.")

    print(f"[sync] Fetching last {args.recent_limit} accepted submissions for '{username}' …")
    recent = fetch_recent_ac(username, args.recent_limit)
    print(f"[sync] {len(recent)} recent AC submissions found.")

    for item in recent:
        slug      = item["titleSlug"]
        completed = unix_to_iso(item["timestamp"])
        upsert(slug, completed, nc_map, notion_token, notion_db, notion_index)
        time.sleep(0.3)

    print("[sync] Done.")


def cmd_backfill(args: argparse.Namespace) -> None:
    notion_token = mustenv("NOTION_TOKEN")
    notion_db    = mustenv("NOTION_DATABASE_ID").replace("-", "")
    username     = mustenv("LEETCODE_USERNAME")   # noqa: F841 (kept for clarity)
    session      = mustenv("LEETCODE_SESSION")
    csrf         = mustenv("LEETCODE_CSRF")
    nc_map       = load_nc_map()

    print("[backfill] Querying Notion index …")
    pages        = notion_query_all(notion_token, notion_db)
    notion_index = notion_index_pages(pages)
    print(f"[backfill] {len(notion_index)} existing pages indexed.")

    print("[backfill] Fetching all solved slugs from LeetCode (authenticated) …")
    solved_slugs = fetch_all_solved_slugs(session, csrf)
    print(f"[backfill] {len(solved_slugs)} solved problems found.")

    for slug in solved_slugs:
        existing = notion_index.get(slug)

        # Skip if already in Notion with a completed date (nothing to do)
        if existing and existing.get("completed") and not args.create_missing:
            continue

        # Try to get the earliest AC date
        completed: str | None = None
        if args.fill_dates:
            if not existing or not existing.get("completed"):
                try:
                    completed = fetch_earliest_ac_date(slug, session, csrf)
                except Exception as exc:
                    print(f"[backfill] WARNING: could not get date for {slug}: {exc}")
                time.sleep(0.4)

        upsert(slug, completed, nc_map, notion_token, notion_db, notion_index)
        time.sleep(0.2)

    print("[backfill] Done.")


# ──────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────
def main() -> None:
    ap  = argparse.ArgumentParser(description="LeetCode → Notion tracker")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_sync = sub.add_parser("sync", help="Cookie-free incremental sync")
    p_sync.add_argument(
        "--recent-limit", type=int, default=20,
        help="How many recent AC submissions to pull (max LeetCode returns: ~20)",
    )

    p_back = sub.add_parser("backfill", help="One-time full history backfill (needs cookies)")
    p_back.add_argument(
        "--create-missing", action="store_true", default=True,
        help="Create Notion pages for every solved problem (default: True)",
    )
    p_back.add_argument(
        "--fill-dates", action="store_true", default=False,
        help="Attempt to find the earliest Accepted date for each problem",
    )

    args = ap.parse_args()

    if args.cmd == "sync":
        cmd_sync(args)
    elif args.cmd == "backfill":
        cmd_backfill(args)


if __name__ == "__main__":
    main()