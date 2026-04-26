"""
sync.py  –  LeetCode  ➜  Notion tracker  (with streak stats)

Commands
--------
python sync.py sync      --recent-limit 20
python sync.py backfill  --create-missing --fill-dates
python sync.py diagnose
"""

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from dateutil import tz

# ──────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────
NOTION_VERSION = "2022-06-28"
NOTION_API     = "https://api.notion.com/v1"
LC_GRAPHQL     = "https://leetcode.com/graphql"

# ✅ fixed paths to match maps/ directory
NC_MAP_FILE  = "maps/nc_allnc.json"
TUF_MAP_FILE = "maps/tuf.json"

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

# ── Tracker DB property names — must match your Notion DB exactly ──
P_NAME       = "Name"
P_ID         = "Problem ID"
P_DIFFICULTY = "Difficulty"
P_STATUS     = "Status"
P_COMPLETED  = "Completed date"
P_CATEGORY   = "Category"
P_TOPICS     = "Topics"
P_NC_TAGS    = "NC Tags"    # ✅ fixed
P_TUF_TAGS   = "TUF Tags"   # ✅ added
P_SLUG       = "Slug"
P_URL        = "LeetCode URL"

# ── Stats DB property names (must match your new pivot DB) ──
SP_NAME = "Name"  # Title
SP_CURRENT_STREAK = "Current Streak"
SP_LONGEST_STREAK = "Longest Streak"
SP_TOTAL_SOLVED = "Total Solved"
SP_THIS_WEEK = "Solved This Week"
SP_THIS_MONTH = "Solved This Month"
SP_UPDATED = "Last Updated"

STATS_ROW_NAME = "Stats"  # the single row title in the pivot DB


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
        print(f"[sync] WARNING: {NC_MAP_FILE} not found – NC Tags will be empty.")
        return {}
    with p.open(encoding="utf-8") as fh:
        return json.load(fh)


def load_tuf_map() -> dict[str, list[str]]:   # ✅ new
    p = Path(TUF_MAP_FILE)
    if not p.exists():
        print(f"[sync] WARNING: {TUF_MAP_FILE} not found – TUF Tags will be empty.")
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


def _title_value(page: dict, prop: str) -> str:
    rts = page.get("properties", {}).get(prop, {}).get("title", [])
    return "".join(rt.get("plain_text", "") for rt in rts).strip()


def _date_value(page: dict, prop: str) -> str | None:
    d = page.get("properties", {}).get(prop, {}).get("date")
    return d.get("start") if d else None


def notion_index_pages(pages: list[dict]) -> dict[str, dict]:
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
    solved: list[str] = []
    skip   = 0
    limit  = 100
    total: int | None = None

    while total is None or skip < total:
        data  = lc_post(_Q_LIST, {
            "categorySlug": "",
            "skip":         skip,
            "limit":        limit,
            "filters":      {},
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
    url = f"https://leetcode.com/api/submissions/{slug}"
    r   = requests.get(
        url,
        headers={
            "User-Agent":  UA,
            "Referer":     f"https://leetcode.com/problems/{slug}/submissions/",
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
    tuf_map:        dict[str, list[str]],   # ✅ added
    completed_date: str | None,
) -> dict:
    slug   = problem["titleSlug"]
    topics = [
        {"name": t["name"]}
        for t in (problem.get("topicTags") or [])
        if t.get("name")
    ]

    nc_tags  = [{"name": c} for c in nc_map.get(slug, [])]    # ✅ fixed
    tuf_tags = [{"name": c} for c in tuf_map.get(slug, [])]   # ✅ added

    props: dict = {
        P_NAME:      {"title": [{"text": {"content": problem["title"]}}]},
        P_ID:        {"number": int(problem["frontendQuestionId"])},
        P_DIFFICULTY:{"select": {"name": problem["difficulty"]}},
        P_SLUG:      {"rich_text": [{"text": {"content": slug}}]},
        P_URL:       {"url": f"https://leetcode.com/problems/{slug}/"},
        P_TOPICS:    {"multi_select": topics},
        P_NC_TAGS:   {"multi_select": nc_tags},    # ✅ fixed
        P_TUF_TAGS:  {"multi_select": tuf_tags},   # ✅ added
    }

    if problem.get("categoryTitle"):
        props[P_CATEGORY] = {"select": {"name": problem["categoryTitle"]}}

    if completed_date:
        props[P_COMPLETED] = {"date": {"start": completed_date}}
        props[P_STATUS]    = {"status": {"name": "Done"}}

    return props


# ──────────────────────────────────────────────
# Core upsert
# ──────────────────────────────────────────────
def upsert(
    slug:           str,
    completed_date: str | None,
    nc_map:         dict[str, list[str]],
    tuf_map:        dict[str, list[str]],   # ✅ added
    notion_token:   str,
    notion_db:      str,
    notion_index:   dict[str, dict],
) -> None:
    try:
        problem = fetch_problem_detail(slug)
    except Exception as exc:
        print(f"[sync] SKIP {slug}: could not fetch detail – {exc}")
        return

    existing = notion_index.get(slug)
    page_id  = existing["page_id"] if existing else None
    props    = build_props(problem, nc_map, tuf_map, completed_date)   # ✅ passes tuf_map

    # Never overwrite an existing completed date
    if existing and existing.get("completed") and completed_date:
        props.pop(P_COMPLETED, None)
        props.pop(P_STATUS,    None)

    result = notion_upsert(notion_token, notion_db, page_id, props)

    # Keep local index in sync so stats reflect this run
    if slug not in notion_index:
        notion_index[slug] = {"page_id": result["id"], "completed": None}

    # Always ensure we have the page id
    notion_index[slug]["page_id"] = result["id"]

    # If we just set a completed date and the index didn't have one, update it
    if completed_date and not notion_index[slug].get("completed"):
        notion_index[slug]["completed"] = completed_date

    action = "updated" if page_id else "created"
    print(f"[sync] {action}: {problem['title']} ({slug})")


# ──────────────────────────────────────────────
# Stats
# ──────────────────────────────────────────────
def compute_stats(notion_index: dict[str, dict]) -> dict[str, int]:
    today            = date.today()
    this_week_start  = today - timedelta(days=today.weekday())
    this_month_start = today.replace(day=1)

    solved_dates: set[date] = set()
    week_count  = 0
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

    # Current streak
    current_streak = 0
    anchor = today if today in solved_dates else today - timedelta(days=1)
    if anchor in solved_dates:
        cursor = anchor
        while cursor in solved_dates:
            current_streak += 1
            cursor -= timedelta(days=1)

    # Longest streak
    longest_streak = 0
    if solved_dates:
        sorted_dates = sorted(solved_dates)
        run_length   = 1
        for i in range(1, len(sorted_dates)):
            if sorted_dates[i] - sorted_dates[i - 1] == timedelta(days=1):
                run_length += 1
            else:
                longest_streak = max(longest_streak, run_length)
                run_length = 1
        longest_streak = max(longest_streak, run_length)

    return {
        SP_CURRENT_STREAK: current_streak,
        SP_LONGEST_STREAK: longest_streak,
        SP_TOTAL_SOLVED:   total_solved,
        SP_THIS_WEEK:      week_count,
        SP_THIS_MONTH:     month_count,
    }


def push_stats(notion_token: str, stats_db_id: str, stats: dict[str, int]) -> None:
    today_iso = date.today().isoformat()

    # Find the single "Stats" row
    pages = notion_query_all(notion_token, stats_db_id)
    page_id = None
    for p in pages:
        if _title_value(p, SP_NAME) == STATS_ROW_NAME:
            page_id = p["id"]
            break

    if not page_id:
        print(
            f"[stats] ERROR: Could not find Stats DB row with Name='{STATS_ROW_NAME}'.",
            file=sys.stderr,
        )
        return

    props = {
        SP_CURRENT_STREAK: {"number": stats[SP_CURRENT_STREAK]},
        SP_LONGEST_STREAK: {"number": stats[SP_LONGEST_STREAK]},
        SP_TOTAL_SOLVED:   {"number": stats[SP_TOTAL_SOLVED]},
        SP_THIS_WEEK:      {"number": stats[SP_THIS_WEEK]},
        SP_THIS_MONTH:     {"number": stats[SP_THIS_MONTH]},
        SP_UPDATED:        {"date": {"start": today_iso}},
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
    notion_db    = mustenv("NOTION_DATABASE_ID").replace("-", "")
    stats_db_id  = optenv("NOTION_STATS_DATABASE_ID").replace("-", "")
    username     = mustenv("LEETCODE_USERNAME")
    nc_map       = load_nc_map()
    tuf_map      = load_tuf_map()   # ✅ added

    print("[sync] Querying Notion tracker index …")
    pages        = notion_query_all(notion_token, notion_db)
    notion_index = notion_index_pages(pages)
    print(f"[sync] {len(notion_index)} existing pages indexed.")

    print(f"[sync] Fetching last {args.recent_limit} accepted submissions for '{username}' …")
    recent = fetch_recent_ac(username, args.recent_limit)
    print(f"[sync] {len(recent)} recent AC submissions found.")

    for item in recent:
        slug      = item["titleSlug"]
        completed = unix_to_iso(item["timestamp"])
        upsert(slug, completed, nc_map, tuf_map, notion_token, notion_db, notion_index)   # ✅
        time.sleep(0.3)

    if stats_db_id:
        print("[sync] Computing stats …")
        push_stats(notion_token, stats_db_id, compute_stats(notion_index))
    else:
        print("[sync] NOTION_STATS_DATABASE_ID not set – skipping stats push.")

    print("[sync] Done.")


def cmd_backfill(args: argparse.Namespace) -> None:
    notion_token = mustenv("NOTION_TOKEN")
    notion_db    = mustenv("NOTION_DATABASE_ID").replace("-", "")
    stats_db_id  = optenv("NOTION_STATS_DATABASE_ID").replace("-", "")
    session      = mustenv("LEETCODE_SESSION")
    csrf         = mustenv("LEETCODE_CSRF")
    nc_map       = load_nc_map()
    tuf_map      = load_tuf_map()   # ✅ added

    print("[backfill] Querying Notion tracker index …")
    pages        = notion_query_all(notion_token, notion_db)
    notion_index = notion_index_pages(pages)
    print(f"[backfill] {len(notion_index)} existing pages indexed.")

    print("[backfill] Fetching all solved slugs from LeetCode (authenticated) …")
    solved_slugs = fetch_all_solved_slugs(session, csrf)
    print(f"[backfill] {len(solved_slugs)} solved problems found.")

    for slug in solved_slugs:
        existing = notion_index.get(slug)

        # ✅ fixed: skip only when --create-missing is NOT passed AND page exists with date
        if existing and existing.get("completed") and not args.create_missing:
            continue

        completed: str | None = None
        if args.fill_dates:
            if not existing or not existing.get("completed"):
                try:
                    completed = fetch_earliest_ac_date(slug, session, csrf)
                except Exception as exc:
                    print(f"[backfill] WARNING: could not get date for {slug}: {exc}")
                time.sleep(0.4)

        upsert(slug, completed, nc_map, tuf_map, notion_token, notion_db, notion_index)   # ✅
        time.sleep(0.2)

    if stats_db_id:
        print("[backfill] Computing stats …")
        push_stats(notion_token, stats_db_id, compute_stats(notion_index))
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
        print(f"\n[diagnose] ── {label} ──")
        print(f"           DB title : {data['title'][0]['plain_text'] if data.get('title') else '(unknown)'}")
        print(f"           DB id    : {db_id}")
        print(f"           {'Property name':<35} {'Type'}")
        print(f"           {'-'*35} {'-'*20}")
        for name, prop in sorted(data["properties"].items()):
            print(f"           {name:<35} {prop['type']}")

    tracker_db = mustenv("NOTION_DATABASE_ID").replace("-", "")
    stats_db   = optenv("NOTION_STATS_DATABASE_ID").replace("-", "")

    print_db_schema("TRACKER DB  (NOTION_DATABASE_ID)", tracker_db)
    print_db_schema("STATS DB    (NOTION_STATS_DATABASE_ID)", stats_db)

    print("\n[diagnose] ── Expected property name constants in sync.py ──")
    constants = {
        "Tracker DB": {
            "P_NAME":       P_NAME,
            "P_ID":         P_ID,
            "P_DIFFICULTY": P_DIFFICULTY,
            "P_STATUS":     P_STATUS,
            "P_COMPLETED":  P_COMPLETED,
            "P_CATEGORY":   P_CATEGORY,
            "P_TOPICS":     P_TOPICS,
            "P_NC_TAGS":    P_NC_TAGS,    # ✅ fixed
            "P_TUF_TAGS":   P_TUF_TAGS,   # ✅ fixed
            "P_SLUG":       P_SLUG,
            "P_URL":        P_URL,
        },
        "Stats DB": {
            "SP_NAME":           SP_NAME,
            "SP_CURRENT_STREAK": SP_CURRENT_STREAK,
            "SP_LONGEST_STREAK": SP_LONGEST_STREAK,
            "SP_TOTAL_SOLVED":   SP_TOTAL_SOLVED,
            "SP_THIS_WEEK":      SP_THIS_WEEK,
            "SP_THIS_MONTH":     SP_THIS_MONTH,
            "SP_UPDATED":        SP_UPDATED,
            "STATS_ROW_NAME":    STATS_ROW_NAME,
        },
    }
    for db_label, props in constants.items():
        print(f"\n           {db_label}:")
        for const, value in props.items():
            print(f"             {const:<15} = '{value}'")

    print("\n[diagnose] Done. Fix any mismatches by editing the constants at the top of sync.py.")


# ──────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────
def main() -> None:
    ap  = argparse.ArgumentParser(description="LeetCode → Notion tracker")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_sync = sub.add_parser("sync", help="Cookie-free incremental sync")
    p_sync.add_argument("--recent-limit", type=int, default=20)

    p_back = sub.add_parser("backfill", help="One-time full history backfill (needs cookies)")
    p_back.add_argument(
        "--create-missing", action="store_true", default=False   # ✅ fixed
    )
    p_back.add_argument("--fill-dates", action="store_true", default=False)

    sub.add_parser("diagnose", help="Print Notion DB property names/types")

    args = ap.parse_args()

    if args.cmd == "sync":
        cmd_sync(args)
    elif args.cmd == "backfill":
        cmd_backfill(args)
    elif args.cmd == "diagnose":
        cmd_diagnose(args)


if __name__ == "__main__":
    main()
# ✅ removed duplicate if __name__ block