import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from dateutil import tz

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28")
LC_GRAPHQL = "https://leetcode.com/graphql"
UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

MAP_NC  = Path("maps/nc_allnc.json")
MAP_TUF = Path("maps/tuf.json")

P_TITLE     = "Name"
P_ID        = "Problem ID"
P_DIFFICULTY = "Difficulty"
P_STATUS    = "Status"
P_COMPLETED = "Completed date"
P_CATEGORY  = "Category"
P_TOPICS    = "Topics"
P_NC_TAGS   = "NC Tags"
P_TUF_TAGS  = "TUF Tags"
P_SLUG      = "Slug"
P_URL       = "LeetCode URL"

DONE = "Done"


def mustenv(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        print(f"[sync] ERROR: missing env var '{name}'", file=sys.stderr)
        sys.exit(1)
    return v


def unix_to_iso(ts: int | str) -> str:
    dt = datetime.fromtimestamp(int(ts), tz=tz.UTC).astimezone(tz.tzlocal())
    return dt.date().isoformat()


def load_map(path: Path) -> dict[str, list[str]]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


# ── Notion ────────────────────────────────────────────────────────────────────

def notion_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def notion_query_all(token: str, db_id: str) -> list[dict]:
    url     = f"{NOTION_API}/databases/{db_id}/query"
    pages:  list[dict] = []
    payload: dict      = {"page_size": 100}

    while True:
        r = requests.post(url, headers=notion_headers(token), json=payload, timeout=60)
        r.raise_for_status()
        data = r.json()
        pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        payload["start_cursor"] = data["next_cursor"]

    return pages


def _rich_text(page: dict, prop: str) -> str:
    rts = page.get("properties", {}).get(prop, {}).get("rich_text", [])
    return "".join(rt.get("plain_text", "") for rt in rts).strip()


def _date(page: dict, prop: str) -> str | None:
    d = page.get("properties", {}).get(prop, {}).get("date")
    return d.get("start") if d else None


def notion_index(pages: list[dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for p in pages:
        slug = _rich_text(p, P_SLUG)
        if slug:
            out[slug] = {"page_id": p["id"], "completed": _date(p, P_COMPLETED)}
    return out


def notion_upsert(token: str, db_id: str, page_id: str | None, props: dict) -> dict:
    if page_id is None:
        r = requests.post(
            f"{NOTION_API}/pages",
            headers=notion_headers(token),
            json={"parent": {"database_id": db_id}, "properties": props},
            timeout=60,
        )
    else:
        r = requests.patch(
            f"{NOTION_API}/pages/{page_id}",
            headers=notion_headers(token),
            json={"properties": props},
            timeout=60,
        )

    if not r.ok:
        print("[notion] upsert failed:", r.status_code)
        try:
            print(json.dumps(r.json(), indent=2))
        except Exception:
            print(r.text)
        r.raise_for_status()

    return r.json()


# ── LeetCode GraphQL ──────────────────────────────────────────────────────────

def lc_post(query: str, variables: dict,
            session: str | None = None, csrf: str | None = None) -> dict:
    headers: dict = {"Content-Type": "application/json", "User-Agent": UA}
    cookies = None
    if session and csrf:
        headers["x-csrftoken"] = csrf
        headers["Referer"]     = "https://leetcode.com/"
        cookies = {"LEETCODE_SESSION": session, "csrftoken": csrf}

    r = requests.post(LC_GRAPHQL, headers=headers, cookies=cookies,
                      json={"query": query, "variables": variables}, timeout=60)
    r.raise_for_status()
    data = r.json()
    if "errors" in data:
        raise RuntimeError(f"LeetCode GraphQL errors: {data['errors']}")
    return data["data"]


_Q_RECENT_AC = """
query recentAcSubmissions($username: String!, $limit: Int!) {
  recentAcSubmissionList(username: $username, limit: $limit) {
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
      status
      titleSlug
    }
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
        "difficulty":         q["difficulty"],
        "title":              q["title"],
        "titleSlug":          q["titleSlug"],
        "topicTags":          q.get("topicTags") or [],
        "categoryTitle":      q.get("categoryTitle"),
    }


def fetch_all_solved_slugs(session: str, csrf: str) -> list[str]:
    solved: list[str]  = []
    skip               = 0
    limit              = 100
    total: int | None  = None

    while total is None or skip < total:
        data  = lc_post(_Q_LIST,
                        {"categorySlug": "", "skip": skip, "limit": limit, "filters": {}},
                        session=session, csrf=csrf)
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
            "User-Agent": UA,
            "Referer":    f"https://leetcode.com/problems/{slug}/submissions/",
            "x-csrftoken": csrf,
        },
        cookies={"LEETCODE_SESSION": session, "csrftoken": csrf},
        timeout=60,
    )
    r.raise_for_status()
    subs = r.json().get("submissions_dump") or []

    accepted_ts: list[int] = []
    for s in subs:
        if s.get("status_display") == "Accepted":
            ts = s.get("timestamp") or s.get("time")
            if ts is not None:
                try:
                    accepted_ts.append(int(ts))
                except Exception:
                    pass

    return unix_to_iso(min(accepted_ts)) if accepted_ts else None


# ── Build + upsert ────────────────────────────────────────────────────────────

def build_props(problem: dict, is_solved: bool, completed_date: str | None,
                nc_map: dict[str, list[str]], tuf_map: dict[str, list[str]]) -> dict:
    slug  = problem["titleSlug"]
    props: dict = {
        P_TITLE:      {"title":      [{"text": {"content": problem["title"]}}]},
        P_ID:         {"number":     int(problem["frontendQuestionId"])},
        P_DIFFICULTY: {"select":     {"name": problem["difficulty"]}},
        P_SLUG:       {"rich_text":  [{"text": {"content": slug}}]},
        P_URL:        {"url":        f"https://leetcode.com/problems/{slug}/"},
        P_TOPICS:     {"multi_select": [
            {"name": t["name"]} for t in (problem.get("topicTags") or []) if t.get("name")
        ]},
    }

    if problem.get("categoryTitle"):
        props[P_CATEGORY] = {"select": {"name": problem["categoryTitle"]}}

    if slug in nc_map:
        props[P_NC_TAGS]  = {"multi_select": [{"name": c} for c in nc_map[slug]]}
    if slug in tuf_map:
        props[P_TUF_TAGS] = {"multi_select": [{"name": c} for c in tuf_map[slug]]}

    if completed_date:
        props[P_COMPLETED] = {"date": {"start": completed_date}}

    if is_solved:
        props[P_STATUS] = {"status": {"name": DONE}}

    return props


def upsert_one(slug: str, is_solved: bool, completed_date: str | None,
               notion_token: str, notion_db: str, idx: dict,
               nc_map: dict, tuf_map: dict) -> None:
    try:
        problem = fetch_problem_detail(slug)
    except Exception as exc:
        print(f"[sync] SKIP {slug}: {exc}")
        return

    existing  = idx.get(slug)
    page_id   = existing["page_id"] if existing else None
    props     = build_props(problem, is_solved, completed_date, nc_map, tuf_map)

    if existing and existing.get("completed") and completed_date:
        props.pop(P_COMPLETED, None)

    res = notion_upsert(notion_token, notion_db, page_id, props)

    if slug not in idx:
        idx[slug] = {"page_id": res["id"], "completed": completed_date}

    print(f"[sync] {'updated' if page_id else 'created'}: {problem['title']} ({slug})")


# ── Commands ──────────────────────────────────────────────────────────────────

def cmd_sync(recent_limit: int) -> None:
    token  = mustenv("NOTION_TOKEN")
    db     = mustenv("NOTION_DATABASE_ID").replace("-", "")
    user   = mustenv("LEETCODE_USERNAME")
    nc_map = load_map(MAP_NC)
    tuf_map= load_map(MAP_TUF)
    idx    = notion_index(notion_query_all(token, db))

    for item in fetch_recent_ac(user, recent_limit):
        upsert_one(item["titleSlug"], True, unix_to_iso(item["timestamp"]),
                   token, db, idx, nc_map, tuf_map)
        time.sleep(0.25)

    print("[sync] Done.")


def cmd_backfill(create_missing: bool, fill_dates: bool) -> None:
    token   = mustenv("NOTION_TOKEN")
    db      = mustenv("NOTION_DATABASE_ID").replace("-", "")
    mustenv("LEETCODE_USERNAME")
    session = mustenv("LEETCODE_SESSION")
    csrf    = mustenv("LEETCODE_CSRF")
    nc_map  = load_map(MAP_NC)
    tuf_map = load_map(MAP_TUF)
    idx     = notion_index(notion_query_all(token, db))

    for slug in fetch_all_solved_slugs(session, csrf):
        existing = idx.get(slug)
        if not create_missing and not existing:
            continue

        completed = None
        if fill_dates and (not existing or not existing.get("completed")):
            try:
                completed = fetch_earliest_ac_date(slug, session, csrf)
            except Exception as exc:
                print(f"[backfill] date fetch failed {slug}: {exc}")
            time.sleep(0.35)

        upsert_one(slug, True, completed, token, db, idx, nc_map, tuf_map)
        time.sleep(0.15)

    print("[backfill] Done.")


def main() -> None:
    ap  = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("sync")
    p1.add_argument("--recent-limit", type=int, default=20)

    p2 = sub.add_parser("backfill")
    p2.add_argument("--create-missing", action="store_true", default=False)
    p2.add_argument("--fill-dates",     action="store_true", default=False)

    args = ap.parse_args()
    if args.cmd == "sync":
        cmd_sync(args.recent_limit)
    else:
        cmd_backfill(args.create_missing, args.fill_dates)


if __name__ == "__main__":
    main()