import os
import time
import math
import requests
import pandas as pd
from datetime import datetime, timezone

# =========================================================
# CONFIG
# =========================================================
INPUT_XLSX = "full_pipeline_with_dockerfiles.xlsx"
OUTPUT_XLSX = "filtered_full_6_stages.xlsx"
OUTPUT_CSV = "filtered_full_6_stages.csv"
REPORT_TXT = "filtering_report.txt"

# You can paste your token directly here if you want
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "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")

# Filtering thresholds
MIN_STARS = 10
MIN_CONTRIBUTORS = 2
MIN_COMMITS = 1
RECENT_YEARS = 3          # set to None to disable recency filtering
REFRESH_FROM_GITHUB = True  # False = only use values already in the sheet
SLEEP_BETWEEN_CALLS = 0.2

# Required ML stage columns
REQUIRED_STAGES = [
    "Acquisition",
    "Preparation",
    "Modeling",
    "Training",
    "Evaluation",
    "Prediction",
]

# =========================================================
# GITHUB HELPERS
# =========================================================
SESSION = requests.Session()
if GITHUB_TOKEN and GITHUB_TOKEN != "PASTE_YOUR_GITHUB_TOKEN_HERE":
    SESSION.headers.update({
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    })
else:
    SESSION.headers.update({
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    })


def github_get(url, params=None, timeout=30):
    r = SESSION.get(url, params=params, timeout=timeout)
    if r.status_code == 403:
        print(f"[WARN] GitHub rate limit or access issue for: {url}")
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r


def parse_repo_full_name(repo_value, html_url_value):
    """
    Tries to extract owner/repo from either:
    - Repository column, if already like owner/repo
    - html_url column, e.g. https://github.com/owner/repo
    """
    if isinstance(repo_value, str) and "/" in repo_value and not repo_value.startswith("http"):
        return repo_value.strip()

    if isinstance(html_url_value, str) and "github.com/" in html_url_value:
        parts = html_url_value.strip().split("github.com/")[-1].strip("/").split("/")
        if len(parts) >= 2:
            return f"{parts[0]}/{parts[1]}"

    return None


def get_last_page_from_link(link_header):
    """
    Extract last page from GitHub Link header if present.
    """
    if not link_header:
        return None

    for part in link_header.split(","):
        if 'rel="last"' in part:
            # example: <https://api.github.com/...page=34>; rel="last"
            start = part.find("<") + 1
            end = part.find(">")
            url = part[start:end]
            if "page=" in url:
                try:
                    return int(url.split("page=")[-1].split("&")[0])
                except Exception:
                    return None
    return None


def get_contributors_count(owner_repo):
    """
    Count contributors using /contributors?per_page=1&anon=true
    and inspect Link header when possible.
    """
    url = f"https://api.github.com/repos/{owner_repo}/contributors"
    r = github_get(url, params={"per_page": 1, "anon": "true"})
    if r is None:
        return None

    data = r.json()
    link_header = r.headers.get("Link")
    last_page = get_last_page_from_link(link_header)

    if last_page is not None:
        return last_page

    if isinstance(data, list):
        return len(data)

    return None


def get_repo_metadata(owner_repo):
    """
    Refresh repo metadata from GitHub.
    """
    repo_url = f"https://api.github.com/repos/{owner_repo}"
    r = github_get(repo_url)
    if r is None:
        return None

    repo = r.json()

    contributors_count = None
    try:
        contributors_count = get_contributors_count(owner_repo)
        time.sleep(SLEEP_BETWEEN_CALLS)
    except Exception as e:
        print(f"[WARN] Could not get contributors for {owner_repo}: {e}")

    metadata = {
        "gh_full_name": repo.get("full_name"),
        "gh_html_url": repo.get("html_url"),
        "gh_stars": repo.get("stargazers_count"),
        "gh_forks": repo.get("forks_count"),
        "gh_watchers": repo.get("subscribers_count", repo.get("watchers_count")),
        "gh_open_issues": repo.get("open_issues_count"),
        "gh_default_branch": repo.get("default_branch"),
        "gh_language": repo.get("language"),
        "gh_created_at": repo.get("created_at"),
        "gh_updated_at": repo.get("updated_at"),
        "gh_pushed_at": repo.get("pushed_at"),
        "gh_size": repo.get("size"),
        "gh_archived": repo.get("archived"),
        "gh_disabled": repo.get("disabled"),
        "gh_fork": repo.get("fork"),
        "gh_contributors": contributors_count,
    }
    return metadata


# =========================================================
# FILTERING HELPERS
# =========================================================
def ensure_stage_columns(df, stage_cols):
    missing = [c for c in stage_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required stage columns: {missing}")


def has_all_6_stages(row):
    for stage in REQUIRED_STAGES:
        value = row.get(stage, 0)
        try:
            if pd.isna(value) or float(value) <= 0:
                return False
        except Exception:
            return False
    return True


def parse_dt(x):
    if pd.isna(x) or x is None:
        return None
    try:
        return pd.to_datetime(x, utc=True)
    except Exception:
        return None


def is_recent_enough(dt_value, recent_years):
    if recent_years is None:
        return True
    dt = parse_dt(dt_value)
    if dt is None:
        return False
    now = pd.Timestamp.now(tz="UTC")
    age_days = (now - dt).days
    return age_days <= recent_years * 365


# =========================================================
# MAIN
# =========================================================
def main():
    if not os.path.exists(INPUT_XLSX):
        raise FileNotFoundError(f"Input file not found: {INPUT_XLSX}")

    df = pd.read_excel(INPUT_XLSX)
    original_count = len(df)

    ensure_stage_columns(df, REQUIRED_STAGES)

    # -----------------------------------------------------
    # Step 1: keep only repos with all 6 stages
    # -----------------------------------------------------
    df["has_full_6_stages"] = df.apply(has_all_6_stages, axis=1)
    df_stage = df[df["has_full_6_stages"]].copy()
    stage_count = len(df_stage)

    # -----------------------------------------------------
    # Step 2: optionally refresh GitHub metadata
    # -----------------------------------------------------
    df_stage["repo_full_name"] = df_stage.apply(
        lambda row: parse_repo_full_name(row.get("Repository"), row.get("html_url")),
        axis=1
    )

    if REFRESH_FROM_GITHUB:
        gh_rows = []
        for idx, row in df_stage.iterrows():
            owner_repo = row["repo_full_name"]
            if not owner_repo:
                gh_rows.append({})
                continue

            try:
                meta = get_repo_metadata(owner_repo)
                gh_rows.append(meta if meta is not None else {})
                print(f"[OK] Refreshed: {owner_repo}")
            except Exception as e:
                print(f"[WARN] Failed GitHub refresh for {owner_repo}: {e}")
                gh_rows.append({})

            time.sleep(SLEEP_BETWEEN_CALLS)

        gh_df = pd.DataFrame(gh_rows, index=df_stage.index)
        df_stage = pd.concat([df_stage, gh_df], axis=1)

        # effective values = GitHub fresh value if present, otherwise sheet value
        df_stage["effective_stars"] = df_stage["gh_stars"].combine_first(df_stage.get("stargazers_count"))
        df_stage["effective_contributors"] = df_stage["gh_contributors"].combine_first(df_stage.get("n_contributors"))
        df_stage["effective_commits"] = df_stage.get("n_commits")
        df_stage["effective_updated_at"] = df_stage["gh_updated_at"].combine_first(df_stage.get("updated_at"))
        df_stage["effective_pushed_at"] = df_stage["gh_pushed_at"].combine_first(df_stage.get("pushed_at"))
    else:
        df_stage["effective_stars"] = df_stage.get("stargazers_count")
        df_stage["effective_contributors"] = df_stage.get("n_contributors")
        df_stage["effective_commits"] = df_stage.get("n_commits")
        df_stage["effective_updated_at"] = df_stage.get("updated_at")
        df_stage["effective_pushed_at"] = df_stage.get("pushed_at")

    # numeric cleanup
    for col in ["effective_stars", "effective_contributors", "effective_commits"]:
        df_stage[col] = pd.to_numeric(df_stage[col], errors="coerce").fillna(0)

    # -----------------------------------------------------
    # Step 3: apply repo quality/activity filters
    # -----------------------------------------------------
    filtered = df_stage[
        (df_stage["effective_stars"] >= MIN_STARS) &
        (df_stage["effective_contributors"] >= MIN_CONTRIBUTORS) &
        (df_stage["effective_commits"] >= MIN_COMMITS)
    ].copy()

    if RECENT_YEARS is not None:
        filtered = filtered[
            filtered["effective_pushed_at"].apply(lambda x: is_recent_enough(x, RECENT_YEARS))
        ].copy()

    final_count = len(filtered)

    # -----------------------------------------------------
    # Step 4: sort output
    # -----------------------------------------------------
    filtered = filtered.sort_values(
        by=["effective_stars", "effective_contributors", "effective_commits"],
        ascending=[False, False, False]
    )

    # -----------------------------------------------------
    # Step 5: save outputs
    # -----------------------------------------------------
    filtered.to_excel(OUTPUT_XLSX, index=False)
    filtered.to_csv(OUTPUT_CSV, index=False)

    # -----------------------------------------------------
    # Step 6: create textual report
    # -----------------------------------------------------
    report_lines = []
    report_lines.append("Filtering Report")
    report_lines.append("=" * 60)
    report_lines.append(f"Input file: {INPUT_XLSX}")
    report_lines.append(f"Original number of repositories: {original_count}")
    report_lines.append("")
    report_lines.append("Step 1: Full 6-stage ML pipeline")
    report_lines.append("Required stages:")
    for s in REQUIRED_STAGES:
        report_lines.append(f"  - {s}")
    report_lines.append(
        "A repository is considered to have the full ML pipeline only if all six stage columns are present and each one has a value greater than 0."
    )
    report_lines.append(f"Repositories after full 6-stage filtering: {stage_count}")
    report_lines.append("")
    report_lines.append("Step 2: GitHub-based filtering")
    report_lines.append(f"Refresh metadata from GitHub API: {REFRESH_FROM_GITHUB}")
    report_lines.append(f"Minimum stars: {MIN_STARS}")
    report_lines.append(f"Minimum contributors: {MIN_CONTRIBUTORS}")
    report_lines.append(f"Minimum commits: {MIN_COMMITS}")
    if RECENT_YEARS is not None:
        report_lines.append(f"Recent activity constraint: pushed within the last {RECENT_YEARS} years")
    else:
        report_lines.append("Recent activity constraint: disabled")
    report_lines.append("")
    report_lines.append(f"Final number of repositories after all filters: {final_count}")
    report_lines.append("")
    report_lines.append("Top repositories after filtering:")
    top_n = min(10, final_count)
    if top_n == 0:
        report_lines.append("  No repositories matched the current thresholds.")
    else:
        for i, (_, row) in enumerate(filtered.head(top_n).iterrows(), start=1):
            report_lines.append(
                f"  {i}. {row.get('repo_full_name', row.get('Repository'))} | "
                f"stars={int(row['effective_stars'])} | "
                f"contributors={int(row['effective_contributors'])} | "
                f"commits={int(row['effective_commits'])}"
            )

    with open(REPORT_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))

    print("\n".join(report_lines))
    print("\nSaved files:")
    print(f" - {OUTPUT_XLSX}")
    print(f" - {OUTPUT_CSV}")
    print(f" - {REPORT_TXT}")


if __name__ == "__main__":
    main()