"""
NBA PlayerStatistics.csv → Google Sheets incremental importer.

Downloads the full CSV from Kaggle, filters to last N hours,
deduplicates against existing rows, and appends new data to
the correct players-YYYY tab in the destination Google Sheet.
"""

import os
import csv
import io
from datetime import datetime, timedelta, timezone
from pathlib import Path

import kagglehub
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ── Config ───────────────────────────────────────────────────────────
SPREADSHEET_ID = os.environ.get(
    "DEST_SPREADSHEET_ID",
    "1OlS4ZVRK_bwFkShtZ5HMC6L7bGs6rlYDSNRwIirEhJo",
)
LOOKBACK_HOURS = int(os.environ.get("LOOKBACK_HOURS", "24"))
SA_KEY_PATH = os.environ.get("GOOGLE_SA_KEY_PATH", "/tmp/gcp-sa.json")

KAGGLE_DATASET = "eoinamoore/historical-nba-data-and-player-box-scores"
KAGGLE_FILE = "PlayerStatistics.csv"

ALLOWED_SEASON_END_YEARS = {2022, 2023, 2024, 2025, 2026}

EXPECTED_HEADERS = [
    "firstName", "lastName", "personId", "gameId", "gameDateTimeEst",
    "playerteamCity", "playerteamName", "opponentteamCity", "opponentteamName",
    "gameType", "gameLabel", "gameSubLabel", "seriesGameNumber", "win", "home",
    "numMinutes", "points", "assists", "blocks", "steals",
    "fieldGoalsAttempted", "fieldGoalsMade", "fieldGoalsPercentage",
    "threePointersAttempted", "threePointersMade", "threePointersPercentage",
    "freeThrowsAttempted", "freeThrowsMade", "freeThrowsPercentage",
    "reboundsDefensive", "reboundsOffensive", "reboundsTotal",
    "foulsPersonal", "turnovers", "plusMinusPoints",
]


# ── Google Sheets helpers ────────────────────────────────────────────
def get_sheets_service():
    """Authenticate and return a Google Sheets API service."""
    creds = Credentials.from_service_account_file(
        SA_KEY_PATH,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds)


def get_existing_keys(service, spreadsheet_id: str, tab_name: str) -> set:
    """
    Read personId (col C) and gameId (col D) from an existing tab.
    Returns a set of "personId|gameId" strings for deduplication.
    """
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"'{tab_name}'!C:D",
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute()
    except Exception:
        # Tab doesn't exist yet → no existing keys
        return set()

    rows = result.get("values", [])
    keys = set()
    for row in rows[1:]:  # skip header
        if len(row) >= 2:
            keys.add(f"{row[0]}|{row[1]}")
    return keys


def ensure_tab_exists(service, spreadsheet_id: str, tab_name: str):
    """Create the tab with headers if it doesn't exist yet."""
    # Check existing sheets
    meta = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields="sheets.properties.title",
    ).execute()
    existing_tabs = {s["properties"]["title"] for s in meta.get("sheets", [])}

    if tab_name not in existing_tabs:
        # Create the sheet
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
        ).execute()
        # Write header row
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{tab_name}'!A1",
            valueInputOption="RAW",
            body={"values": [EXPECTED_HEADERS]},
        ).execute()
        print(f"  Created new tab: {tab_name}")


def append_rows(service, spreadsheet_id: str, tab_name: str, rows: list):
    """Append rows to the bottom of a tab."""
    if not rows:
        return
    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"'{tab_name}'!A:AI",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()


# ── Date / season helpers ────────────────────────────────────────────
def parse_date(s: str) -> datetime | None:
    """Parse gameDateTimeEst flexibly. Returns a timezone-aware (ET) dt."""
    if not s:
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%m/%d/%Y %I:%M %p",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(s.strip(), fmt)
            # Treat as US/Eastern (UTC-5) if no timezone info
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone(timedelta(hours=-5)))
            return dt
        except ValueError:
            continue
    return None


def season_end_year(dt: datetime) -> int:
    """NBA season mapping: Jul–Dec → year+1, Jan–Jun → year."""
    return dt.year + 1 if dt.month >= 7 else dt.year


# ── Main ─────────────────────────────────────────────────────────────
def main():
    # 1. Download the CSV from Kaggle
    print("Downloading PlayerStatistics.csv from Kaggle...")
    csv_path = kagglehub.dataset_download(
        KAGGLE_DATASET,
        path=KAGGLE_FILE,
        force_download=True,
    )
    csv_path = Path(csv_path)
    print(f"  Downloaded to: {csv_path}")
    print(f"  File size: {csv_path.stat().st_size / 1024 / 1024:.1f} MB")

    # 2. Calculate the cutoff time
    cutoff = datetime.now(timezone(timedelta(hours=-5))) - timedelta(hours=LOOKBACK_HOURS)
    print(f"  Cutoff (ET): {cutoff.isoformat()}")

    # 3. Read CSV and filter to recent rows
    print("Reading and filtering CSV...")
    recent_rows = []  # list of dicts keyed by header name
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # Validate headers
        csv_headers = [h.strip() for h in reader.fieldnames]
        missing = [h for h in EXPECTED_HEADERS if h not in csv_headers]
        if missing:
            raise ValueError(f"CSV is missing expected columns: {missing}")
        print(f"  CSV headers validated OK ({len(csv_headers)} columns).")

        for row in reader:
            dt_str = (row.get("gameDateTimeEst") or "").strip()
            dt = parse_date(dt_str)
            if not dt:
                continue
            if dt < cutoff:
                continue
            recent_rows.append(row)

    print(f"  Rows within last {LOOKBACK_HOURS} hours: {len(recent_rows)}")
    if not recent_rows:
        print("Nothing new to import. Done.")
        return

    # 4. Connect to Google Sheets
    print("Connecting to Google Sheets...")
    service = get_sheets_service()

    # 5. Bucket by season tab, dedup, and append
    by_tab: dict[str, list] = {}
    for row in recent_rows:
        dt = parse_date(row["gameDateTimeEst"])
        yr = season_end_year(dt)
        if yr not in ALLOWED_SEASON_END_YEARS:
            continue
        tab = f"players-{yr}"
        if tab not in by_tab:
            by_tab[tab] = []
        by_tab[tab].append(row)

    total_appended = 0
    for tab_name, rows in by_tab.items():
        ensure_tab_exists(service, SPREADSHEET_ID, tab_name)

        # Get existing keys for dedup
        existing_keys = get_existing_keys(service, SPREADSHEET_ID, tab_name)
        print(f"  {tab_name}: {len(existing_keys)} existing keys, {len(rows)} candidate rows")

        # Filter out duplicates
        new_rows = []
        for row in rows:
            pid = str(row.get("personId", "")).strip()
            gid = str(row.get("gameId", "")).strip()
            key = f"{pid}|{gid}"
            if key in existing_keys:
                continue
            # Build output in exact expected column order
            new_rows.append([row.get(h, "") for h in EXPECTED_HEADERS])
            existing_keys.add(key)  # prevent dupes within this batch too

        if new_rows:
            append_rows(service, SPREADSHEET_ID, tab_name, new_rows)
            print(f"  ✓ Appended {len(new_rows)} new rows to {tab_name}")
            total_appended += len(new_rows)
        else:
            print(f"  – {tab_name}: no new rows (all duplicates)")

    print(f"\nDone. Total rows appended: {total_appended}")


if __name__ == "__main__":
    main()
