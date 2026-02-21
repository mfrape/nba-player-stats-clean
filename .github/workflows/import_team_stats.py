"""
NBA TeamStatistics.csv → Google Sheets incremental importer.

Downloads the full CSV from Kaggle, filters to last N hours,
deduplicates against existing rows, and appends new data to
the '2026' tab in the destination Google Sheet.
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
    "DEST_TEAM_SPREADSHEET_ID",
    "1m3egsHWbasuBQdHUpZcDlyObcomcCkfdCMOGA8Kk44w",
)
LOOKBACK_HOURS = int(os.environ.get("LOOKBACK_HOURS", "24"))
SA_KEY_PATH = os.environ.get("GOOGLE_SA_KEY_PATH", "/tmp/gcp-sa.json")

KAGGLE_DATASET = "eoinamoore/historical-nba-data-and-player-box-scores"
KAGGLE_FILE = "TeamStatistics.csv"

TARGET_TAB = "2026"

EXPECTED_HEADERS = [
    "gameId", "gameDateTimeEst", "teamCity", "teamName", "teamId",
    "opponentTeamCity", "opponentTeamName", "opponentTeamId",
    "home", "win", "teamScore", "opponentScore",
    "assists", "blocks", "steals",
    "fieldGoalsAttempted", "fieldGoalsMade", "fieldGoalsPercentage",
    "threePointersAttempted", "threePointersMade", "threePointersPercentage",
    "freeThrowsAttempted", "freeThrowsMade", "freeThrowsPercentage",
    "reboundsDefensive", "reboundsOffensive", "reboundsTotal",
    "foulsPersonal", "turnovers", "plusMinusPoints",
    "numMinutes",
    "q1Points", "q2Points", "q3Points", "q4Points",
    "benchPoints", "biggestLead", "biggestScoringRun", "leadChanges",
    "pointsFastBreak", "pointsFromTurnovers", "pointsInThePaint",
    "pointsSecondChance", "timesTied", "timeoutsRemaining",
    "seasonWins", "seasonLosses", "coachId",
]


def get_sheets_service():
    creds = Credentials.from_service_account_file(
        SA_KEY_PATH,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds)


def get_existing_keys(service, spreadsheet_id, tab_name):
    try:
        result_a = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"'{tab_name}'!A:A",
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute()
        result_e = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"'{tab_name}'!E:E",
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute()
    except Exception:
        return set()

    rows_a = result_a.get("values", [])
    rows_e = result_e.get("values", [])
    keys = set()
    for i in range(1, min(len(rows_a), len(rows_e))):
        game_id = str(rows_a[i][0]) if rows_a[i] else ""
        team_id = str(rows_e[i][0]) if rows_e[i] else ""
        if game_id and team_id:
            keys.add(f"{team_id}|{game_id}")
    return keys


def ensure_tab_exists(service, spreadsheet_id, tab_name):
    meta = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields="sheets.properties.title",
    ).execute()
    existing_tabs = {s["properties"]["title"] for s in meta.get("sheets", [])}
    if tab_name not in existing_tabs:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
        ).execute()
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{tab_name}'!A1",
            valueInputOption="RAW",
            body={"values": [EXPECTED_HEADERS]},
        ).execute()
        print(f"  Created new tab: {tab_name}")


def append_rows(service, spreadsheet_id, tab_name, rows):
    if not rows:
        return
    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"'{tab_name}'!A:AV",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()


def parse_date(s):
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
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone(timedelta(hours=-5)))
            return dt
        except ValueError:
            continue
    return None


def is_current_season(dt):
    if dt.year == 2025 and dt.month >= 7:
        return True
    if dt.year == 2026 and dt.month <= 6:
        return True
    return False


def main():
    print("Downloading TeamStatistics.csv from Kaggle...")
    csv_path = kagglehub.dataset_download(
        KAGGLE_DATASET,
        path=KAGGLE_FILE,
        force_download=True,
    )
    csv_path = Path(csv_path)
    print(f"  Downloaded to: {csv_path}")
    print(f"  File size: {csv_path.stat().st_size / 1024 / 1024:.1f} MB")

    cutoff = datetime.now(timezone(timedelta(hours=-5))) - timedelta(hours=LOOKBACK_HOURS)
    print(f"  Cutoff (ET): {cutoff.isoformat()}")

    print("Reading and filtering CSV...")
    recent_rows = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
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
            if not is_current_season(dt):
                continue
            recent_rows.append(row)

    print(f"  Rows within last {LOOKBACK_HOURS} hours (2025-26 season): {len(recent_rows)}")
    if not recent_rows:
        print("Nothing new to import. Done.")
        return

    print("Connecting to Google Sheets...")
    service = get_sheets_service()

    ensure_tab_exists(service, SPREADSHEET_ID, TARGET_TAB)
    existing_keys = get_existing_keys(service, SPREADSHEET_ID, TARGET_TAB)
    print(f"  {TARGET_TAB}: {len(existing_keys)} existing keys, {len(recent_rows)} candidate rows")

    new_rows = []
    for row in recent_rows:
        team_id = str(row.get("teamId", "")).strip()
        game_id = str(row.get("gameId", "")).strip()
        key = f"{team_id}|{game_id}"
        if key in existing_keys:
            continue
        new_rows.append([row.get(h, "") for h in EXPECTED_HEADERS])
        existing_keys.add(key)

    if new_rows:
        append_rows(service, SPREADSHEET_ID, TARGET_TAB, new_rows)
        print(f"  ✓ Appended {len(new_rows)} new rows to {TARGET_TAB}")
    else:
        print(f"  – {TARGET_TAB}: no new rows (all duplicates)")

    print(f"\nDone. Total rows appended: {len(new_rows)}")


if __name__ == "__main__":
    main()
