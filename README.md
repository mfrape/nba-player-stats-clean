# NBA Player Stats → Google Sheets Importer

Daily GitHub Action that downloads NBA player box scores from Kaggle, filters to the last 24 hours, and appends only new entries into your Google Sheet.

## How it works

1. **GitHub Action** fires daily at 10:00 UTC (5:00 AM ET)
2. **Python script** downloads `PlayerStatistics.csv` from the Kaggle dataset [eoinamoore/historical-nba-data-and-player-box-scores](https://www.kaggle.com/datasets/eoinamoore/historical-nba-data-and-player-box-scores)
3. Filters to rows where `gameDateTimeEst` is within the last 24 hours
4. Deduplicates by `personId + gameId` against what already exists in the sheet
5. Appends new rows to the correct `players-YYYY` tab via Google Sheets API

## Setup (one-time, ~10 minutes)

### 1. Create a GitHub repo

Create a new private repo and push these files into it:

```
.github/workflows/nba-import.yml
import_player_stats.py
requirements.txt
```

### 2. Create a Google Cloud service account

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a project (or use an existing one)
3. Enable the **Google Sheets API** (APIs & Services → Library → search "Google Sheets API" → Enable)
4. Go to **IAM & Admin → Service Accounts** → Create Service Account
5. Name it something like `nba-import`
6. Click the service account → **Keys** tab → Add Key → Create new key → **JSON**
7. Download the JSON key file

### 3. Share your Google Sheet with the service account

Open your destination spreadsheet and click **Share**. Add the service account's email address (looks like `nba-import@your-project.iam.gserviceaccount.com`) as an **Editor**.

### 4. Get your Kaggle API token

1. Go to [kaggle.com/settings](https://www.kaggle.com/settings)
2. Scroll to **API** → Create New Token
3. This downloads a `kaggle.json` file

### 5. Add GitHub Secrets

In your GitHub repo → **Settings → Secrets and variables → Actions → New repository secret**:

| Secret name | Value |
|---|---|
| `KAGGLE_JSON` | Paste the entire contents of your `kaggle.json` file |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Paste the entire contents of the Google service account JSON key file |

### 6. Test it

Go to **Actions** tab in your repo → select "NBA Player Stats Import" → **Run workflow** (manual trigger).

Check the logs to confirm it connects, filters, and appends correctly.

## Configuration

All configuration is in the workflow file and script:

| Setting | Where | Default |
|---|---|---|
| Schedule | `.github/workflows/nba-import.yml` | Daily at 10:00 UTC |
| Lookback window | `LOOKBACK_HOURS` env var in workflow | `24` hours |
| Destination sheet | `DEST_SPREADSHEET_ID` env var in workflow | Your sheet ID |
| Allowed seasons | `ALLOWED_SEASON_END_YEARS` in script | 2022–2026 |

## Season tab routing

Games are routed into `players-YYYY` tabs by NBA season logic:
- **Jul–Dec** games → `players-(year + 1)` (start of new season)
- **Jan–Jun** games → `players-(year)` (end of season)

Example: A game on Oct 25, 2025 → `players-2026`. A game on Feb 10, 2026 → `players-2026`.

## Deduplication

Each run reads the existing `personId` + `gameId` pairs from the target tab and skips any row that already exists. This means it's safe to re-run multiple times — it will never create duplicate entries.
