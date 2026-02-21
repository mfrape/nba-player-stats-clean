name: NBA Team Stats Import

on:
  schedule:
    - cron: "15 10 * * *"
  workflow_dispatch:

jobs:
  import-team-stats:
    runs-on: ubuntu-latest
    timeout-minutes: 15

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Set up Kaggle credentials
        run: |
          mkdir -p ~/.kaggle
          echo '${{ secrets.KAGGLE_JSON }}' > ~/.kaggle/kaggle.json
          chmod 600 ~/.kaggle/kaggle.json

      - name: Set up Google service account
        run: |
          echo '${{ secrets.GOOGLE_SERVICE_ACCOUNT_JSON }}' > /tmp/gcp-sa.json

      - name: Run team stats import
        env:
          GOOGLE_SA_KEY_PATH: /tmp/gcp-sa.json
          DEST_TEAM_SPREADSHEET_ID: "1m3egsHWbasuBQdHUpZcDlyObcomcCkfdCMOGA8Kk44w"
          LOOKBACK_HOURS: "24"
        run: python import_team_stats.py

      - name: Cleanup credentials
        if: always()
        run: |
          rm -f /tmp/gcp-sa.json
          rm -f ~/.kaggle/kaggle.json
