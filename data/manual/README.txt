This folder holds files that must be manually downloaded and committed.

Files expected here:

1. REH_Info_Cleaned.csv
   - Tracks hospitals that converted from CAH to REH status
   - Maintained manually; update when CMS announces new REH conversions
   - Source: CMS REH conversion announcements

2. NASHP_HCT_Data_latest.xlsx  (fallback only)
   - Used if the NASHP web scraper fails
   - Download from: https://nashp.org/hospital-cost-tool/
   - Click "Download the data" → save here → commit
   - Only needed if automated scraping breaks

Both files are small enough to commit directly (no Git LFS needed).
