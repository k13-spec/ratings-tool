@echo off
REM Manual / Task-Scheduler fallback for the fortnightly ratings refresh.
REM Mirrors .github\workflows\ratings-refresh.yml — use this if an agency
REM starts blocking GitHub runner IPs, or to refresh on demand.
REM Run from the ratings-tool repo root (folder containing data\ratings.db).
cd /d "%~dp0"
if not exist data\ratings.db (
    echo ERROR: data\ratings.db not found here. Run from the ratings-tool repo root.
    pause
    exit /b 1
)
echo === ICRA ===
python run_scraper.py --icra
echo === CRISIL ===
python run_scraper.py --crisil
echo === India Ratings (full rescan) ===
python run_scraper.py --india-ratings-reset
echo === CARE Edge (incremental discovery, 200) ===
python run_scraper.py --care-edge --limit 200
echo === Normalize DB + export ratings_current.csv ===
python db_maintenance.py
if errorlevel 1 (
    echo EXPORT GUARD TRIPPED - CSV untouched. Review output above before pushing.
    pause
    exit /b 1
)
echo === Rebuild ISIN issuer-prefix map ===
python build_isin_map.py
echo === Commit and push ===
git add data/ratings.db data/ratings_current.csv data/issuer_prefix_map.csv
git diff --cached --quiet && echo Nothing to commit. && pause && exit /b 0
git commit -m "Ratings refresh (manual run)"
git push
echo Done. Streamlit Cloud redeploys automatically; the bond tracker picks up
echo the fresh CSVs within its 1-hour cache.
pause
