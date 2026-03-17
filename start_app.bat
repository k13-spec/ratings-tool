@echo off
start "" "C:\Users\Kriti\AppData\Local\Python\pythoncore-3.14-64\Scripts\streamlit.exe" run "C:\Users\Kriti\Documents\Claude Code\ratings-tool\app.py" --server.headless true --browser.gatherUsageStats false
timeout /t 3 /nobreak >nul
start "" "http://localhost:8501"
