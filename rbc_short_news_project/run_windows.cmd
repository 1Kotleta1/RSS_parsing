@echo off
setlocal
cd /d "%~dp0"

if not exist ".venv" (
  py -3 -m venv .venv
)

call ".venv\Scripts\activate.bat"
python -m pip install --upgrade pip >nul
python -m pip install -r requirements.txt
python ".\src\rbc_short_news_parser.py"
