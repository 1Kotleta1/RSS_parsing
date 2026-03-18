@echo off
setlocal
cd /d "%~dp0"
docker compose --profile batch up --build
