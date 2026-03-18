@echo off
setlocal
cd /d "%~dp0"
docker compose --profile api up -d --build
docker compose --profile api ps
