@echo off
setlocal
cd /d "%~dp0"
docker compose --profile api stop
docker compose --profile api ps
