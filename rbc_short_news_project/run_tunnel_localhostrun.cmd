@echo off
setlocal

set PORT=8080
if not "%~1"=="" set PORT=%~1

echo Starting localhost.run tunnel to local port %PORT%
echo Keep this window open while tunnel is needed.
echo.
echo After connect you will get a public URL like https://xxxxx.localhost.run
echo Use:
echo   GET  https://xxxxx.localhost.run/health
echo   POST https://xxxxx.localhost.run/run
echo.

ssh -o StrictHostKeyChecking=no -o ServerAliveInterval=30 -R 80:localhost:%PORT% nokey@localhost.run
