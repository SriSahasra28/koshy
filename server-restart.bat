@echo off
title koshy server

:RESTART
echo [%TIME%] Cleaning up...
taskkill /F /IM node.exe >nul 2>&1
taskkill /F /IM nodemon.exe >nul 2>&1

REM Wait with error handling
ping 127.0.0.1 -n 4 >nul

REM Change to server directory
cd /d "C:\Users\Administrator\Desktop\koshy-trading-app-client_2025\koshy-trading-app-server"

if not exist "package.json" (
    echo [%TIME%] ERROR: package.json not found in server directory!
    echo [%TIME%] Current directory: %CD%
    timeout /t 10 /nobreak
    goto RESTART
)

echo [%TIME%] Starting server...

REM Start npm and capture exit code
call npm start
set EXITCODE=%ERRORLEVEL%

REM Only reaches here if server crashes/exits
echo.
echo [%TIME%] ============================================
echo [%TIME%] SERVER EXITED with code %EXITCODE%
echo [%TIME%] Auto-restarting in 5 seconds...
echo [%TIME%] ============================================
echo.

REM Wait 5 seconds before restart
ping 127.0.0.1 -n 6 >nul

goto RESTART
