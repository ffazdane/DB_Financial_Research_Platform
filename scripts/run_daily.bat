@echo off
:: FazDane Daily Data Refresh
:: Runs after market close — downloads price + option chain data and uploads to Databricks
:: Schedule via Task Scheduler: daily at 5:15 PM ET on weekdays

set PYTHON=C:\Users\KP9MWT\AppData\Local\Python\pythoncore-3.14-64\python.exe
set SCRIPTS=%~dp0
set LOG_DIR=%~dp0..\logs
set LOG_FILE=%LOG_DIR%\daily_refresh_%DATE:~-4%%DATE:~4,2%%DATE:~7,2%.log

:: ── Secrets — set these as Windows System Environment Variables, NOT here ────
:: Go to: System Properties -> Advanced -> Environment Variables -> System Variables
:: Add: DATABRICKS_TOKEN, TT_CLIENT_SECRET, TT_REFRESH_TOKEN
if "%DATABRICKS_TOKEN%"=="" (
    echo ERROR: DATABRICKS_TOKEN environment variable not set. Exiting.
    exit /b 1
)
if "%TT_CLIENT_SECRET%"=="" (
    echo ERROR: TT_CLIENT_SECRET environment variable not set. Exiting.
    exit /b 1
)
if "%TT_REFRESH_TOKEN%"=="" (
    echo ERROR: TT_REFRESH_TOKEN environment variable not set. Exiting.
    exit /b 1
)

:: ── Create log directory ──────────────────────────────────────────────────────
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

echo ============================================================ >> "%LOG_FILE%"
echo FazDane Daily Refresh — %DATE% %TIME%                        >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

:: ── 1. Price refresh (last 5 days — fast incremental update) ─────────────────
echo [%TIME%] Starting price refresh...
echo [%TIME%] Starting price refresh... >> "%LOG_FILE%"
"%PYTHON%" "%SCRIPTS%daily_price_refresh.py" >> "%LOG_FILE%" 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [%TIME%] ERROR: price refresh failed. See log.
    echo [%TIME%] ERROR: price refresh failed >> "%LOG_FILE%"
)

:: ── 2. Option chain refresh ───────────────────────────────────────────────────
echo [%TIME%] Starting option chain refresh...
echo [%TIME%] Starting option chain refresh... >> "%LOG_FILE%"
"%PYTHON%" "%SCRIPTS%daily_option_loader.py" >> "%LOG_FILE%" 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [%TIME%] ERROR: option loader failed. See log.
    echo [%TIME%] ERROR: option loader failed >> "%LOG_FILE%"
)

echo [%TIME%] Daily refresh complete.
echo [%TIME%] Daily refresh complete. >> "%LOG_FILE%"
