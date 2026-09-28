@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPT_DIR%.."
cd /d "%PROJECT_ROOT%"

echo Stopping application cluster and cleaning up resources...
docker compose down -v --remove-orphans
echo.
echo  Cluster stopped successfully.
exit /b 0
