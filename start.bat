@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

echo [1/3] Checking Python virtual environment...
if not exist ".venv" (
    echo Creating virtual environment in .venv...
    python -m venv .venv 2>nul
)

if exist ".venv\Scripts\python.exe" (
    echo [2/3] Installing / verifying dependencies...
    ".venv\Scripts\python.exe" -m pip install -q --disable-pip-version-check -r requirements.txt
    if not exist "generated" mkdir generated
    echo Compiling protobuf stubs...
    ".venv\Scripts\python.exe" -m grpc_tools.protoc -I ./proto --python_out=./generated --grpc_python_out=./generated ./proto/chat.proto ./proto/llm.proto
)

:run_ps
echo [3/3] Launching application cluster...
powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%scripts\start.ps1" %*
exit /b %ERRORLEVEL%
