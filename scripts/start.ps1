param(
    [switch]$NoRebuild
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
Set-Location -LiteralPath $ProjectRoot

$LogDir = Join-Path $ProjectRoot 'logs'
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }
$LogFile = Join-Path $LogDir 'app.log'
Set-Content -Path $LogFile -Value ''

Write-Host 'Preparing Python environment...' -ForegroundColor Cyan
$SystemPython = Get-Command python -ErrorAction Stop
$VenvPath = Join-Path $ProjectRoot '.venv'
$VenvPy = Join-Path $VenvPath 'Scripts\python.exe'

if (-not (Test-Path $VenvPy)) { & $SystemPython.Source -m venv $VenvPath }
& $VenvPy -m pip install -q --disable-pip-version-check -r (Join-Path $ProjectRoot 'requirements.txt')

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw 'Docker CLI was not found.' }
docker compose version | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Docker Compose v2 is unavailable.' }
docker info | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Docker Desktop is not running.' }

Write-Host 'Removing previous containers and volumes...' -ForegroundColor Cyan
cmd.exe /d /c 'docker compose down -v --remove-orphans >nul 2>&1'
if ($LASTEXITCODE -ne 0) { throw 'Could not remove the previous Docker Compose stack.' }

if (-not $NoRebuild) {
    Write-Host 'Building Docker images...' -ForegroundColor Cyan
    $BuildLog = Join-Path $LogDir 'build.log'
    $BuildCommand = 'docker compose build > "' + $BuildLog + '" 2>&1'
    cmd.exe /d /c $BuildCommand
    $BuildExitCode = $LASTEXITCODE
    if ($BuildExitCode -ne 0) {
        Get-Content $BuildLog -Tail 40
        throw 'Docker image build failed. See logs/build.log for details.'
    }
}

Write-Host 'Starting application services (3-Node Raft Cluster + LLM)...' -ForegroundColor Cyan
cmd.exe /d /c 'docker compose up --detach --no-build llm-server app-node-1 app-node-2 app-node-3 >nul 2>&1'
if ($LASTEXITCODE -ne 0) { throw 'Application services failed to start.' }

Write-Host ''
Write-Host '=================================================================' -ForegroundColor Cyan
Write-Host ' DISTRIBUTED REAL-TIME CHAT & COLLABORATION PLATFORM (M2 RAFT)' -ForegroundColor Cyan
Write-Host '=================================================================' -ForegroundColor Cyan
Write-Host ' Web UI Node 1:       http://localhost:8000' -ForegroundColor Green
Write-Host ' Web UI Node 2:       http://localhost:8001' -ForegroundColor Green
Write-Host ' Web UI Node 3:       http://localhost:8002' -ForegroundColor Green
Write-Host ' gRPC App Node 1:     localhost:50051 (Raft: 50071)' -ForegroundColor White
Write-Host ' gRPC App Node 2:     localhost:50052 (Raft: 50072)' -ForegroundColor White
Write-Host ' gRPC App Node 3:     localhost:50053 (Raft: 50073)' -ForegroundColor White
Write-Host ' LLM Server port:     localhost:50060' -ForegroundColor White
Write-Host ''
Write-Host ' 3-Node Raft cluster is live. Open http://localhost:8000 in browser.' -ForegroundColor Yellow
Write-Host ' Logs: logs\app.log' -ForegroundColor Yellow
Write-Host ' To stop: .\scripts\stop.ps1' -ForegroundColor Cyan
Write-Host '=================================================================' -ForegroundColor Cyan
