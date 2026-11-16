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
    $BuildCommand = 'docker compose build app-node-1 > "' + $BuildLog + '" 2>&1'
    cmd.exe /d /c $BuildCommand
    $BuildExitCode = $LASTEXITCODE
    if ($BuildExitCode -ne 0) {
        Get-Content $BuildLog -Tail 40
        throw 'Docker image build failed. See logs/build.log for details.'
    }
}

Write-Host 'Starting application services (Nginx Load Balancer + 5-Node Raft Cluster + LLM)...' -ForegroundColor Cyan
cmd.exe /d /c 'docker compose up --detach --no-build >nul 2>&1'
if ($LASTEXITCODE -ne 0) { throw 'Application services failed to start.' }

Write-Host ''
Write-Host '=================================================================' -ForegroundColor Cyan
Write-Host ' DISTRIBUTED REAL-TIME CHAT & COLLABORATION PLATFORM (M2 RAFT)' -ForegroundColor Cyan
Write-Host '=================================================================' -ForegroundColor Cyan
Write-Host ' Nginx Load Balancer (Main UI): http://localhost:8000' -ForegroundColor Green
Write-Host ' Direct Node UIs (Nodes 1..5):  http://localhost:8001 .. 8005' -ForegroundColor Green
Write-Host ' gRPC App Nodes (1..5):         localhost:50051 .. 50055' -ForegroundColor White
Write-Host ' Raft Peer Nodes (1..5):        localhost:50071 .. 50075' -ForegroundColor White
Write-Host ' LLM Server port:               localhost:50060' -ForegroundColor White
Write-Host ''
Write-Host ' 5-Node Raft cluster is live behind Nginx (least_conn).' -ForegroundColor Yellow
Write-Host ' Open multiple tabs at http://localhost:8000 in your browser!' -ForegroundColor Yellow
Write-Host ' Logs: logs\app.log' -ForegroundColor Yellow
Write-Host ' To stop: .\scripts\stop.ps1' -ForegroundColor Cyan
Write-Host '=================================================================' -ForegroundColor Cyan
