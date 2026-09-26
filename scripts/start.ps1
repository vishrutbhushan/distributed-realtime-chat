param(
    [switch]$NoRebuild
)

$ErrorActionPreference = 'Continue'
$ProjectRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$HasLocation = $false
$ExitCode = 0

function Stop-Cluster {
    Write-Host ''
    Write-Host 'Shutting down services and cleaning up volumes (no lingering containers)...' -ForegroundColor Cyan
    & docker compose down -v --remove-orphans
}

try {
    Push-Location -LiteralPath $ProjectRoot
    $HasLocation = $true

    # Setup log directory and clear log file on every start
    $LogDir = Join-Path $ProjectRoot 'logs'
    if (-not (Test-Path $LogDir)) {
        New-Item -ItemType Directory -Path $LogDir | Out-Null
    }
    $LogFile = Join-Path $LogDir 'app.log'
    if (Test-Path $LogFile) {
        Clear-Content -Path $LogFile -ErrorAction SilentlyContinue
    } else {
        New-Item -ItemType File -Path $LogFile | Out-Null
    }
    Write-Host "[LOGS] Initialized clean log file at: $LogFile" -ForegroundColor Cyan

    # 1. Virtual environment setup and dependency verification
    if (Get-Command python -ErrorAction SilentlyContinue) {
        $VenvPath = Join-Path $ProjectRoot '.venv'
        $VenvPy = Join-Path $VenvPath 'Scripts\python.exe'
        if (-not (Test-Path $VenvPath)) {
            Write-Host 'Creating virtual environment in .venv...' -ForegroundColor Cyan
            & python -m venv $VenvPath 2>$null
        }
        if (Test-Path $VenvPy) {
            Write-Host 'Verifying dependencies in virtual environment...' -ForegroundColor Cyan
            & $VenvPy -m pip install -q --disable-pip-version-check -r (Join-Path $ProjectRoot 'requirements.txt')
            $GenDir = Join-Path $ProjectRoot 'generated'
            if (-not (Test-Path $GenDir)) { New-Item -ItemType Directory -Path $GenDir | Out-Null }
            & $VenvPy -m grpc_tools.protoc -I ./proto --python_out=./generated --grpc_python_out=./generated ./proto/chat.proto ./proto/llm.proto
        }
    }

    # 2. Docker verification
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw 'Docker CLI was not found. Install Docker Desktop or Docker Engine, then retry.'
    }

    $null = & docker compose version
    if ($LASTEXITCODE -ne 0) {
        throw 'Docker Compose v2 is unavailable. Update Docker Desktop or install the Compose plugin.'
    }

    $null = & docker info 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw 'The Docker engine is not ready. Start Docker Desktop (or the Docker service), wait for it to finish starting, then retry.'
    }

    Write-Host 'Cleaning up previous containers and volumes (clean slate)...' -ForegroundColor Cyan
    & docker compose down -v --remove-orphans
    if ($LASTEXITCODE -ne 0) {
        throw 'Could not tear down existing Docker Compose containers and volumes.'
    }

    Write-Host 'Building project images (static tests run during build)...' -ForegroundColor Cyan
    & docker compose build
    if ($LASTEXITCODE -ne 0) {
        throw 'Docker Compose could not build the project images or static tests failed.'
    }

    Write-Host 'Starting the app and local model services...' -ForegroundColor Cyan
    & docker compose up --no-build --detach llm-server app-node-1
    if ($LASTEXITCODE -ne 0) {
        throw 'Docker Compose could not start the app and model services.'
    }

    $Deadline = [DateTime]::UtcNow.AddMinutes(10)
    $Services = @('llm-server', 'app-node-1')
    while ($true) {
        $AllHealthy = $true
        foreach ($Service in $Services) {
            $ContainerOutput = & docker compose ps --quiet $Service
            if ($LASTEXITCODE -ne 0) {
                throw "Could not check the '$Service' container status."
            }
            $ContainerId = @($ContainerOutput | Where-Object { $_ } | Select-Object -First 1)
            if ($ContainerId.Count -eq 0) {
                $AllHealthy = $false
                continue
            }

            $HealthOutput = & docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' $ContainerId[0]
            if ($LASTEXITCODE -ne 0) {
                throw "Could not inspect the '$Service' container."
            }
            $Health = "$HealthOutput".Trim()
            if ($Health -eq 'unhealthy' -or $Health -eq 'exited' -or $Health -eq 'dead') {
                Write-Host "The '$Service' service reported '$Health'. Recent logs follow:" -ForegroundColor Red
                & docker compose logs --no-color --tail=100 $Service
                throw "The '$Service' service did not become healthy."
            }
            if ($Health -ne 'healthy' -and $Health -ne 'running') {
                $AllHealthy = $false
            }
        }

        if ($AllHealthy) {
            break
        }
        if ([DateTime]::UtcNow -ge $Deadline) {
            Write-Host 'Timed out waiting for service health. Recent logs follow:' -ForegroundColor Red
            & docker compose logs --no-color --tail=100 llm-server app-node-1
            throw 'The services did not become healthy within 10 minutes.'
        }
        Start-Sleep -Seconds 3
    }

    Write-Host ''
    Write-Host '=================================================================' -ForegroundColor Cyan
    Write-Host ' DISTRIBUTED REAL-TIME CHAT & COLLABORATION PLATFORM' -ForegroundColor Cyan
    Write-Host '=================================================================' -ForegroundColor Cyan
    Write-Host ' Web UI available at: http://localhost:8000' -ForegroundColor Green
    Write-Host ' gRPC Server port:    localhost:50051' -ForegroundColor White
    Write-Host ' LLM Server port:     localhost:50060' -ForegroundColor White
    Write-Host " Log file location:   $LogFile (cleared on start)" -ForegroundColor Yellow
    Write-Host ''
    Write-Host ' Cluster is live. Open http://localhost:8000 in your browser.' -ForegroundColor Yellow
    Write-Host ' To stop the cluster: run .\scripts\stop.ps1 or press Ctrl+C here.' -ForegroundColor DarkGray
    Write-Host '=================================================================' -ForegroundColor Cyan
    Write-Host ''

    # Stream service logs live to both console and logs/app.log until user presses Ctrl+C
    & docker compose logs -f --tail=20 app-node-1 llm-server | Tee-Object -FilePath $LogFile -Append
}
catch {
    Write-Host "Startup failed: $($_.Exception.Message)" -ForegroundColor Red
    $ExitCode = 1
}
finally {
    Stop-Cluster
    if ($HasLocation) {
        Pop-Location
    }
}

exit $ExitCode
