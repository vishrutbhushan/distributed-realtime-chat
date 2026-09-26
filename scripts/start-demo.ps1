param(
    [switch]$NoRebuild
)

$ErrorActionPreference = 'Continue'
$ProjectRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$HasLocation = $false
$ExitCode = 1

try {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw 'Docker CLI was not found. Install Docker Desktop or Docker Engine, then retry.'
    }

    Push-Location -LiteralPath $ProjectRoot
    $HasLocation = $true

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
        Start-Sleep -Seconds 5
    }

    Write-Host 'Both services are healthy.' -ForegroundColor Green
    Write-Host 'Web UI available at: http://localhost:8000' -ForegroundColor Cyan
    Write-Host 'Starting the self-checking demo suite...' -ForegroundColor Cyan
    & docker compose up --no-build --abort-on-container-exit --exit-code-from client-runner
    $ExitCode = $LASTEXITCODE
    if ($ExitCode -eq 0) {
        Write-Host 'Demo succeeded (exit code 0).' -ForegroundColor Green
    }
    else {
        Write-Host "Demo failed (exit code $ExitCode). Review the service output above." -ForegroundColor Red
    }
}
catch {
    Write-Host "Startup failed: $($_.Exception.Message)" -ForegroundColor Red
    $ExitCode = 1
}
finally {
    if ($HasLocation) {
        Pop-Location
    }
}

exit $ExitCode
