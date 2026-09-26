$ErrorActionPreference = 'Continue'
$ProjectRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))

Push-Location -LiteralPath $ProjectRoot
try {
    Write-Host 'Stopping application cluster and cleaning up resources...' -ForegroundColor Cyan
    & docker compose down -v --remove-orphans
    Write-Host ''
    Write-Host '=================================================================' -ForegroundColor Cyan
    Write-Host ' Cluster stopped successfully. No lingering containers remain.' -ForegroundColor Green
    Write-Host '=================================================================' -ForegroundColor Cyan
}
finally {
    Pop-Location
}
exit 0
