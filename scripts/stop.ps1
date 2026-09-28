$ErrorActionPreference = 'Continue'
$ProjectRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))

Push-Location -LiteralPath $ProjectRoot
try {
    Write-Host 'Stopping application cluster and cleaning up resources...' -ForegroundColor Cyan
    & docker compose down -v --remove-orphans
    Write-Host ''
    Write-Host ' Cluster stopped successfully.' -ForegroundColor Green
}
finally {
    Pop-Location
}
exit 0
