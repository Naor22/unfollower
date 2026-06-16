# Push local changes to GitHub so the dashboard "Deploy latest" button can pull them.
# Usage:  ./push.ps1                 (auto commit message with timestamp)
#         ./push.ps1 "my message"    (custom commit message)
param([string]$Message)

$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot

if (-not $Message -or $Message.Trim() -eq "") {
    $Message = "update " + (Get-Date -Format "yyyy-MM-dd HH:mm")
}

git add -A
# Nothing staged? Skip the commit but still try to push (in case of unpushed commits).
$staged = git diff --cached --name-only
if ($staged) {
    git commit -m $Message
    Write-Host "Committed: $Message" -ForegroundColor Green
} else {
    Write-Host "No changes to commit." -ForegroundColor Yellow
}

git push
if ($LASTEXITCODE -eq 0) {
    Write-Host "Pushed. Now tap 'Deploy latest' in the dashboard (System tab)." -ForegroundColor Cyan
} else {
    Write-Host "Push failed (exit $LASTEXITCODE)." -ForegroundColor Red
}
