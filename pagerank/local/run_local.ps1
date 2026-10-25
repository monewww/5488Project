# =============================================================================
# 小批量版 · Windows / PowerShell 本地验证 PageRank (Python, 不需要 Hadoop)
# 用法:
#   .\run_local.ps1                          # 跑仓库内置的 1.7MB 样本
#   .\run_local.ps1 -InputCsv C:\path\UserBehavior.csv   # 跑自己的文件 (按 uid%330 抽样)
# =============================================================================
param(
    [string]$InputCsv = "",
    [int]$SampleMod   = 0,      # 0 = 自动(样本文件用 1, 别的文件用 330)
    [int]$MaxIter     = 0,      # 0 = 自动(样本 10 轮, 全量抽样 50 轮)
    [string]$OutDir   = "results\local_run",
    [string]$Python   = "python"
)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

$sample = Join-Path "data\sample" "UserBehavior_sample.csv"
if ([string]::IsNullOrWhiteSpace($InputCsv)) { $InputCsv = $sample }
$isSample = ([System.IO.Path]::GetFullPath($InputCsv) -eq [System.IO.Path]::GetFullPath($sample))

if ($SampleMod -eq 0) { $SampleMod = if ($isSample) { 1 } else { 330 } }
if ($MaxIter   -eq 0) { $MaxIter   = if ($isSample) { 10 } else { 50 } }

Write-Host "=== 输入: $InputCsv  (sample-mod=$SampleMod, max-iter=$MaxIter) ===" -ForegroundColor Cyan
& $Python local\pagerank_local.py `
    --input $InputCsv `
    --sample-mod $SampleMod `
    --session-gap 1800 `
    --max-session-items 50 `
    --damping 0.85 `
    --max-iter $MaxIter `
    --tol 1e-6 `
    --behaviors "pv,buy,cart,fav" `
    --top 30 `
    --outdir $OutDir

Write-Host ""
Write-Host "=== 结果 ===" -ForegroundColor Cyan
Write-Host "--- stats.txt ---"      -ForegroundColor Yellow; Get-Content (Join-Path $OutDir "stats.txt")
Write-Host "--- top_items.csv ---"  -ForegroundColor Yellow; Get-Content (Join-Path $OutDir "top_items.csv") -TotalCount 11
Write-Host "--- convergence.csv ---" -ForegroundColor Yellow; Get-Content (Join-Path $OutDir "convergence.csv")
