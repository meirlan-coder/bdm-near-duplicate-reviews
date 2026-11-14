# One-command experiment for Windows (PowerShell), from the repository folder:
#
#     powershell -ExecutionPolicy Bypass -File .\run_all.ps1
#
# Builds the image, starts the cluster and launches scripts/run_all.sh in the
# background inside the client container. Progress: results\logs\run_all.log
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

Write-Host "== Recording host hardware (results\env\host.json)"
New-Item -ItemType Directory -Force -Path "results\env", "results\logs" | Out-Null
$cs  = Get-CimInstance Win32_ComputerSystem
$cpu = Get-CimInstance Win32_Processor | Select-Object -First 1
$os  = Get-CimInstance Win32_OperatingSystem
$disk = Get-CimInstance Win32_DiskDrive | Select-Object -First 1
@{
  manufacturer   = $cs.Manufacturer
  model          = $cs.Model
  cpu            = $cpu.Name
  physical_cores = $cpu.NumberOfCores
  logical_cpus   = $cpu.NumberOfLogicalProcessors
  ram_gb         = [math]::Round($cs.TotalPhysicalMemory / 1GB, 1)
  os             = "$($os.Caption) $($os.Version)"
  disk           = "$($disk.Model) ($([math]::Round($disk.Size / 1GB)) GB)"
} | ConvertTo-Json | Set-Content -Encoding UTF8 "results\env\host.json"

Write-Host "== Building the Docker image (first time: ~10 minutes, downloads Hadoop and Spark)"
docker compose build
if ($LASTEXITCODE -ne 0) { throw "docker compose build failed - is Docker Desktop running?" }

Write-Host "== Starting the cluster"
docker compose up -d
if ($LASTEXITCODE -ne 0) { throw "docker compose up failed" }

Write-Host "== Launching the experiment in the background"
docker compose exec -d client bash -c "bash scripts/run_all.sh >> results/logs/run_all.log 2>&1"

Write-Host ""
Write-Host "The experiment is running (several hours for the 5M dataset)."
Write-Host "Keep the laptop plugged in and do not let it sleep."
Write-Host "Progress:  Get-Content results\logs\run_all.log -Wait"
Write-Host "Web UIs:   http://localhost:8088 (YARN)   http://localhost:9870 (HDFS)"
