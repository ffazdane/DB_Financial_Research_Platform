# setup_task_scheduler.ps1
# Registers the FazDane daily refresh as a Windows Task Scheduler job.
# Run ONCE as Administrator:
#   Right-click PowerShell -> "Run as Administrator"
#   Then: .\scripts\setup_task_scheduler.ps1

$TaskName   = "FazDane_Daily_Refresh"
$ScriptsDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$BatFile    = Join-Path $ScriptsDir "run_daily.bat"

# Trigger: Mon-Fri at 5:15 PM
$trigger = New-ScheduledTaskTrigger `
    -Weekly `
    -DaysOfWeek Monday, Tuesday, Wednesday, Thursday, Friday `
    -At "5:15PM"

# Action: run the batch file
$action = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
    -Argument "/c `"$BatFile`""

# Settings: run whether user is logged on or not, stop if runs > 3 hours
$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 3) `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable

# Principal: run as current user with highest privileges
$principal = New-ScheduledTaskPrincipal `
    -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) `
    -LogonType Interactive `
    -RunLevel Highest

# Register (or update if already exists)
$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Removed existing task '$TaskName'"
}

Register-ScheduledTask `
    -TaskName  $TaskName `
    -Trigger   $trigger `
    -Action    $action `
    -Settings  $settings `
    -Principal $principal `
    -Description "FazDane: download daily price + option chain data and upload to Databricks. Runs Mon-Fri at 5:15 PM."

Write-Host ""
Write-Host "Task '$TaskName' registered successfully."
Write-Host "Schedule: Mon-Fri at 5:15 PM"
Write-Host "Batch:    $BatFile"
Write-Host ""
Write-Host "To verify: Get-ScheduledTask -TaskName '$TaskName' | Format-List"
Write-Host "To run now: Start-ScheduledTask -TaskName '$TaskName'"
Write-Host "Logs: $ScriptsDir\..\logs\"
