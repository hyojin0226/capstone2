# Windows 호스트 1초 샘플러. ADR 0006 H9b(가끔 오는 수십 초 멈춤) 진단
# WSL VM(vmmemWSL) 밖에서 멈춤 순간에 무엇이 일어나는지 본다: 호스트 CPU 포화, CPU 클럭(성능 %),
# 메모리 여유·페이징, 디스크 지연, Defender(MsMpEng)·vmmemWSL CPU.
# 성능 카운터 이름은 Windows 언어마다 달라서 Get-Counter 대신 언어와 무관한 CIM 클래스(Win32_PerfFormattedData_*)를 쓴다.
#
# 사용법 (Windows PowerShell): powershell -ExecutionPolicy Bypass -File host_probe.ps1 <out.csv> <seconds>
param([string]$Out, [int]$Seconds)

$end = (Get-Date).AddSeconds($Seconds)
"ts_ms,cpu_pct,cpu_perf_pct,avail_mb,pages_per_s,disk_idle_pct,disk_sec_per_xfer_ms,vmmem_cpu_pct,defender_cpu_pct,top_proc,top_proc_cpu_pct" |
    Out-File -FilePath $Out -Encoding ascii
$ncpu = (Get-CimInstance Win32_ComputerSystem).NumberOfLogicalProcessors
while ((Get-Date) -lt $end) {
    $t0 = Get-Date
    $ts = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    $cpu = Get-CimInstance Win32_PerfFormattedData_PerfOS_Processor -Filter "Name='_Total'"
    $perf = Get-CimInstance Win32_PerfFormattedData_Counters_ProcessorInformation -Filter "Name='_Total'"
    $mem = Get-CimInstance Win32_PerfFormattedData_PerfOS_Memory
    $disk = Get-CimInstance Win32_PerfFormattedData_PerfDisk_PhysicalDisk -Filter "Name='_Total'"
    $procs = Get-CimInstance Win32_PerfFormattedData_PerfProc_Process |
        Where-Object { $_.Name -notin @('_Total', 'Idle') }
    $vm = ($procs | Where-Object { $_.Name -like 'vmmem*' } | Measure-Object PercentProcessorTime -Sum).Sum
    $def = ($procs | Where-Object { $_.Name -like 'MsMpEng*' } | Measure-Object PercentProcessorTime -Sum).Sum
    $top = $procs | Where-Object { $_.Name -notlike 'vmmem*' } | Sort-Object PercentProcessorTime -Descending | Select-Object -First 1
    # 프로세스 % 는 코어 1개 = 100% 기준이다. 전체 CPU 대비 %로 바꾼다
    $vals = @($ts, $cpu.PercentProcessorTime, $perf.PercentProcessorPerformance)
    $vals += @($mem.AvailableMBytes, $mem.PagesPersec, $disk.PercentIdleTime, $disk.AvgDisksecPerTransfer)
    $vals += @([math]::Round($vm / $ncpu, 1), [math]::Round($def / $ncpu, 1), $top.Name)
    $vals += @([math]::Round($top.PercentProcessorTime / $ncpu, 1))
    ($vals -join ",") | Out-File -FilePath $Out -Append -Encoding ascii
    $wait = 1000 - ((Get-Date) - $t0).TotalMilliseconds
    if ($wait -gt 0) { Start-Sleep -Milliseconds $wait }
}
