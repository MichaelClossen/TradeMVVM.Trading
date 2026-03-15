@echo off
REM Start Poller server in a visible console (workspace root assumed)
REM Sets DB env so GUI and server use the same SQLite file during development
set "TRADE_DB_CONNECTION=Data Source=C:\Users\micha\Desktop\Trade\trading.db"
cd /d "%~dp0"

REM Prefer native exe if present (build output path)
REM Prefer compiled Release exe, then Debug exe, otherwise fallback to dotnet run
if exist "%~dp0TradeMVVM.Poller.Server\bin\Release\net8.0-windows\TradeMVVM.Poller.Server.exe" (
  echo Starting native poller Release exe...
  start "Poller" "%~dp0TradeMVVM.Poller.Server\bin\Release\net8.0-windows\TradeMVVM.Poller.Server.exe"
  goto :eof
)

if exist "%~dp0TradeMVVM.Poller.Server\bin\Debug\net8.0-windows\TradeMVVM.Poller.Server.exe" (
  echo Starting native poller Debug exe...
  start "Poller" "%~dp0TradeMVVM.Poller.Server\bin\Debug\net8.0-windows\TradeMVVM.Poller.Server.exe"
  goto :eof
)

REM Fallback to dotnet run using the project file
echo Starting poller via dotnet run...
start "Poller" dotnet run --project "%~dp0TradeMVVM.Poller.Server\TradeMVVM.Poller.Server.csproj" --no-build

:eof
