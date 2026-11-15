@echo off
chcp 65001 >nul
cd /d "%~dp0"
if "%~1"=="" (
  echo 用法: start.bat ^<直播间号^>   例如: start.bat 79423499229
  pause
  exit /b 1
)
pip install -r requirements.txt
python -m server.service %*
