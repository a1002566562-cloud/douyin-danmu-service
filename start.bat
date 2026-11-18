@echo off
chcp 65001 >nul
cd /d "%~dp0"
if "%~1"=="" (
  echo.
  echo  直播弹幕采集服务 - 国内抖音 / 国外 TikTok
  echo.
  echo  抖音    : start.bat 79423499229
  echo  TikTok  : start.bat @user70367980266313
  echo  整条链接: start.bat https://www.tiktok.com/@user/live
  echo.
  echo  可选参数（直接跟在后面）:
  echo    --forward http://127.0.0.1:8010/human   转发给星耀数字人工作台
  echo    --platform tiktok                       强制平台
  echo    --no-log                                不写 JSONL
  echo.
  pause
  exit /b 1
)
pip install -r requirements.txt
python -m server.service %*
pause
