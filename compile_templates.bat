echo off
SETLOCAL ENABLEEXTENSIONS ENABLEDELAYEDEXPANSION
cd "D:\work\Projects\taskserver-source\"
c:\python26\python.exe cheetah-compile -R --nobackup

pause

rem echo.
rem echo window will automatically be closed in 3 seconds...
rem set /a paused=%time:~7,-3%+3
rem if %paused% GEQ 10 set /a paused=!paused!-10
rem :check
rem set /a ttt=%time:~7,-3%
rem if not %paused%==%ttt% goto :check
