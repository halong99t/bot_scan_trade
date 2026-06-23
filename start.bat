@echo off
echo ========================================
echo   Binance Unified Bot ^& Scanner
echo ========================================
echo.

:: Check py launcher first, fallback to python
py --version >nul 2>&1
if errorlevel 1 (
    python --version >nul 2>&1
    if errorlevel 1 (
        echo [ERROR] Python hoac Py launcher chua duoc cai dat!
        pause
        exit /b 1
    ) else (
        set PYTHON_CMD=python
    )
) else (
    set PYTHON_CMD=py
)

:: Create virtual environment if needed
if not exist ".venv" (
    echo Dang tao virtual environment...
    %PYTHON_CMD% -m venv .venv
)

call .venv\Scripts\activate.bat

echo Dang tai va cap nhat cac thu vien...
pip install -q -r requirements.txt

echo.
echo Khoi chay may chu bot dang tai: http://localhost:8000
echo Nhan Ctrl+C de dung bot.
echo.
start "" http://localhost:8000
%PYTHON_CMD% main.py
pause
