@echo off
echo Запуск Kaspi E-commerce Scheduler Service...
echo.

REM Проверяем Python
python --version >nul 2>&1
if errorlevel 1 (
    echo Ошибка: Python не найден в PATH
    pause
    exit /b 1
)

REM Устанавливаем зависимости если нужно
if not exist "logs\deps_installed.flag" (
    echo Устанавливаю зависимости...
    pip install -r requirements.txt
    if errorlevel 1 (
        echo Ошибка установки зависимостей
        pause
        exit /b 1
    )
    echo. > "logs\deps_installed.flag"
)

REM Запускаем сервис
echo Сервис запущен. Для остановки нажмите Ctrl+C
python service.py start

pause