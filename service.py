#!/usr/bin/env python3
"""
Сервис для запуска планировщика задач Kaspi E-commerce
Может работать как Windows Service или обычный процесс
"""

import sys
import time
import signal
from pathlib import Path

# Добавляем пути
APP_DIR = Path(__file__).resolve().parent / "app"
ROOT_DIR = Path(__file__).resolve().parent
sys.path.extend([str(APP_DIR), str(ROOT_DIR)])

from app.scheduler import get_scheduler
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SchedulerService:
    def __init__(self):
        self.scheduler = get_scheduler()
        self.running = False
    
    def signal_handler(self, signum, frame):
        """Обработчик сигналов для graceful shutdown"""
        logger.info(f"Получен сигнал {signum}, останавливаюсь...")
        self.stop()
        sys.exit(0)
    
    def start(self):
        """Запуск сервиса"""
        logger.info("Запуск Kaspi E-commerce Scheduler Service")
        
        # Регистрируем обработчики сигналов
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)
        
        try:
            # Запускаем планировщик
            self.scheduler.start()
            self.running = True
            
            logger.info("Сервис запущен, ожидаем команд...")
            
            # Основной цикл
            while self.running:
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"Ошибка сервиса: {e}")
        finally:
            self.stop()
    
    def stop(self):
        """Остановка сервиса"""
        if self.running:
            logger.info("Останавливаю сервис...")
            self.scheduler.stop()
            self.running = False
            logger.info("Сервис остановлен")

def main():
    """Главная функция"""
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "start":
            service = SchedulerService()
            service.start()
        
        elif command == "stop":
            # Для остановки можно использовать файл-флаг
            stop_file = Path("logs/stop_service")
            stop_file.touch()
            print("Команда остановки отправлена")
        
        elif command == "status":
            scheduler = get_scheduler()
            status = scheduler.get_status()
            print(f"Планировщик: {'Запущен' if status['running'] else 'Остановлен'}")
            print(f"Задач запланировано: {len(status['next_jobs'])}")
            if status['next_jobs']:
                print("Следующие задачи:")
                for job in status['next_jobs']:
                    print(f"  - {job}")
        
        elif command == "test":
            print("Тестирую компоненты...")
            scheduler = get_scheduler()
            
            print("1. Тест сбора данных...")
            success = scheduler.collect_market_data()
            print(f"   Результат: {'Успех' if success else 'Ошибка'}")
            
            print("2. Тест генерации прогноза...")
            scheduler.generate_forecast_report()
            print("   Результат: Выполнено")
            
        else:
            print(f"Неизвестная команда: {command}")
            print("Доступные команды: start, stop, status, test")
    
    else:
        print("Kaspi E-commerce Scheduler Service")
        print("Использование: python service.py [команда]")
        print("Команды:")
        print("  start  - Запустить сервис")
        print("  stop   - Остановить сервис")
        print("  status - Показать статус")
        print("  test   - Тестировать компоненты")

if __name__ == "__main__":
    # Создаем необходимые директории
    (Path(__file__).resolve().parent / "logs").mkdir(exist_ok=True)
    (Path(__file__).resolve().parent / "config").mkdir(exist_ok=True)
    
    main()