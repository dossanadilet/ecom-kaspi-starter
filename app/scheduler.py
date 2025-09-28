"""
Планировщик автоматических задач для Kaspi E-commerce MVP
Поддерживает:
- Автоматический сбор данных 3 раза в день
- Настраиваемые уведомления (ежедневно/еженедельно/каждые 2 недели/ежемесячно)
- Автоматическое обновление прогнозов и рекомендаций
"""

import schedule
import time
import threading
import logging
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Callable, Optional
import pandas as pd

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Импорты приложения
import sys
APP_DIR = Path(__file__).resolve().parent
ROOT_DIR = APP_DIR.parent
sys.path.extend([str(APP_DIR), str(ROOT_DIR)])

from etl.scrape_kaspi import collect
from notify import tg_send
from economics import CostInputs, landed_cost, profit_per_unit
from pricing import choose_price_grid
from forecast import price_to_demand_linear


class TaskScheduler:
    """Планировщик автоматических задач"""
    
    def __init__(self, config_path: str = "config/scheduler.json"):
        self.config_path = Path(config_path)
        self.data_dir = ROOT_DIR / "data"
        self.is_running = False
        self.scheduler_thread = None
        self.config = self.load_config()
        
        # Создаем необходимые директории
        self.config_path.parent.mkdir(exist_ok=True)
        (ROOT_DIR / "logs").mkdir(exist_ok=True)
    
    def load_config(self) -> dict:
        """Загрузка конфигурации планировщика"""
        default_config = {
            "data_collection": {
                "enabled": True,
                "times": ["08:00", "14:00", "20:00"],  # 3 раза в день
                "categories": ["smartphones", "laptops", "tablets"],
                "pages": 2,
                "max_items": 100
            },
            "notifications": {
                "forecast_frequency": "daily",  # daily, weekly, biweekly, monthly
                "enabled": True,
                "time": "09:00"
            },
            "telegram": {
                "enabled": True,
                "send_errors": True,
                "send_summaries": True
            },
            "pricing": {
                "auto_update": True,
                "margin_target": 0.2,
                "elasticity": -1.0
            }
        }
        
        if self.config_path.exists():
            try:
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                # Объединяем с дефолтными настройками
                for key, value in default_config.items():
                    if key not in config:
                        config[key] = value
                    elif isinstance(value, dict):
                        for subkey, subvalue in value.items():
                            if subkey not in config[key]:
                                config[key][subkey] = subvalue
                return config
            except Exception as e:
                logger.error(f"Ошибка загрузки конфигурации: {e}")
                return default_config
        else:
            self.save_config(default_config)
            return default_config
    
    def save_config(self, config: dict = None):
        """Сохранение конфигурации"""
        if config is None:
            config = self.config
        
        try:
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
            logger.info("Конфигурация сохранена")
        except Exception as e:
            logger.error(f"Ошибка сохранения конфигурации: {e}")
    
    def collect_market_data(self):
        """Сбор данных с рынка"""
        try:
            logger.info("Начинаю сбор данных с Kaspi...")
            
            all_items = []
            config = self.config["data_collection"]
            
            for category in config["categories"]:
                try:
                    # Базовый запрос по категории
                    items = collect(
                        query_text=category,
                        pages=config["pages"],
                        delay=1.0,
                        headful=False,
                        mode="category",
                        max_items=config["max_items"],
                        detail_limit=0,
                        category=category,
                        sort="popularity"
                    )
                    
                    all_items.extend(items)
                    logger.info(f"Собрано {len(items)} товаров из категории {category}")
                    
                except Exception as e:
                    logger.error(f"Ошибка сбора данных для категории {category}: {e}")
                    if self.config["telegram"]["send_errors"]:
                        tg_send(f"❌ Ошибка сбора данных ({category}): {str(e)[:200]}")
            
            if all_items:
                # Сохраняем данные
                df = pd.DataFrame([{
                    "product_id": item.product_id,
                    "dt": datetime.now().strftime("%Y-%m-%d"),
                    "price_min": getattr(item, 'price', 0) or getattr(item, 'min_price', 0) or 0,
                    "price_med": getattr(item, 'price', 0) or getattr(item, 'median_price', 0) or 0,
                    "sellers": getattr(item, 'sellers_count', 1) or 1,
                    "rating": item.rating or 0.0,
                    "reviews": item.reviews or 0,
                    "page_rank": getattr(item, 'position', 0) or getattr(item, 'rank', 0) or 0,
                    "title": item.title or "",
                    "brand": item.brand or "",
                    "category": item.category or "",
                    "url": item.url or ""
                } for item in all_items])
                
                # Обновляем market_snapshot
                snapshot_path = self.data_dir / "market_snapshot_example.csv"
                df.to_csv(snapshot_path, index=False, encoding='utf-8-sig')
                
                # Архивируем в папку daily
                daily_dir = self.data_dir / "daily"
                daily_dir.mkdir(exist_ok=True)
                daily_path = daily_dir / f"market_snapshot_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
                df.to_csv(daily_path, index=False, encoding='utf-8-sig')
                
                logger.info(f"Данные сохранены: {len(all_items)} товаров")
                
                if self.config["telegram"]["send_summaries"]:
                    summary_msg = (
                        f"📊 <b>Сбор данных завершен</b>\n"
                        f"Товаров собрано: <b>{len(all_items)}</b>\n"
                        f"Категорий: {', '.join(config['categories'])}\n"
                        f"Время: {datetime.now().strftime('%H:%M')}"
                    )
                    tg_send(summary_msg)
                
                return True
            else:
                logger.warning("Не удалось собрать данные")
                return False
                
        except Exception as e:
            logger.error(f"Критическая ошибка сбора данных: {e}")
            if self.config["telegram"]["send_errors"]:
                tg_send(f"🚨 Критическая ошибка сбора данных: {str(e)[:200]}")
            return False
    
    def generate_forecast_report(self):
        """Генерация и отправка прогноза"""
        try:
            logger.info("Генерирую прогноз...")
            
            # Загружаем данные
            market_path = self.data_dir / "market_snapshot_example.csv"
            costs_path = self.data_dir / "costs_template.csv"
            
            if not market_path.exists() or not costs_path.exists():
                logger.warning("Нет данных для генерации прогноза")
                return
            
            market_df = pd.read_csv(market_path)
            costs_df = pd.read_csv(costs_path)
            
            # Объединяем данные
            merged = market_df.merge(costs_df, on="product_id", how="inner")
            
            if merged.empty:
                logger.warning("Нет совпадающих товаров для прогноза")
                return
            
            # Генерируем рекомендации для топ товаров
            top_items = merged.head(10)
            recommendations = []
            
            for _, row in top_items.iterrows():
                try:
                    # Расчет полной себестоимости
                    costs = CostInputs(
                        purchase_cn=row["purchase_cn"],
                        intl_ship=row["intl_ship"],
                        customs=row["customs"],
                        last_mile=row["last_mile"],
                        pack=row["pack"],
                        return_rate=row["return_rate"],
                        mp_fee=row["mp_fee"],
                        ads_alloc=row["ads_alloc"],
                        overhead=row["overhead"]
                    )
                    
                    c_land = landed_cost(costs)
                    current_price = float(row["price_med"])
                    
                    # Простой прогноз спроса
                    base_demand = 30.0  # базовый спрос в неделю
                    elasticity = self.config["pricing"]["elasticity"]
                    q_func = price_to_demand_linear(base_demand, current_price, elasticity)
                    
                    # Выбор оптимальной цены
                    best, grid = choose_price_grid(current_price, c_land, row["mp_fee"], q_func)
                    
                    recommendations.append({
                        "sku": row["product_id"],
                        "title": row.get("title", "")[:50],
                        "current_price": current_price,
                        "recommended_price": best[0],
                        "expected_profit": best[1],
                        "expected_demand": best[2],
                        "price_change": ((best[0] - current_price) / current_price * 100)
                    })
                    
                except Exception as e:
                    logger.error(f"Ошибка расчета для товара {row['product_id']}: {e}")
                    continue
            
            if recommendations:
                # Формируем отчет
                report_lines = ["📈 <b>Прогноз и рекомендации</b>\n"]
                
                total_profit = sum(r["expected_profit"] for r in recommendations)
                avg_price_change = sum(abs(r["price_change"]) for r in recommendations) / len(recommendations)
                
                report_lines.append(f"Проанализировано: <b>{len(recommendations)}</b> товаров")
                report_lines.append(f"Ожидаемая прибыль/нед: <b>{total_profit:,.0f} ₸</b>")
                report_lines.append(f"Средн. изменение цены: <b>{avg_price_change:.1f}%</b>\n")
                
                # Топ-5 рекомендаций
                sorted_recs = sorted(recommendations, key=lambda x: x["expected_profit"], reverse=True)
                report_lines.append("<b>Топ-5 по прибыли:</b>")
                
                for i, rec in enumerate(sorted_recs[:5], 1):
                    change_emoji = "🔺" if rec["price_change"] > 0 else "🔻" if rec["price_change"] < 0 else "➡️"
                    report_lines.append(
                        f"{i}. {rec['title']} | "
                        f"{rec['recommended_price']:.0f}₸ {change_emoji}{abs(rec['price_change']):.1f}% | "
                        f"~{rec['expected_profit']:.0f}₸/нед"
                    )
                
                report_text = "\n".join(report_lines)
                
                # Отправляем отчет
                if tg_send(report_text):
                    logger.info("Прогноз отправлен в Telegram")
                else:
                    logger.error("Ошибка отправки прогноза")
                
                # Сохраняем CSV с рекомендациями
                rec_df = pd.DataFrame(recommendations)
                rec_path = self.data_dir / f"recommendations_{datetime.now().strftime('%Y%m%d')}.csv"
                rec_df.to_csv(rec_path, index=False, encoding='utf-8-sig')
                logger.info(f"Рекомендации сохранены: {rec_path}")
                
        except Exception as e:
            logger.error(f"Ошибка генерации прогноза: {e}")
            if self.config["telegram"]["send_errors"]:
                tg_send(f"❌ Ошибка генерации прогноза: {str(e)[:200]}")
    
    def setup_schedule(self):
        """Настройка расписания задач"""
        schedule.clear()
        
        # Сбор данных 3 раза в день
        if self.config["data_collection"]["enabled"]:
            for time_str in self.config["data_collection"]["times"]:
                schedule.every().day.at(time_str).do(self.collect_market_data)
                logger.info(f"Запланирован сбор данных в {time_str}")
        
        # Уведомления по расписанию
        if self.config["notifications"]["enabled"]:
            freq = self.config["notifications"]["forecast_frequency"]  # Изменено с "frequency" на "forecast_frequency"
            time_str = self.config["notifications"]["time"]
            
            if freq == "daily":
                schedule.every().day.at(time_str).do(self.generate_forecast_report)
            elif freq == "weekly":
                schedule.every().monday.at(time_str).do(self.generate_forecast_report)
            elif freq == "biweekly":
                schedule.every(2).weeks.at(time_str).do(self.generate_forecast_report)
            elif freq == "monthly":
                schedule.every(4).weeks.at(time_str).do(self.generate_forecast_report)
            
            logger.info(f"Запланированы уведомления: {freq} в {time_str}")
    
    def start(self):
        """Запуск планировщика"""
        if self.is_running:
            logger.warning("Планировщик уже запущен")
            return
        
        self.setup_schedule()
        self.is_running = True
        
        def run_scheduler():
            logger.info("Планировщик запущен")
            try:
                while self.is_running:
                    schedule.run_pending()
                    time.sleep(60)  # Проверяем каждую минуту
            except Exception as e:
                logger.error(f"Ошибка планировщика: {e}")
            finally:
                logger.info("Планировщик остановлен")
        
        self.scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        self.scheduler_thread.start()
        
        # Отправляем уведомление о запуске
        if self.config["telegram"]["enabled"]:
            msg = (
                f"🚀 <b>Планировщик запущен</b>\n"
                f"Сбор данных: {', '.join(self.config['data_collection']['times'])}\n"
                f"Уведомления: {self.config['notifications']['frequency']} в {self.config['notifications']['time']}"
            )
            tg_send(msg)
    
    def stop(self):
        """Остановка планировщика"""
        if not self.is_running:
            logger.warning("Планировщик не запущен")
            return
        
        self.is_running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)
        
        logger.info("Планировщик остановлен")
        
        if self.config["telegram"]["enabled"]:
            tg_send("⏹️ Планировщик остановлен")
    
    def get_status(self) -> dict:
        """Получение статуса планировщика"""
        return {
            "running": self.is_running,
            "next_jobs": [str(job) for job in schedule.jobs],
            "config": self.config,
            "last_run": getattr(self, 'last_run', None)
        }
    
    def update_config(self, new_config: dict):
        """Обновление конфигурации"""
        self.config.update(new_config)
        self.save_config()
        
        if self.is_running:
            self.setup_schedule()
        
        logger.info("Конфигурация обновлена")


# Глобальный экземпляр планировщика
scheduler_instance = None

def get_scheduler() -> TaskScheduler:
    """Получение экземпляра планировщика (синглтон)"""
    global scheduler_instance
    if scheduler_instance is None:
        scheduler_instance = TaskScheduler()
    return scheduler_instance


if __name__ == "__main__":
    # Тестовый запуск
    scheduler = get_scheduler()
    
    print("Тестирую сбор данных...")
    scheduler.collect_market_data()
    
    print("Тестирую генерацию прогноза...")
    scheduler.generate_forecast_report()
    
    print("Запускаю планировщик на 30 секунд...")
    scheduler.start()
    time.sleep(30)
    scheduler.stop()