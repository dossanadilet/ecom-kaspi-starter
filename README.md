# Kaspi E-commerce Starter (MVP)

Автоматизированный магазин на Kaspi.kz с планировщиком задач и уведомлениями.
Содержит Streamlit-приложение, автоматический парсинг, ML-прогнозы и Telegram-уведомления.

## 🚀 Быстрый старт

### 1) Установка зависимостей
```bash
pip install -r requirements.txt
```

### 2) Настройка Telegram Bot (опционально)

#### Способ 1: Через secrets.toml (рекомендуется для Streamlit)
```toml
# Файл: .streamlit/secrets.toml
TG_BOT_TOKEN = "1234567890:AAF7qR5jD8mZ1Gh2Tk3LnOp4Qs9XvUw1ABC"
TG_CHAT_ID = "-1001234567890"
```

#### Способ 2: Через .env файл
```bash
# Файл: .env
TG_BOT_TOKEN=1234567890:AAF7qR5jD8mZ1Gh2Tk3LnOp4Qs9XvUw1ABC
TG_CHAT_ID=-1001234567890
```

#### Способ 3: Переменные окружения
```bash
set TG_BOT_TOKEN=1234567890:AAF7qR5jD8mZ1Gh2Tk3LnOp4Qs9XvUw1ABC
set TG_CHAT_ID=-1001234567890
```

#### Как получить токен и Chat ID:
1. **Создайте бота**: Напишите @BotFather → `/newbot` → следуйте инструкциям
2. **Получите Chat ID**: 
   - Добавьте бота в чат
   - Напишите боту сообщение
   - Перейдите: `https://api.telegram.org/bot<TOKEN>/getUpdates`
   - Найдите `"chat":{"id":-1234567890}`

### 3) Запуск веб-приложения
```bash
streamlit run app/main.py
```

### 4) Запуск автоматического планировщика
```bash
# Windows
start_service.bat

# Или вручную
python service.py start
```

## 🤖 Автоматизация

### Планировщик задач
- **Сбор данных**: 3 раза в день (08:00, 14:00, 20:00)
- **Прогнозы**: Настраиваемая частота (ежедневно/еженедельно/каждые 2 недели/ежемесячно)
- **Уведомления**: Автоматическая отправка в Telegram

### Управление через веб-интерфейс
1. Откройте вкладку "Автоматизация" в Streamlit
2. Настройте расписание сбора данных
3. Выберите частоту уведомлений
4. Запустите/остановите планировщик

### Команды управления сервисом
```bash
python service.py start    # Запуск
python service.py stop     # Остановка  
python service.py status   # Статус
python service.py test     # Тестирование
```

## 📊 Функциональность

### Основные возможности
- 🔄 **Автоматический парсинг** Kaspi.kz (смартфоны, ноутбуки, планшеты)
- 💰 **Калькулятор себестоимости** с учетом доставки и пошлин
- 📈 **Прогнозирование спроса** и оптимизация цен
- 📦 **Управление запасами** (ROP, Safety Stock, EOQ)
- 🤖 **Планировщик задач** с настраиваемым расписанием
- 📱 **Telegram уведомления** по результатам анализа

### Структура данных
- `data/market_snapshot_example.csv` - текущие данные рынка
- `data/costs_template.csv` - структура затрат
- `data/inventory_template.csv` - остатки и параметры закупки
- `data/daily/` - архив ежедневных сборов
- `config/scheduler.json` - настройки планировщика

## ⚙️ Конфигурация

### Настройки планировщика (`config/scheduler.json`)
```json
{
  "data_collection": {
    "enabled": true,
    "times": ["08:00", "14:00", "20:00"],
    "categories": ["smartphones", "laptops", "tablets"],
    "pages": 2,
    "max_items": 100
  },
  "notifications": {
    "forecast_frequency": "daily",
    "enabled": true,
    "time": "09:00"
  }
}
```

### Частота уведомлений
- `daily` - Ежедневно
- `weekly` - Еженедельно (по понедельникам)
- `biweekly` - Каждые 2 недели
- `monthly` - Ежемесячно

## 📝 Логирование

### Файлы логов
- `logs/scheduler.log` - логи планировщика
- `logs/service.log` - логи сервиса
- `logs/xhr_resp_*.json` - дампы XHR-запросов парсера

### Мониторинг
- Статус через веб-интерфейс
- Telegram уведомления об ошибках
- Проверка через `check_status.bat`

## 🔧 Troubleshooting

### Если планировщик не запускается
1. Проверьте `pip install schedule`
2. Убедитесь что `config/scheduler.json` существует
3. Проверьте права на запись в папку `logs/`

### Если не приходят уведомления
1. Проверьте настройки Telegram в `streamlit/secrets.toml`
2. Убедитесь что бот добавлен в чат
3. Проверьте логи на ошибки отправки

### Если парсер не работает
1. Установите Playwright: `playwright install chromium`
2. Проверьте подключение к интернету
3. Увеличьте задержки в настройках
