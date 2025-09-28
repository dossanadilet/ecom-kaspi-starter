import os
import requests
from pathlib import Path

def _load_env_file():
    """Загрузка переменных из .env файла"""
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        try:
            with open(env_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        if key and value and not os.getenv(key):
                            os.environ[key] = value
        except Exception as e:
            pass  # Игнорируем ошибки загрузки .env

# Загружаем .env при импорте
_load_env_file()

def _get_secret(name: str, default: str = "") -> str:
    # Работает и локально, и в Streamlit Cloud
    try:
        import streamlit as st
        return st.secrets.get(name, os.getenv(name, default))
    except Exception:
        return os.getenv(name, default)

def tg_send(text: str, parse_mode: str = "HTML") -> bool:
    token = _get_secret("TG_BOT_TOKEN")
    chat_id = _get_secret("TG_CHAT_ID")
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    resp = requests.post(url, data={
        "chat_id": chat_id,
        "text": text[:4000],   # лимит Telegram
        "parse_mode": parse_mode,
        "disable_web_page_preview": True
    }, timeout=15)
    return 200 <= resp.status_code < 300

def tg_test_connection() -> dict:
    """Тестирует подключение к Telegram боту"""
    token = _get_secret("TG_BOT_TOKEN")
    chat_id = _get_secret("TG_CHAT_ID")
    
    result = {
        "configured": False,
        "token_valid": False,
        "chat_accessible": False,
        "error": None
    }
    
    if not token or not chat_id:
        result["error"] = "TG_BOT_TOKEN или TG_CHAT_ID не настроены"
        return result
    
    result["configured"] = True
    
    try:
        # Проверяем токен
        url = f"https://api.telegram.org/bot{token}/getMe"
        resp = requests.get(url, timeout=10)
        
        if resp.status_code == 200:
            result["token_valid"] = True
            bot_info = resp.json()
            
            # Проверяем доступ к чату
            test_msg = "🔧 Тест подключения - настройка завершена успешно!"
            if tg_send(test_msg):
                result["chat_accessible"] = True
            else:
                result["error"] = "Не удается отправить сообщение в чат. Проверьте CHAT_ID и права бота."
        else:
            result["error"] = f"Неверный токен бота (HTTP {resp.status_code})"
            
    except Exception as e:
        result["error"] = f"Ошибка подключения: {str(e)}"
    
    return result

def get_telegram_status() -> str:
    """Возвращает статус настройки Telegram"""
    token = _get_secret("TG_BOT_TOKEN")
    chat_id = _get_secret("TG_CHAT_ID")
    
    if not token or not chat_id or token == "your_telegram_bot_token_here" or chat_id == "your_telegram_chat_id_here":
        return "❌ Не настроен"
    else:
        return "✅ Настроен"
