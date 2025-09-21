import streamlit as st
from apps.dashboard.utils.auth import require_token
from apps.dashboard.utils.api_client import api_get, api_post, api_post_file
from apps.dashboard.utils.nav import render_nav

st.header("Администрирование")
token = require_token()
render_nav(active="Администрирование")

if st.button("Запустить ночной сценарий (демо)"):
    r = api_post("/admin/flow/nightly", token)
    st.success("Запланировано")

st.subheader("Алерты")
alerts = api_get("/alerts", token)
st.json(alerts)

st.subheader("Импорт снапшота (CSV)")
uploaded = st.file_uploader("Выберите market_snapshot.csv", type=["csv"])
if uploaded is not None and st.button("Импортировать"):
    res = api_post_file("/admin/import-snapshot", token, uploaded)
    st.success(f"Импорт: товаров создано {res.get('products_created',0)}, офферов {res.get('offers',0)}")

st.subheader("Обновить рекомендации (price_reco)")
if st.button("Обновить инференс"):
    r2 = api_post("/admin/flow/inference", token)
    st.success("Запланировано")

st.subheader("Telegram")
col1, col2 = st.columns(2)
with col1:
    if st.button("Тестовое сообщение"):
        try:
            r = api_post("/admin/tg/test", token)
            st.success("Отправлено" if r.get("ok") else "Не удалось отправить")
        except Exception as e:
            st.error(f"Ошибка: {e}")
with col2:
    if st.button("Отправить рекомендации (CSV)"):
        try:
            r = api_post("/admin/tg/send-recos", token)
            st.success(f"Отправлено {r.get('rows',0)} строк" if r.get("ok") else "Не удалось отправить")
        except Exception as e:
            st.error(f"Ошибка: {e}")

st.subheader("Ночной дайджест")
if st.button("Отправить дайджест сейчас"):
    try:
        r = api_post("/admin/tg/digest", token)
        st.success(f"Отправлено {r.get('rows',0)} строк" if r.get("ok") else "Не удалось отправить")
    except Exception as e:
        st.error(f"Ошибка: {e}")

st.subheader("Аномалии")
if st.button("Запустить анализ аномалий"):
    try:
        r = api_post("/admin/run-anomaly", token)
        st.success(f"Найдено {r.get('total',0)} аномалий")
    except Exception as e:
        st.error(f"Ошибка: {e}")
