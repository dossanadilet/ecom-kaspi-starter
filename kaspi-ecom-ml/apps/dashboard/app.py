import os
import sys
from pathlib import Path
import streamlit as st

# Ensure project root is on PYTHONPATH so `apps.*` imports work under Streamlit
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Prefer absolute imports so Streamlit can run scripts standalone
from apps.dashboard.utils.auth import login_and_store
from apps.dashboard.utils.api_client import api_get
from apps.dashboard.utils.nav import render_nav

st.set_page_config(page_title="Панель Kaspi E‑commerce", layout="wide")

st.title("Панель Kaspi E‑commerce (MVP)")

token = login_and_store()
if not token:
    st.stop()

render_nav(active="Обзор")
username = st.session_state.get("username", "пользователь")
st.success(f"Привет, {username}! 👋 Добро пожаловать в панель.")

st.markdown("Выберите раздел в левом меню: Обзор, Карточка SKU, Рекомендации, Ассортимент, Настройки, Администрирование.")

st.divider()
st.subheader("Быстрый доступ")

try:
    sku_rows = api_get("/products/sku-list?limit=20", token)
    popular_skus = [r.get("sku") for r in sku_rows] or [
        "SKU-IPH-13-128",
        "SKU-IPH-15-128",
        "SKU-RDM-13C-8-256",
        "SKU-SAM-S24-128",
        "SKU-RLM-11-128",
    ]
except Exception:
    popular_skus = [
        "SKU-IPH-13-128",
        "SKU-IPH-15-128",
        "SKU-RDM-13C-8-256",
        "SKU-SAM-S24-128",
        "SKU-RLM-11-128",
    ]

col1, col2, col3 = st.columns([2,1,1])
with col1:
    sel = st.selectbox("Популярные SKU", options=popular_skus, index=0)
with col2:
    if st.button("Открыть карточку"):
        st.session_state["quick_sku"] = sel
        try:
            st.switch_page("pages/02_SKU_Details.py")
        except Exception:
            st.info("Перейдите в раздел 'Карточка SKU' из меню.")
with col3:
    if st.button("Рекомендации по цене"):
        st.session_state["quick_sku"] = sel
        try:
            st.switch_page("pages/03_Recommendations.py")
        except Exception:
            st.info("Перейдите в раздел 'Рекомендации по цене' из меню.")
