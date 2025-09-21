import streamlit as st
from apps.dashboard.utils.auth import require_token
from apps.dashboard.utils.api_client import api_post
from apps.dashboard.utils.nav import render_nav

st.header("Настройки стратегии")
token = require_token()
render_nav(active="Настройки")
sku = st.text_input("Артикул (SKU)", value="SKU-IPH-13-128")
min_p = st.number_input("Мин. цена", value=260000.0, step=1000.0)
max_p = st.number_input("Макс. цена", value=300000.0, step=1000.0)
margin = st.number_input("Целевая маржа", value=0.2, step=0.05)
sens = st.number_input("Чувствительность", value=1.0, step=0.1)
if st.button("Сохранить"):
    ok = api_post("/strategy", token, json={
        "sku": sku,
        "min_price": min_p,
        "max_price": max_p,
        "target_margin": margin,
        "sensitivity": sens,
    })
    st.success("Сохранено" if ok.get("ok") else "Ошибка")
