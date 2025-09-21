import streamlit as st
from apps.dashboard.utils.auth import require_token
from apps.dashboard.utils.api_client import api_get
from apps.dashboard.utils.nav import render_nav

st.header("Обзор")
token = require_token()
render_nav(active="Обзор")
data = api_get("/dashboard/overview", token)

col1, col2, col3 = st.columns(3)
col1.metric("SKU в базе", f"{int(data.get('total_sku', 0))}")
col2.metric("Открытые алерты", f"{int(data.get('open_alerts', 0))}")
col3.metric("Продажи за 7 дней", f"{float(data.get('sales_units_7d', 0.0)):.0f}")

with st.expander("Сырые данные ответа", expanded=False):
    st.json(data)
