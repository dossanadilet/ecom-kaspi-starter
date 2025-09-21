import streamlit as st
from apps.dashboard.utils.auth import require_token
from apps.dashboard.utils.api_client import api_get
from apps.dashboard.utils.nav import render_nav
import pandas as pd
import altair as alt

st.header("Карточка SKU")
token = require_token()
render_nav(active="Карточка SKU")
default_sku = st.session_state.get("quick_sku", "SKU-IPH-13-128")
sku = st.text_input("Артикул (SKU)", value=default_sku)
if st.button("Загрузить"):
    st.subheader("Сводка")
    summary = api_get(f"/products/{sku}/summary", token)
    feats = summary.get("features", [])
    if not feats:
        st.info("Нет витринных данных по SKU. Импортируйте снапшот или запустите ETL.")
    else:
        df = pd.DataFrame(feats)
        # Expect columns: date, competitor_min_price, competitor_avg_price, own_price, sales_units
        # Coerce types
        for c in ["competitor_min_price", "competitor_avg_price", "own_price", "sales_units"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        latest = df.sort_values("date").dropna(subset=["date"]).tail(1)
        c1, c2, c3 = st.columns(3)
        if not latest.empty:
            c1.metric("Мин. цена конкурентов", f"{latest.iloc[0].get('competitor_min_price', 0):,.0f}")
            c2.metric("Средняя цена конкурентов", f"{latest.iloc[0].get('competitor_avg_price', 0):,.0f}")
            c3.metric("Наша цена", f"{latest.iloc[0].get('own_price', 0):,.0f}")
        # Charts
        if "date" in df.columns:
            st.caption("Динамика цен")
            price_df = df[["date", "competitor_min_price", "competitor_avg_price", "own_price"]].melt("date", var_name="metric", value_name="price")
            price_chart = alt.Chart(price_df.dropna()).mark_line(point=True).encode(
                x="date:T", y=alt.Y("price:Q", title="Цена, ₸"), color=alt.Color("metric:N", title="Показатель")
            ).properties(height=300)
            st.altair_chart(price_chart, use_container_width=True)
            st.caption("Продажи (шт.)")
            if "sales_units" in df.columns:
                sales_chart = alt.Chart(df.dropna(subset=["sales_units"]))\
                    .mark_bar()\
                    .encode(x="date:T", y=alt.Y("sales_units:Q", title="Шт."))\
                    .properties(height=200)
                st.altair_chart(sales_chart, use_container_width=True)

    st.subheader("Прогноз спроса")
    fc = api_get(f"/products/{sku}/forecast", token)
    fc_list = fc.get("forecast", [])
    if not fc_list:
        st.info("Нет прогноза — запустите тренировки/инференс в Администрировании.")
    else:
        dff = pd.DataFrame(fc_list)
        dff["date"] = pd.to_datetime(dff["date"], errors="coerce")
        chart = alt.Chart(dff.dropna()).mark_line(point=True).encode(x="date:T", y=alt.Y("q:Q", title="Прогноз, шт."))
        st.altair_chart(chart, use_container_width=True)

    st.subheader("Быстрая рекомендация цены")
    base = float(latest.iloc[0].get("own_price", 0)) if "latest" in locals() and not latest.empty else 0.0
    trial = st.number_input("Пробная цена", value=base if base > 0 else 100000.0, step=1000.0)
    if st.button("Рассчитать рекомендацию"):
        reco = api_get(f"/products/{sku}/price-reco?trial_price={trial}", token)
        st.json(reco)
