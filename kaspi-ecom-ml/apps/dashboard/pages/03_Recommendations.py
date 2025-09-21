import streamlit as st
from apps.dashboard.utils.auth import require_token
from apps.dashboard.utils.api_client import api_get, api_post
from apps.dashboard.utils.nav import render_nav
import pandas as pd
import altair as alt

st.header("Рекомендации по цене")
token = require_token()
render_nav(active="Рекомендации по цене")
default_sku = st.session_state.get("quick_sku", "SKU-IPH-13-128")
sku = st.text_input("Артикул (SKU)", value=default_sku)
trial = st.number_input("Пробная цена", value=275000.0, step=1000.0)
if st.button("Получить рекомендацию"):
    data = api_get(f"/products/{sku}/price-reco?trial_price={trial}", token)
    st.json(data)

st.divider()
st.subheader("Сводка по всем SKU")
colA, colB, colC = st.columns([1,1,1])
with colA:
    lim = st.number_input("Кол-во строк", min_value=10, max_value=1000, value=100, step=10)
with colB:
    min_profit = st.number_input("Мин. прибыль", value=0.0, step=1000.0, help="Фильтр по ожидаемой прибыли")
with colC:
    qty_filter = st.number_input("Мин. объём", value=0.0, step=1.0, help="Фильтр по ожидаемому спросу")
colR1, colR2, colR3 = st.columns([1,1,1])
with colR1:
    refresh = st.button("Обновить список")
with colR2:
    export = st.button("Скачать CSV")
with colR3:
    send_tg = st.button("Отправить в Telegram")

if refresh or export or send_tg:
    items = api_get(f"/products/recommendations?limit={int(lim)}", token)
    if not items:
        st.info("Рекомендаций пока нет. Запустите инференс в разделе 'Администрирование'.")
    else:
        df = pd.DataFrame(items)
        df["expected_profit"] = pd.to_numeric(df.get("expected_profit"), errors="coerce")
        df["expected_qty"] = pd.to_numeric(df.get("expected_qty"), errors="coerce")
        df = df[(df["expected_profit"].fillna(0) >= min_profit) & (df["expected_qty"].fillna(0) >= qty_filter)]
        if df.empty:
            st.info("Нет рекомендаций по заданным фильтрам.")
        else:
            st.dataframe(df, use_container_width=True)
            csv = df.to_csv(index=False).encode("utf-8")
            if export:
                st.download_button("Скачать CSV", data=csv, file_name="price_recommendations.csv", mime="text/csv")
            if send_tg:
                try:
                    r = api_post("/admin/tg/send-recos", token)
                    st.success(f"Телеграм: отправлено {r.get('rows',0)} строк" if r.get("ok") else "Не удалось отправить")
                except Exception as e:
                    st.error(f"Ошибка: {e}")

            st.subheader("Распределение ожидаемой прибыли")
            hist = alt.Chart(df.dropna(subset=["expected_profit"])).mark_bar().encode(
                x=alt.X("expected_profit:Q", bin=alt.Bin(maxbins=40), title="Прибыль, ₸"),
                y="count()"
            )
            st.altair_chart(hist, use_container_width=True)
