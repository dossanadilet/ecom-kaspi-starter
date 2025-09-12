import streamlit as st
import pandas as pd
from pathlib import Path

from app.economics import CostInputs, landed_cost, min_price_for_margin, roi_on_turnover, profit_per_unit
from app.pricing import choose_price_grid
from app.forecast import price_to_demand_linear

st.set_page_config(page_title="Kaspi E-commerce MVP", layout="wide")
st.title("Kaspi E-commerce MVP — Автоматизированный магазин (прототип)")

st.markdown("""
Этот прототип работает без API Kaspi. 
1) Обнови CSV в папке `data/`.
2) Используй вкладки: Market Scan → Landed Cost → Forecast & Pricing → Inventory & KPI.
""")

tab1, tab2, tab3, tab4 = st.tabs(["Market Scan", "Landed Cost", "Forecast & Pricing", "Inventory & KPI"])

# Пути к данным
data_dir = Path(__file__).resolve().parent.parent / "data"
market_path = data_dir / "market_snapshot_example.csv"
costs_path  = data_dir / "costs_template.csv"
inv_path    = data_dir / "inventory_template.csv"

with tab1:
    st.header("Market Scan (примерные данные)")
    if market_path.exists():
        dfm = pd.read_csv(market_path)
        st.dataframe(dfm, use_container_width=True)
        st.caption("Это пример среза рынка. Позже подключим парсер/ETL (1 и 15 числа).")
    else:
        st.warning(f"Файл {market_path.name} не найден")

with tab2:
    st.header("Landed Cost калькулятор")
    if costs_path.exists():
        dfc = pd.read_csv(costs_path)
        st.dataframe(dfc, use_container_width=True)
        sku = st.selectbox("Выбери SKU", dfc["product_id"].tolist())
        row = dfc[dfc["product_id"]==sku].iloc[0].to_dict()
        c = CostInputs(
            purchase_cn=row["purchase_cn"],
            intl_ship=row["intl_ship"],
            customs=row["customs"],
            last_mile=row["last_mile"],
            pack=row["pack"],
            return_rate=row["return_rate"],
            mp_fee=row["mp_fee"],
            ads_alloc=row["ads_alloc"],
            overhead=row["overhead"],
        )
        c_land = landed_cost(c)
        col1, col2 = st.columns(2)
        col1.metric("Landed cost (тг/шт)", f"{c_land:,.0f}")
        pmin = min_price_for_margin(c_land, target_margin=0.2)
        col2.metric("Мин. цена при 20% марже", f"{pmin:,.0f}")
        st.caption("Целевую маржу и комиссию MP дальше учтём в ценообразовании.")
    else:
        st.warning(f"Файл {costs_path.name} не найден")

with tab3:
    st.header("Forecast & Pricing (демо)")
    if market_path.exists() and costs_path.exists():
        dfm = pd.read_csv(market_path)
        dfc = pd.read_csv(costs_path)
        merged = dfm.merge(dfc, on="product_id", how="inner")
        sku = st.selectbox("SKU для расчёта", merged["product_id"].tolist(), key="sku_fp")
        r = merged[merged["product_id"]==sku].iloc[0]

        st.subheader("Входные параметры")
        base_price = st.number_input("Текущая цена (p0)", value=float(r["price_med"]), step=10.0)
        base_q = st.number_input("Базовый спрос/неделю (оценка)", value=30.0, step=1.0)
        elasticity = st.slider("Эластичность (отрицательная)", min_value=-3.0, max_value=-0.1, value=-1.0, step=0.1)

        q_func = price_to_demand_linear(base_q=base_q, base_price=base_price, elasticity=elasticity)

        c = CostInputs(
            purchase_cn=r["purchase_cn"], intl_ship=r["intl_ship"], customs=r["customs"],
            last_mile=r["last_mile"], pack=r["pack"], return_rate=r["return_rate"],
            mp_fee=r["mp_fee"], ads_alloc=r["ads_alloc"], overhead=r["overhead"]
        )
        c_land = landed_cost(c)
        mp_fee = float(r["mp_fee"])

        best, grid = choose_price_grid(base_price, c_land, mp_fee, q_func)
        st.write("**Сетка цен (±3%)** — цена, ожидаемая прибыль/нед, прогноз спроса/нед:")
        res_df = pd.DataFrame(grid, columns=["price","profit_week","q_week"])
        st.dataframe(res_df.style.format({"price":"{:.0f}","profit_week":"{:.0f}","q_week":"{:.1f}"}), use_container_width=True)
        st.success(f"Рекомендованная цена: **{best[0]:.0f} ₸**; прибыль/нед: **{best[1]:.0f} ₸**; спрос: **{best[2]:.1f} шт**")
        st.caption("Дальше заменим на ML-прогноз (LightGBM) с реальными фичами.")
    else:
        st.warning("Загрузи market_snapshot_example.csv и costs_template.csv")

with tab4:
    st.header("Inventory & KPI (черновик)")
    if inv_path.exists():
        dfi = pd.read_csv(inv_path)
        st.dataframe(dfi, use_container_width=True)
        st.caption("В следующих версиях добавим ROP/EOQ, риск OOS и KPI-дашборд.")
    else:
        st.warning(f"Файл {inv_path.name} не найден")

