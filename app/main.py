# --- robust sys.path setup (добавляем папку app/ и корень репо) ---
import sys
from pathlib import Path
APP_DIR = Path(__file__).resolve().parent
ROOT_DIR = APP_DIR.parent
for p in (str(APP_DIR), str(ROOT_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)
# -------------------------------------------------------------------

import streamlit as st
import pandas as pd
from notify import tg_send, tg_test_connection, get_telegram_status

# локальные импорты (без префикса app.)
from economics import (
    CostInputs, landed_cost, min_price_for_margin,
    roi_on_turnover, profit_per_unit,
    reorder_point, safety_stock, eoq, z_value_for_service
)

from pricing   import choose_price_grid
from forecast  import price_to_demand_linear
from scheduler import get_scheduler

def read_csv_smart(path):
    # Пытаемся разными кодировками; игнорируем «битые» строки, если такие есть
    encodings = ["utf-8", "utf-8-sig", "cp1251", "latin1"]
    last_err = None
    for enc in encodings:
        try:
            return pd.read_csv(path, encoding=enc, on_bad_lines="skip")
        except UnicodeDecodeError as e:
            last_err = e
            continue
    # Если все попытки провалились — пробрасываем последнюю ошибку
    raise last_err

st.set_page_config(page_title="Kaspi E-commerce MVP", layout="wide")
st.title("Kaspi E-commerce MVP — Автоматизированный магазин (прототип)")

st.markdown("""
Этот прототип работает без API Kaspi. 
1) Обнови CSV в папке `data/`.
2) Используй вкладки: Анализ рынка → Полная себестоимость (с учетом доставки, пошлин и т. д.) → Прогноз и ценообразование →Запасы и ключевые показатели (KPI).
""")

tab1, tab2, tab3, tab4, tab5 = st.tabs(["Анализ рынка", "Полная себестоимость (с учетом доставки, пошлин и т. д.)", "Прогноз и ценообразование", "Запасы и ключевые показатели (KPI)", "Автоматизация"])

# Пути к данным
data_dir = Path(__file__).resolve().parent.parent / "data"
market_path = data_dir / "market_snapshot_example.csv"
costs_path  = data_dir / "costs_template.csv"
inv_path    = data_dir / "inventory_template.csv"

with tab1:
    st.header("Анализ рынка (примерные данные)")
    if market_path.exists():
        dfm = read_csv_smart(market_path)
        st.dataframe(dfm, use_container_width=True)
        st.caption("Это пример среза рынка. Позже подключим парсер/ETL (1 и 15 числа).")
    else:
        st.warning(f"Файл {market_path.name} не найден")

with tab2:
    st.header("Калькулятор полной себестоимости (с учетом доставки, пошлин и т. д.)")
    if costs_path.exists():
        dfc = read_csv_smart(costs_path)
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
        col1.metric("Полная себестоимость (с учетом доставки, пошлин и т. д.) (тг/шт)", f"{c_land:,.0f}")
        pmin = min_price_for_margin(c_land, target_margin=0.2)
        col2.metric("Мин. цена при 20% марже", f"{pmin:,.0f}")
        st.caption("Целевую маржу и комиссию MP дальше учтём в ценообразовании.")
    else:
        st.warning(f"Файл {costs_path.name} не найден")

with tab3:
    st.header("Прогноз и ценообразование (демо)")
    if market_path.exists() and costs_path.exists():
        dfm = read_csv_smart(market_path)
        dfc = read_csv_smart(costs_path)
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

        # Обоснование решения
        best_price, best_profit, best_q = best
        reason_lines = [
            f"Базовая цена p0 = {base_price:.0f} ₸",
            f"Landed cost = {c_land:.0f} ₸; комиссия MP = {mp_fee*100:.1f}%",
            f"Эластичность спроса = {elasticity:.2f}; базовый спрос/нед = {base_q:.1f} шт",
            f"Выбрана цена {best_price:.0f} ₸ → прибыль/нед ~ {best_profit:.0f} ₸ при спросе ~ {best_q:.1f} шт"
        ]
        st.info("Причина выбора цены:\n- " + "\n- ".join(reason_lines))
        
        # Экспорт прайса в CSV (на одну выбранную SKU)
        price_export_df = pd.DataFrame([{
            "product_id": r["product_id"],
            "new_price": round(best_price, 2),
            "explain": "; ".join(reason_lines)
        }])
        st.download_button(
            "📥 Скачать price_export.csv (только выбранная SKU)",
            data=price_export_df.to_csv(index=False).encode("utf-8"),
            file_name="price_export.csv",
            mime="text/csv"
        )

        
        st.write("**Сетка цен (±3%)** — цена, ожидаемая прибыль/нед, прогноз спроса/нед:")
        res_df = pd.DataFrame(grid, columns=["price","profit_week","q_week"])
        st.dataframe(res_df.style.format({"price":"{:.0f}","profit_week":"{:.0f}","q_week":"{:.1f}"}), use_container_width=True)
        st.success(f"Рекомендованная цена: **{best[0]:.0f} ₸**; прибыль/нед: **{best[1]:.0f} ₸**; спрос: **{best[2]:.1f} шт**")
        st.caption("Дальше заменим на ML-прогноз (LightGBM) с реальными фичами.")
        
        # --- Telegram notify button (Pricing) ---
        if st.button("Отправить рекомендацию цены в Telegram", key="notify_price_one"):
            sku_id = str(r["product_id"])
            best_price, best_profit, best_q = best
            msg = (
                f"🧮 <b>Pricing</b>\n"
                f"SKU: <code>{sku_id}</code>\n"
                f"Рекомендуемая цена: <b>{best_price:.0f} ₸</b>\n"
                f"Ожидаемый профит/нед: ~{best_profit:.0f} ₸ при спросе ≈{best_q:.1f}\n"
                f"Параметры: p0={base_price:.0f}, landed={c_land:.0f}, "
                f"fee={mp_fee*100:.0f}%, эласт={elasticity:.2f}, базовый спрос={base_q:.1f}/нед"
            )
            ok = tg_send(msg)
            st.toast("Ушло в Telegram ✅" if ok else "Не удалось отправить ❌")

        
    else:
        st.warning("Загрузи market_snapshot_example.csv и costs_template.csv")

with tab4:
    st.header("Запасы и ключевые показатели (KPI) (черновик)")
    if inv_path.exists():
        dfi = read_csv_smart(inv_path)

        st.subheader("Расчёт точки заказа (ROP), страхового запаса и EOQ")

        if market_path.exists():
            dfm = read_csv_smart(market_path)
        else:
            dfm = None
        
        if costs_path.exists():
            dfc = read_csv_smart(costs_path)
        else:
            dfc = None
        
        # Выбор SKU
        sku_inv = st.selectbox("SKU для расчёта закупа", dfi["product_id"].tolist(), key="sku_inv")
        row_inv = dfi[dfi["product_id"]==sku_inv].iloc[0].to_dict()
        
        # Прокинем расходы и комиссию (если есть)
        if dfc is not None and sku_inv in dfc["product_id"].values:
            row_cost = dfc[dfc["product_id"]==sku_inv].iloc[0].to_dict()
            c_obj = CostInputs(
                purchase_cn=row_cost["purchase_cn"],
                intl_ship=row_cost["intl_ship"],
                customs=row_cost["customs"],
                last_mile=row_cost["last_mile"],
                pack=row_cost["pack"],
                return_rate=row_cost["return_rate"],
                mp_fee=row_cost["mp_fee"],
                ads_alloc=row_cost["ads_alloc"],
                overhead=row_cost["overhead"],
            )
            c_land_inv = landed_cost(c_obj)
        else:
            c_obj, c_land_inv = None, 0.0
        
        colA, colB, colC = st.columns(3)
        weekly_mean = colA.number_input("Прогноз спроса/нед (шт)", value=30.0, step=1.0)
        weekly_sigma = colB.number_input("Стд. отклонение/нед (шт)", value=8.0, step=1.0)
        service = colC.selectbox("Уровень сервиса", [0.90, 0.95, 0.97, 0.98, 0.99], index=1)
        
        LT = int(row_inv["lead_time_days"])
        R  = int(row_inv["review_period_days"])
        on_hand  = int(row_inv["on_hand"])
        on_order = int(row_inv.get("on_order", 0))
        
        rop = reorder_point(weekly_mean, weekly_sigma, LT, R, service)
        ss  = safety_stock(weekly_sigma, LT, R, service)
        
        need_qty = max(0.0, rop - (on_hand + on_order))
        rec_qty = int(round(need_qty))
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Страховой запас (шт)", f"{ss:.0f}")
        col2.metric("Точка повторного заказа ROP (шт)", f"{rop:.0f}")
        col3.metric("Рекомендуемый заказ (шт)", f"{rec_qty}")
        
        st.caption(f"LT={LT} дн, Review={R} дн; On-hand={on_hand}, On-order={on_order}; Z≈{z_value_for_service(service):.2f}")
        
        # --- Telegram notify button ---
        if st.button("Отправить рекомендацию закупа в Telegram", key="notify_po_one"):
            msg = (
                f"📦 <b>Закуп</b>\n"
                f"SKU: <code>{sku_inv}</code>\n"
                f"ROP={rop:.0f}, SS={ss:.0f}, рекомендую заказать: <b>{rec_qty} шт</b>\n"
                f"On-hand={on_hand}, On-order={on_order}, LT={LT} д, Review={R} д"
            )
            ok = tg_send(msg)
            st.toast("Ушло в Telegram ✅" if ok else "Не удалось отправить ❌")
        
        # EOQ (по желанию) — оценка годового спроса = weekly_mean*52
        with st.expander("EOQ (экономический размер заказа)", expanded=False):
            D_annual = st.number_input("Годовой спрос, шт/год", value=float(weekly_mean*52), step=50.0)
            S_order  = st.number_input("Стоимость оформления заказа S (тг/заказ)", value=20000.0, step=1000.0)
            H_hold   = st.number_input("Годовая стоимость хранения H (тг/шт/год)", value= c_land_inv*0.20 if c_land_inv else 100.0, step=10.0)
            eoq_qty  = eoq(D_annual, S_order, H_hold)
            st.write(f"EOQ ≈ **{eoq_qty:.0f} шт**")
        
        # Экспорт purchase_list.csv
        purchase_df = pd.DataFrame([{
            "product_id": sku_inv,
            "recommended_qty": rec_qty,
            "on_hand": on_hand,
            "on_order": on_order,
            "LT_days": LT,
            "review_days": R,
            "weekly_mean": weekly_mean,
            "weekly_sigma": weekly_sigma,
            "service_level": service,
            "ROP": int(round(rop)),
            "SafetyStock": int(round(ss)),
            "EOQ": int(round(eoq_qty)) if 'eoq_qty' in locals() else "",
        }])
        st.download_button(
            "📥 Скачать purchase_list.csv (рекомендация закупа)",
            data=purchase_df.to_csv(index=False).encode("utf-8"),
            file_name="purchase_list.csv",
            mime="text/csv"
        )

        
        st.dataframe(dfi, use_container_width=True)
        st.caption("В следующих версиях добавим ROP/EOQ, риск OOS и KPI-дашборд.")
    else:
        st.warning(f"Файл {inv_path.name} не найден")

with tab5:
    st.header("🤖 Автоматизация и планировщик задач")
    
    scheduler = get_scheduler()
    status = scheduler.get_status()
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if status["running"]:
            st.success("✅ Планировщик запущен")
            if st.button("⏹️ Остановить", type="secondary"):
                scheduler.stop()
                st.rerun()
        else:
            st.error("⏸️ Планировщик остановлен")
            if st.button("▶️ Запустить", type="primary"):
                scheduler.start()
                st.rerun()
    
    with col2:
        st.metric("Запланированных задач", len(status["next_jobs"]))
    
    with col3:
        if st.button("🔄 Тест сбора данных"):
            with st.spinner("Собираю данные..."):
                success = scheduler.collect_market_data()
                if success:
                    st.success("Данные собраны успешно!")
                else:
                    st.error("Ошибка сбора данных")
    
    # Настройки планировщика
    st.subheader("⚙️ Настройки")
    
    config = status["config"]
    
    with st.form("scheduler_config"):
        st.write("**Сбор данных**")
        col1, col2 = st.columns(2)
        
        with col1:
            data_enabled = st.checkbox("Включить автосбор", value=config["data_collection"]["enabled"])
            categories = st.multiselect(
                "Категории для сбора",
                ["smartphones", "laptops", "tablets", "gaming", "home_appliances"],
                default=config["data_collection"]["categories"]
            )
        
        with col2:
            pages = st.number_input("Страниц для сбора", min_value=1, max_value=5, value=config["data_collection"]["pages"])
            max_items = st.number_input("Макс. товаров", min_value=50, max_value=500, value=config["data_collection"]["max_items"])
        
        st.write("**Время сбора (3 раза в день)**")
        time_cols = st.columns(3)
        current_times = config["data_collection"]["times"]
        
        with time_cols[0]:
            time1 = st.time_input("Утром", value=pd.to_datetime(current_times[0]).time() if len(current_times) > 0 else pd.to_datetime("08:00").time())
        with time_cols[1]:
            time2 = st.time_input("Днём", value=pd.to_datetime(current_times[1]).time() if len(current_times) > 1 else pd.to_datetime("14:00").time())
        with time_cols[2]:
            time3 = st.time_input("Вечером", value=pd.to_datetime(current_times[2]).time() if len(current_times) > 2 else pd.to_datetime("20:00").time())
        
        st.write("**Уведомления и прогнозы**")
        col1, col2 = st.columns(2)
        
        with col1:
            notifications_enabled = st.checkbox("Включить уведомления", value=config["notifications"]["enabled"])
            forecast_freq = st.selectbox(
                "Частота прогнозов",
                ["daily", "weekly", "biweekly", "monthly"],
                index=["daily", "weekly", "biweekly", "monthly"].index(config["notifications"]["forecast_frequency"]),
                format_func=lambda x: {"daily": "Ежедневно", "weekly": "Еженедельно", "biweekly": "Каждые 2 недели", "monthly": "Ежемесячно"}[x]
            )
        
        with col2:
            notification_time = st.time_input("Время уведомлений", value=pd.to_datetime(config["notifications"]["time"]).time())
            elasticity = st.slider("Эластичность спроса", min_value=-3.0, max_value=-0.1, value=config["pricing"]["elasticity"], step=0.1)
        
        submitted = st.form_submit_button("💾 Сохранить настройки", type="primary")
        
        if submitted:
            new_config = {
                "data_collection": {
                    "enabled": data_enabled,
                    "times": [time1.strftime("%H:%M"), time2.strftime("%H:%M"), time3.strftime("%H:%M")],
                    "categories": categories,
                    "pages": pages,
                    "max_items": max_items
                },
                "notifications": {
                    "forecast_frequency": forecast_freq,
                    "enabled": notifications_enabled,
                    "time": notification_time.strftime("%H:%M")
                },
                "telegram": config["telegram"],
                "pricing": {
                    "auto_update": True,
                    "margin_target": 0.2,
                    "elasticity": elasticity
                }
            }
            
            scheduler.update_config(new_config)
            st.success("Настройки сохранены!")
            st.rerun()
    
    # Статистика и журнал
    st.subheader("📊 Статистика")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if status["next_jobs"]:
            st.write("**Запланированные задачи:**")
            for i, job in enumerate(status["next_jobs"], 1):
                st.text(f"{i}. {job}")
        else:
            st.info("Нет запланированных задач")
    
    with col2:
        st.write("**Статус Telegram:**")
        tg_status = get_telegram_status()
        st.write(tg_status)
        
        if "❌" in tg_status:
            st.warning("Настройте Telegram для уведомлений")
            with st.expander("📱 Настройка Telegram бота", expanded=False):
                st.markdown("""
                **Пошаговая настройка:**
                
                1. **Создайте бота:**
                   - Напишите @BotFather в Telegram
                   - Отправьте команду `/newbot`
                   - Следуйте инструкциям
                   - Скопируйте полученный токен
                
                2. **Получите Chat ID:**
                   - Добавьте бота в чат или группу
                   - Напишите боту любое сообщение
                   - Перейдите по ссылке: `https://api.telegram.org/bot<TOKEN>/getUpdates`
                   - Найдите `"chat":{"id":-1234567890}` и скопируйте ID
                
                3. **Настройте конфигурацию:**
                   - Отредактируйте файл `.streamlit/secrets.toml`
                   - Или установите переменные окружения
                   - Или отредактируйте `.env`
                """)
        else:
            if st.button("🧪 Тест Telegram"):
                with st.spinner("Тестирую подключение..."):
                    test_result = tg_test_connection()
                    if test_result["chat_accessible"]:
                        st.success("✅ Telegram настроен правильно!")
                    else:
                        st.error(f"❌ {test_result.get('error', 'Неизвестная ошибка')}")
    
    # Ручные действия
    st.subheader("🎯 Ручные действия")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📈 Сгенерировать прогноз"):
            with st.spinner("Генерирую прогноз..."):
                scheduler.generate_forecast_report()
                st.success("Прогноз отправлен!")
    
    with col2:
        if st.button("🔄 Обновить market_snapshot"):
            with st.spinner("Обновляю данные..."):
                success = scheduler.collect_market_data()
                if success:
                    st.success("Данные обновлены!")
                    st.rerun()
                else:
                    st.error("Ошибка обновления")
    
    with col3:
        if st.button("🧹 Очистить логи"):
            try:
                log_path = Path(__file__).resolve().parent.parent / "logs" / "scheduler.log"
                if log_path.exists():
                    log_path.unlink()
                st.success("Логи очищены")
            except Exception as e:
                st.error(f"Ошибка: {e}")
    
    # Показать последние логи
    with st.expander("📝 Последние логи планировщика", expanded=False):
        try:
            log_path = Path(__file__).resolve().parent.parent / "logs" / "scheduler.log"
            if log_path.exists():
                with open(log_path, 'r', encoding='utf-8') as f:
                    logs = f.readlines()[-50:]  # Последние 50 строк
                st.text_area("Логи", "\n".join(logs), height=300, disabled=True)
            else:
                st.info("Логи пока отсутствуют")
        except Exception as e:
            st.error(f"Не удается прочитать логи: {e}")













