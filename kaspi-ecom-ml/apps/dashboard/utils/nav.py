from __future__ import annotations

import streamlit as st


PAGES = {
    "Обзор": "pages/01_Overview.py",
    "Карточка SKU": "pages/02_SKU_Details.py",
    "Рекомендации по цене": "pages/03_Recommendations.py",
    "Ассортимент": "pages/04_Assortment.py",
    "Настройки": "pages/05_Settings.py",
    "Администрирование": "pages/06_Admin.py",
}


def render_nav(active: str | None = None) -> None:
    st.sidebar.header("Навигация")
    choice = st.sidebar.radio("Раздел", list(PAGES.keys()), index=list(PAGES.keys()).index(active) if active in PAGES else 0)
    if active and choice == active:
        return
    # Switch to selected page
    path = PAGES[choice]
    try:
        st.switch_page(path)
    except Exception:
        # Fallback: show hint if running older Streamlit
        st.sidebar.info("Используйте боковое меню для перехода между страницами.")

