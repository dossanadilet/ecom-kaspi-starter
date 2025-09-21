import streamlit as st
from apps.dashboard.utils.nav import render_nav

render_nav(active="Ассортимент")
st.header("Ассортимент (демо)")
st.info("Здесь будет скоринг SKU по ожидаемой прибыли и флаги 'добавить/исключить'.")
