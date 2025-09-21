from __future__ import annotations

import os
import streamlit as st
import requests
from requests.exceptions import RequestException

API_BASE = os.getenv("API_BASE", "http://localhost:8000")


def login_and_store() -> str | None:
    token = st.session_state.get("token")
    if token:
        return token
    with st.form("login"):
        st.write("Вход (демо: любые логин/пароль)")
        u = st.text_input("Логин", value="demo")
        p = st.text_input("Пароль", type="password", value="demo")
        submitted = st.form_submit_button("Войти")
    if submitted:
        try:
            r = requests.post(API_BASE + "/auth/token", data={"username": u, "password": p}, timeout=10)
            r.raise_for_status()
            token = r.json()["access_token"]
            st.session_state["token"] = token
            st.session_state["username"] = u
            return token
        except RequestException as e:
            st.error(f"Нет связи с API по адресу {API_BASE}. Сервис 'api' запущен?\n{e}")
    return None


def require_token() -> str:
    tok = st.session_state.get("token")
    if not tok:
        st.error("Пожалуйста, авторизуйтесь на главной странице.")
        st.stop()
    return tok
