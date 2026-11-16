import os
import streamlit as st
from dotenv import load_dotenv
from medinfo_mas import run

load_dotenv()

st.set_page_config(page_title="SNOMED CT ↔ ICD-11", layout="wide")
st.title("🧬 SNOMED CT ↔ ICD-11: мультиагентный маппинг")
st.caption("Supervisor: SNOMED → ICD-11 → Mapper → Verifier → Reporter")

with st.sidebar:
    st.header("Архитектура")
    st.code(
        "medinfo_agents.py  — агенты\n"
        "medinfo_mas.py     — supervisor\n"
        "streamlit_app.py   — UI"
    )
    st.warning(
        "Демо-режим: SNOMED/ICD-11 возвращают демо-данные. "
        "Замените функции в `medinfo_agents.py` для боевого режима."
    )

term = st.text_input(
    "Клиническое понятие",
    value="type 2 diabetes mellitus with diabetic nephropathy",
)

if st.button("🚀 Запустить маппинг", type="primary"):
    if not os.getenv("OPENAI_API_KEY"):
        st.error("Установите OPENAI_API_KEY в .env")
        st.stop()
    if not term.strip():
        st.error("Введите понятие.")
        st.stop()

    with st.status("Работают агенты...", expanded=True) as status:
        try:
            state = run(term.strip(), on_step=lambda name: st.write(f"→ {name}"))
            status.update(label="Готово", state="complete")
        except Exception as e:
            status.update(label="Ошибка", state="error")
            st.exception(e)
            st.stop()

    with st.expander("📋 Журнал агентов"):
        for line in state.get("history", []):
            st.write(f"• {line}")

    st.subheader("1. SNOMED CT — кандидаты")
    st.dataframe(state["snomed"], use_container_width=True)

    st.subheader("2. ICD-11 — кандидаты")
    st.dataframe(state["icd11"], use_container_width=True)

    st.subheader("3. Маппинг")
    st.json(state["mapping"])

    st.subheader("4. Верификация")
    st.json(state["verification"])

    st.subheader("5. Итоговый отчёт")
    st.markdown(state["report"])