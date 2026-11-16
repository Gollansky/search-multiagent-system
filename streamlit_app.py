import os
import streamlit as st
import medinfo_mas as mas

st.set_page_config(page_title="medinfo MAS", page_icon="💊", layout="wide")
st.title("medinfo: мультиагентная система")
st.caption("Supervisor + RxNorm-агент + DailyMed-агент + верификатор. Не медицинская рекомендация.")


def secret(name, default=""):
    try:
        return st.secrets.get(name, os.environ.get(name, default))
    except Exception:
        return os.environ.get(name, default)


with st.sidebar:
    st.header("Модель")
    key = st.text_input("API key", value=secret("LLM_API_KEY"), type="password")
    base = st.text_input("Base URL (OpenAI-совместимый)", value=secret("LLM_BASE_URL", "https://api.groq.com/openai/v1"))
    model = st.text_input("Model", value=secret("LLM_MODEL", "llama-3.3-70b-versatile"))
    if st.button("Показать доступные модели"):
        try:
            st.write(mas.LLM(key, base, model).list_models())
        except Exception as ex:
            st.error(f"Не удалось получить список: {ex}")

q = st.text_input("Препарат", placeholder="metformin")
if st.button("Запустить агентов") and q.strip():
    if not key:
        st.error("Укажите API key в боковой панели или в Secrets.")
        st.stop()
    log = st.status("Агенты работают…", expanded=True)

    def on_event(e):
        icon = {"tool_call": "🔧", "tool_result": "📥", "delegate": "➡️", "verdict": "🧐",
                "rejected": "↩️", "tool_error": "❌", "message": "💬"}.get(e["kind"], "•")
        log.write(f"{icon} **{e['agent']}** · {e['kind']}: `{e['text'][:160]}`")

    try:
        res = mas.run_system(q.strip()[:100], mas.LLM(key, base, model), on_event=on_event)
        log.update(label="Готово", state="complete")
    except Exception as ex:
        log.update(label="Ошибка", state="error")
        st.exception(ex)
        st.stop()

    st.success("Проверено верификатором") if res["verified"] else st.warning("Отчёт НЕ прошёл верификацию")
    st.markdown(res["report"])
    with st.expander(f"Доказательства ({len(res['evidence'])})"):
        for e in res["evidence"]:
            st.markdown(f"**{e['id']}** · {e['agent']} · `{e['tool']}`")
            st.json(e["result"], expanded=False)
    if res["verification"] and res["verification"]["issues"]:
        st.write("Замечания верификатора:", res["verification"]["issues"])
    st.caption(f"Время: {res['elapsed_s']} с · сдач отчёта: {res['submits']}")
