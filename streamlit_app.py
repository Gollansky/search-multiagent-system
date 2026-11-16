import streamlit as st
import medinfo_agents as ma

st.set_page_config(page_title="medinfo agents", page_icon="💊")
st.title("medinfo agents")
st.caption("Агенты собирают данные из RxNorm (RxNav) и DailyMed. Не медицинская рекомендация.")


@st.cache_data(ttl=3600, show_spinner=False)
def search(q: str, max_labels: int):
    rep = ma.Orchestrator(max_labels=max_labels).collect(q)
    return rep


with st.form("f"):
    q = st.text_input("Название препарата", placeholder="metformin")
    n = st.slider("Сколько лейблов DailyMed показать", 1, 10, 5)
    go = st.form_submit_button("Найти")

if go and q.strip():
    q = q.strip()[:100]
    with st.spinner("Агенты работают…"):
        rep = search(q, n)
    if rep["status"] == "unresolved":
        st.warning(f"RxNorm не нашёл «{q}».")
    else:
        c = rep["concept"]
        st.subheader("RxNorm")
        st.write(f"**{c['name']}** · RxCUI {c['rxcui']} · {c['tty']} ({c['method']})")
        for tty, items in (rep["rxnorm"].get("related") or {}).items():
            st.write(f"- {tty}: " + "; ".join(i["name"] for i in items[:6]))
        st.subheader("DailyMed")
        dm = rep.get("dailymed") or {}
        st.caption(f"лейблов всего: {dm.get('total_labels')}")
        for l in dm.get("labels", []):
            st.markdown(f"- [{l['title']}]({l['url']}) — {l['published']}")
        for f in rep["reconciliation"]["flags"]:
            st.warning(f)
        with st.expander("Агенты и сырой отчёт"):
            st.table([{"agent": a["agent"], "ok": a["ok"], "sec": a["elapsed_s"],
                       "errors": "; ".join(a["errors"])} for a in rep["agents"]])
            st.json(rep)
