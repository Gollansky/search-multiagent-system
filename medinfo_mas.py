#!/usr/bin/env python3
"""
medinfo-mas: LLM-мультиагентная система сбора сведений о лекарствах (RxNorm + DailyMed).

Архитектура: Supervisor + специалисты + верификатор, общий блокнот доказательств.

  Supervisor (LLM) ── delegate ──► RxNormAgent   (LLM + инструменты RxNav)     ┐ параллельно
        │            └─────────► DailyMedAgent  (LLM + инструменты DailyMed)   ┘
        ├── compare_ndcs  (детерминированный инструмент сверки кодов NDC)
        └── submit_report ─► проверки цитат (код) ─► VerifierAgent (LLM) ─► принят / вернули на доработку

  EvidenceStore - общий "блокнот": каждый результат инструмента получает id (E1, E2, ...).
  Отчёт обязан ссылаться на эти id, верификатор сверяет утверждения с их содержимым.

LLM - любой OpenAI-совместимый API (tool calling). Переменные окружения:
  LLM_API_KEY, LLM_BASE_URL (по умолчанию Groq), LLM_MODEL
Только стандартная библиотека (+ medinfo_agents.py для HTTP-клиента и нормализации NDC).
Не медицинская рекомендация.
"""
from __future__ import annotations

import argparse, json, os, re, sys, threading, time, urllib.error, urllib.request
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Optional

from medinfo_agents import HttpClient, RXNAV, DAILYMED, _norm_ndc

# ============================================================ LLM-клиент ====
class LLM:
    """Клиент OpenAI-совместимого /chat/completions с tool calling и ретраями."""

    def __init__(self, api_key=None, base_url=None, model=None, timeout=90, retries=4):
        self.api_key = api_key or os.environ.get("LLM_API_KEY", "")
        self.base_url = (base_url or os.environ.get("LLM_BASE_URL") or "https://api.groq.com/openai/v1").rstrip("/")
        self.model = model or os.environ.get("LLM_MODEL") or "llama-3.3-70b-versatile"
        self.timeout, self.retries = timeout, retries
        if not self.api_key:
            raise RuntimeError("Не задан LLM_API_KEY")

    def chat(self, messages: list, tools: Optional[list] = None) -> dict:
        body = {"model": self.model, "messages": messages, "temperature": 0}
        if tools:
            body["tools"], body["tool_choice"] = tools, "auto"
        data = json.dumps(body).encode("utf-8")
        for attempt in range(1, self.retries + 1):
            req = urllib.request.Request(
                f"{self.base_url}/chat/completions", data=data, method="POST",
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {self.api_key}"})
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as r:
                    return json.loads(r.read().decode("utf-8"))["choices"][0]["message"]
            except urllib.error.HTTPError as e:
                if e.code not in (429, 500, 502, 503, 504) or attempt == self.retries:
                    raise RuntimeError(f"LLM HTTP {e.code}: {e.read().decode('utf-8', 'ignore')[:300]}")
                time.sleep(float(e.headers.get("Retry-After") or 2 ** attempt))
            except (urllib.error.URLError, TimeoutError):
                if attempt == self.retries:
                    raise
                time.sleep(2 ** attempt)


# ===================================================== блокнот доказательств ==
class EvidenceStore:
    """Потокобезопасное общее хранилище результатов инструментов."""

    def __init__(self):
        self._items, self._lock = [], threading.Lock()

    def add(self, agent: str, tool: str, args: dict, result: Any) -> str:
        with self._lock:
            eid = f"E{len(self._items) + 1}"
            self._items.append({"id": eid, "agent": agent, "tool": tool, "args": args, "result": result})
            return eid

    def get(self, eid: str) -> Optional[dict]:
        with self._lock:
            return next((x for x in self._items if x["id"] == eid), None)

    def all(self) -> list:
        with self._lock:
            return list(self._items)


class Trace:
    """Журнал событий для UI и отладки."""

    def __init__(self, on_event: Optional[Callable] = None):
        self.events, self.on_event = [], on_event

    def add(self, agent: str, kind: str, text: str):
        ev = {"t": round(time.time(), 2), "agent": agent, "kind": kind, "text": text}
        self.events.append(ev)
        if self.on_event:
            self.on_event(ev)


def _shrink(o, n=30):
    """Версия результата для LLM: длинные списки режем (в блокноте остаётся полный)."""
    if isinstance(o, list):
        return [_shrink(x, n) for x in o[:n]] + ([f"...(ещё {len(o) - n})"] if len(o) > n else [])
    if isinstance(o, dict):
        return {k: _shrink(v, n) for k, v in o.items()}
    return o


def _spec(name, desc, props, required):
    return {"type": "function", "function": {"name": name, "description": desc,
            "parameters": {"type": "object", "properties": props, "required": required}}}


# ============================================================ инструменты ====
class SourceTools:
    """Тонкие обёртки над API. Возвращают компактный JSON-совместимый результат."""

    def __init__(self, http: HttpClient):
        self.http = http

    # --- RxNorm ---
    def rxnorm_find(self, name: str) -> dict:
        j = self.http.get_json(f"{RXNAV}/rxcui.json", {"name": name, "search": 2})
        ids = ((j or {}).get("idGroup") or {}).get("rxnormId") or []
        match = "exact"
        if not ids:
            j = self.http.get_json(f"{RXNAV}/approximateTerm.json", {"term": name, "maxEntries": 5})
            cand = ((j or {}).get("approximateGroup") or {}).get("candidate") or []
            ids, match = list(dict.fromkeys(c["rxcui"] for c in cand)), "approximate"
        out = []
        for i in ids[:3]:
            p = ((self.http.get_json(f"{RXNAV}/rxcui/{i}/properties.json") or {}).get("properties")) or {}
            out.append({"rxcui": i, "name": p.get("name"), "tty": p.get("tty")})
        return {"match": match, "candidates": out} if out else {"candidates": [], "note": "RxNorm ничего не нашёл"}

    def rxnorm_related(self, rxcui: str) -> dict:
        j = self.http.get_json(f"{RXNAV}/rxcui/{rxcui}/related.json", {"tty": "IN+BN+SCD+SBD+DF"}) or {}
        rel = {}
        for g in (j.get("relatedGroup") or {}).get("conceptGroup") or []:
            items = [{"rxcui": c["rxcui"], "name": c["name"]} for c in g.get("conceptProperties", [])]
            if items:
                rel[g["tty"]] = items[:10]
        return {"rxcui": rxcui, "related": rel}

    def rxnorm_ndcs(self, rxcui: str) -> dict:
        j = self.http.get_json(f"{RXNAV}/rxcui/{rxcui}/ndcs.json") or {}
        nd = ((j.get("ndcGroup") or {}).get("ndcList") or {}).get("ndc") or []
        return {"rxcui": rxcui, "count": len(nd), "ndcs": nd}

    # --- DailyMed ---
    def dailymed_labels(self, drug_name: str = "", rxcui: str = "", limit: int = 5) -> dict:
        p = {"pagesize": min(int(limit), 10)}
        if rxcui:
            p["rxcui"] = rxcui
        elif drug_name:
            p["drug_name"] = drug_name
        else:
            raise ValueError("нужен drug_name или rxcui")
        j = self.http.get_json(f"{DAILYMED}/spls.json", p) or {}
        return {"total": (j.get("metadata") or {}).get("total_elements"),
                "labels": [{"setid": d.get("setid"), "title": d.get("title"),
                            "published": d.get("published_date"), "version": d.get("spl_version"),
                            "url": f"https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid={d.get('setid')}"}
                           for d in (j.get("data") or [])]}

    def dailymed_label_ndcs(self, setid: str) -> dict:
        j = self.http.get_json(f"{DAILYMED}/spls/{setid}/ndcs.json") or {}
        raw = (j.get("data") or {}).get("ndcs") or []
        nd = [x.get("ndc") if isinstance(x, dict) else x for x in raw]
        return {"setid": setid, "count": len(nd), "ndcs": [n for n in nd if n]}


# ================================================================== агенты ===
MAX_TOOL_CHARS = 6000


class Agent:
    """LLM-агент: цикл «модель -> вызовы инструментов -> результаты -> модель» до финального текста."""

    def __init__(self, name, system, tools: dict, llm, store: EvidenceStore, trace: Trace,
                 max_steps=6, record_evidence=True):
        self.name, self.system, self.llm, self.store, self.trace = name, system, llm, store, trace
        self.tools = tools  # имя -> (spec, функция)
        self.max_steps, self.record_evidence = max_steps, record_evidence
        self.final: Optional[str] = None  # терминальные инструменты кладут сюда результат

    def _exec(self, call: dict, my_ids: list) -> str:
        fn_name = call["function"]["name"]
        try:
            args = json.loads(call["function"].get("arguments") or "{}")
        except json.JSONDecodeError:
            return "ОШИБКА инструмента: аргументы не являются корректным JSON"
        self.trace.add(self.name, "tool_call", f"{fn_name}({json.dumps(args, ensure_ascii=False)})")
        if fn_name not in self.tools:
            return f"ОШИБКА инструмента: неизвестный инструмент {fn_name}"
        try:
            result = self.tools[fn_name][1](**args)
        except Exception as e:
            self.trace.add(self.name, "tool_error", f"{type(e).__name__}: {e}")
            return f"ОШИБКА инструмента: {type(e).__name__}: {e}"
        if self.record_evidence and not isinstance(result, str):
            eid = self.store.add(self.name, fn_name, args, result)
            my_ids.append(eid)
            out = json.dumps({"evidence_id": eid, "result": _shrink(result)}, ensure_ascii=False)
        else:
            out = result if isinstance(result, str) else json.dumps(result, ensure_ascii=False)
        self.trace.add(self.name, "tool_result", out[:300])
        return out[:MAX_TOOL_CHARS]

    def run(self, task: str) -> str:
        msgs = [{"role": "system", "content": self.system}, {"role": "user", "content": task}]
        specs, my_ids, last_text = [t[0] for t in self.tools.values()], [], ""
        for _ in range(self.max_steps):
            m = self.llm.chat(msgs, specs)
            msgs.append({k: v for k, v in m.items() if k in ("role", "content", "tool_calls")} | {"role": "assistant"})
            last_text = m.get("content") or last_text
            calls = m.get("tool_calls") or []
            if not calls:
                self.trace.add(self.name, "message", last_text[:500])
                break
            if len(calls) > 1:  # независимые вызовы - параллельно
                with ThreadPoolExecutor(max_workers=len(calls)) as pool:
                    outs = list(pool.map(lambda c: self._exec(c, my_ids), calls))
            else:
                outs = [self._exec(calls[0], my_ids)]
            for c, o in zip(calls, outs):
                msgs.append({"role": "tool", "tool_call_id": c["id"], "content": o})
            if self.final is not None:
                return self.final
        else:
            last_text += "\n(достигнут лимит шагов)"
        return last_text + (f"\n[evidence: {', '.join(my_ids)}]" if my_ids else "")


RULES = ("Правила: используй ТОЛЬКО данные, полученные инструментами; ничего не додумывай. "
         "Не давай медицинских советов (дозировки, показания, рекомендации). "
         "Если инструмент вернул ошибку или пусто, так и скажи. Отвечай по-русски, названия препаратов не переводи.")


def make_rxnorm_agent(tools: SourceTools, llm, store, trace) -> Agent:
    t = {
        "rxnorm_find": (_spec("rxnorm_find", "Найти препарат в RxNorm по названию (допускает опечатки). Возвращает RxCUI.",
                              {"name": {"type": "string"}}, ["name"]), tools.rxnorm_find),
        "rxnorm_related": (_spec("rxnorm_related", "Связанные понятия RxNorm по RxCUI: вещество(IN), бренды(BN), формы(SCD/SBD/DF).",
                                 {"rxcui": {"type": "string"}}, ["rxcui"]), tools.rxnorm_related),
        "rxnorm_ndcs": (_spec("rxnorm_ndcs", "Коды упаковок NDC по RxCUI.", {"rxcui": {"type": "string"}}, ["rxcui"]),
                        tools.rxnorm_ndcs),
    }
    sys_ = ("ROLE:rxnorm\nТы агент-специалист по RxNorm (номенклатура лекарств США). Задача: нормализовать название "
            "препарата (rxnorm_find), затем получить связанные понятия (rxnorm_related) и NDC (rxnorm_ndcs). "
            "В итоговом ответе кратко перечисли найденное и укажи id доказательств в формате [E#] после каждого факта. "
            + RULES)
    return Agent("rxnorm", sys_, t, llm, store, trace)


def make_dailymed_agent(tools: SourceTools, llm, store, trace) -> Agent:
    t = {
        "dailymed_labels": (_spec("dailymed_labels", "Список лейблов (SPL) в DailyMed по названию или RxCUI.",
                                  {"drug_name": {"type": "string"}, "rxcui": {"type": "string"},
                                   "limit": {"type": "integer"}}, []), tools.dailymed_labels),
        "dailymed_label_ndcs": (_spec("dailymed_label_ndcs", "NDC конкретного лейбла по setid.",
                                      {"setid": {"type": "string"}}, ["setid"]), tools.dailymed_label_ndcs),
    }
    sys_ = ("ROLE:dailymed\nТы агент-специалист по DailyMed (официальные инструкции к препаратам FDA/NLM). Задача: найти "
            "лейблы препарата (dailymed_labels), для 1-3 самых свежих получить NDC (dailymed_label_ndcs). "
            "В итоговом ответе перечисли лейблы (заголовок, дата, ссылка) и укажи [E#] после каждого факта. " + RULES)
    return Agent("dailymed", sys_, t, llm, store, trace)


def verify_report(llm, store: EvidenceStore, report: str, trace: Trace) -> dict:
    """Агент-верификатор: отдельный LLM-вызов без инструментов и без доступа к истории супервизора."""
    ev = json.dumps([{"id": e["id"], "tool": e["tool"], "args": e["args"], "result": _shrink(e["result"], 15)}
                     for e in store.all()], ensure_ascii=False)[:14000]
    msgs = [{"role": "system", "content": (
        "ROLE:verifier\nТы строгий проверяющий. Тебе даны отчёт и доказательства (результаты инструментов). "
        "Для каждого утверждения отчёта проверь, что указанный [E#] действительно его подтверждает. "
        "Утверждения без опоры в доказательствах, искажения чисел, медицинские советы - нарушения. "
        'Ответь ТОЛЬКО JSON: {"verdict":"pass"|"fail","issues":["..."]}')},
        {"role": "user", "content": f"ОТЧЁТ:\n{report}\n\nДОКАЗАТЕЛЬСТВА:\n{ev}"}]
    raw = llm.chat(msgs).get("content") or ""
    m = re.search(r"\{.*\}", raw, re.S)
    try:
        v = json.loads(m.group(0))
        v = {"verdict": "pass" if v.get("verdict") == "pass" else "fail", "issues": list(v.get("issues") or [])}
    except Exception:
        v = {"verdict": "fail", "issues": ["верификатор вернул не-JSON"]}
    trace.add("verifier", "verdict", json.dumps(v, ensure_ascii=False))
    return v


class Supervisor(Agent):
    MAX_SUBMITS = 3

    def __init__(self, tools: SourceTools, llm, store, trace):
        self.workers = {"rxnorm": make_rxnorm_agent(tools, llm, store, trace),
                        "dailymed": make_dailymed_agent(tools, llm, store, trace)}
        self.submits, self.verified, self.verification = 0, False, None
        t = {
            "delegate": (_spec("delegate", "Поручить задачу агенту-специалисту. Можно вызвать несколько раз в одном ответе: они выполнятся параллельно.",
                               {"agent": {"type": "string", "enum": ["rxnorm", "dailymed"]},
                                "task": {"type": "string"}}, ["agent", "task"]), self._delegate),
            "compare_ndcs": (_spec("compare_ndcs", "Детерминированно сверить NDC из RxNorm и DailyMed (учитывает форматы 10/11 цифр). "
                                   "Передай id доказательств от rxnorm_ndcs и dailymed_label_ndcs.",
                                   {"evidence_ids": {"type": "array", "items": {"type": "string"}}}, ["evidence_ids"]),
                             self._compare),
            "submit_report": (_spec("submit_report", "Сдать итоговый отчёт (Markdown). Каждая строка-факт должна заканчиваться ссылками [E#].",
                                    {"report": {"type": "string"}}, ["report"]), self._submit),
        }
        sys_ = ("ROLE:supervisor\nТы супервизор. Пользователь называет препарат; нужно собрать данные из двух источников и "
                "выдать проверенный отчёт.\nПлан: 1) вызови delegate для rxnorm и для dailymed ОДНОВРЕМЕННО (в одном ответе; "
                "dailymed ищи по названию); 2) вызови compare_ndcs с id доказательств NDC из обоих источников (если есть оба); "
                "3) вызови submit_report. Структура отчёта: что за препарат (RxNorm), какие лейблы есть (DailyMed), "
                "результат сверки, ограничения/сбои. Каждый факт - с [E#]. Если отчёт отклонён, исправь замечания и сдай заново. "
                + RULES)
        super().__init__("supervisor", sys_, t, llm, store, trace, max_steps=10, record_evidence=False)

    def _delegate(self, agent: str, task: str) -> str:
        if agent not in self.workers:
            return f"ОШИБКА: нет агента {agent}"
        self.trace.add("supervisor", "delegate", f"{agent}: {task}")
        return self.workers[agent].run(task)

    def _compare(self, evidence_ids: list) -> Any:
        rx, dm = set(), set()
        for eid in evidence_ids:
            e = self.store.get(eid)
            if not e:
                continue
            if e["tool"] == "rxnorm_ndcs":
                rx |= {_norm_ndc(n) for n in e["result"]["ndcs"]}
            elif e["tool"] == "dailymed_label_ndcs":
                dm |= {_norm_ndc(n) for n in e["result"]["ndcs"]}
        if not rx or not dm:
            return "ОШИБКА инструмента: нужны NDC из обоих источников (rxnorm_ndcs и dailymed_label_ndcs)"
        com = rx & dm
        eid = self.store.add("supervisor", "compare_ndcs", {"evidence_ids": evidence_ids},
                             {"rxnorm_count": len(rx), "dailymed_count": len(dm), "common_count": len(com),
                              "common": sorted(com)[:20], "only_rxnorm_count": len(rx - dm),
                              "only_dailymed_count": len(dm - rx)})
        return {"evidence_id": eid, **self.store.get(eid)["result"]}

    def _submit(self, report: str) -> str:
        self.submits += 1
        cited = re.findall(r"\[E(\d+)\]", report)
        problems = [f"ссылка [E{i}] не существует" for i in dict.fromkeys(cited) if not self.store.get(f"E{i}")]
        for line in report.splitlines():
            if line.strip().startswith(("-", "*")) and not re.search(r"\[E\d+\]", line):
                problems.append(f"строка без ссылки на доказательство: «{line.strip()[:60]}»")
        if not cited:
            problems.append("в отчёте нет ни одной ссылки [E#]")
        if not problems:
            v = verify_report(self.llm, self.store, report, self.trace)
            problems = v["issues"] if v["verdict"] != "pass" else []
            if not problems:
                self.verified, self.verification, self.final = True, v, report
                return "ПРИНЯТО"
        if self.submits >= self.MAX_SUBMITS:  # лимит доработок: отдаём как непроверенный
            self.verified, self.verification, self.final = False, {"verdict": "fail", "issues": problems}, report
            return "ОТКЛОНЕНО, лимит исчерпан"
        self.trace.add("supervisor", "rejected", "; ".join(problems))
        return "ОТКЛОНЕНО. Исправь замечания и вызови submit_report снова:\n- " + "\n- ".join(problems)


# ================================================================ точка входа ==
def run_system(query: str, llm, http: Optional[HttpClient] = None, on_event: Optional[Callable] = None) -> dict:
    t0 = time.monotonic()
    store, trace = EvidenceStore(), Trace(on_event)
    sup = Supervisor(SourceTools(http or HttpClient()), llm, store, trace)
    trace.add("system", "start", query)
    text = sup.run(f"Препарат: {query}")
    return {"query": query, "report": sup.final or text, "verified": sup.verified,
            "verification": sup.verification, "submits": sup.submits,
            "evidence": store.all(), "trace": trace.events, "elapsed_s": round(time.monotonic() - t0, 2)}


# ================================================================== самотест ==
class FakeLLM:
    """Скриптованная «модель»: проверяет механику системы (цикл, инструменты, параллельность, верификацию)
    без сети и без настоящей LLM. Качество реальной модели этим НЕ проверяется."""

    def __init__(self, verifier_fail_first=False, bad_citation=False):
        self.verifier_fail_first, self.bad_citation, self.v_calls = verifier_fail_first, bad_citation, 0

    @staticmethod
    def _call(name, args, i=0, n=0):
        return {"role": "assistant", "content": None, "tool_calls": [
            {"id": f"c{n}_{i}", "type": "function", "function": {"name": name, "arguments": json.dumps(args)}}]}

    def chat(self, messages, tools=None):
        role = re.search(r"ROLE:(\w+)", messages[0]["content"]).group(1)
        return getattr(self, "_" + role)(messages)

    def _supervisor(self, msgs):
        tool = [m for m in msgs if m["role"] == "tool"]
        n, last = len(tool), (tool[-1]["content"] if tool else "")
        q = re.search(r"Препарат: (.+)", msgs[1]["content"]).group(1)
        if n == 0:
            a = self._call("delegate", {"agent": "rxnorm", "task": f"Найди {q}"}, 0, 0)
            b = self._call("delegate", {"agent": "dailymed", "task": f"Найди лейблы {q}"}, 1, 0)
            a["tool_calls"] += b["tool_calls"]
            return a
        ids = list(dict.fromkeys(re.findall(r"E\d+", " ".join(m["content"] for m in tool if not m["content"].startswith("ОТКЛОНЕНО")))))
        if n == 2:
            return self._call("compare_ndcs", {"evidence_ids": ids}, 0, n)
        cites = "[E99]" if (self.bad_citation and not last.startswith("ОТКЛОНЕНО")) else "".join(f"[{i}]" for i in ids)
        if last.startswith("ОТКЛОНЕНО") or n == 3:
            tail = " (исправлено)" if last.startswith("ОТКЛОНЕНО") else ""
            rep = f"- Препарат {q}{tail} {cites}\n- Источники: RxNorm, DailyMed {cites}"
            return self._call("submit_report", {"report": rep}, 0, n)
        return {"role": "assistant", "content": "готово"}

    def _rxnorm(self, msgs):
        tool = [m for m in msgs if m["role"] == "tool"]
        n = len(tool)
        if n == 0:
            return self._call("rxnorm_find", {"name": msgs[1]["content"].split()[-1]}, 0, n)
        if n == 1:
            return self._call("rxnorm_ndcs", {"rxcui": json.loads(tool[0]["content"])["result"]["candidates"][0]["rxcui"]}, 0, n)
        return {"role": "assistant", "content": "RxNorm: найдено " + " ".join(f"[{x}]" for x in re.findall(r"E\d+", " ".join(m["content"] for m in tool)))}

    def _dailymed(self, msgs):
        tool = [m for m in msgs if m["role"] == "tool"]
        n = len(tool)
        if n == 0:
            return self._call("dailymed_labels", {"drug_name": msgs[1]["content"].split()[-1]}, 0, n)
        if tool[-1]["content"].startswith("ОШИБКА"):
            return {"role": "assistant", "content": "DailyMed недоступен (ошибка инструмента)"}
        if n == 1:
            return self._call("dailymed_label_ndcs", {"setid": json.loads(tool[0]["content"])["result"]["labels"][0]["setid"]}, 0, n)
        return {"role": "assistant", "content": "DailyMed: лейблы найдены " + " ".join(f"[{x}]" for x in re.findall(r"E\d+", " ".join(m["content"] for m in tool)))}

    def _verifier(self, msgs):
        self.v_calls += 1
        fail = self.verifier_fail_first and "исправлено" not in msgs[1]["content"]
        return {"role": "assistant", "content": json.dumps(
            {"verdict": "fail", "issues": ["утверждение не подтверждено"]} if fail else {"verdict": "pass", "issues": []})}


def selftest() -> int:
    from medinfo_agents import FakeHttp, BrokenDailyMed
    ok = True

    def check(c, msg):
        nonlocal ok
        print(("PASS " if c else "FAIL ") + msg)
        ok &= bool(c)

    r = run_system("metformin", FakeLLM(), FakeHttp())
    agents = {e["agent"] for e in r["trace"]}
    cmp_ = next(e for e in r["evidence"] if e["tool"] == "compare_ndcs")
    check(r["verified"] and r["submits"] == 1, "happy path: отчёт принят верификатором с первого раза")
    check({"supervisor", "rxnorm", "dailymed", "verifier"} <= agents, "задействованы все 4 агента")
    check(cmp_["result"]["common_count"] == 1, "compare_ndcs: 0093-1048-01 == 00093104801")
    r = run_system("metformin", FakeLLM(verifier_fail_first=True), FakeHttp())
    check(r["verified"] and r["submits"] == 2, "верификатор вернул на доработку -> исправлено -> принято")
    r = run_system("metformin", FakeLLM(bad_citation=True), FakeHttp())
    check(r["verified"] and r["submits"] == 2 and any(e["kind"] == "rejected" and "E99" in e["text"] for e in r["trace"]),
          "несуществующая ссылка [E99] отсеяна кодом ещё до верификатора")
    r = run_system("metformin", FakeLLM(), BrokenDailyMed())
    check(any(e["kind"] == "tool_error" for e in r["trace"]) and r["report"] and "ОШИБКА" not in r["report"][:6],
          "сбой DailyMed не роняет систему")
    print("\n" + run_system("metformin", FakeLLM(), FakeHttp())["report"])
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("query", nargs="?")
    ap.add_argument("--json")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(selftest())
    if not a.query:
        ap.error("нужен query или --selftest")
    res = run_system(a.query, LLM(), on_event=lambda e: print(f"[{e['agent']}:{e['kind']}] {e['text'][:140]}", file=sys.stderr))
    print(("✅ проверено" if res["verified"] else "⚠ НЕ ПРОВЕРЕНО") + "\n\n" + res["report"])
    if a.json:
        json.dump(res, open(a.json, "w", encoding="utf-8"), ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
