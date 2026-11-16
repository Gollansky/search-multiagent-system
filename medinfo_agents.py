#!/usr/bin/env python3
"""
Мультиагентная система сбора информации о лекарствах из двух источников реестра medinfo:
  - RxNav / RxNorm (NLM)   https://rxnav.nlm.nih.gov/REST
  - DailyMed REST (NLM)    https://dailymed.nlm.nih.gov/dailymed/services/v2

Агенты:
  Orchestrator   - принимает запрос, запускает остальных, собирает отчёт
  ResolverAgent  - RxNorm: свободный текст -> RxCUI (нормализация названия)
  RxNormAgent    - RxNorm: свойства, связанные понятия (IN/BN/SCD/SBD), NDC
  DailyMedAgent  - DailyMed: список SPL-лейблов по RxCUI/названию, NDC каждого лейбла
  ReconcilerAgent- сверка источников: пересечение NDC, расхождения, флаги качества

RxNormAgent и DailyMedAgent работают параллельно (ThreadPoolExecutor).
Только стандартная библиотека. Не медицинская рекомендация - инфраструктура сбора данных.

Запуск:
  python medinfo_agents.py "metformin"            # живые запросы
  python medinfo_agents.py "metformin" --json out.json
  python medinfo_agents.py --selftest             # офлайн-проверка на заглушке
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Optional

RXNAV = "https://rxnav.nlm.nih.gov/REST"
DAILYMED = "https://dailymed.nlm.nih.gov/dailymed/services/v2"

log = logging.getLogger("medinfo")


# ---------------------------------------------------------------- транспорт --
class HttpClient:
    """GET -> JSON с ретраями и простым ограничением частоты."""

    def __init__(self, min_interval: float = 0.1, retries: int = 3, timeout: float = 20):
        self.min_interval, self.retries, self.timeout = min_interval, retries, timeout
        self._last = 0.0

    def get_json(self, url: str, params: Optional[dict] = None) -> Any:
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"
        for attempt in range(1, self.retries + 1):
            wait = self.min_interval - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()
            try:
                req = urllib.request.Request(
                    url, headers={"Accept": "application/json", "User-Agent": "medinfo-agents/0.1"}
                )
                with urllib.request.urlopen(req, timeout=self.timeout) as r:
                    return json.loads(r.read().decode("utf-8"))
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    return None
                if e.code not in (429, 500, 502, 503, 504) or attempt == self.retries:
                    raise
            except (urllib.error.URLError, TimeoutError):
                if attempt == self.retries:
                    raise
            time.sleep(2 ** attempt * 0.5)


# ------------------------------------------------------------------ данные --
@dataclass
class AgentResult:
    agent: str
    source: str
    ok: bool = True
    data: dict = field(default_factory=dict)
    errors: list = field(default_factory=list)
    elapsed_s: float = 0.0


class Agent:
    name = "agent"
    source = ""

    def __init__(self, http: HttpClient):
        self.http = http

    def run(self, **kw) -> AgentResult:
        t0 = time.monotonic()
        res = AgentResult(self.name, self.source)
        try:
            res.data = self._run(res, **kw)
        except Exception as e:  # агент не должен валить всю систему
            res.ok = False
            res.errors.append(f"{type(e).__name__}: {e}")
            log.warning("%s failed: %s", self.name, e)
        res.elapsed_s = round(time.monotonic() - t0, 3)
        return res

    def _run(self, res: AgentResult, **kw) -> dict:
        raise NotImplementedError


def _norm_ndc(ndc: str) -> str:
    """Приводит NDC к 11-значному виду 5-4-2 (формат RxNorm).
    С дефисами (DailyMed: 4-4-2, 5-3-2, 5-4-1) недостающий ноль добавляется в нужный сегмент.
    Без дефисов 10-значный NDC неоднозначен - возвращаем цифры как есть."""
    s = str(ndc).strip()
    parts = s.split("-")
    if len(parts) == 3 and all(p.isdigit() for p in parts):
        return parts[0].zfill(5) + parts[1].zfill(4) + parts[2].zfill(2)
    return "".join(ch for ch in s if ch.isdigit())


# ------------------------------------------------------------------ агенты --
class ResolverAgent(Agent):
    name, source = "ResolverAgent", "RxNorm"

    def _run(self, res, query: str) -> dict:
        # 1) точное имя
        j = self.http.get_json(f"{RXNAV}/rxcui.json", {"name": query, "search": 2})
        ids = ((j or {}).get("idGroup") or {}).get("rxnormId") or []
        method = "exact"
        candidates = [{"rxcui": i, "score": None} for i in ids]
        # 2) приблизительный поиск (опечатки, частичные названия)
        if not candidates:
            j = self.http.get_json(f"{RXNAV}/approximateTerm.json", {"term": query, "maxEntries": 5})
            cand = ((j or {}).get("approximateGroup") or {}).get("candidate") or []
            seen, candidates = set(), []
            for c in cand:
                if c["rxcui"] not in seen:
                    seen.add(c["rxcui"])
                    candidates.append({"rxcui": c["rxcui"], "score": c.get("score")})
            method = "approximate"
        if not candidates:
            raise LookupError(f"RxNorm не нашёл '{query}'")
        best = candidates[0]["rxcui"]
        props = self.http.get_json(f"{RXNAV}/rxcui/{best}/properties.json") or {}
        p = props.get("properties") or {}
        return {"method": method, "rxcui": best, "name": p.get("name"), "tty": p.get("tty"),
                "candidates": candidates}


class RxNormAgent(Agent):
    name, source = "RxNormAgent", "RxNorm"
    TTYS = "IN+BN+SCD+SBD+DF"

    def _run(self, res, rxcui: str) -> dict:
        rel = self.http.get_json(f"{RXNAV}/rxcui/{rxcui}/related.json", {"tty": self.TTYS}) or {}
        groups = (rel.get("relatedGroup") or {}).get("conceptGroup") or []
        related = {}
        for g in groups:
            items = [{"rxcui": c["rxcui"], "name": c["name"]} for c in g.get("conceptProperties", [])]
            if items:
                related[g["tty"]] = items
        nd = self.http.get_json(f"{RXNAV}/rxcui/{rxcui}/ndcs.json") or {}
        ndcs = ((nd.get("ndcGroup") or {}).get("ndcList") or {}).get("ndc") or []
        if not ndcs:
            res.errors.append("RxNorm: для этого RxCUI нет NDC (нормально для ингредиентов, смотрите SCD/SBD)")
        return {"rxcui": rxcui, "related": related, "ndcs": ndcs}


class DailyMedAgent(Agent):
    name, source = "DailyMedAgent", "DailyMed"

    def __init__(self, http, max_labels: int = 5):
        super().__init__(http)
        self.max_labels = max_labels

    def _run(self, res, rxcui: str, name: Optional[str] = None) -> dict:
        j = self.http.get_json(f"{DAILYMED}/spls.json", {"rxcui": rxcui, "pagesize": self.max_labels})
        data = (j or {}).get("data") or []
        by = "rxcui"
        if not data and name:  # запасной путь: поиск по названию
            j = self.http.get_json(f"{DAILYMED}/spls.json", {"drug_name": name, "pagesize": self.max_labels})
            data = (j or {}).get("data") or []
            by = "drug_name"
        total = ((j or {}).get("metadata") or {}).get("total_elements")
        labels = []
        for d in data[: self.max_labels]:
            sid = d.get("setid")
            nj = self.http.get_json(f"{DAILYMED}/spls/{sid}/ndcs.json") if sid else None
            raw = ((nj or {}).get("data") or {}).get("ndcs") or []
            ndcs = [x.get("ndc") if isinstance(x, dict) else x for x in raw]
            labels.append({
                "setid": sid, "title": d.get("title"),
                "published": d.get("published_date"), "version": d.get("spl_version"),
                "ndcs": [n for n in ndcs if n],
                "url": f"https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid={sid}",
            })
        if not labels:
            res.errors.append("DailyMed: лейблы не найдены")
        return {"searched_by": by, "total_labels": total, "labels": labels}


class ReconcilerAgent(Agent):
    name, source = "ReconcilerAgent", "RxNorm+DailyMed"

    def _run(self, res, rx: AgentResult, dm: AgentResult) -> dict:
        flags = []
        rx_ndc = {_norm_ndc(n) for n in rx.data.get("ndcs", [])} if rx.ok else set()
        dm_ndc = {_norm_ndc(n) for l in dm.data.get("labels", []) for n in l["ndcs"]} if dm.ok else set()
        if not rx.ok:
            flags.append("RxNorm недоступен: связанные понятия и NDC отсутствуют")
        if not dm.ok:
            flags.append("DailyMed недоступен: лейблы отсутствуют")
        if rx_ndc and dm_ndc:
            common = rx_ndc & dm_ndc
            if not common:
                flags.append("NDC из RxNorm и DailyMed не пересеклись (проверьте формат 10/11 цифр "
                             "или актуальность - DailyMed содержит только 'in use')")
        else:
            common = set()
        if dm.ok:
            dates = [l["published"] for l in dm.data["labels"] if l.get("published")]
            if dates:
                flags.append(f"Даты публикации лейблов (как в DailyMed): {', '.join(dates)} - проверьте актуальность")
        return {"ndc_rxnorm": len(rx_ndc), "ndc_dailymed": len(dm_ndc),
                "ndc_common": sorted(common), "flags": flags}


# ------------------------------------------------------------- оркестратор --
class Orchestrator:
    def __init__(self, http: Optional[HttpClient] = None, max_labels: int = 5):
        http = http or HttpClient()
        self.resolver = ResolverAgent(http)
        self.rxnorm = RxNormAgent(http)
        self.dailymed = DailyMedAgent(http, max_labels)
        self.reconciler = ReconcilerAgent(http)

    def collect(self, query: str) -> dict:
        t0 = time.monotonic()
        r = self.resolver.run(query=query)
        if not r.ok:
            return {"query": query, "status": "unresolved", "agents": [asdict(r)]}
        rxcui, name = r.data["rxcui"], r.data.get("name")
        with ThreadPoolExecutor(max_workers=2) as pool:  # параллельно
            f_rx = pool.submit(self.rxnorm.run, rxcui=rxcui)
            f_dm = pool.submit(self.dailymed.run, rxcui=rxcui, name=name or query)
            rx, dm = f_rx.result(), f_dm.result()
        rec = self.reconciler.run(rx=rx, dm=dm)
        return {
            "query": query, "status": "ok" if (rx.ok or dm.ok) else "failed",
            "concept": r.data, "rxnorm": rx.data, "dailymed": dm.data,
            "reconciliation": rec.data,
            "agents": [asdict(a) for a in (r, rx, dm, rec)],
            "elapsed_s": round(time.monotonic() - t0, 3),
        }


def to_markdown(rep: dict) -> str:
    L = [f"# {rep['query']}", f"Статус: {rep['status']}"]
    c = rep.get("concept")
    if c:
        L.append(f"\nRxNorm: **{c['name']}** (RxCUI {c['rxcui']}, tty={c['tty']}, поиск: {c['method']})")
    for tty, items in (rep.get("rxnorm", {}).get("related") or {}).items():
        L.append(f"- {tty}: " + "; ".join(i["name"] for i in items[:5]) + (" …" if len(items) > 5 else ""))
    dm = rep.get("dailymed") or {}
    if dm:
        L.append(f"\nDailyMed: лейблов всего {dm.get('total_labels')}, показано {len(dm.get('labels', []))}")
        for l in dm.get("labels", []):
            L.append(f"- {l['title']} ({l['published']}) {l['url']}")
    rec = rep.get("reconciliation") or {}
    for f in rec.get("flags", []):
        L.append(f"⚠ {f}")
    L.append("\nАгенты: " + ", ".join(f"{a['agent']} {'ok' if a['ok'] else 'FAIL'} {a['elapsed_s']}s"
                                      for a in rep["agents"]))
    return "\n".join(L)


# -------------------------------------------------------------- самотест --
class FakeHttp(HttpClient):
    """Заглушка с ответами в формате RxNav/DailyMed (для офлайн-проверки логики)."""

    def get_json(self, url, params=None):
        p = params or {}
        if url.endswith("/rxcui.json"):
            return {"idGroup": {"rxnormId": ["6809"]}} if p["name"] == "metformin" else {"idGroup": {}}
        if url.endswith("/approximateTerm.json"):
            return {"approximateGroup": {"candidate": [{"rxcui": "6809", "score": "9"},
                                                       {"rxcui": "6809", "score": "7"}]}}
        if url.endswith("/properties.json"):
            return {"properties": {"rxcui": "6809", "name": "metformin", "tty": "IN"}}
        if url.endswith("/related.json"):
            return {"relatedGroup": {"conceptGroup": [
                {"tty": "IN", "conceptProperties": [{"rxcui": "6809", "name": "metformin"}]},
                {"tty": "BN", "conceptProperties": [{"rxcui": "1", "name": "Glucophage"}]}]}}
        if url.endswith("/ndcs.json") and "rxnav" in url:
            return {"ndcGroup": {"ndcList": {"ndc": ["00093104801", "00093104805"]}}}
        if url.endswith("/spls.json"):
            return {"metadata": {"total_elements": 120}, "data": [
                {"setid": "aaa", "title": "METFORMIN TABLET", "published_date": "Oct 01, 2025", "spl_version": 3}]}
        if url.endswith("/ndcs.json"):
            return {"data": {"ndcs": [{"ndc": "0093-1048-01"}]}}
        return None


class BrokenDailyMed(FakeHttp):
    def get_json(self, url, params=None):
        if "dailymed" in url:
            raise urllib.error.URLError("simulated outage")
        return super().get_json(url, params)


def selftest() -> int:
    ok = True

    def check(cond, msg):
        nonlocal ok
        print(("PASS " if cond else "FAIL ") + msg)
        ok &= bool(cond)

    rep = Orchestrator(FakeHttp()).collect("metformin")
    check(rep["status"] == "ok" and rep["concept"]["rxcui"] == "6809", "exact resolve + оба источника")
    check(rep["reconciliation"]["ndc_common"] == ["00093104801"], "сверка NDC: 0093-1048-01 == 00093104801")
    rep = Orchestrator(FakeHttp()).collect("metformn")
    check(rep["concept"]["method"] == "approximate" and len(rep["concept"]["candidates"]) == 1,
          "approximate-поиск + дедупликация кандидатов")
    rep = Orchestrator(BrokenDailyMed()).collect("metformin")
    check(rep["status"] == "ok" and any("DailyMed недоступен" in f for f in rep["reconciliation"]["flags"]),
          "падение DailyMed не ломает систему")
    class Empty(FakeHttp):
        def get_json(self, url, params=None):
            return {"idGroup": {}} if url.endswith("rxcui.json") else {"approximateGroup": {}}
    check(Orchestrator(Empty()).collect("zzz")["status"] == "unresolved", "неизвестное название -> unresolved")
    print(to_markdown(Orchestrator(FakeHttp()).collect("metformin")))
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("query", nargs="?", help="название препарата")
    ap.add_argument("--json", help="сохранить полный отчёт в JSON")
    ap.add_argument("--max-labels", type=int, default=5)
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO if a.verbose else logging.WARNING)
    if a.selftest:
        sys.exit(selftest())
    if not a.query:
        ap.error("нужен query или --selftest")
    rep = Orchestrator(max_labels=a.max_labels).collect(a.query)
    print(to_markdown(rep))
    if a.json:
        with open(a.json, "w", encoding="utf-8") as f:
            json.dump(rep, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
