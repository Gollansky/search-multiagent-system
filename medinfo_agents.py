import os
import json
from typing import Any, Dict, List
from openai import OpenAI

_client = None

def _client_get() -> OpenAI:
    global _client
    if _client is None:
        key = os.getenv("OPENAI_API_KEY")
        if not key:
            raise RuntimeError("OPENAI_API_KEY не задан в .env")
        _client = OpenAI(api_key=key)
    return _client

def ask_llm(system: str, user: str) -> str:
    model = os.getenv("LLM_MODEL", "gpt-4o-mini")
    resp = _client_get().chat.completions.create(
        model=model,
        temperature=0,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    )
    return resp.choices[0].message.content

# ---------- SNOMED CT ----------
def search_snomed(term: str) -> List[Dict[str, Any]]:
    """Демо-данные. Реальный: browser.ihtsdotools.org/snowstorm/snomed-ct."""
    return [
        {"code": "38341003", "term": f"{term} (SNOMED concept A)", "active": True},
        {"code": "195967001", "term": f"{term} — related finding (SNOMED)", "active": True},
    ]

# ---------- ICD-11 ----------
def search_icd11(term: str) -> List[Dict[str, Any]]:
    """Демо-данные. Реальный: icd.who.int/icdapi (OAuth2)."""
    return [
        {"code": "BA00.0", "title": f"{term} — ICD-11 entity"},
        {"code": "BA00.Z", "title": f"{term}, unspecified — ICD-11"},
    ]

# ---------- Mapper ----------
def map_terms(term: str, snomed: list, icd11: list) -> Dict[str, Any]:
    system = (
        "Ты эксперт по клиническим терминологиям (SNOMED CT, ICD-11). "
        "Сопоставь концепты SNOMED CT и коды ICD-11 для заданного понятия. "
        "Для каждой пары укажи confidence 0..1 и краткое обоснование. "
        "Если соответствие неполное или отсутствует — оставь icd11_code=null и объясни. "
        "Верни СТРОГО JSON: "
        '{"mapping":[{"snomed_code":"","icd11_code":"","confidence":0.0,"reason":""}]}'
    )
    user = f"Понятие: {term}\nSNOMED CT: {snomed}\nICD-11: {icd11}"
    raw = ask_llm(system, user)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"mapping": []}

# ---------- Verifier ----------
def verify_mapping(term: str, mapping: Dict[str, Any]) -> Dict[str, Any]:
    system = (
        "Ты клинический терминовед. Проверь маппинг SNOMED→ICD-11. "
        "Для каждой пары поставь флаг: ok / needs_review / missing. "
        "Верни СТРОГО JSON: "
        '{"verification":[{"snomed_code":"","icd11_code":"","flag":"","note":""}]}'
    )
    user = f"Понятие: {term}\nМаппинг: {mapping}"
    raw = ask_llm(system, user)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"verification": []}

# ---------- Reporter ----------
def build_report(
    term: str,
    snomed: list,
    icd11: list,
    mapping: Dict[str, Any],
    verification: Dict[str, Any],
) -> str:
    system = (
        "Ты аналитик. Составь краткий отчёт: понятие, кандидаты SNOMED, "
        "кандидаты ICD-11, итоговый маппинг, спорные случаи, ограничения "
        "и рекомендация ручной проверки. По-русски."
    )
    user = (
        f"Понятие: {term}\nSNOMED: {snomed}\nICD-11: {icd11}\n"
        f"Маппинг: {mapping}\nВерификация: {verification}"
    )
    return ask_llm(system, user)