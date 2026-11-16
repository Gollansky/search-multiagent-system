from typing import Any, Dict
from medinfo_agents import (
    search_snomed,
    search_icd11,
    map_terms,
    verify_mapping,
    build_report,
)

def run(term: str, on_step=None) -> Dict[str, Any]:
    """
    Supervisor: последовательно вызывает агентов.
    on_step(name:str) — колбэк для UI (опционально).
    """
    state: Dict[str, Any] = {"term": term, "history": []}

    def step(name, fn, *args):
        if on_step:
            on_step(name)
        result = fn(*args)
        state["history"].append(f"{name}: ok")
        return result

    state["snomed"] = step("SNOMED", search_snomed, term)
    state["icd11"] = step("ICD-11", search_icd11, term)
    state["mapping"] = step("Mapper", map_terms, term, state["snomed"], state["icd11"])
    state["verification"] = step("Verifier", verify_mapping, term, state["mapping"])
    state["report"] = step(
        "Reporter", build_report, term,
        state["snomed"], state["icd11"], state["mapping"], state["verification"],
    )
    return state