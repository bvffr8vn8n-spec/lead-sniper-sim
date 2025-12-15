from __future__ import annotations

import json
import os
import time
from dotenv import load_dotenv

from fetch_list import iter_companies_from_search
from fetch_multinfo import multinfo_enrich
from normalize import to_csv

PROGRESS_PATH = "data/progress.json"
OUT_CSV = "data/companies.csv"


def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v.strip() if v is not None else default


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env(name, str(default)))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(_env(name, str(default)))
    except Exception:
        return default


def load_progress() -> dict:
    if not os.path.exists(PROGRESS_PATH):
        return {"seen_inn": [], "rows": []}
    try:
        with open(PROGRESS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"seen_inn": [], "rows": []}


def save_progress(seen_inn: set[str], rows: list[dict]) -> None:
    os.makedirs("data", exist_ok=True)
    with open(PROGRESS_PATH, "w", encoding="utf-8") as f:
        json.dump({"seen_inn": sorted(seen_inn), "rows": rows}, f, ensure_ascii=False, indent=2)


def _is_it_okved(code: str) -> bool:
    code = (code or "").strip()
    return code.startswith("62") or code.startswith("63") or code.startswith("58")


def main() -> None:
    load_dotenv()

    key = _env("API_FNS_KEY")
    if not key:
        raise SystemExit("API_FNS_KEY is missing. Put it to .env")

    target_count = _env_int("TARGET_COUNT", 200)
    min_emp = _env_int("MIN_EMPLOYEES", 100)
    max_emp = _env_int("MAX_EMPLOYEES", 500)

    sleep_sec = _env_float("SLEEP_SEC", 1.0)
    q = _env("SEARCH_Q", "it")
    filter_str = _env("SEARCH_FILTER", "")
    max_pages = _env_int("MAX_PAGES", 40)

    batch_size = _env_int("MULTINFO_BATCH", 25)

    print("START")
    print(f"CONFIG: target={target_count} min_emp={min_emp} max_emp={max_emp} q={q} sleep={sleep_sec} max_pages={max_pages}")
    print(f"FILTER: {filter_str if filter_str else '(none)'}")
    print(f"MULTINFO: batch_size={batch_size}")

    prog = load_progress()
    seen_inn = set(prog.get("seen_inn") or [])
    rows: list[dict] = prog.get("rows") or []
    print(f"Loaded progress: rows={len(rows)} seen_inn={len(seen_inn)}")

    # 1) /search — кандидаты
    fetched = iter_companies_from_search(
        key=key,
        q=q,
        filter_str=filter_str if filter_str else None,
        sleep_sec=sleep_sec,
        max_pages=max_pages,
    )
    print(f"Fetched raw from search: {len(fetched)}")

    # 2) соберём список ИНН для multinfo (НЕ трогаем seen_inn тут!)
    candidates: list[dict] = []
    new_inns: list[str] = []
    cand_seen: set[str] = set()

    for it in fetched:
        inn = (it.get("inn") or "").strip()
        if not inn:
            continue
        if inn in seen_inn:
            continue
        if inn in cand_seen:
            continue

        cand_seen.add(inn)
        candidates.append(it)
        new_inns.append(inn)

        if len(candidates) >= target_count * 5:
            break

    print(f"Candidates to enrich: {len(candidates)}")

    if not candidates:
        save_progress(seen_inn, rows)
        to_csv(rows, OUT_CSV)
        print(f"DONE: rows={len(rows)} saved={OUT_CSV}")
        return

    # 3) multinfo
    enriched = multinfo_enrich(
        key=key,
        inns=new_inns,
        sleep_sec=sleep_sec,
        batch_size=batch_size,
    )
    print(f"Enriched cards: {len(enriched)}")

    # 4) merge + фильтры
    added = 0
    for it in candidates:
        inn = (it.get("inn") or "").strip()
        card = enriched.get(inn)
        if not card:
            continue

        it["employees"] = int(card.get("employees") or 0)
        if not it.get("okved_main"):
            it["okved_main"] = (card.get("okved_main") or "").strip()
        if not it.get("region"):
            it["region"] = (card.get("region") or "").strip()
        if not it.get("contacts"):it["contacts"] = (card.get("contacts") or "").strip()

        # доберём выручку (плюс к ТЗ)
        it["revenue_year"] = (card.get("revenue_year") or "").strip()
        it["revenue"] = (card.get("revenue") or "").strip()

        it["source"] = "api-fns/search+multinfo"

        emp = int(it["employees"] or 0)

        # ФИЛЬТР ТЗ: 100–500
        if emp < min_emp or emp > max_emp:
            continue
        if not _is_it_okved(it.get("okved_main", "")):
            continue

        rows.append(it)
        seen_inn.add(inn)  # ✅ отмечаем как seen только когда реально добавили в итог
        added += 1

        if added % 10 == 0:
            save_progress(seen_inn, rows)
            print(f"CHECKPOINT: rows={len(rows)} added={added}")

        if len(rows) >= target_count:
            break

    print(f"After multinfo+filters: rows={len(rows)} (added={added})")

    save_progress(seen_inn, rows)
    to_csv(rows, OUT_CSV)
    print(f"DONE: rows={len(rows)} saved={OUT_CSV}")
    time.sleep(0.2)


if __name__ == "__main__":
    main()