from __future__ import annotations

import time
import requests
from typing import Any

BASE = "https://api-fns.ru/api"


def _pick(d: Any, *keys: str) -> Any:
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _extract_items(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    for k in ("items", "data", "result", "results", "rows", "Результат", "Результаты"):
        v = payload.get(k)
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]
    v = _pick(payload, "data", "items")
    if isinstance(v, list):
        return [x for x in v if isinstance(x, dict)]
    return []


def _to_int(x: Any) -> int:
    try:
        if x is None:
            return 0
        s = str(x).strip().replace("+", "")
        num = ""
        for ch in s:
            if ch.isdigit():
                num += ch
            elif num:
                break
        return int(num) if num else 0
    except Exception:
        return 0


def _chunks(lst: list[str], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def multinfo_enrich(*, key: str, inns: list[str], sleep_sec: float = 1.0, batch_size: int = 25) -> dict[str, dict]:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) LeadSniperSim/1.0",
        "Accept": "application/json,text/plain,*/*",
        "Connection": "close",
    })

    out: dict[str, dict] = {}

    parts = list(_chunks(inns, batch_size))
    for bi, part in enumerate(parts, start=1):
        params = {"req": ",".join(part), "key": key}
        r = session.get(f"{BASE}/multinfo", params=params, timeout=35)

        if r.status_code != 200:
            print(f"multinfo HTTP {r.status_code}: {r.text[:200]}")
            return out

        payload = r.json()
        items = _extract_items(payload)

        for it in items:
            ul = it.get("ЮЛ") or it.get("ИП") or it
            if not isinstance(ul, dict):
                continue

            inn = str(ul.get("ИНН") or it.get("ИНН") or "").strip()
            if not inn:
                continue

            # ✅ КЛЮЧЕВОЕ: сотрудники часто лежат в ОткрСведения.КолРаб
            employees = _to_int(
                ul.get("КолРаб")
                or ul.get("ССЧР")
                or _pick(ul, "ОткрСведения", "КолРаб")
                or _pick(ul, "ОткрСведения", "ССЧР")
                or 0
            )

            okved_main = str(_pick(ul, "ОснВидДеят", "Код") or "").strip()
            region = str(_pick(ul, "Адрес", "АдресПолн") or "").strip()

            # В multinfo контакты обычно лежат в "Контакты" (а не НомТел/E-mail)
            contacts = str(ul.get("Контакты") or "").strip()

            fin = ul.get("Финансы") if isinstance(ul.get("Финансы"), dict) else {}
            revenue_year = str(fin.get("Год") or "").strip()
            revenue = _to_int(fin.get("Выручка"))  # обычно тыс.руб
            revenue_rub = str(revenue * 1000) if revenue > 0 else ""

            out[inn] = {
                "employees": employees,
                "okved_main": okved_main,
                "region": region,
                "contacts": contacts,
                "revenue_year": revenue_year,
                "revenue": revenue_rub,
            }

        print(f"multinfo batch {bi}/{len(parts)}: enriched={len(out)}")
        time.sleep(sleep_sec)

    return out