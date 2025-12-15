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
    """
    API может отдавать разные структуры. Делаем максимально “живучий” парсер.
    """
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]

    if not isinstance(payload, dict):
        return []

    # частые варианты
    for k in ("items", "data", "result", "results", "rows", "Результат", "Результаты"):
        v = payload.get(k)
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]

    # иногда список лежит внутри payload["data"]["items"]
    v = _pick(payload, "data", "items")
    if isinstance(v, list):
        return [x for x in v if isinstance(x, dict)]

    return []


def _guess_inn(item: dict) -> str:
    return str(
        item.get("ИНН")
        or item.get("inn")
        or _pick(item, "ЮЛ", "ИНН")
        or _pick(item, "ul", "inn")
        or ""
    ).strip()


def _guess_name(item: dict) -> str:
    return str(
        item.get("НаимСокрЮЛ")
        or item.get("НаимПолнЮЛ")
        or item.get("name")
        or _pick(item, "ЮЛ", "НаимСокрЮЛ")
        or _pick(item, "ЮЛ", "НаимПолнЮЛ")
        or ""
    ).strip()


def _guess_okved(item: dict) -> str:
    return str(
        _pick(item, "ОснВидДеят", "Код")
        or item.get("okved_main")
        or item.get("ОКВЭД")
        or _pick(item, "ЮЛ", "ОснВидДеят", "Код")
        or ""
    ).strip()


def _guess_employees(item: dict) -> int:
    raw = (
        item.get("КолРаб")
        or item.get("ССЧР")
        or item.get("employees")
        or _pick(item, "ЮЛ", "КолРаб")
        or _pick(item, "ЮЛ", "ССЧР")
        or 0
    )
    try:
        return int(str(raw).strip().replace("+", ""))
    except Exception:
        return 0


def search_page(*, key: str, q: str, page: int, filter_str: str | None, timeout: int = 30) -> tuple[int, Any]:
    params = {"q": q, "page": page, "key": key}
    if filter_str:
        params["filter"] = filter_str

    r = requests.get(f"{BASE}/search", params=params, timeout=timeout)
    # вернём статус и json/текст
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, r.text


def iter_companies_from_search(
    *,
    key: str,
    q: str,
    filter_str: str | None,
    sleep_sec: float,
    max_pages: int,
) -> list[dict]:
    rows: list[dict] = []

    for page in range(1, max_pages + 1):
        status, payload = search_page(key=key, q=q, page=page, filter_str=filter_str)

        if status == 403:
            # это твоя текущая проблема
            msg = ""
            if isinstance(payload, dict):
                msg = str(payload.get("error") or payload.get("message") or payload)[:200]
            else:
                msg = str(payload)[:200]
            raise RuntimeError(f"HTTP 403 from /search: {msg}")

        if status >= 400:
            # просто пропускаем страницу, но логируем
            raise RuntimeError(f"HTTP {status} from /search: {str(payload)[:200]}")

        items = _extract_items(payload)
        if not items:
            # если пусто — дальше смысла нет
            break

        for it in items:
            inn = _guess_inn(it)
            if not inn:
                continue

            rows.append({
                "inn": inn,
                "name": _guess_name(it),
                "employees": _guess_employees(it),
                "okved_main": _guess_okved(it),
                "source": "api-fns/search",
                # желательные поля (если вдруг появятся)
                "revenue_year": "",
                "revenue": "",
                "site": "",
                "description": "",
                "region": str(_pick(it, "Адрес", "АдресПолн") or it.get("region") or "").strip(),
                "contacts": "",})

        time.sleep(sleep_sec)

    return rows