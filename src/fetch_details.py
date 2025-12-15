import time
import requests

BASE = "https://api-fns.ru/api"


def _pick(d: dict, *keys):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def enrich_companies(*, key: str, inns: list[str], sleep_sec: float):
    session = requests.Session()

    # чтобы меньше похоже на "бота"
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) LeadSniperSim/1.0",
        "Accept": "application/json,text/plain,*/*",
        "Connection": "close",
    })

    rows = []
    total = len(inns)

    for idx, inn in enumerate(inns, start=1):
        print(f"[{idx}/{total}] INN={inn} -> /egr", flush=True)

        params = {"req": inn, "key": key}
        data = None

        for attempt in range(1, 4):  # 3 попытки
            try:
                r = session.get(f"{BASE}/egr", params=params, timeout=25)

                # покажем HTTP-код если не 200
                if r.status_code != 200:
                    print(f"   HTTP {r.status_code}: {r.text[:200]}", flush=True)

                    # если лимит/бан — дальше нет смысла долбиться
                    if r.status_code in (401, 402, 403, 429):
                        return rows

                    time.sleep(2 * attempt)
                    continue

                data = r.json()
                break

            except requests.exceptions.RequestException as e:
                print(f"   EXC {type(e).__name__}: {e}", flush=True)
                time.sleep(2 * attempt)

        if not data:
            print(f"   FAIL – no data after retries", flush=True)
            time.sleep(sleep_sec)
            continue

        ul = data.get("ЮЛ") or data

        try:
            employees = int(_pick(ul, "КолРаб") or _pick(ul, "ССЧР") or 0)
        except Exception:
            employees = 0

        if employees < 100:
            print(f"   SKIP – employees={employees}", flush=True)
            time.sleep(sleep_sec)
            continue

        row = {
            "inn": inn,
            "name": _pick(ul, "НаимСокрЮЛ") or _pick(ul, "НаимПолнЮЛ") or "",
            "employees": employees,
            "okved_main": _pick(ul, "ОснВидДеят", "Код") or "",
            "source": "api-fns",
            "revenue_year": "",
            "revenue": "",
            "site": "",
            "description": "",
            "region": _pick(ul, "Адрес", "АдресПолн") or "",
            "contacts": " / ".join(filter(None, [
                _pick(ul, "НомТел"),
                _pick(ul, "E-mail"),
            ])),
        }

        rows.append(row)
        print(f"   OK – employees={employees} | rows={len(rows)}", flush=True)

        # пауза + доп. охлаждение каждые 5 запросов
        time.sleep(sleep_sec)
        if idx % 5 == 0:
            print("   cooldown 5s", flush=True)
            time.sleep(5)

    return rows