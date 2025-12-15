from __future__ import annotations

import csv
import os
from typing import Any


CSV_HEADER = [
    "inn",
    "name",
    "employees",
    "okved_main",
    "source",
    "revenue_year",
    "revenue",
    "site",
    "email",
    "phone",
    "phones_other",
    "description",
    "region",
]


def _clean_str(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    # нормализация пробелов
    return " ".join(s.split())


def _to_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        s = str(x).strip()  
        if not s:
            return default
        # иногда приходят "100+" или "100-200" — берём первое число
        num = ""
        for ch in s:
            if ch.isdigit():
                num += ch
            elif num:
                break
        return int(num) if num else default
    except Exception:
        return default

def split_contacts(s: str):
    phone = ""
    phones_other = []
    email = ""
    site = ""

    if not s:
        return phone, "", email, site

    parts = [p.strip() for p in s.split(",") if p.strip()]

    for p in parts:
        if "@" in p:
            email = p
        elif p.startswith("http") or "." in p and not p.replace(".", "").isdigit():
            site = p
        elif p.isdigit():
            if not phone:
                phone = p
            else:
                phones_other.append(p)

    return phone, ",".join(phones_other), email, site

def to_csv(rows: list[dict], out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADER, delimiter=";")
        w.writeheader()

        for r in rows:
            phone, phones_other, email, site = split_contacts(
                _clean_str(r.get("contacts"))
            )
            
            w.writerow({
                "inn": _clean_str(r.get("inn")),
                "name": _clean_str(r.get("name")),
                "employees": _to_int(r.get("employees")),
                "okved_main": _clean_str(r.get("okved_main")),
                "source": _clean_str(r.get("source")),
                
                "revenue_year": _clean_str(r.get("revenue_year")),
                "revenue": _clean_str(r.get("revenue")),
                
                "site": site,
                "email": email,
                "phone": phone,
                "phones_other": phones_other,
                
                "description": _clean_str(r.get("description")),
                "region": _clean_str(r.get("region")),
            })