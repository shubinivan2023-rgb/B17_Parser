"""Повторный парсинг телефонов и telegram-ников для контактов, у которых не подтянулись данные."""

import csv
import time
import random
from b17_parser import get_contacts, safe_get, OUTPUT_FILE, DELAY_MIN, DELAY_MAX

INPUT_FILE = OUTPUT_FILE  # b17_contacts.csv

with open(INPUT_FILE, encoding="utf-8-sig") as f:
    rows = list(csv.DictReader(f))

fieldnames = rows[0].keys()

need_reparse = [r for r in rows if not r.get("phone") or r.get("telegram") in ("True", "False", "")]
print(f"Нужно перепарсить: {len(need_reparse)} из {len(rows)}")

updated = 0
for i, row in enumerate(need_reparse):
    print(f"  [{i+1}/{len(need_reparse)}] {row['name']}...", end=" ")
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    contacts = get_contacts(row["spec_id"])

    changed = False
    if contacts.get("phone") and not row.get("phone"):
        row["phone"] = contacts["phone"]
        changed = True
    if contacts.get("telegram") and row.get("telegram") in ("True", "False", ""):
        row["telegram"] = contacts["telegram"]
        changed = True
    if contacts.get("whatsapp"):
        row["whatsapp"] = contacts["whatsapp"]

    if changed:
        print(f"✅ тел: {row.get('phone', '—')} tg: {row.get('telegram', '—')}")
        updated += 1
    else:
        print("— без изменений")

with open(INPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)

print(f"\n💾 Обновлено {updated} контактов в {INPUT_FILE}")
