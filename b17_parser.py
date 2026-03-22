"""
Парсер психологов с b17.ru
==========================
Deep scan с узкими военными ключевыми словами.
Останавливается при наборе MAX_RESULTS.

НАСТРОЙКА:
1. Открой Chrome -> b17.ru (авторизуйся)
2. F12 -> вкладка Network -> кликни любой запрос к b17.ru
3. Headers -> Request Headers -> скопируй всю строку Cookie:
4. Вставь в .env файл: RAW_COOKIE="..."
5. Запусти: python b17_parser.py

Результат: b17_contacts.csv
"""

from __future__ import annotations
import requests
from bs4 import BeautifulSoup
import csv
import time
import re
import random
import os
from dotenv import load_dotenv

load_dotenv()

# ——— НАСТРОЙКА ————————————————————————————————————————————————————

RAW_COOKIE = os.getenv("RAW_COOKIE", "")

CITIES = [
    "moskva",
    "spb",
    "novosibirsk",
    "ekaterinburg",
]

MAX_PAGES = None
MAX_RESULTS = 30

KEYWORDS = [
    "военн", "военнослужащ", "комбатант", "ветеран боев",
    "СВО", "боевой стресс", "боевая травма", "боевых действий",
]

DELAY_MIN = 12.0
DELAY_MAX = 20.0

OUTPUT_FILE = "b17_contacts.csv"

# ——— КОД ——————————————————————————————————————————————————————————

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
]

# Пауза на отдых: каждые N страниц — перерыв
PAGES_BEFORE_REST = 30
REST_MIN = 300   # секунды (5 мин)
REST_MAX = 600   # секунды (10 мин)

# Если забанили — ждём и пробуем снова
BAN_WAIT = 3600  # секунды (60 мин)
BAN_RETRIES = 5  # сколько раз пытаться после бана

session = requests.Session()
session.headers.update({
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "Referer": "https://www.b17.ru/",
    "Cookie": RAW_COOKIE,
})

_request_count = 0


def safe_get(url: str, **kwargs) -> requests.Response:
    """Обёртка над session.get с ротацией User-Agent, паузами и обходом бана."""
    global _request_count
    session.headers["User-Agent"] = random.choice(USER_AGENTS)
    _request_count += 1

    # Каждые PAGES_BEFORE_REST запросов — отдыхаем
    if _request_count % (PAGES_BEFORE_REST * 10) == 0:
        rest = random.uniform(REST_MIN, REST_MAX)
        print(f"\n  😴 Отдых {int(rest // 60)} мин ({_request_count} запросов сделано)...\n")
        time.sleep(rest)

    for ban_attempt in range(BAN_RETRIES):
        r = session.get(url, **kwargs)

        # Проверяем бан
        if "IP" in r.text and "заблокирован" in r.text:
            print(f"\n  🚫 БАН! Ждём {BAN_WAIT // 60} мин (попытка {ban_attempt + 1}/{BAN_RETRIES})...")
            time.sleep(BAN_WAIT)
            session.headers["User-Agent"] = random.choice(USER_AGENTS)
            continue

        return r

    raise requests.RequestException("Не удалось обойти бан после нескольких попыток")


def matches_keywords(text: str) -> list[str]:
    if not KEYWORDS:
        return []
    found = []
    text_lower = text.lower()
    for kw in KEYWORDS:
        if len(kw) <= 3:
            if re.search(r'\b' + re.escape(kw) + r'\b', text, re.IGNORECASE):
                found.append(kw)
        else:
            if kw.lower() in text_lower:
                found.append(kw)
    return found


def get_specialist_ids_from_page(city: str, page: int) -> list[dict]:
    url = f"https://www.b17.ru/psiholog/{city}/"
    params = {"page": page} if page > 1 else {}

    try:
        r = safe_get(url, params=params, timeout=15)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"  ⚠️  Ошибка загрузки страницы {page}: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    specialists = []

    for el in soup.find_all(onclick=re.compile(r"show_kontakt")):
        onclick = el.get("onclick", "")
        match = re.search(r"show_kontakt\('spec_list','([^']+)'", onclick)
        if not match:
            continue
        spec_id = match.group(1)

        card = el.find_parent("div", class_="text")
        name = ""
        profile_url = ""
        description = ""

        if card:
            name_el = card.find("a", class_="h")
            if name_el:
                name = name_el.get_text(strip=True)
                href = name_el.get("href", "")
                profile_url = "https://www.b17.ru" + href if href.startswith("/") else href

            desc_el = card.find("div", class_="t")
            if desc_el:
                description = desc_el.get_text(strip=True)

        specialists.append({
            "spec_id": spec_id,
            "name": name,
            "profile_url": profile_url,
            "city": city,
            "description": description,
        })

    return specialists


def get_profile_text(profile_url: str) -> str:
    if not profile_url:
        return ""
    try:
        r = safe_get(profile_url, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["script", "style"]):
            tag.decompose()
        return soup.get_text(separator=" ", strip=True)
    except requests.RequestException:
        return ""


def get_contacts(spec_id: str, retries: int = 3) -> dict:
    url = "https://www.b17.ru/telefon_backend.php"
    params = {"mod": "spec_list", "id": spec_id, "k": "0"}

    for attempt in range(retries):
        try:
            r = safe_get(url, params=params, timeout=10)
            r.raise_for_status()
            break
        except requests.RequestException as e:
            if attempt < retries - 1 and "503" in str(e):
                wait = 5 * (attempt + 1)
                print(f"⏳ 503, жду {wait}с...", end=" ")
                time.sleep(wait)
            else:
                print(f"⚠️ Ошибка контактов: {e}")
                return {"phone": "", "whatsapp": False, "telegram": False, "contact_error": "503"}

    contacts = {"phone": "", "whatsapp": False, "telegram": False}

    try:
        data = r.json()
        html = data.get("kontakt", "")
    except (ValueError, KeyError):
        html = r.text

    phone_match = re.search(r"\+7[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}", html)
    if phone_match:
        contacts["phone"] = phone_match.group(0)

    html_lower = html.lower()
    if "whatsapp" in html_lower:
        contacts["whatsapp"] = True
    if "telegram" in html_lower:
        contacts["telegram"] = True

    return contacts


def save_csv(results: list[dict], filename: str):
    if not results:
        print("⚠️  Нет данных для сохранения")
        return

    fieldnames = ["name", "phone", "whatsapp", "telegram", "profile_url", "city",
                  "matched_keywords", "contact_error", "description", "spec_id"]

    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    print(f"\n💾 Сохранено {len(results)} записей в {filename}")


def main():
    print("🔍 Парсер b17.ru — deep scan, военные ключевые")
    print(f"   Города: {', '.join(CITIES)}")
    print(f"   Лимит: {MAX_RESULTS} результатов")
    print(f"   Ключевые слова: {', '.join(KEYWORDS[:5])}...")

    r = safe_get("https://www.b17.ru/", timeout=10)
    if "Войти" in r.text and "Выйти" not in r.text:
        print("\n❌ Не авторизован! Проверь RAW_COOKIE в .env")
        return

    print("✅ Авторизация OK\n")

    all_results = []
    done = False

    cities = CITIES[:]
    random.shuffle(cities)
    print(f"   Порядок городов: {', '.join(cities)}")

    for city in cities:
        if done:
            break

        print(f"🏙️  Город: {city} (набрано {len(all_results)}/{MAX_RESULTS})")
        page = 1

        while True:
            if MAX_PAGES and page > MAX_PAGES:
                break

            print(f"  📄 Стр. {page}...", end=" ")
            specialists = get_specialist_ids_from_page(city, page)

            if not specialists:
                print("пусто, следующий город")
                break

            print(f"{len(specialists)} чел.")

            for i, spec in enumerate(specialists):
                label = spec['name'] or spec['spec_id']
                print(f"    [{i+1}/{len(specialists)}] {label}...", end=" ")

                # Ищем в описании карточки
                kw_found = matches_keywords(spec.get("description", ""))

                # Deep scan — загружаем профиль
                if not kw_found:
                    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
                    profile_text = get_profile_text(spec["profile_url"])
                    kw_found = matches_keywords(profile_text)

                if not kw_found:
                    print("—")
                    time.sleep(random.uniform(8.0, 14.0))
                    continue

                # Подходит!
                spec["matched_keywords"] = ", ".join(set(kw_found))
                print(f"✅ [{spec['matched_keywords']}] ", end="")

                # Пауза перед запросом контактов
                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
                contacts = get_contacts(spec["spec_id"])
                spec.update(contacts)

                phone = spec.get("phone", "")
                wa = "📱WA" if spec.get("whatsapp") else ""
                tg = "✈️TG" if spec.get("telegram") else ""
                print(f"{phone} {wa} {tg}")

                all_results.append(spec)

                # Промежуточное сохранение каждые 5 результатов
                if len(all_results) % 5 == 0:
                    save_csv(all_results, OUTPUT_FILE)

                if len(all_results) >= MAX_RESULTS:
                    print("\n" + "=" * 60)
                    print(f"🎯 ГОТОВО! Набрано {len(all_results)}/{MAX_RESULTS} результатов!")
                    print(f"💾 Данные сохранены в {OUTPUT_FILE}")
                    print("=" * 60 + "\n")
                    done = True
                    break

                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

            page += 1
            time.sleep(random.uniform(25.0, 45.0))  # Долгая пауза между страницами

    save_csv(all_results, OUTPUT_FILE)

    with_phone = sum(1 for r in all_results if r.get("phone"))
    with_wa = sum(1 for r in all_results if r.get("whatsapp"))
    with_tg = sum(1 for r in all_results if r.get("telegram"))

    with_error = sum(1 for r in all_results if r.get("contact_error"))

    print(f"\n📊 Итого:")
    print(f"   Найдено: {len(all_results)}")
    print(f"   С телефоном: {with_phone}")
    print(f"   С WhatsApp: {with_wa}")
    print(f"   С Telegram: {with_tg}")
    if with_error:
        print(f"   ⚠️  С ошибкой 503 (нет контактов): {with_error}")


if __name__ == "__main__":
    main()
