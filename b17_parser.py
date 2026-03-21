"""
Парсер психологов с b17.ru (двухэтапный)
=========================================
Этап 1: Быстрый прогон по карточкам каталога с широкими ключевыми словами
Этап 2: Deep scan только по кандидатам из этапа 1, фильтр по узким военным ключевым

НАСТРОЙКА:
1. Открой Chrome -> b17.ru (авторизуйся)
2. F12 -> вкладка Network -> кликни любой запрос к b17.ru
3. Headers -> Request Headers -> скопируй всю строку Cookie:
4. Вставь в RAW_COOKIE ниже
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

# ——— НАСТРОЙКА ————————————————————————————————————————————————————

# Вставь сюда ВСЮ строку Cookie из Network tab браузера:
RAW_COOKIE = "__ddg9_=95.24.125.196; __ddg1_=R8hXeSp8ku4CJKtKTMfI; c_city_ip=321; _ym_uid=1774102149407106425; _ym_d=1774102149; _ym_isad=2; ck_stick_visit=1; utm=consultation_help; c_city=91; visits=10; c_id=1308005; password=x8bc5eb82f15f535594028251abca7a2f; sid=%201308005%20; active_update_text=0-0-0-0-; retina=1; active_update_unix=1774106119; __ddg8_=lLf7rmRfxdwwDTMa; __ddg10_=1774106310"

# Города для парсинга (slug из URL b17.ru/psiholog/ГОРОД/)
CITIES = [
    "moskva",
    "spb",
    # "novosibirsk",
    # "ekaterinburg",
]

# Сколько страниц парсить на город (на каждой ~10 специалистов)
# None = все страницы
MAX_PAGES = None

# Этап 1: широкие ключевые слова для быстрого отсева по карточкам каталога
BROAD_KEYWORDS = [
    "ПТСР", "птср", "посттравматическ", "травм", "ДПДГ", "EMDR",
    "военн", "военнослужащ", "комбатант", "СВО", "боевых действий",
]

# Этап 2: узкие ключевые слова — ищем на полном профиле кандидатов
NARROW_KEYWORDS = [
    "военн", "военнослужащ", "комбатант", "ветеран боев",
    "СВО", "боевой стресс", "боевая травма", "боевых действий",
    "участник боев", "зона боев", "вооружённ", "мобилизац",
    "афган", "чечн", "горячая точка", "горячих точ",
]

# Сколько финальных результатов набрать (None = без лимита)
MAX_RESULTS = 30

# Задержка между запросами (секунды)
DELAY_MIN = 1.0
DELAY_MAX = 2.0

OUTPUT_FILE = "b17_contacts.csv"

# ——— КОД ——————————————————————————————————————————————————————————

HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Referer": "https://www.b17.ru/",
    "Accept-Language": "ru-RU,ru;q=0.9",
    "Cookie": RAW_COOKIE,
}

session = requests.Session()
session.headers.update(HTTP_HEADERS)


def matches_keywords(text: str, keywords: list[str]) -> list[str]:
    """Проверяет текст на наличие ключевых слов. Возвращает найденные."""
    if not keywords:
        return []
    found = []
    text_lower = text.lower()
    for kw in keywords:
        if len(kw) <= 3:
            if re.search(r'\b' + re.escape(kw) + r'\b', text, re.IGNORECASE):
                found.append(kw)
        else:
            if kw.lower() in text_lower:
                found.append(kw)
    return found


def get_specialist_ids_from_page(city: str, page: int) -> list[dict]:
    """Получает список специалистов со страницы каталога."""
    url = f"https://www.b17.ru/psiholog/{city}/"
    params = {"page": page} if page > 1 else {}

    try:
        r = session.get(url, params=params, timeout=15)
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
    """Загружает полный текст профиля специалиста."""
    if not profile_url:
        return ""
    try:
        r = session.get(profile_url, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["script", "style"]):
            tag.decompose()
        return soup.get_text(separator=" ", strip=True)
    except requests.RequestException:
        return ""


def get_contacts(spec_id: str) -> dict:
    """Запрашивает контакты специалиста через API b17."""
    url = "https://www.b17.ru/telefon_backend.php"
    params = {"mod": "spec_list", "id": spec_id, "k": "0"}

    try:
        r = session.get(url, params=params, timeout=10)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"    ⚠️  Ошибка получения контактов {spec_id}: {e}")
        return {}

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
    """Сохраняет результаты в CSV."""
    if not results:
        print("⚠️  Нет данных для сохранения")
        return

    fieldnames = ["name", "phone", "whatsapp", "telegram", "profile_url", "city",
                  "matched_keywords", "description", "spec_id"]

    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    print(f"\n✅ Сохранено {len(results)} записей в {filename}")


def stage1_collect_candidates() -> list[dict]:
    """Этап 1: быстрый прогон по карточкам с широкими ключевыми словами."""
    print("=" * 60)
    print("ЭТАП 1: Быстрый сбор кандидатов по карточкам каталога")
    print(f"Широкие ключевые: {', '.join(BROAD_KEYWORDS[:6])}...")
    print("=" * 60)

    candidates = []

    for city in CITIES:
        print(f"\n🏙️  Город: {city}")
        page = 1

        while True:
            if MAX_PAGES and page > MAX_PAGES:
                break

            print(f"  📄 Страница {page}...", end=" ")
            specialists = get_specialist_ids_from_page(city, page)

            if not specialists:
                print("пусто, останавливаемся")
                break

            matched_on_page = 0
            for spec in specialists:
                kw_found = matches_keywords(spec.get("description", ""), BROAD_KEYWORDS)
                if kw_found:
                    spec["broad_keywords"] = ", ".join(set(kw_found))
                    candidates.append(spec)
                    matched_on_page += 1

            print(f"{len(specialists)} спец-тов, подходит: {matched_on_page}")

            page += 1
            time.sleep(random.uniform(1.0, 1.5))

    print(f"\n📋 Этап 1 завершён: {len(candidates)} кандидатов для глубокой проверки")
    return candidates


def stage2_deep_filter(candidates: list[dict]) -> list[dict]:
    """Этап 2: deep scan кандидатов, фильтр по узким военным ключевым."""
    print("\n" + "=" * 60)
    print("ЭТАП 2: Глубокая проверка профилей по военным ключевым")
    print(f"Узкие ключевые: {', '.join(NARROW_KEYWORDS[:6])}...")
    print(f"Кандидатов: {len(candidates)}, лимит: {MAX_RESULTS or 'без лимита'}")
    print("=" * 60)

    results = []

    for i, spec in enumerate(candidates):
        if MAX_RESULTS and len(results) >= MAX_RESULTS:
            print(f"\n🎯 Набрано {MAX_RESULTS} результатов!")
            break

        label = spec['name'] or spec['spec_id']
        print(f"  [{i+1}/{len(candidates)}] {label}...", end=" ")

        # Сначала проверяем описание карточки по узким ключевым
        kw_found = matches_keywords(spec.get("description", ""), NARROW_KEYWORDS)

        # Если не нашли — загружаем полный профиль
        if not kw_found:
            profile_text = get_profile_text(spec["profile_url"])
            kw_found = matches_keywords(profile_text, NARROW_KEYWORDS)
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

        if not kw_found:
            print("— не подходит")
            continue

        # Подходит! Получаем контакты
        spec["matched_keywords"] = ", ".join(set(kw_found))
        print(f"✅ [{spec['matched_keywords']}] ", end="")

        contacts = get_contacts(spec["spec_id"])
        spec.update(contacts)

        phone = spec.get("phone", "")
        wa = "📱WA" if spec.get("whatsapp") else ""
        tg = "✈️TG" if spec.get("telegram") else ""
        print(f"{phone} {wa} {tg}")

        results.append(spec)
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    return results


def main():
    print("🔍 Парсер b17.ru (двухэтапный)")
    print(f"   Городов: {len(CITIES)}, страниц на город: {MAX_PAGES or 'все'}")
    print(f"   Лимит результатов: {MAX_RESULTS or 'без лимита'}")

    r = session.get("https://www.b17.ru/", timeout=10)
    if "Войти" in r.text and "Выйти" not in r.text:
        print("\n❌ Не авторизован! Проверь RAW_COOKIE в настройках скрипта.")
        return

    print("✅ Авторизация OK\n")

    # Этап 1: быстрый сбор кандидатов (~20-40 мин на Мск+СПб)
    candidates = stage1_collect_candidates()

    if not candidates:
        print("\n⚠️  Кандидатов не найдено. Попробуй расширить BROAD_KEYWORDS.")
        return

    # Промежуточное сохранение кандидатов
    save_csv(candidates, "b17_candidates.csv")

    # Этап 2: deep scan кандидатов (~1-2 сек на каждого)
    results = stage2_deep_filter(candidates)

    save_csv(results, OUTPUT_FILE)

    with_phone = sum(1 for r in results if r.get("phone"))
    with_wa = sum(1 for r in results if r.get("whatsapp"))
    with_tg = sum(1 for r in results if r.get("telegram"))

    print(f"\n📊 Итоговая статистика:")
    print(f"   Кандидатов (этап 1): {len(candidates)}")
    print(f"   Подходящих (этап 2): {len(results)}")
    print(f"   С телефоном: {with_phone}")
    print(f"   С WhatsApp: {with_wa}")
    print(f"   С Telegram: {with_tg}")


if __name__ == "__main__":
    main()
