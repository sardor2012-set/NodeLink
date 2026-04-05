import asyncio
import logging
import os
import re
import threading
import time
from datetime import datetime, timezone, timedelta

import psycopg2
import psycopg2.extras
from flask import Flask, render_template, request, Response, jsonify
from flask_cors import CORS
import requests as http_requests
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message,
    CallbackQuery,
    PreCheckoutQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    WebAppInfo,
    LabeledPrice,
)
from aiogram.filters import CommandStart, CommandObject, Command
from aiogram.exceptions import TelegramAPIError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Online presence tracking: {user_id: last_seen_unix_timestamp}
_online_sessions: dict = {}
_ONLINE_TTL = 90  # seconds — user considered online if pinged within this window
_peak_online: int = 0

BOT_TOKEN = os.getenv("BOT_TOKEN", "8727272136:AAFrs1TtgXSeFO06RPnM7v_wd9CqSqPuRm0")
MINI_APP_URL = os.getenv(
    "MINI_APP_URL",
    "https://nodelink-wgst.onrender.com",
)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://node:ksp4VvuGPR7PMzbiAp7BltZXOTPlxlRm@dpg-d71qmo2a214c73e9aue0-a.oregon-postgres.render.com/nodelink_db",
)

REQUIRED_CHANNELS = {
    "NodeLink news": "@NodeLink_news",
}

MENU = (
    ""
)

MENU_TEXT = (
    "<tg-emoji emoji-id=\"5413694143601842851\">👋</tg-emoji> <b><u>Добро пожаловать в лучший реферальный бот — NodeLink! </u><tg-emoji emoji-id=\"5463071033256848094\">🔝</tg-emoji>\n\n"
    "<tg-emoji emoji-id=\"5346123450358444391\">🤑</tg-emoji> Зарабатывай по 5 коинов (HolyWorld Lite) за каждого приглашённого друга — быстро, просто и без ограничений!\n"
"А хочешь ещё больше? Выполняй задания и получай ещё более ценные награды! <tg-emoji emoji-id=\"5282843764451195532\">💰</tg-emoji>\n\n"
    "<tg-emoji emoji-id=\"5188481279963715781\">🚀</tg-emoji> Поделись своей реферальной ссылкой и наблюдай, как растёт твой баланс.\n"
"Все заработанные монеты ты можешь тратить на коины, товары и привилегии на сервере HolyWorld Lite! <tg-emoji emoji-id=\"5345959142089573046\">🔗</tg-emoji>\n\n"
    "<tg-emoji emoji-id=\"5278467510604160626\">💸</tg-emoji> Выбирай нужные кнопки ниже и начинай путь к большим заработкам!</b>"
)

MENU_PHOTO = os.getenv(
    "MENU_PHOTO",
    "https://ibb.co/jNhJT8w",
)

MENU_P = os.getenv(
    "MENU_P",
    "https://ibb.co/pvYq6HTs",
)

BOT_USERNAME = "NodeLinkBot"

CRYPTO_PAY_TOKEN = os.getenv(
    "CRYPTO_PAY_TOKEN", "558886:AAvUrdyCP8DoVBtT6Rp9Z9AidGKzCXXKAxd"
)
CRYPTO_PAY_API = "https://pay.crypt.bot/api"

PREMIUM_PRICE_STARS = 69
PREMIUM_PRICE_USDT = "1.30"
PREMIUM_DAYS = 30
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "NodeLinkSupport")

# ==================== EVENT CONFIG ====================
MSK = timezone(timedelta(hours=3))
EVENT_HOUR_MSK = 17  # 17:00 по МСК
ANNOUNCEMENT_CHAT_ID = os.getenv("ANNOUNCEMENT_CHAT_ID", "")

EVENT_PRIZES = {1: 150, 2: 100, 3: 50}
MEDAL = {1: "🥇", 2: "🥈", 3: "🥉"}

ADMIN_IDS = {7592032451}
ADMIN_PASSWORD = "789456123"

pending_inviters: dict[int, int] = {}

app = Flask(__name__)
CORS(app)

_bot_app = None
_bot_loop = None


# ==================== DATABASE ====================


def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    return conn


# ==================== EVENT HELPERS ====================


def get_last_sunday_17_msk() -> datetime:
    """Return the most recent Sunday at 17:00 MSK (as timezone-aware datetime)."""
    now = datetime.now(MSK)
    # weekday(): Mon=0 ... Sun=6
    days_since_sunday = (now.weekday() + 1) % 7  # 0 if today is Sunday
    last_sunday = now - timedelta(days=days_since_sunday)
    candidate = last_sunday.replace(
        hour=EVENT_HOUR_MSK, minute=0, second=0, microsecond=0
    )
    # If today is Sunday but 17:00 hasn't arrived yet, step back one week
    if candidate > now:
        candidate -= timedelta(weeks=1)
    return candidate


def get_next_sunday_17_msk() -> datetime:
    """Return the next Sunday at 17:00 MSK (as timezone-aware datetime)."""
    return get_last_sunday_17_msk() + timedelta(weeks=1)


def send_telegram_message(chat_id, text: str):
    """Send a Telegram message via Bot API (synchronous, for background threads)."""
    if not chat_id:
        return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        http_requests.post(
            url,
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
            },
            timeout=10,
        )
    except Exception as e:
        logger.error("send_telegram_message error: %s", e)


def finalize_event():
    """
    Finalize the current weekly event:
    1. Find top-3 users by event_referral_count
    2. Award coins to winners
    3. Send announcement to ANNOUNCEMENT_CHAT_ID and notify winners
    4. Reset event_referral_count for all users
    5. Create a new event starting now
    """
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Get the current unfinalized event
        cur.execute("""
            SELECT id, started_at FROM weekly_events
            WHERE finalized = FALSE
            ORDER BY started_at ASC
            LIMIT 1
        """)
        event = cur.fetchone()
        if not event:
            cur.close()
            conn.close()
            return

        event_id = event["id"]

        # Get top-3 winners
        cur.execute("""
            SELECT telegram_id, username, first_name, nick, event_referral_count
            FROM users
            WHERE event_referral_count > 0
            ORDER BY event_referral_count DESC
            LIMIT 3
        """)
        winners = cur.fetchall()

        # Award coins to winners
        winner_ids = [None, None, None]
        winner_counts = [0, 0, 0]
        for idx, w in enumerate(winners):
            prize = EVENT_PRIZES.get(idx + 1, 0)
            cur.execute(
                "UPDATE users SET balance = balance + %s WHERE telegram_id = %s",
                (prize, w["telegram_id"]),
            )
            winner_ids[idx] = w["telegram_id"]
            winner_counts[idx] = w["event_referral_count"]

        now_msk = datetime.now(MSK)

        # Mark event as finalized
        cur.execute(
            """
            UPDATE weekly_events
            SET finalized = TRUE,
                ended_at = %s,
                winner1_id = %s, winner2_id = %s, winner3_id = %s,
                winner1_count = %s, winner2_count = %s, winner3_count = %s
            WHERE id = %s
        """,
            (
                now_msk,
                winner_ids[0],
                winner_ids[1],
                winner_ids[2],
                winner_counts[0],
                winner_counts[1],
                winner_counts[2],
                event_id,
            ),
        )

        # Reset event_referral_count for all users
        cur.execute("UPDATE users SET event_referral_count = 0")

        # Create new event starting now
        new_start = get_last_sunday_17_msk()
        cur.execute("INSERT INTO weekly_events (started_at) VALUES (%s)", (new_start,))

        conn.commit()
        cur.close()
        conn.close()

        logger.info("Weekly event #%d finalized. Winners: %s", event_id, winner_ids)

        # Build announcement message
        _send_event_announcement(winners)

        # Notify each winner personally
        for idx, w in enumerate(winners):
            prize = EVENT_PRIZES.get(idx + 1, 0)
            medal = MEDAL.get(idx + 1, "")
            nick = (
                w["nick"] or w["first_name"] or w["username"] or str(w["telegram_id"])
            )
            send_telegram_message(
                w["telegram_id"],
                f"{medal} Поздравляем!\n\n"
                f"Ты занял(а) <b>{idx + 1} место</b> в еженедельном ивенте!\n"
                f"Твой результат: <b>{w['event_referral_count']} приглашений</b>\n"
                f"Награда: <b>+{prize} коинов</b> уже зачислена на баланс 🎉",
            )

    except Exception as e:
        logger.error("finalize_event error: %s", e)


def _send_event_announcement(winners: list):
    """Build and send the event results message to the announcement channel."""
    lines = ["<tg-emoji emoji-id=\"5217822164362739968\">👑</tg-emoji> <b>Еженедельный ивент завершён!</b>\n"]

    if winners:
        lines.append("<tg-emoji emoji-id=\"5217822164362739968\">🏆</tg-emoji> <b>Победители:</b>")
        for idx, w in enumerate(winners):
            medal = MEDAL.get(idx + 1, "")
            nick = (
                w["nick"] or w["first_name"] or w["username"] or str(w["telegram_id"])
            )
            uname = f" (@{w['username']})" if w["username"] else ""
            count = w["event_referral_count"]
            lines.append(f"{medal} {nick}{uname} — {count} приглашений")
    else:
        lines.append("В этом ивенте никто не пригласил друзей.")

    lines.append("")
    lines.append(
        "<tg-emoji emoji-id=\"5193018401810822951\">🎉</tg-emoji> <b>Новый ивент уже начался!</b> Приглашайте друзей, чтобы попасть в топ!"
    )
    lines.append("")
    lines.append("▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")
    lines.append("<tg-emoji emoji-id=\"5431420156532235514\">⚜️</tg-emoji> <b>Призы за ТОП-3:</b>")
    lines.append("<tg-emoji emoji-id=\"5440539497383087970\">🥇</tg-emoji> 1 место — 150 коинов")
    lines.append("<tg-emoji emoji-id=\"5447203607294265305\">🥈</tg-emoji> 2 место — 100 коинов")
    lines.append("<tg-emoji emoji-id=\"5453902265922376865\">🥉</tg-emoji> 3 место — 50 коинов")
    lines.append("")
    lines.append("<tg-emoji emoji-id=\"5397782088634081594\">🔹</tg-emoji> Итоги подводятся каждое воскресенье в 17:00 (МСК)")

    text = "\n".join(lines)

    if ANNOUNCEMENT_CHAT_ID:
        send_telegram_message(ANNOUNCEMENT_CHAT_ID, text)
    else:
        logger.warning(
            "ANNOUNCEMENT_CHAT_ID не задан — объявление не отправлено в канал"
        )


def ensure_current_event():
    """Check if the current event has ended; if so, finalize it."""
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id, started_at FROM weekly_events
            WHERE finalized = FALSE
            ORDER BY started_at ASC
            LIMIT 1
        """)
        event = cur.fetchone()
        cur.close()
        conn.close()

        if not event:
            return

        event_end = event["started_at"] + timedelta(weeks=1)
        now = datetime.now(timezone.utc)

        if now >= event_end.astimezone(timezone.utc):
            logger.info("Event #%d has ended, finalizing...", event["id"])
            finalize_event()

    except Exception as e:
        logger.error("ensure_current_event error: %s", e)


def event_loop():
    """Background thread: check every minute if the current event should end."""
    while True:
        ensure_current_event()
        time.sleep(60)


def init_db():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            telegram_id BIGINT PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            nick TEXT,
            balance INTEGER DEFAULT 0,
            referral_count INTEGER DEFAULT 0,
            status TEXT DEFAULT 'Игрок',
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    cur.execute("""
        ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_count INTEGER DEFAULT 0
    """)

    cur.execute("""
        ALTER TABLE users ADD COLUMN IF NOT EXISTS event_referral_count INTEGER DEFAULT 0
    """)

    cur.execute("""
        ALTER TABLE users ADD COLUMN IF NOT EXISTS is_blocked BOOLEAN DEFAULT FALSE
    """)

    cur.execute("""
        ALTER TABLE users ADD COLUMN IF NOT EXISTS block_reason TEXT
    """)

    cur.execute("""
        ALTER TABLE users ADD COLUMN IF NOT EXISTS premium_until TIMESTAMPTZ
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS weekly_events (
            id SERIAL PRIMARY KEY,
            started_at TIMESTAMPTZ NOT NULL,
            ended_at TIMESTAMPTZ,
            finalized BOOLEAN DEFAULT FALSE,
            winner1_id BIGINT,
            winner2_id BIGINT,
            winner3_id BIGINT,
            winner1_count INTEGER DEFAULT 0,
            winner2_count INTEGER DEFAULT 0,
            winner3_count INTEGER DEFAULT 0
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS referrals (
            id SERIAL PRIMARY KEY,
            inviter_id BIGINT NOT NULL,
            invitee_id BIGINT NOT NULL,
            joined_at TIMESTAMPTZ DEFAULT NOW(),
            confirmed BOOLEAN DEFAULT FALSE,
            confirmed_at TIMESTAMPTZ,
            expired BOOLEAN DEFAULT FALSE,
            expires_at TIMESTAMPTZ DEFAULT (NOW() + INTERVAL '7 days'),
            UNIQUE(inviter_id, invitee_id)
        )
    """)
    cur.execute("""
        ALTER TABLE referrals ADD COLUMN IF NOT EXISTS expired BOOLEAN DEFAULT FALSE
    """)
    cur.execute("""
        ALTER TABLE referrals DROP CONSTRAINT IF EXISTS referrals_invitee_id_key
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price INTEGER NOT NULL,
            description TEXT,
            icon TEXT DEFAULT 'fa-solid fa-star',
            color TEXT DEFAULT 'purple',
            is_active BOOLEAN DEFAULT TRUE,
            sort_order INTEGER DEFAULT 0
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS purchases (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL REFERENCES users(telegram_id),
            product_id INTEGER NOT NULL REFERENCES products(id),
            purchased_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute(
        "ALTER TABLE purchases ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'pending'"
    )
    cur.execute("ALTER TABLE purchases ADD COLUMN IF NOT EXISTS item_name TEXT")
    cur.execute("ALTER TABLE purchases ADD COLUMN IF NOT EXISTS price INTEGER")
    cur.execute("ALTER TABLE purchases ADD COLUMN IF NOT EXISTS completed_by BIGINT")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS promo_codes (
            id SERIAL PRIMARY KEY,
            code TEXT UNIQUE NOT NULL,
            coins_amount INTEGER NOT NULL DEFAULT 0,
            max_uses INTEGER NOT NULL DEFAULT 1,
            used_count INTEGER DEFAULT 0,
            expires_at TIMESTAMPTZ,
            is_active BOOLEAN DEFAULT TRUE
        )
    """)
    cur.execute(
        "ALTER TABLE promo_codes ADD COLUMN IF NOT EXISTS discount_percent INTEGER DEFAULT 0"
    )
    cur.execute(
        "ALTER TABLE promo_codes ADD COLUMN IF NOT EXISTS categories TEXT DEFAULT 'all'"
    )
    cur.execute(
        "ALTER TABLE promo_codes ADD COLUMN IF NOT EXISTS single_use BOOLEAN DEFAULT TRUE"
    )
    cur.execute(
        "ALTER TABLE promo_codes ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()"
    )

    cur.execute("""
        CREATE TABLE IF NOT EXISTS promo_usages (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            promo_id INTEGER NOT NULL REFERENCES promo_codes(id),
            used_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(user_id, promo_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_activity (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            activity_date DATE NOT NULL,
            UNIQUE(user_id, activity_date)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS moderators (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            added_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            photo_url TEXT,
            reward INTEGER NOT NULL DEFAULT 0,
            task_type TEXT NOT NULL DEFAULT 'other',
            video_url TEXT,
            channel_link TEXT,
            button_text TEXT,
            button_url TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS task_completions (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL REFERENCES users(telegram_id),
            task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
            completed_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(user_id, task_id)
        )
    """)

    # Sync product catalog — upsert by explicit ID so updates apply on every restart
    catalog = [
        # id, name, category, price, description, icon, color, sort_order
        # Коины — каждый в своём цвете
        (101, "🌕 10 Коинов",   "currency", 10,  "Пополни баланс в HolyWorld на 10 коинов. Быстро и удобно.",                "fa-solid fa-coins", "green",  101),
        (102, "🌕 15 Коинов",   "currency", 15,  "Пополни баланс в HolyWorld на 15 коинов.",                                  "fa-solid fa-coins", "teal",   102),
        (103, "🌕 25 Коинов",   "currency", 25,  "25 коинов на игровой счёт — начни тратить с выгодой!",                      "fa-solid fa-coins", "blue",   103),
        (104, "🌕 35 Коинов",   "currency", 35,  "35 коинов — отличный старт для новичка.",                                   "fa-solid fa-coins", "orange", 104),
        (105, "🌕 50 Коинов",   "currency", 45,  "50 коинов по выгодной цене. Экономия 10%!",                                 "fa-solid fa-coins", "yellow", 105),
        (106, "🌕 65 Коинов",   "currency", 55,  "65 коинов — больше возможностей в HolyWorld.",                              "fa-solid fa-coins", "orange", 106),
        (107, "🌕 80 Коинов",   "currency", 75,  "80 коинов с хорошей скидкой. Успей взять!",                                 "fa-solid fa-coins", "pink",   107),
        (108, "🌕 100 Коинов",  "currency", 80,  "100 коинов — выгоднее на 20% по сравнению с поштучной покупкой!",           "fa-solid fa-coins", "purple", 108),
        (109, "🌕 150 Коинов",  "currency", 120, "150 коинов оптом — серьёзная экономия для опытных игроков.",                "fa-solid fa-coins", "pink",   109),
        (110, "🌕 200 Коинов",  "currency", 150, "200 коинов по максимально выгодной цене!",                                  "fa-solid fa-coins", "yellow", 110),
        # Кейсы — чередуем цвета
        (401, "🎁 Кейс с донатом [Без удачи]",   "cases", 120, "Рандомный донат внутри — стандартный шанс на выигрыш.",                  "fa-solid fa-gift",     "blue",   401),
        (402, "🎁 Кейс с донатом [Удача +15%]",  "cases", 140, "Кейс с донатом и повышенным шансом удачи +15%.",                        "fa-solid fa-gift",     "green",  402),
        (403, "🎁 Кейс с донатом [Удача +30%]",  "cases", 160, "Максимальная удача +30% — лучший шанс выбить ценный донат.",            "fa-solid fa-gift",     "orange", 403),
        (404, "🗳 Кейс с сапфирами [3 штуки]",   "cases",  70, "3 сапфира случайного номинала в одном кейсе.",                          "fa-solid fa-box-open", "teal",   404),
        (405, "🗳 Кейс с сапфирами [10 штук]",   "cases", 220, "10 сапфиров в кейсе — отличный выбор для коллекционеров.",              "fa-solid fa-box-open", "purple", 405),
        (406, "🗳 Кейс с сапфирами [25 штук]",   "cases", 480, "25 сапфиров из кейса — максимальный запас!",                            "fa-solid fa-box-open", "pink",   406),
        # Сапфиры — плавный переход от teal к purple
        (501, "💎 100 Сапфиров",   "sapphires",   25, "100 сапфиров для внутриигровых улучшений на HolyWorld.",                "fa-regular fa-gem", "teal",   501),
        (502, "💎 250 Сапфиров",   "sapphires",   55, "250 сапфиров — больше возможностей для развития.",                      "fa-regular fa-gem", "green",  502),
        (503, "💎 500 Сапфиров",   "sapphires",  100, "500 сапфиров — крупный пакет по выгодной цене.",                        "fa-regular fa-gem", "blue",   503),
        (504, "💎 1000 Сапфиров",  "sapphires",  200, "1000 сапфиров — серьёзный запас для любого игрока.",                    "fa-regular fa-gem", "teal",   504),
        (505, "💎 2500 Сапфиров",  "sapphires",  480, "2500 сапфиров оптом — экономия 15%!",                                   "fa-regular fa-gem", "blue",   505),
        (506, "💎 5000 Сапфиров",  "sapphires",  950, "5000 сапфиров — выгодная закупка для хардкор-игроков.",                 "fa-regular fa-gem", "purple", 506),
        (507, "💎 7500 Сапфиров",  "sapphires", 1400, "7500 сапфиров — крупнейший пакет по лучшей цене.",                      "fa-regular fa-gem", "orange", 507),
        (508, "💎 12500 Сапфиров", "sapphires", 2300, "12500 сапфиров — элитный запас со скидкой.",                            "fa-regular fa-gem", "pink",   508),
        (509, "💎 25000 Сапфиров", "sapphires", 4390, "25000 сапфиров — максимальный пакет для настоящих ценителей.",          "fa-regular fa-gem", "purple", 509),
        # Прочие — у каждого свой цвет
        (601, "📁 PREMIUM+ [30 дней]",        "others", 260, "30 дней PREMIUM+ статуса на сервере HolyWorld Lite.",                       "fa-solid fa-folder-open", "orange", 601),
        (602, "📁 Восстановление аккаунта",   "others", 250, "Восстановление потерянного игрового аккаунта по запросу администрации.",   "fa-solid fa-rotate-left", "blue",   602),
        (603, "📁 Перенос доната",            "others", 250, "Перенос приобретённого доната на другой аккаунт или сервер.",              "fa-solid fa-right-left",  "green",  603),
        (605, "📁 Разбан",                    "others", 200, "Снятие бана с игрового аккаунта на HolyWorld.",                            "fa-solid fa-lock-open",   "teal",   605),
        (606, "📁 Размут",                    "others", 100, "Снятие мута (запрета на общение) в игре или на сервере.",                  "fa-solid fa-microphone",  "purple", 606),
        # Привилегии — цвета растут вместе с рангом
        (11,  "⚜️ Грифер [1 мес]",           "privileges",   15, "Привилегия Грифер на 1 месяц — начальный уровень доступа в HolyWorld.",         "fa-solid fa-shield",       "green",  11),
        (12,  "⚜️ Мустанг [1 мес]",          "privileges",   60, "Привилегия Мустанг на 1 месяц — расширенные игровые возможности.",               "fa-solid fa-shield-halved","teal",   12),
        (13,  "⚜️ Гаст [1 мес]",             "privileges",  180, "Привилегия Гаст на 1 месяц — для опытных и уважаемых игроков.",                  "fa-solid fa-medal",        "blue",   13),
        (14,  "⚜️ Визер [1 мес]",            "privileges",  390, "Привилегия Визер на 1 месяц — высокий уровень привилегий сервера.",               "fa-solid fa-crown",        "purple", 14),
        (15,  "⚜️ Кракен [1 мес]",           "privileges",  760, "Привилегия Кракен на 1 месяц — элитный статус с особыми бонусами.",               "fa-solid fa-crown",        "orange", 15),
        (16,  "⚜️ Драгон [1 мес]",           "privileges", 1000, "Привилегия Драгон на 1 месяц — легендарный доступ и возможности.",                "fa-solid fa-fire",         "orange", 16),
        (17,  "⚜️ Стингер [1 мес]",          "privileges", 1390, "Привилегия Стингер на 1 месяц — мощные бонусы для самых активных.",               "fa-solid fa-bolt",         "yellow", 17),
        (18,  "⚜️ Етернити [1 мес]",         "privileges", 2190, "Привилегия Етернити на 1 месяц — максимальный статус на сервере.",                 "fa-solid fa-infinity",     "pink",   18),
        (19,  "⚜️ Кастомный Донат [1 мес]",  "privileges", 3290, "Персональный кастомный донат на 1 месяц — уникальный пакет привилегий.",           "fa-solid fa-star",         "yellow", 19),
    ]
    new_ids = tuple(p[0] for p in catalog)
    # Remove obsolete products (no existing purchases referencing them)
    cur.execute(
        "DELETE FROM products WHERE id NOT IN %s AND id NOT IN (SELECT DISTINCT product_id FROM purchases)",
        (new_ids,)
    )
    # Upsert: insert or update all catalog entries
    for p in catalog:
        cur.execute(
            """INSERT INTO products (id, name, category, price, description, icon, color, sort_order)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (id) DO UPDATE SET
                   name=EXCLUDED.name, category=EXCLUDED.category, price=EXCLUDED.price,
                   description=EXCLUDED.description, icon=EXCLUDED.icon,
                   color=EXCLUDED.color, sort_order=EXCLUDED.sort_order""",
            p,
        )
    # Advance sequence past all explicit IDs so admin-added products don't collide
    cur.execute("SELECT setval('products_id_seq', GREATEST(nextval('products_id_seq'), 10000))")

    # Seed a test promo code
    cur.execute("SELECT COUNT(*) FROM promo_codes")
    pc = cur.fetchone()[0]
    if pc == 0:
        cur.execute(
            "INSERT INTO promo_codes (code, coins_amount, discount_percent, max_uses) VALUES (%s, %s, %s, %s)",
            ("NODELINK2025", 0, 10, 100),
        )
    else:
        cur.execute(
            "UPDATE promo_codes SET discount_percent = 10 WHERE code = 'NODELINK2025' AND discount_percent = 0"
        )

    # Ensure an active (unfinalized) weekly event exists
    cur.execute("SELECT id FROM weekly_events WHERE finalized = FALSE LIMIT 1")
    if not cur.fetchone():
        event_start = get_last_sunday_17_msk()
        cur.execute(
            "INSERT INTO weekly_events (started_at) VALUES (%s)", (event_start,)
        )
        logger.info("Created initial weekly event starting at %s", event_start)

    conn.commit()
    cur.close()
    conn.close()
    logger.info("Database initialized successfully.")


def expire_old_referrals():
    """Mark unconfirmed referrals older than 7 days as expired."""
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            UPDATE referrals
            SET expired = TRUE
            WHERE confirmed = FALSE AND expired = FALSE AND expires_at < NOW()
        """)
        updated = cur.rowcount
        if updated > 0:
            logger.info("Marked %d referrals as expired", updated)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error("expire_old_referrals error: %s", e)


def referral_cleanup_loop():
    """Background thread: expire old referrals every hour."""
    while True:
        expire_old_referrals()
        time.sleep(3600)


# ==================== FLASK API ====================

_BLOCK_PROTECTED_ROUTES = {
    "/api/purchase",
    "/api/promo/check",
    "/api/promo/activate",
    "/api/user/nick",
    "/api/tasks/complete",
    "/api/tasks/check-subscription",
    "/api/premium/invoice/stars",
    "/api/premium/invoice/crypto",
}


@app.before_request
def enforce_block_on_api():
    """Block blocked users from using any sensitive API endpoint."""
    if request.path not in _BLOCK_PROTECTED_ROUTES:
        return None
    data = request.get_json(silent=True) or {}
    telegram_id = (
        data.get("telegram_id")
        or data.get("user_id")
        or request.args.get("telegram_id")
        or request.args.get("user_id")
    )
    if not telegram_id:
        return None
    try:
        is_blocked, block_reason = get_user_block_status(int(telegram_id))
        if is_blocked:
            return jsonify({"error": "blocked", "is_blocked": True, "block_reason": block_reason}), 403
    except Exception:
        pass
    return None


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/admin")
def admin():
    return render_template("admin.html")


@app.route("/api/user/register", methods=["POST"])
def register_user():
    data = request.json or {}
    telegram_id = data.get("telegram_id")
    username = data.get("username", "")
    first_name = data.get("first_name", "")

    if not telegram_id:
        return jsonify({"error": "missing telegram_id"}), 400

    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Check block status BEFORE upsert — blocked users must not update their data
        cur.execute(
            "SELECT * FROM users WHERE telegram_id = %s",
            (telegram_id,),
        )
        existing = cur.fetchone()
        if existing and existing.get("is_blocked"):
            cur.close()
            conn.close()
            return jsonify({
                "is_blocked": True,
                "block_reason": existing.get("block_reason"),
            })

        cur.execute(
            """
            INSERT INTO users (telegram_id, username, first_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (telegram_id) DO UPDATE
              SET username = EXCLUDED.username,
                  first_name = EXCLUDED.first_name
            RETURNING *
        """,
            (telegram_id, username, first_name),
        )
        user = cur.fetchone()
        # Check if premium has expired and revert to Игрок
        if user and user.get("status") == "Premium" and user.get("premium_until"):
            if user["premium_until"] < datetime.now(timezone.utc):
                cur.execute(
                    "UPDATE users SET status = 'Игрок', premium_until = NULL WHERE telegram_id = %s RETURNING *",
                    (telegram_id,),
                )
                user = cur.fetchone()
        conn.commit()
        result = dict(user)
        if result.get("premium_until"):
            result["premium_until"] = result["premium_until"].isoformat()
        if result.get("created_at"):
            result["created_at"] = result["created_at"].isoformat()
        cur.close()
        conn.close()
        return jsonify(result)
    except Exception as e:
        logger.error("register_user error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/user/<int:user_id>")
def get_user(user_id):
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM users WHERE telegram_id = %s", (user_id,))
        user = cur.fetchone()
        cur.close()
        conn.close()
        if not user:
            return jsonify({"error": "not found"}), 404
        return jsonify(dict(user))
    except Exception as e:
        logger.error("get_user error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/user/nick", methods=["POST"])
def set_nick():
    data = request.json or {}
    telegram_id = data.get("telegram_id")
    nick = (data.get("nick") or "").strip()

    if not telegram_id:
        return jsonify({"error": "missing telegram_id"}), 400
    if not nick:
        return jsonify({"error": "nick is empty"}), 400
    if len(nick) > 32:
        return jsonify({"error": "nick too long (max 32)"}), 400
    if not re.fullmatch(r"[A-Za-z0-9_]+", nick):
        return jsonify({"error": "nick_invalid_format"}), 400

    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "UPDATE users SET nick = %s WHERE telegram_id = %s RETURNING *",
            (nick, telegram_id),
        )
        user = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        if not user:
            return jsonify({"error": "user not found"}), 404
        return jsonify(dict(user))
    except Exception as e:
        logger.error("set_nick error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/products")
def get_products():
    category = request.args.get("category", "")
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if category and category != "all":
            cur.execute(
                "SELECT * FROM products WHERE is_active = TRUE AND category = %s ORDER BY sort_order",
                (category,),
            )
        else:
            cur.execute(
                "SELECT * FROM products WHERE is_active = TRUE ORDER BY sort_order"
            )
        products = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify([dict(p) for p in products])
    except Exception as e:
        logger.error("get_products error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/purchase", methods=["POST"])
def purchase():
    data = request.json or {}
    telegram_id = data.get("telegram_id")
    product_id = data.get("product_id")
    promo_code = (data.get("promo_code") or "").strip().upper()

    if not telegram_id or not product_id:
        return jsonify({"error": "missing fields"}), 400

    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Get user
        cur.execute("SELECT * FROM users WHERE telegram_id = %s", (telegram_id,))
        user = cur.fetchone()
        if not user:
            cur.close()
            conn.close()
            return jsonify({"error": "user not found"}), 404

        # Get product
        cur.execute(
            "SELECT * FROM products WHERE id = %s AND is_active = TRUE", (product_id,)
        )
        product = cur.fetchone()
        if not product:
            cur.close()
            conn.close()
            return jsonify({"error": "product not found"}), 404

        final_price = product["price"]
        promo_discount = 0

        # Apply promo code
        if promo_code:
            cur.execute(
                """
                SELECT * FROM promo_codes
                WHERE code = %s AND is_active = TRUE
                  AND (expires_at IS NULL OR expires_at > NOW())
                  AND used_count < max_uses
            """,
                (promo_code,),
            )
            promo = cur.fetchone()

            if promo:
                # Check if user already used this promo
                cur.execute(
                    "SELECT 1 FROM promo_usages WHERE user_id = %s AND promo_id = %s",
                    (telegram_id, promo["id"]),
                )
                already_used = cur.fetchone()
                if not already_used:
                    pct = promo.get("discount_percent") or 0
                    if pct > 0:
                        promo_discount = min(
                            round(final_price * pct / 100), final_price
                        )
                    else:
                        promo_discount = min(promo["coins_amount"], final_price)
                    final_price = max(0, final_price - promo_discount)

        # Check balance
        if user["balance"] < final_price:
            cur.close()
            conn.close()
            return jsonify(
                {
                    "error": "insufficient_balance",
                    "need": final_price,
                    "have": user["balance"],
                }
            ), 400

        # Deduct balance
        cur.execute(
            "UPDATE users SET balance = balance - %s WHERE telegram_id = %s",
            (final_price, telegram_id),
        )

        # Record purchase
        cur.execute(
            "INSERT INTO purchases (user_id, product_id, status, item_name, price) VALUES (%s, %s, 'pending', %s, %s) RETURNING id",
            (telegram_id, product_id, product["name"], final_price),
        )
        purchase_id = cur.fetchone()["id"]

        # Record promo usage
        if promo_code and promo_discount > 0:
            cur.execute(
                "UPDATE promo_codes SET used_count = used_count + 1 WHERE code = %s",
                (promo_code,),
            )
            cur.execute(
                "INSERT INTO promo_usages (user_id, promo_id) SELECT %s, id FROM promo_codes WHERE code = %s",
                (telegram_id, promo_code),
            )

        # Get updated user
        cur.execute("SELECT balance FROM users WHERE telegram_id = %s", (telegram_id,))
        new_balance = cur.fetchone()["balance"]

        conn.commit()
        cur.close()
        conn.close()

        return jsonify(
            {
                "success": True,
                "purchase_id": purchase_id,
                "product": dict(product),
                "paid": final_price,
                "discount": promo_discount,
                "new_balance": new_balance,
            }
        )

    except Exception as e:
        logger.error("purchase error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/promo/check", methods=["POST"])
def check_promo():
    data = request.json or {}
    code = (data.get("code") or "").strip().upper()
    telegram_id = data.get("telegram_id")

    if not code:
        return jsonify({"error": "missing code"}), 400

    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT * FROM promo_codes
            WHERE code = %s AND is_active = TRUE
              AND (expires_at IS NULL OR expires_at > NOW())
              AND used_count < max_uses
        """,
            (code,),
        )
        promo = cur.fetchone()

        if not promo:
            cur.close()
            conn.close()
            return jsonify({"valid": False, "error": "Промокод не найден или истёк"})

        already_used = False
        if telegram_id:
            cur.execute(
                "SELECT 1 FROM promo_usages WHERE user_id = %s AND promo_id = %s",
                (telegram_id, promo["id"]),
            )
            already_used = bool(cur.fetchone())

        cur.close()
        conn.close()

        if already_used:
            return jsonify(
                {"valid": False, "error": "Вы уже использовали этот промокод"}
            )

        pct = promo.get("discount_percent") or 0
        if pct > 0:
            msg = f"Скидка {pct}% применена!"
        else:
            msg = f"Скидка {promo['coins_amount']} монет!"
        return jsonify(
            {
                "valid": True,
                "coins_amount": promo["coins_amount"],
                "discount_percent": pct,
                "message": msg,
            }
        )

    except Exception as e:
        logger.error("check_promo error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/promo/activate", methods=["POST"])
def activate_promo():
    data = request.json or {}
    code = (data.get("code") or "").strip().upper()
    telegram_id = data.get("telegram_id")

    if not code or not telegram_id:
        return jsonify({"error": "missing fields"}), 400

    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute(
            """
            SELECT * FROM promo_codes
            WHERE code = %s AND is_active = TRUE
              AND (expires_at IS NULL OR expires_at > NOW())
              AND used_count < max_uses
        """,
            (code,),
        )
        promo = cur.fetchone()

        if not promo:
            cur.close()
            conn.close()
            return jsonify({"success": False, "error": "Промокод не найден или истёк"})

        cur.execute(
            "SELECT 1 FROM promo_usages WHERE user_id = %s AND promo_id = %s",
            (telegram_id, promo["id"]),
        )
        if cur.fetchone():
            cur.close()
            conn.close()
            return jsonify(
                {"success": False, "error": "Вы уже использовали этот промокод"}
            )

        # Check user exists
        cur.execute("SELECT 1 FROM users WHERE telegram_id = %s", (telegram_id,))
        if not cur.fetchone():
            cur.close()
            conn.close()
            return jsonify({"success": False, "error": "Пользователь не найден"})

        # Add coins
        cur.execute(
            "UPDATE users SET balance = balance + %s WHERE telegram_id = %s",
            (promo["coins_amount"], telegram_id),
        )
        cur.execute(
            "UPDATE promo_codes SET used_count = used_count + 1 WHERE id = %s",
            (promo["id"],),
        )
        cur.execute(
            "INSERT INTO promo_usages (user_id, promo_id) VALUES (%s, %s)",
            (telegram_id, promo["id"]),
        )

        cur.execute("SELECT balance FROM users WHERE telegram_id = %s", (telegram_id,))
        new_balance = cur.fetchone()["balance"]

        conn.commit()
        cur.close()
        conn.close()

        return jsonify(
            {
                "success": True,
                "coins_added": promo["coins_amount"],
                "new_balance": new_balance,
                "message": f"+{promo['coins_amount']} монет зачислено!",
            }
        )

    except Exception as e:
        logger.error("activate_promo error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/purchases/<int:user_id>")
def get_purchases(user_id):
    status_filter = request.args.get("status", "all")
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if status_filter == "all":
            cur.execute(
                """
                SELECT p.id, p.purchased_at, p.status,
                       COALESCE(p.item_name, pr.name) AS name,
                       pr.category, pr.icon, pr.color,
                       COALESCE(p.price, pr.price) AS price
                FROM purchases p
                JOIN products pr ON p.product_id = pr.id
                WHERE p.user_id = %s
                ORDER BY p.purchased_at DESC
                """,
                (user_id,),
            )
        else:
            cur.execute(
                """
                SELECT p.id, p.purchased_at, p.status,
                       COALESCE(p.item_name, pr.name) AS name,
                       pr.category, pr.icon, pr.color,
                       COALESCE(p.price, pr.price) AS price
                FROM purchases p
                JOIN products pr ON p.product_id = pr.id
                WHERE p.user_id = %s AND p.status = %s
                ORDER BY p.purchased_at DESC
                """,
                (user_id, status_filter),
            )
        purchases = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify([dict(p) for p in purchases])
    except Exception as e:
        logger.error("get_purchases error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/orders")
def admin_orders():
    admin_id = request.args.get("admin_id")
    err = require_admin_or_mod(admin_id)
    if err:
        return err
    status_filter = request.args.get("status", "pending")
    moderator_id = request.args.get("moderator_id")
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if moderator_id and status_filter == "completed":
            cur.execute(
                """
                SELECT p.id, p.purchased_at, p.status, p.completed_by,
                       COALESCE(p.item_name, pr.name) AS item_name,
                       COALESCE(p.price, pr.price) AS price,
                       p.user_id,
                       u.first_name, u.username, u.nick, u.status AS user_status,
                       m.name AS completed_by_name
                FROM purchases p
                JOIN products pr ON p.product_id = pr.id
                JOIN users u ON p.user_id = u.telegram_id
                LEFT JOIN moderators m ON p.completed_by = m.telegram_id
                WHERE p.status = %s AND p.completed_by = %s
                ORDER BY p.purchased_at DESC
                """,
                (status_filter, int(moderator_id)),
            )
        else:
            cur.execute(
                """
                SELECT p.id, p.purchased_at, p.status, p.completed_by,
                       COALESCE(p.item_name, pr.name) AS item_name,
                       COALESCE(p.price, pr.price) AS price,
                       p.user_id,
                       u.first_name, u.username, u.nick, u.status AS user_status,
                       m.name AS completed_by_name
                FROM purchases p
                JOIN products pr ON p.product_id = pr.id
                JOIN users u ON p.user_id = u.telegram_id
                LEFT JOIN moderators m ON p.completed_by = m.telegram_id
                WHERE p.status = %s
                ORDER BY p.purchased_at DESC
                """,
                (status_filter,),
            )
        orders = cur.fetchall()
        result = []
        for o in orders:
            row = dict(o)
            if row.get("purchased_at"):
                row["purchased_at"] = row["purchased_at"].isoformat()
            result.append(row)
        cur.close()
        conn.close()
        return jsonify(result)
    except Exception as e:
        logger.error("admin_orders error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/order/<int:order_id>/confirm", methods=["POST"])
def admin_confirm_order(order_id):
    data = request.json or {}
    admin_id = data.get("admin_id")
    err = require_admin_or_mod(str(admin_id))
    if err:
        return err
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT p.*, COALESCE(p.item_name, pr.name) AS item_name FROM purchases p JOIN products pr ON p.product_id = pr.id WHERE p.id = %s",
            (order_id,),
        )
        order = cur.fetchone()
        if not order:
            cur.close()
            conn.close()
            return jsonify({"error": "order not found"}), 404
        cur.execute(
            "UPDATE purchases SET status = 'completed', completed_by = %s WHERE id = %s",
            (int(data.get("admin_id", 0)), order_id),
        )
        conn.commit()
        cur.close()
        conn.close()
        send_telegram_message(
            order["user_id"],
            f"✅ Ваш заказ #{order_id} ({order['item_name']}) выдан! Спасибо за покупку.",
        )
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("admin_confirm_order error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/order/<int:order_id>/reject", methods=["POST"])
def admin_reject_order(order_id):
    data = request.json or {}
    admin_id = data.get("admin_id")
    err = require_admin_or_mod(str(admin_id))
    if err:
        return err
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT p.*, COALESCE(p.item_name, pr.name) AS item_name, COALESCE(p.price, pr.price) AS price FROM purchases p JOIN products pr ON p.product_id = pr.id WHERE p.id = %s",
            (order_id,),
        )
        order = cur.fetchone()
        if not order:
            cur.close()
            conn.close()
            return jsonify({"error": "order not found"}), 404
        cur.execute(
            "UPDATE purchases SET status = 'rejected' WHERE id = %s",
            (order_id,),
        )
        refund_price = order["price"] or 0
        if refund_price > 0:
            cur.execute(
                "UPDATE users SET balance = balance + %s WHERE telegram_id = %s",
                (refund_price, order["user_id"]),
            )
        conn.commit()
        cur.close()
        conn.close()
        msg = f"<tg-emoji emoji-id=\"5465665476971471368\">❌</tg-emoji> Ваш заказ #{order_id} ({order['item_name']}) отклонён."
        if refund_price > 0:
            msg += f" Монеты ({refund_price}) возвращены на ваш баланс."
        send_telegram_message(order["user_id"], msg)
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("admin_reject_order error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/leaderboard")
def get_leaderboard():
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT telegram_id, username, first_name, nick, balance, status
            FROM users
            ORDER BY balance DESC
            LIMIT 10
        """)
        users = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify([dict(u) for u in users])
    except Exception as e:
        logger.error("get_leaderboard error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/photo")
def user_photo():
    user_id = request.args.get("user_id", "").strip()
    if not user_id:
        return jsonify({"error": "missing user_id"}), 400

    try:
        photos_resp = http_requests.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/getUserProfilePhotos",
            params={"user_id": user_id, "limit": 1},
            timeout=5,
        )
        photos_data = photos_resp.json()

        if not photos_data.get("ok"):
            return jsonify({"error": "telegram error"}), 502

        photos = photos_data.get("result", {}).get("photos", [])
        if not photos:
            return Response(status=204)

        file_id = photos[0][-1]["file_id"]
        file_resp = http_requests.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/getFile",
            params={"file_id": file_id},
            timeout=5,
        )
        file_data = file_resp.json()

        if not file_data.get("ok"):
            return jsonify({"photo_url": None}), 200

        file_path = file_data["result"]["file_path"]
        photo_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"

        img_resp = http_requests.get(photo_url, timeout=10)
        content_type = img_resp.headers.get("Content-Type", "image/jpeg")
        return Response(img_resp.content, content_type=content_type)

    except Exception as e:
        logger.error("Ошибка получения фото: %s", e)
        return jsonify({"error": "internal error"}), 500


# ==================== REFERRAL API ====================


@app.route("/api/referrals/<int:user_id>")
def get_referrals(user_id):
    """Return list of users invited by this user (both pending and confirmed)."""
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # First mark expired unconfirmed referrals
        cur.execute(
            """
            UPDATE referrals
            SET expired = TRUE
            WHERE inviter_id = %s AND confirmed = FALSE AND expired = FALSE AND expires_at < NOW()
        """,
            (user_id,),
        )

        cur.execute(
            """
            SELECT r.id, r.invitee_id, r.joined_at, r.confirmed, r.confirmed_at,
                   r.expired, r.expires_at,
                   u.username, u.first_name, u.nick
            FROM referrals r
            JOIN users u ON r.invitee_id = u.telegram_id
            WHERE r.inviter_id = %s
            ORDER BY r.joined_at DESC
        """,
            (user_id,),
        )
        referrals = cur.fetchall()

        # Get stats
        cur.execute(
            """
            SELECT referral_count FROM users WHERE telegram_id = %s
        """,
            (user_id,),
        )
        user_row = cur.fetchone()

        # Count expired referrals for this user
        cur.execute(
            """
            SELECT COUNT(*) as cnt FROM referrals
            WHERE inviter_id = %s AND expired = TRUE
        """,
            (user_id,),
        )
        expired_count_row = cur.fetchone()

        conn.commit()
        cur.close()
        conn.close()

        result = []
        for r in referrals:
            result.append(
                {
                    "id": r["id"],
                    "invitee_id": r["invitee_id"],
                    "username": r["username"] or "",
                    "first_name": r["first_name"] or "",
                    "nick": r["nick"] or "",
                    "joined_at": r["joined_at"].isoformat() if r["joined_at"] else None,
                    "confirmed": r["confirmed"],
                    "expired": r["expired"],
                    "confirmed_at": r["confirmed_at"].isoformat()
                    if r["confirmed_at"]
                    else None,
                    "expires_at": r["expires_at"].isoformat()
                    if r["expires_at"]
                    else None,
                }
            )

        referral_count = user_row["referral_count"] if user_row else 0
        expired_count = expired_count_row["cnt"] if expired_count_row else 0

        return jsonify(
            {
                "referrals": result,
                "referral_count": referral_count,
                "expired_count": expired_count,
            }
        )
    except Exception as e:
        logger.error("get_referrals error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/bot-info")
def get_bot_info():
    return jsonify({"username": BOT_USERNAME, "admin_username": ADMIN_USERNAME})


@app.route("/api/online", methods=["POST", "GET"])
def online_ping():
    global _peak_online
    now = time.time()
    if request.method == "POST":
        data = request.json or {}
        uid = data.get("user_id")
        if uid:
            _online_sessions[str(uid)] = now
    # Clean up stale sessions and return count
    cutoff = now - _ONLINE_TTL
    stale = [k for k, v in _online_sessions.items() if v < cutoff]
    for k in stale:
        del _online_sessions[k]
    current = len(_online_sessions)
    if current > _peak_online:
        _peak_online = current
    return jsonify({"online": current, "peak": _peak_online})


# ==================== PREMIUM PAYMENT ENDPOINTS ====================


@app.route("/api/premium/invoice/stars", methods=["POST"])
def create_stars_invoice():
    """Create a Telegram Stars invoice for Premium."""
    data = request.json or {}
    user_id = data.get("user_id")
    if not user_id:
        return jsonify({"ok": False, "error": "missing user_id"}), 400
    try:
        payload = f"premium_stars_{user_id}"
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/createInvoiceLink"
        resp = http_requests.post(
            url,
            json={
                "title": "NodeLink Premium",
                "description": f"Премиум на {PREMIUM_DAYS} дней — все привилегии и бонусы!",
                "payload": payload,
                "currency": "XTR",
                "prices": [
                    {
                        "label": f"Premium {PREMIUM_DAYS} дней",
                        "amount": PREMIUM_PRICE_STARS,
                    }
                ],
            },
            timeout=10,
        )
        result = resp.json()
        if result.get("ok"):
            invoice_link = result["result"]
            return jsonify({"ok": True, "invoice_link": invoice_link})
        else:
            logger.error("Stars invoice error: %s", result)
            return jsonify(
                {"ok": False, "error": result.get("description", "Ошибка Telegram")}
            ), 500
    except Exception as e:
        logger.error("create_stars_invoice error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/premium/invoice/crypto", methods=["POST"])
def create_crypto_invoice():
    """Create a CryptoPay invoice for Premium."""
    data = request.json or {}
    user_id = data.get("user_id")
    if not user_id:
        return jsonify({"ok": False, "error": "missing user_id"}), 400
    if not CRYPTO_PAY_TOKEN:
        return jsonify({"ok": False, "error": "CryptoPay не настроен"}), 500
    try:
        payload = f"premium_crypto_{user_id}"
        url = f"{CRYPTO_PAY_API}/createInvoice"
        resp = http_requests.post(
            url,
            json={
                "asset": "USDT",
                "amount": PREMIUM_PRICE_USDT,
                "description": f"NodeLink Premium — {PREMIUM_DAYS} дней",
                "payload": payload,
                "paid_btn_name": "callback",
                "paid_btn_url": MINI_APP_URL,
                "allow_comments": False,
                "allow_anonymous": False,
            },
            headers={
                "Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN,
            },
            timeout=10,
        )
        result = resp.json()
        if result.get("ok"):
            invoice = result["result"]
            pay_url = invoice.get("pay_url") or invoice.get("bot_invoice_url")
            return jsonify(
                {
                    "ok": True,
                    "pay_url": pay_url,
                    "invoice_id": invoice.get("invoice_id"),
                }
            )
        else:
            logger.error("CryptoPay invoice error: %s", result)
            err = result.get("error", {})
            return jsonify(
                {"ok": False, "error": err.get("message", "Ошибка CryptoPay")}
            ), 500
    except Exception as e:
        logger.error("create_crypto_invoice error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/premium/activate", methods=["POST"])
def activate_premium():
    """Activate premium for a user (called after payment confirmation)."""
    data = request.json or {}
    user_id = data.get("user_id")
    if not user_id:
        return jsonify({"ok": False, "error": "missing user_id"}), 400
    try:
        conn = get_db()
        cur = conn.cursor()
        interval = f"{PREMIUM_DAYS} days"
        cur.execute(
            "UPDATE users SET status = 'Premium', premium_until = NOW() + INTERVAL %s WHERE telegram_id = %s",
            (interval, user_id),
        )
        conn.commit()
        cur.close()
        conn.close()
        send_telegram_message(
            user_id,
            f"🌟 <b>Premium активирован!</b>\n\n"
            f"Ваш статус обновлён до <b>Premium</b> на {PREMIUM_DAYS} дней.\n"
            f"Наслаждайтесь всеми привилегиями! 🎉",
        )
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("activate_premium error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/premium/webhook/crypto", methods=["POST"])
def crypto_pay_webhook():
    """Handle CryptoPay payment webhook to auto-activate Premium."""
    token = request.headers.get("Crypto-Pay-API-Token", "")
    if token and token != CRYPTO_PAY_TOKEN:
        return jsonify({"error": "unauthorized"}), 403
    try:
        data = request.json or {}
        update_type = data.get("update_type")
        if update_type == "invoice_paid":
            payload = data.get("payload", {}).get("payload", "")
            if payload.startswith("premium_crypto_"):
                user_id_str = payload.replace("premium_crypto_", "")
                try:
                    user_id = int(user_id_str)
                    interval = f"{PREMIUM_DAYS} days"
                    conn = get_db()
                    cur = conn.cursor()
                    cur.execute(
                        "UPDATE users SET status = 'Premium', premium_until = NOW() + INTERVAL %s WHERE telegram_id = %s",
                        (interval, user_id),
                    )
                    conn.commit()
                    cur.close()
                    conn.close()
                    send_telegram_message(
                        user_id,
                        f"🌟 <b>Premium активирован!</b>\n\n"
                        f"Ваш статус обновлён до <b>Premium</b> на {PREMIUM_DAYS} дней.\n"
                        f"Наслаждайтесь всеми привилегиями! 🎉",
                    )
                    logger.info("Premium activated via CryptoPay for user %s", user_id)
                except Exception as e:
                    logger.error("crypto webhook activate error: %s", e)
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("crypto_pay_webhook error: %s", e)
        return jsonify({"error": str(e)}), 500


# ==================== END PREMIUM PAYMENT ENDPOINTS ====================


@app.route("/api/event/current")
def get_current_event():
    """Return info about the current weekly event."""
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("""
            SELECT id, started_at FROM weekly_events
            WHERE finalized = FALSE
            ORDER BY started_at ASC
            LIMIT 1
        """)
        event = cur.fetchone()

        # Top-3 leaderboard for current event
        cur.execute("""
            SELECT telegram_id, username, first_name, nick, event_referral_count
            FROM users
            WHERE event_referral_count > 0
            ORDER BY event_referral_count DESC
            LIMIT 10
        """)
        top = cur.fetchall()

        cur.close()
        conn.close()

        next_end = get_next_sunday_17_msk()

        return jsonify(
            {
                "event_id": event["id"] if event else None,
                "started_at": event["started_at"].isoformat() if event else None,
                "ends_at": next_end.isoformat(),
                "leaderboard": [dict(u) for u in top],
            }
        )
    except Exception as e:
        logger.error("get_current_event error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/event/user/<int:user_id>")
def get_event_user(user_id):
    """Return current event referral count and rank for a user."""
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute(
            """
            SELECT event_referral_count FROM users WHERE telegram_id = %s
        """,
            (user_id,),
        )
        row = cur.fetchone()

        if not row:
            cur.close()
            conn.close()
            return jsonify({"error": "not found"}), 404

        # Determine rank
        cur.execute(
            """
            SELECT COUNT(*) as cnt FROM users
            WHERE event_referral_count > %s
        """,
            (row["event_referral_count"],),
        )
        rank_row = cur.fetchone()
        rank = rank_row["cnt"] + 1 if rank_row else None

        cur.close()
        conn.close()

        return jsonify(
            {
                "event_referral_count": row["event_referral_count"],
                "rank": rank,
            }
        )
    except Exception as e:
        logger.error("get_event_user error: %s", e)
        return jsonify({"error": str(e)}), 500


# ==================== TASKS API ====================


@app.route("/api/tasks")
def get_tasks():
    user_id = request.args.get("user_id")
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT * FROM tasks WHERE is_active = TRUE ORDER BY created_at DESC"
        )
        tasks = [dict(t) for t in cur.fetchall()]
        for t in tasks:
            if t.get("created_at"):
                t["created_at"] = t["created_at"].isoformat()
        completed_ids = set()
        if user_id:
            try:
                uid = int(user_id)
                cur.execute(
                    "SELECT task_id FROM task_completions WHERE user_id = %s",
                    (uid,),
                )
                completed_ids = {row["task_id"] for row in cur.fetchall()}
            except (TypeError, ValueError):
                pass
        for t in tasks:
            t["completed"] = t["id"] in completed_ids
        cur.close()
        conn.close()
        return jsonify(tasks)
    except Exception as e:
        logger.error("get_tasks error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/tasks/check-subscription", methods=["POST"])
def check_subscription():
    data = request.json or {}
    user_id = data.get("user_id")
    task_id = data.get("task_id")
    if not user_id or not task_id:
        return jsonify({"error": "missing fields"}), 400
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT * FROM tasks WHERE id = %s AND is_active = TRUE", (task_id,)
        )
        task = cur.fetchone()
        if not task or task["task_type"] != "subscription":
            cur.close()
            conn.close()
            return jsonify({"error": "task not found"}), 404
        cur.execute(
            "SELECT 1 FROM task_completions WHERE user_id = %s AND task_id = %s",
            (user_id, task_id),
        )
        if cur.fetchone():
            cur.close()
            conn.close()
            return jsonify({"ok": True, "already_completed": True})
        channel = task["channel_link"]
        try:
            resp = http_requests.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getChatMember",
                params={"chat_id": channel, "user_id": user_id},
                timeout=10,
            )
            result = resp.json()
            status = result.get("result", {}).get("status", "")
            if status not in ("member", "administrator", "creator"):
                cur.close()
                conn.close()
                return jsonify({"ok": False, "error": "not_subscribed"})
        except Exception as e:
            cur.close()
            conn.close()
            return jsonify({"error": "check_failed"}), 500
        cur.execute(
            "INSERT INTO task_completions (user_id, task_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (user_id, task_id),
        )
        cur.execute(
            "UPDATE users SET balance = balance + %s WHERE telegram_id = %s",
            (task["reward"], user_id),
        )
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True, "reward": task["reward"]})
    except Exception as e:
        logger.error("check_subscription error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/tasks/complete", methods=["POST"])
def complete_task():
    data = request.json or {}
    user_id = data.get("user_id")
    task_id = data.get("task_id")
    if not user_id or not task_id:
        return jsonify({"error": "missing fields"}), 400
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT * FROM tasks WHERE id = %s AND is_active = TRUE", (task_id,)
        )
        task = cur.fetchone()
        if not task:
            cur.close()
            conn.close()
            return jsonify({"error": "task not found"}), 404
        if task["task_type"] == "subscription":
            cur.close()
            conn.close()
            return jsonify({"error": "use check-subscription endpoint"}), 400
        cur.execute(
            "SELECT 1 FROM task_completions WHERE user_id = %s AND task_id = %s",
            (user_id, task_id),
        )
        if cur.fetchone():
            cur.close()
            conn.close()
            return jsonify({"ok": True, "already_completed": True})
        cur.execute(
            "INSERT INTO task_completions (user_id, task_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (user_id, task_id),
        )
        cur.execute(
            "UPDATE users SET balance = balance + %s WHERE telegram_id = %s",
            (task["reward"], user_id),
        )
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True, "reward": task["reward"]})
    except Exception as e:
        logger.error("complete_task error: %s", e)
        return jsonify({"error": str(e)}), 500


# ==================== ADMIN API ====================


def require_admin(admin_id):
    """Return error response if admin_id is not in ADMIN_IDS, else None."""
    try:
        aid = int(admin_id)
    except (TypeError, ValueError):
        return jsonify({"error": "forbidden"}), 403
    if aid not in ADMIN_IDS:
        return jsonify({"error": "forbidden"}), 403
    return None


def require_admin_or_mod(admin_id):
    """Return error response if admin_id is not admin or moderator, else None."""
    try:
        aid = int(admin_id)
    except (TypeError, ValueError):
        return jsonify({"error": "forbidden"}), 403
    if aid in ADMIN_IDS:
        return None
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM moderators WHERE telegram_id = %s", (aid,))
        found = cur.fetchone()
        cur.close()
        conn.close()
        if found:
            return None
    except Exception:
        pass
    return jsonify({"error": "forbidden"}), 403


@app.route("/api/admin/verify", methods=["POST"])
def admin_verify():
    data = request.json or {}
    admin_id = data.get("admin_id")
    password = data.get("password", "")
    err = require_admin(admin_id)
    if err:
        return err
    if password != ADMIN_PASSWORD:
        return jsonify({"ok": False, "error": "Неверный пароль"}), 401
    return jsonify({"ok": True})


@app.route("/api/admin/stats")
def admin_stats():
    admin_id = request.args.get("admin_id")
    err = require_admin_or_mod(admin_id)
    if err:
        return err
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("SELECT COUNT(*) AS cnt FROM users")
        total_users = cur.fetchone()["cnt"]

        cur.execute("SELECT COALESCE(SUM(referral_count),0) AS cnt FROM users")
        total_referrals = cur.fetchone()["cnt"]

        cur.execute("SELECT COALESCE(SUM(balance),0) AS cnt FROM users")
        total_coins = cur.fetchone()["cnt"]

        cur.execute("SELECT COALESCE(SUM(event_referral_count),0) AS cnt FROM users")
        total_event_refs = cur.fetchone()["cnt"]

        cur.execute("""
            SELECT id, started_at FROM weekly_events
            WHERE finalized = FALSE ORDER BY started_at ASC LIMIT 1
        """)
        event = cur.fetchone()

        cur.execute("""
            SELECT COUNT(*) AS cnt FROM referrals WHERE confirmed = TRUE
        """)
        confirmed_refs = cur.fetchone()["cnt"]

        cur.execute("""
            SELECT COUNT(*) AS cnt FROM purchases
        """)
        total_purchases = cur.fetchone()["cnt"]

        cur.execute("""
            SELECT COUNT(*) AS cnt FROM users WHERE premium_until IS NOT NULL
        """)
        total_premium_ever = cur.fetchone()["cnt"]

        cur.execute("""
            SELECT COUNT(*) AS cnt FROM users
            WHERE status = 'Premium' AND premium_until > NOW()
        """)
        active_premium = cur.fetchone()["cnt"]

        cur.execute("""
            SELECT COUNT(*) AS cnt FROM users
            WHERE nick IS NOT NULL AND nick != ''
        """)
        users_with_nick = cur.fetchone()["cnt"]

        cur.execute("""
            SELECT COUNT(*) AS cnt FROM (
                SELECT user_id
                FROM user_activity
                WHERE activity_date >= CURRENT_DATE - INTERVAL '6 days'
                GROUP BY user_id
                HAVING COUNT(DISTINCT activity_date) = 7
            ) sub
        """)
        active_users = cur.fetchone()["cnt"]

        cur.execute("""
            SELECT telegram_id, username, first_name, nick, balance, referral_count, event_referral_count, status, created_at
            FROM users ORDER BY created_at DESC LIMIT 5
        """)
        recent_users = [dict(u) for u in cur.fetchall()]
        for u in recent_users:
            if u.get("created_at"):
                u["created_at"] = u["created_at"].isoformat()

        cur.close()
        conn.close()

        # Current online count (live in-memory, no DB needed)
        now_ts = time.time()
        cutoff_ts = now_ts - _ONLINE_TTL
        online_count = sum(1 for v in _online_sessions.values() if v >= cutoff_ts)

        return jsonify(
            {
                "total_users": total_users,
                "total_referrals": total_referrals,
                "total_coins": total_coins,
                "total_event_refs": total_event_refs,
                "confirmed_referrals": confirmed_refs,
                "total_purchases": total_purchases,
                "total_premium_ever": total_premium_ever,
                "active_premium": active_premium,
                "users_with_nick": users_with_nick,
                "active_users": active_users,
                "online_players": online_count,
                "peak_online": _peak_online,
                "event": dict(event) if event else None,
                "recent_users": recent_users,
            }
        )
    except Exception as e:
        logger.error("admin_stats error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/users")
def admin_users():
    admin_id = request.args.get("admin_id")
    err = require_admin_or_mod(admin_id)
    if err:
        return err
    search = request.args.get("search", "").strip()
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if search:
            # Try numeric ID match first, then username/nick search
            try:
                tid = int(search)
                cur.execute(
                    """
                    SELECT telegram_id, username, first_name, nick, balance,
                           referral_count, event_referral_count, status, created_at,
                           is_blocked, block_reason, premium_until
                    FROM users WHERE telegram_id = %s LIMIT 20
                """,
                    (tid,),
                )
            except ValueError:
                cur.execute(
                    """
                    SELECT telegram_id, username, first_name, nick, balance,
                           referral_count, event_referral_count, status, created_at,
                           is_blocked, block_reason, premium_until
                    FROM users
                    WHERE username ILIKE %s OR first_name ILIKE %s OR nick ILIKE %s
                    ORDER BY balance DESC LIMIT 20
                """,
                    (f"%{search}%", f"%{search}%", f"%{search}%"),
                )
        else:
            cur.execute("""
                SELECT telegram_id, username, first_name, nick, balance,
                       referral_count, event_referral_count, status, created_at,
                       is_blocked, block_reason, premium_until
                FROM users ORDER BY balance DESC LIMIT 100
            """)
        users = [dict(u) for u in cur.fetchall()]
        for u in users:
            if u.get("created_at"):
                u["created_at"] = u["created_at"].isoformat()
            if u.get("premium_until"):
                u["premium_until"] = u["premium_until"].isoformat()
        cur.close()
        conn.close()
        return jsonify(users)
    except Exception as e:
        logger.error("admin_users error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/edit-balance", methods=["POST"])
def admin_edit_balance():
    data = request.json or {}
    admin_id = data.get("admin_id")
    err = require_admin_or_mod(admin_id)
    if err:
        return err
    target_id = data.get("user_id")
    amount = data.get("amount", 0)
    operation = data.get("operation", "add")  # "add" or "subtract"
    if not target_id:
        return jsonify({"error": "missing user_id"}), 400
    try:
        amount = int(amount)
        if amount <= 0:
            return jsonify({"error": "amount must be positive"}), 400
    except (TypeError, ValueError):
        return jsonify({"error": "invalid amount"}), 400
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT balance FROM users WHERE telegram_id = %s", (target_id,))
        user = cur.fetchone()
        if not user:
            cur.close()
            conn.close()
            return jsonify({"error": "user not found"}), 404
        if operation == "subtract":
            new_balance = max(0, user["balance"] - amount)
            cur.execute(
                "UPDATE users SET balance = %s WHERE telegram_id = %s RETURNING balance",
                (new_balance, target_id),
            )
        else:
            cur.execute(
                "UPDATE users SET balance = balance + %s WHERE telegram_id = %s RETURNING balance",
                (amount, target_id),
            )
        result = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True, "new_balance": result["balance"]})
    except Exception as e:
        logger.error("admin_edit_balance error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/edit-status", methods=["POST"])
def admin_edit_status():
    data = request.json or {}
    admin_id = data.get("admin_id")
    err = require_admin_or_mod(admin_id)
    if err:
        return err
    target_id = data.get("user_id")
    status = (data.get("status") or "").strip()
    if not target_id or not status:
        return jsonify({"error": "missing fields"}), 400
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "UPDATE users SET status = %s WHERE telegram_id = %s RETURNING status",
            (status, target_id),
        )
        result = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        if not result:
            return jsonify({"error": "user not found"}), 404
        return jsonify({"ok": True, "new_status": result["status"]})
    except Exception as e:
        logger.error("admin_edit_status error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/edit-user", methods=["POST"])
def admin_edit_user():
    data = request.json or {}
    admin_id = data.get("admin_id")
    err = require_admin_or_mod(admin_id)
    if err:
        return err
    target_id = data.get("user_id")
    if not target_id:
        return jsonify({"error": "missing user_id"}), 400
    balance = data.get("balance")
    referral_count = data.get("referral_count")
    event_referral_count = data.get("event_referral_count")
    status = data.get("status")
    premium_days = data.get("premium_days")
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM users WHERE telegram_id = %s", (target_id,))
        user = cur.fetchone()
        if not user:
            cur.close()
            conn.close()
            return jsonify({"error": "user not found"}), 404
        fields = []
        values = []
        if balance is not None:
            try:
                b = int(balance)
                if b < 0:
                    b = 0
                fields.append("balance = %s")
                values.append(b)
            except (TypeError, ValueError):
                pass
        if referral_count is not None:
            try:
                rc = int(referral_count)
                if rc < 0:
                    rc = 0
                fields.append("referral_count = %s")
                values.append(rc)
            except (TypeError, ValueError):
                pass
        if event_referral_count is not None:
            try:
                erc = int(event_referral_count)
                if erc < 0:
                    erc = 0
                fields.append("event_referral_count = %s")
                values.append(erc)
            except (TypeError, ValueError):
                pass
        if status is not None:
            st = str(status).strip()
            fields.append("status = %s")
            values.append(st)
            # Handle Premium with days
            if st == "Premium" and premium_days is not None:
                try:
                    days = int(premium_days)
                    if days > 0:
                        premium_until = datetime.now(timezone.utc) + timedelta(
                            days=days
                        )
                        fields.append("premium_until = %s")
                        values.append(premium_until)
                except (TypeError, ValueError):
                    pass
            elif st != "Premium":
                # Clear premium_until if not premium
                fields.append("premium_until = NULL")
        if not fields:
            cur.close()
            conn.close()
            return jsonify({"error": "no fields to update"}), 400
        values.append(target_id)
        cur.execute(
            f"UPDATE users SET {', '.join(fields)} WHERE telegram_id = %s RETURNING *",
            values,
        )
        updated = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        result = dict(updated)
        if result.get("created_at"):
            result["created_at"] = result["created_at"].isoformat()
        if result.get("premium_until"):
            result["premium_until"] = result["premium_until"].isoformat()
        return jsonify({"ok": True, "user": result})
    except Exception as e:
        logger.error("admin_edit_user error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/delete-user", methods=["POST"])
def admin_delete_user():
    data = request.json or {}
    admin_id = data.get("admin_id")
    err = require_admin_or_mod(admin_id)
    if err:
        return err
    target_id = data.get("user_id")
    if not target_id:
        return jsonify({"error": "missing user_id"}), 400
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM users WHERE telegram_id = %s", (target_id,))
        if not cur.fetchone():
            cur.close()
            conn.close()
            return jsonify({"error": "user not found"}), 404
        cur.execute("DELETE FROM task_completions WHERE user_id = %s", (target_id,))
        cur.execute("DELETE FROM promo_usages WHERE user_id = %s", (target_id,))
        cur.execute("DELETE FROM purchases WHERE user_id = %s", (target_id,))
        cur.execute(
            "DELETE FROM referrals WHERE inviter_id = %s OR invitee_id = %s",
            (target_id, target_id),
        )
        cur.execute("DELETE FROM users WHERE telegram_id = %s", (target_id,))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("admin_delete_user error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/block-user", methods=["POST"])
def admin_block_user():
    data = request.json or {}
    admin_id = data.get("admin_id")
    err = require_admin_or_mod(admin_id)
    if err:
        return err
    target_id = data.get("user_id")
    block = data.get("block", True)
    reason = (data.get("reason") or "").strip()
    if not target_id:
        return jsonify({"error": "missing user_id"}), 400
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if block:
            cur.execute(
                "UPDATE users SET is_blocked = TRUE, block_reason = %s WHERE telegram_id = %s RETURNING is_blocked, block_reason",
                (reason, target_id),
            )
        else:
            cur.execute(
                "UPDATE users SET is_blocked = FALSE, block_reason = NULL WHERE telegram_id = %s RETURNING is_blocked, block_reason",
                (target_id,),
            )
        result = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        if not result:
            return jsonify({"error": "user not found"}), 404
        return jsonify(
            {
                "ok": True,
                "is_blocked": result["is_blocked"],
                "block_reason": result["block_reason"],
            }
        )
    except Exception as e:
        logger.error("admin_block_user error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/user/status/<int:user_id>")
def get_user_status(user_id):
    """Quick endpoint for Mini App to check if user is blocked."""
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT is_blocked, block_reason FROM users WHERE telegram_id = %s",
            (user_id,),
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return jsonify({"is_blocked": False, "block_reason": None})
        return jsonify(
            {"is_blocked": bool(row["is_blocked"]), "block_reason": row["block_reason"]}
        )
    except Exception as e:
        logger.error("get_user_status error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/is-moderator/<int:user_id>")
def is_moderator(user_id):
    """Check if the given user is a moderator."""
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM moderators WHERE telegram_id = %s", (user_id,))
        found = bool(cur.fetchone())
        cur.close()
        conn.close()
        return jsonify({"is_moderator": found})
    except Exception as e:
        logger.error("is_moderator error: %s", e)
        return jsonify({"is_moderator": False})


@app.route("/api/admin/moderators")
def admin_get_moderators():
    admin_id = request.args.get("admin_id")
    err = require_admin(admin_id)
    if err:
        return err
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT telegram_id, name, added_at FROM moderators ORDER BY added_at DESC"
        )
        mods = []
        for row in cur.fetchall():
            r = dict(row)
            if r.get("added_at"):
                r["added_at"] = r["added_at"].isoformat()
            mods.append(r)
        cur.close()
        conn.close()
        return jsonify(mods)
    except Exception as e:
        logger.error("admin_get_moderators error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/add-moderator", methods=["POST"])
def admin_add_moderator():
    data = request.json or {}
    admin_id = data.get("admin_id")
    err = require_admin(str(admin_id) if admin_id else None)
    if err:
        return err
    mod_id = data.get("telegram_id")
    mod_name = (data.get("name") or "").strip()
    if not mod_id or not mod_name:
        return jsonify({"error": "Укажите Telegram ID и имя"}), 400
    try:
        mod_id = int(mod_id)
    except (TypeError, ValueError):
        return jsonify({"error": "Неверный Telegram ID"}), 400
    if mod_id in ADMIN_IDS:
        return jsonify({"error": "Этот пользователь уже является администратором"}), 400
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "INSERT INTO moderators (telegram_id, name) VALUES (%s, %s) ON CONFLICT (telegram_id) DO UPDATE SET name = EXCLUDED.name RETURNING *",
            (mod_id, mod_name),
        )
        row = dict(cur.fetchone())
        if row.get("added_at"):
            row["added_at"] = row["added_at"].isoformat()
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True, "moderator": row})
    except Exception as e:
        logger.error("admin_add_moderator error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/remove-moderator", methods=["POST"])
def admin_remove_moderator():
    data = request.json or {}
    admin_id = data.get("admin_id")
    err = require_admin(str(admin_id) if admin_id else None)
    if err:
        return err
    mod_id = data.get("telegram_id")
    if not mod_id:
        return jsonify({"error": "missing telegram_id"}), 400
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("DELETE FROM moderators WHERE telegram_id = %s", (int(mod_id),))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("admin_remove_moderator error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/broadcast", methods=["POST"])
def admin_broadcast():
    admin_id = request.form.get("admin_id") or (request.json or {}).get("admin_id")
    err = require_admin(str(admin_id) if admin_id else None)
    if err:
        return err
    text = (request.form.get("text") or "").strip()
    photo_file = request.files.get("photo")
    if not text and not photo_file:
        return jsonify({"error": "Укажите текст или прикрепите фото"}), 400
    photo_bytes = photo_file.read() if photo_file else None
    photo_name = (photo_file.filename or "photo.jpg") if photo_file else None
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT telegram_id FROM users")
        user_ids = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    def do_broadcast():
        file_id = None
        for uid in user_ids:
            try:
                if photo_bytes:
                    if file_id:
                        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
                        http_requests.post(
                            url,
                            json={
                                "chat_id": uid,
                                "photo": file_id,
                                "caption": text,
                                "parse_mode": "HTML",
                            },
                            timeout=10,
                        )
                    else:
                        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
                        resp = http_requests.post(
                            url,
                            data={
                                "chat_id": uid,
                                "caption": text,
                                "parse_mode": "HTML",
                            },
                            files={"photo": (photo_name, photo_bytes, "image/jpeg")},
                            timeout=15,
                        )
                        if resp.ok:
                            result = resp.json()
                            if result.get("ok"):
                                photos = result["result"].get("photo", [])
                                if photos:
                                    file_id = photos[-1]["file_id"]
                else:
                    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
                    http_requests.post(
                        url,
                        json={
                            "chat_id": uid,
                            "text": text,
                            "parse_mode": "HTML",
                        },
                        timeout=10,
                    )
                time.sleep(0.05)
            except Exception as ex:
                logger.error("broadcast error uid=%s: %s", uid, ex)

    threading.Thread(target=do_broadcast, daemon=True).start()
    return jsonify({"ok": True, "count": len(user_ids)})


@app.route("/api/admin/broadcast-tasks", methods=["POST"])
def admin_broadcast_tasks():
    data = request.json or {}
    admin_id = data.get("admin_id")
    err = require_admin(str(admin_id) if admin_id else None)
    if err:
        return err
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT telegram_id FROM users")
        user_ids = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    mini_app_url = MINI_APP_URL

    def do_broadcast():
        for uid in user_ids:
            try:
                url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
                http_requests.post(
                    url,
                    json={
                        "chat_id": uid,
                        "text": "<tg-emoji emoji-id=\"5282843764451195532\">📋</tg-emoji> <b>Новые задания уже в боте!</b>\n\nСкорее выполни их и зарабатывай лёгкие деньги <tg-emoji emoji-id=\"5287231198098117669\">💰</tg-emoji>",
                        "parse_mode": "HTML",
                        "reply_markup": {
                            "inline_keyboard": [
                                [
                                    {
                                        "text": " Задания",
                                        "web_app": {"url": mini_app_url},
                                        "style": "success",
                                        "icon_custom_emoji_id": "5282843764451195532",

                                    }
                                ]
                            ]
                        },
                    },
                    timeout=10,
                )
                time.sleep(0.05)
            except Exception as ex:
                logger.error("broadcast_tasks error uid=%s: %s", uid, ex)

    threading.Thread(target=do_broadcast, daemon=True).start()
    return jsonify({"ok": True, "count": len(user_ids)})


@app.route("/api/admin/promo-codes")
def admin_list_promo_codes():
    admin_id = request.args.get("admin_id")
    err = require_admin(admin_id)
    if err:
        return err
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id, code, discount_percent, categories, single_use,
                   max_uses, used_count, is_active, created_at
            FROM promo_codes
            ORDER BY created_at DESC
        """)
        promos = cur.fetchall()
        cur.close()
        conn.close()
        result = []
        for p in promos:
            row = dict(p)
            if row.get("created_at"):
                row["created_at"] = row["created_at"].isoformat()
            result.append(row)
        return jsonify(result)
    except Exception as e:
        logger.error("admin_list_promo_codes error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/promo-codes/create", methods=["POST"])
def admin_create_promo_code():
    data = request.json or {}
    admin_id = data.get("admin_id")
    err = require_admin(str(admin_id) if admin_id else None)
    if err:
        return err
    code = (data.get("code") or "").strip().upper()
    if not code:
        return jsonify({"error": "Укажите кодовое слово"}), 400
    discount_percent = data.get("discount_percent", 0)
    categories = data.get("categories", "all")
    single_use = bool(data.get("single_use", True))
    max_uses = data.get("max_uses", 100)
    try:
        discount_percent = int(discount_percent)
        if discount_percent < 1 or discount_percent > 100:
            return jsonify({"error": "Процент скидки должен быть от 1 до 100"}), 400
    except (TypeError, ValueError):
        return jsonify({"error": "Некорректный процент скидки"}), 400
    try:
        max_uses = int(max_uses)
        if max_uses < 1:
            max_uses = 1
    except (TypeError, ValueError):
        max_uses = 100
    if isinstance(categories, list):
        import json as _json

        categories_str = _json.dumps(categories)
    else:
        categories_str = str(categories)
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            INSERT INTO promo_codes (code, coins_amount, discount_percent, categories, single_use, max_uses, is_active)
            VALUES (%s, 0, %s, %s, %s, %s, TRUE)
            RETURNING id, code, discount_percent, categories, single_use, max_uses, used_count, is_active, created_at
            """,
            (code, discount_percent, categories_str, single_use, max_uses),
        )
        new_promo = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        row = dict(new_promo)
        if row.get("created_at"):
            row["created_at"] = row["created_at"].isoformat()
        return jsonify({"ok": True, "promo": row})
    except psycopg2.errors.UniqueViolation:
        return jsonify({"error": "Промокод с таким кодом уже существует"}), 409
    except Exception as e:
        logger.error("admin_create_promo_code error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/promo-codes/<int:promo_id>/delete", methods=["POST"])
def admin_delete_promo_code(promo_id):
    data = request.json or {}
    admin_id = data.get("admin_id")
    err = require_admin(str(admin_id) if admin_id else None)
    if err:
        return err
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM promo_codes WHERE id = %s", (promo_id,))
        if not cur.fetchone():
            cur.close()
            conn.close()
            return jsonify({"error": "Промокод не найден"}), 404
        cur.execute("DELETE FROM promo_usages WHERE promo_id = %s", (promo_id,))
        cur.execute("DELETE FROM promo_codes WHERE id = %s", (promo_id,))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("admin_delete_promo_code error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/promo-codes/<int:promo_id>/stats")
def admin_promo_stats(promo_id):
    admin_id = request.args.get("admin_id")
    err = require_admin(admin_id)
    if err:
        return err
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT id, code, discount_percent, categories, single_use,
                   max_uses, used_count, is_active, created_at
            FROM promo_codes WHERE id = %s
        """,
            (promo_id,),
        )
        promo = cur.fetchone()
        if not promo:
            cur.close()
            conn.close()
            return jsonify({"error": "Промокод не найден"}), 404
        cur.execute(
            """
            SELECT pu.used_at, u.username, u.first_name, u.nick, u.telegram_id
            FROM promo_usages pu
            JOIN users u ON pu.user_id = u.telegram_id
            WHERE pu.promo_id = %s
            ORDER BY pu.used_at DESC
            LIMIT 50
        """,
            (promo_id,),
        )
        usages = cur.fetchall()
        cur.close()
        conn.close()
        row = dict(promo)
        if row.get("created_at"):
            row["created_at"] = row["created_at"].isoformat()
        usage_list = []
        for u in usages:
            ur = dict(u)
            if ur.get("used_at"):
                ur["used_at"] = ur["used_at"].isoformat()
            usage_list.append(ur)
        return jsonify({"promo": row, "usages": usage_list})
    except Exception as e:
        logger.error("admin_promo_stats error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/clear-all", methods=["POST"])
def admin_clear_all():
    data = request.json or {}
    admin_id = data.get("admin_id")
    password = data.get("password", "")
    err = require_admin(str(admin_id) if admin_id else None)
    if err:
        return err
    if password != ADMIN_PASSWORD:
        return jsonify({"ok": False, "error": "Неверный пароль"}), 403
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("DELETE FROM promo_usages")
        cur.execute("DELETE FROM user_activity")
        cur.execute("DELETE FROM purchases")
        cur.execute("DELETE FROM referrals")
        cur.execute("DELETE FROM moderators")
        cur.execute("DELETE FROM promo_codes")
        cur.execute("DELETE FROM users")
        cur.execute("DELETE FROM weekly_events")
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("admin_clear_all error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/tasks")
def admin_get_tasks():
    admin_id = request.args.get("admin_id")
    err = require_admin_or_mod(admin_id)
    if err:
        return err
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM tasks ORDER BY created_at DESC")
        tasks = [dict(t) for t in cur.fetchall()]
        for t in tasks:
            if t.get("created_at"):
                t["created_at"] = t["created_at"].isoformat()
        cur.close()
        conn.close()
        return jsonify(tasks)
    except Exception as e:
        logger.error("admin_get_tasks error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/tasks/create", methods=["POST"])
def admin_create_task():
    data = request.json or {}
    admin_id = data.get("admin_id")
    err = require_admin_or_mod(admin_id)
    if err:
        return err
    name = (data.get("name") or "").strip()
    photo_url = (data.get("photo_url") or "").strip() or None
    reward = data.get("reward", 0)
    task_type = (data.get("task_type") or "other").strip()
    video_url = (data.get("video_url") or "").strip() or None
    channel_link = (data.get("channel_link") or "").strip() or None
    button_text = (data.get("button_text") or "").strip() or None
    button_url = (data.get("button_url") or "").strip() or None

    if not name:
        return jsonify({"error": "name is required"}), 400
    if task_type not in ("video", "subscription", "other"):
        return jsonify({"error": "invalid task_type"}), 400
    try:
        reward = int(reward)
        if reward < 0:
            reward = 0
    except (TypeError, ValueError):
        reward = 0

    if task_type == "video" and not video_url:
        return jsonify({"error": "video_url is required for video tasks"}), 400
    if task_type == "subscription":
        if not channel_link:
            return jsonify(
                {"error": "channel_link is required for subscription tasks"}
            ), 400
        try:
            resp = http_requests.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getChat",
                params={"chat_id": channel_link},
                timeout=10,
            )
            chat_data = resp.json()
            if not chat_data.get("ok"):
                return jsonify({"error": "channel_not_found"}), 400
            resp2 = http_requests.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getChatAdministrators",
                params={"chat_id": channel_link},
                timeout=10,
            )
            admins_data = resp2.json()
            if not admins_data.get("ok"):
                return jsonify({"error": "cannot_get_admins"}), 400
            bot_info_resp = http_requests.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getMe",
                timeout=10,
            )
            bot_info = bot_info_resp.json().get("result", {})
            bot_id = bot_info.get("id")
            admin_ids_list = [a["user"]["id"] for a in admins_data.get("result", [])]
            if bot_id not in admin_ids_list:
                return jsonify({"error": "bot_not_admin"}), 400
        except Exception as e:
            logger.error("admin_create_task subscription check error: %s", e)
            return jsonify({"error": "channel_check_failed"}), 500
    if task_type == "other":
        if not button_text or not button_url:
            return jsonify(
                {"error": "button_text and button_url are required for other tasks"}
            ), 400

    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            INSERT INTO tasks (name, photo_url, reward, task_type, video_url, channel_link, button_text, button_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                name,
                photo_url,
                reward,
                task_type,
                video_url,
                channel_link,
                button_text,
                button_url,
            ),
        )
        task = dict(cur.fetchone())
        if task.get("created_at"):
            task["created_at"] = task["created_at"].isoformat()
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True, "task": task})
    except Exception as e:
        logger.error("admin_create_task error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/tasks/<int:task_id>/delete", methods=["POST"])
def admin_delete_task(task_id):
    data = request.json or {}
    admin_id = data.get("admin_id")
    err = require_admin_or_mod(admin_id)
    if err:
        return err
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("DELETE FROM tasks WHERE id = %s", (task_id,))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("admin_delete_task error: %s", e)
        return jsonify({"error": str(e)}), 500


# ==================== TELEGRAM BOT ====================


async def check_subscriptions(bot: Bot, user_id: int) -> list:
    not_subscribed = []
    for name, username in REQUIRED_CHANNELS.items():
        try:
            member = await bot.get_chat_member(chat_id=username, user_id=user_id)
            if member.status in ("left", "kicked", "banned"):
                not_subscribed.append(name)
        except TelegramAPIError as e:
            logger.warning("Не удалось проверить канал %s: %s", username, e)
            not_subscribed.append(name)
    return not_subscribed


def build_subscribe_keyboard(not_subscribed: list) -> InlineKeyboardMarkup:
    buttons = []
    for name in not_subscribed:
        username = REQUIRED_CHANNELS[name]
        link = f"https://t.me/{username.lstrip('@')}"
        buttons.append([InlineKeyboardButton(text=f" {name}", url=link, style="primary", icon_custom_emoji_id="6039422865189638057")])
    buttons.append(
        [InlineKeyboardButton(text=" Я подписался(ась)", callback_data="check_sub", style="success", icon_custom_emoji_id="5206607081334906820")]
    )
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def build_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=" Открыть приложение", style="success", icon_custom_emoji_id="5282843764451195532", web_app=WebAppInfo(url=MINI_APP_URL)
                )
            ],
            [
                InlineKeyboardButton(
                    text=" Профиль", style="primary", icon_custom_emoji_id="5197269100878907942", web_app=WebAppInfo(url=MINI_APP_URL + "?section=profile")
                ),
                InlineKeyboardButton(
                    text=" Ивент", style="primary", icon_custom_emoji_id="5462927083132970373", web_app=WebAppInfo(url=MINI_APP_URL + "?section=event")
                ),
                InlineKeyboardButton(
                    text=" Задания", style="primary", icon_custom_emoji_id="5193177581888755275", web_app=WebAppInfo(url=MINI_APP_URL + "?section=tasks")
                ),
            ],
            [InlineKeyboardButton(text=" Реферальная ссылка", style="success", icon_custom_emoji_id="5271604874419647061", callback_data="referral")],
            [
                InlineKeyboardButton(
                    text=" Поддержка", style="danger", icon_custom_emoji_id="5238025132177369293", callback_data="help"
                ),
                InlineKeyboardButton(
                    text=" Инструкция", style="danger", icon_custom_emoji_id="5222444124698853913", callback_data="instruction"
                ),
            ],
            [InlineKeyboardButton(text=" NodeLink PREMIUM", style="primary", icon_custom_emoji_id="6028338546736107668", callback_data="premium")],
        ]
    )


def upsert_user_db(telegram_id: int, username: str, first_name: str):
    """Register or update user in DB synchronously."""
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO users (telegram_id, username, first_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (telegram_id) DO UPDATE
              SET username = EXCLUDED.username,
                  first_name = EXCLUDED.first_name
        """,
            (telegram_id, username or "", first_name or ""),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error("upsert_user_db error: %s", e)


def log_user_activity(telegram_id: int):
    """Record today's activity for the user (upsert, no duplicates)."""
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO user_activity (user_id, activity_date)
            VALUES (%s, CURRENT_DATE)
            ON CONFLICT (user_id, activity_date) DO NOTHING
            """,
            (telegram_id,),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error("log_user_activity error: %s", e)


def _confirm_referral(
    cur, inviter_id: int, invitee_id: int, invitee_name: str, result: dict
):
    """Confirm referral and reward inviter. Uses existing cursor."""
    cur.execute(
        """
        UPDATE referrals
        SET confirmed = TRUE, confirmed_at = NOW()
        WHERE inviter_id = %s AND invitee_id = %s AND confirmed = FALSE AND expired = FALSE
        RETURNING id
    """,
        (inviter_id, invitee_id),
    )
    confirmed = cur.fetchone()
    if confirmed:
        cur.execute("SELECT status FROM users WHERE telegram_id = %s", (inviter_id,))
        inviter_status_row = cur.fetchone()
        is_premium = inviter_status_row and inviter_status_row["status"] == "Premium"
        coins = 10 if is_premium else 5
        cur.execute(
            """
            UPDATE users
            SET referral_count = referral_count + 1,
                event_referral_count = event_referral_count + 1,
                balance = balance + %s
            WHERE telegram_id = %s
        """,
            (coins, inviter_id),
        )
        result["confirm_inviter_id"] = inviter_id
        result["confirm_invitee_display"] = invitee_name
        result["coins_awarded"] = coins
        result["confirm_is_premium"] = is_premium


def process_referral_db(inviter_id: int, invitee_id: int) -> dict:
    """
    Create a pending referral record (inviter → invitee).
    Then check if the INVITER themselves was previously invited by someone with a pending
    unconfirmed referral. If the inviter now has ≥1 referral (this newly created one),
    that counts as "invited 1 friend", so we confirm the inviter's own pending referral.

    Returns info for notifications:
      - invitee_display: display name for the newly joined user (to notify inviter)
      - inviter_id: who to notify about the new join
      - confirm_inviter_id: who gets confirmed referral reward (inviter's own inviter)
      - confirm_invitee_display: display name for confirmation notification
    """
    result = {
        "inviter_id": None,
        "invitee_display": None,
        "confirm_inviter_id": None,
        "confirm_invitee_display": None,
        "coins_awarded": 5,
        "confirm_is_premium": False,
    }
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Ensure inviter exists
        cur.execute("SELECT * FROM users WHERE telegram_id = %s", (inviter_id,))
        inviter = cur.fetchone()
        if not inviter:
            cur.close()
            conn.close()
            return result

        # Ensure invitee exists
        cur.execute("SELECT * FROM users WHERE telegram_id = %s", (invitee_id,))
        invitee = cur.fetchone()
        if not invitee:
            cur.close()
            conn.close()
            return result

        # Don't allow self-referral
        if inviter_id == invitee_id:
            cur.close()
            conn.close()
            return result

        # Prevent mutual referrals: invitee must not have ever invited the inviter
        cur.execute(
            """
            SELECT 1 FROM referrals
            WHERE inviter_id = %s AND invitee_id = %s
        """,
            (invitee_id, inviter_id),
        )
        if cur.fetchone():
            # These two already have a referral relationship in the opposite direction
            cur.close()
            conn.close()
            return result

        # Create pending referral (inviter invited invitee)
        cur.execute(
            """
            INSERT INTO referrals (inviter_id, invitee_id)
            VALUES (%s, %s)
            ON CONFLICT (inviter_id, invitee_id) DO NOTHING
            RETURNING id
        """,
            (inviter_id, invitee_id),
        )
        inserted = cur.fetchone()
        if not inserted:
            cur.close()
            conn.close()
            return result

        invitee_name = invitee["username"] or invitee["first_name"] or str(invitee_id)
        result["inviter_id"] = inviter_id
        result["invitee_display"] = invitee_name

        # Now check: was the INVITER themselves referred by someone with a pending unconfirmed link?
        # If so, this new referral counts as the inviter's "first invited friend" → confirm it.
        cur.execute(
            """
            SELECT r.inviter_id, u.username, u.first_name
            FROM referrals r
            JOIN users u ON r.invitee_id = u.telegram_id
            WHERE r.invitee_id = %s
              AND r.confirmed = FALSE
              AND r.expires_at > NOW()
        """,
            (inviter_id,),
        )
        inviter_pending = cur.fetchone()

        if inviter_pending:
            # The inviter was previously invited by someone (inviter_pending["inviter_id"])
            # Inviter just got their first referral → confirm
            inviter_name = (
                inviter["username"] or inviter["first_name"] or str(inviter_id)
            )
            _confirm_referral(
                cur, inviter_pending["inviter_id"], inviter_id, inviter_name, result
            )

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error("process_referral_db error: %s", e)

    return result


def get_user_block_status(telegram_id: int):
    """Check if user is blocked. Returns (is_blocked, block_reason)."""
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT is_blocked, block_reason FROM users WHERE telegram_id = %s",
            (telegram_id,),
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return False, None
        return bool(row["is_blocked"]), row.get("block_reason")
    except Exception as e:
        logger.error("get_user_block_status error: %s", e)
        return False, None


async def cmd_start(message: Message, command: CommandObject, bot: Bot):
    user = message.from_user
    args_str = command.args or ""
    args = args_str.split() if args_str else []

    # Check if user is blocked
    is_blocked, block_reason = get_user_block_status(user.id)
    if is_blocked:
        reason_text = f"\n\n<tg-emoji emoji-id=\"5197269100878907942\">📋</tg-emoji> <b>Причина:</b> {block_reason}" if block_reason else ""
        await message.answer(
            f"<tg-emoji emoji-id=\"5240241223632954241\">🚫</tg-emoji> <b>Вы заблокированы</b>\n\nВы не можете использовать этого бота.{reason_text}",
            parse_mode="HTML",
        )
        return

    # Parse referral parameter
    inviter_id = None
    if args:
        param = args[0]
        raw = param[4:] if param.startswith("ref_") else param
        try:
            inviter_id = int(raw)
        except ValueError:
            inviter_id = None

    # Register / update user in DB
    upsert_user_db(user.id, user.username or "", user.first_name or "")
    log_user_activity(user.id)

    # Check subscription
    not_subscribed = await check_subscriptions(bot, user.id)

    if not_subscribed:
        # Store pending referral inviter to process after subscription
        if inviter_id:
            pending_inviters[user.id] = inviter_id
        text = (
            "<tg-emoji emoji-id=\"5461130232025078672\">👋</tg-emoji> Привет!\n\n"
            "Чтобы использовать бота, нужно подписаться на наши каналы:\n\n"
            + "\n".join(f"• <b>{name}</b>" for name in not_subscribed)
            + "\n\nПодпишись и нажми кнопку ниже <tg-emoji emoji-id=\"5231102735817918643\">👇</tg-emoji>"
        )
        await message.answer(
            text,
            parse_mode="HTML",
            reply_markup=build_subscribe_keyboard(not_subscribed),
        )
    else:
        # Process referral if provided
        if inviter_id:
            await handle_referral(bot, inviter_id, user)

        await send_start_menu(bot, message.chat.id)


async def handle_referral(bot: Bot, inviter_id: int, invitee_user):
    """Process referral: create DB record and send notifications."""
    ref_result = process_referral_db(inviter_id, invitee_user.id)

    if ref_result.get("invitee_display"):
        invitee_name = (
            invitee_user.username or invitee_user.first_name or str(invitee_user.id)
        )
        display = f"@{invitee_name}" if invitee_user.username else invitee_name

        # Check if inviter is premium to show correct reward amount
        try:
            conn_chk = get_db()
            cur_chk = conn_chk.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur_chk.execute("SELECT status FROM users WHERE telegram_id = %s", (inviter_id,))
            inviter_row = cur_chk.fetchone()
            cur_chk.close()
            conn_chk.close()
            inviter_is_premium = inviter_row and inviter_row["status"] == "Premium"
        except Exception:
            inviter_is_premium = False
        coins_text = "+10 монет (Premium)" if inviter_is_premium else "+5 монет"

        # Notify inviter about new join
        try:
            await bot.send_message(
                chat_id=inviter_id,
                text=(
                    f"<tg-emoji emoji-id=\"5456140674028019486\">⚡️</tg-emoji> Пользователь {display} присоединился по твоей ссылке!\n\n"
                    f"Ты получишь {coins_text}, как только он пригласит 1 друга."
                ),
                parse_mode="HTML",
            )
        except TelegramAPIError as e:
            logger.warning("Не удалось уведомить пригласившего %s: %s", inviter_id, e)

    # If referral was immediately confirmed (invitee already had referrals)
    if ref_result.get("confirm_inviter_id"):
        await send_referral_confirmed_notification(bot, ref_result)


async def send_referral_confirmed_notification(bot: Bot, result: dict):
    """Notify inviter that their referral has been confirmed."""
    inviter_id = result.get("confirm_inviter_id")
    invitee_display = result.get("confirm_invitee_display", "пользователь")
    if not inviter_id:
        return
    display = (
        f"@{invitee_display}" if not invitee_display.isdigit() else invitee_display
    )
    coins = result.get("coins_awarded", 5)
    is_premium = result.get("confirm_is_premium", False)
    coins_label = f"+{coins} монет (Premium)" if is_premium else f"+{coins} монет"
    try:
        await bot.send_message(
            chat_id=inviter_id,
            text=(
                f"<tg-emoji emoji-id=\"5197474765387864959\">✅</tg-emoji> Реферал подтверждён!\n\n"
                f"{display} пригласил друга.\n"
                f"Тебе начислено: <b>{coins_label}</b> <tg-emoji emoji-id=\"5395755469660762251\">🎉</tg-emoji>"
            ),
            parse_mode="HTML",
        )
    except TelegramAPIError as e:
        logger.warning("Не удалось уведомить о подтверждении %s: %s", inviter_id, e)


async def callback_pre_checkout(pre_checkout_query: PreCheckoutQuery):
    """Approve all pre-checkout queries (required for Stars payments)."""
    await pre_checkout_query.answer(ok=True)


async def callback_successful_payment(message: Message):
    """Handle successful Stars payment — activate Premium for user."""
    user = message.from_user
    payment = message.successful_payment
    payload = payment.invoice_payload

    if payload.startswith("premium_stars_"):
        try:
            interval = f"{PREMIUM_DAYS} days"
            conn = get_db()
            cur = conn.cursor()
            cur.execute(
                "UPDATE users SET status = 'Premium', premium_until = NOW() + INTERVAL %s WHERE telegram_id = %s",
                (interval, user.id),
            )
            conn.commit()
            cur.close()
            conn.close()
            await message.answer(
                f"<tg-emoji emoji-id=\"6028338546736107668\">🌟</tg-emoji> <b>Premium активирован!</b>\n\n"
                f"Вы оплатили {payment.total_amount} <tg-emoji emoji-id=\"5951810621887484519\">⭐</tg-emoji> Stars.\n"
                f"Ваш статус обновлён до <b>Premium</b> на {PREMIUM_DAYS} дней.\n"
                f"Наслаждайтесь всеми привилегиями! <tg-emoji emoji-id=\"5235711785482341993\">🎉</tg-emoji>",
                parse_mode="HTML",
            )
        except Exception as e:
            logger.error("callback_successful_payment error: %s", e)


async def handle_any_message(message: Message):
    """Catch-all handler: block check for any message."""
    user = message.from_user
    if not user:
        return
    log_user_activity(user.id)
    is_blocked, block_reason = get_user_block_status(user.id)
    if is_blocked:
        reason_text = f"\n\n<tg-emoji emoji-id=\"5197269100878907942\">📋</tg-emoji> <b>Причина:</b> {block_reason}" if block_reason else ""
        try:
            await message.answer(
                f"<tg-emoji emoji-id=\"5240241223632954241\">🚫</tg-emoji> <b>Вы заблокированы</b>\n\nВы не можете использовать этого бота.{reason_text}",
                parse_mode="HTML",
            )
        except Exception:
            pass


async def callback_check_sub(callback: CallbackQuery, bot: Bot):
    await callback.answer()
    user = callback.from_user

    is_blocked, block_reason = get_user_block_status(user.id)
    if is_blocked:
        reason_text = f"\n\n<tg-emoji emoji-id=\"5197269100878907942\">📋</tg-emoji> Причина: {block_reason}" if block_reason else ""
        await callback.answer(f"<tg-emoji emoji-id=\"5240241223632954241\">🚫</tg-emoji> Вы заблокированы.{reason_text}", show_alert=True)
        return

    not_subscribed = await check_subscriptions(bot, user.id)

    if not_subscribed:
        text = (
            "<tg-emoji emoji-id=\"5465665476971471368\">❌</tg-emoji> Ты ещё не подписан(а) на:\n\n"
            + "\n".join(f"• <b>{name}</b>" for name in not_subscribed)
            + "\n\nПодпишись и попробуй снова <tg-emoji emoji-id=\"5231102735817918643\">👇</tg-emoji>"
        )
        await callback.answer("Подписка не найдена.", show_alert=True)
        await callback.message.edit_text(
            text,
            parse_mode="HTML",
            reply_markup=build_subscribe_keyboard(not_subscribed),
        )
    else:
        # Process pending referral if any
        pending_inviter = pending_inviters.pop(user.id, None)
        if pending_inviter:
            await handle_referral(bot, pending_inviter, user)

        await callback.message.delete()
        await send_start_menu(bot, callback.message.chat.id)


def back_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=" Назад", callback_data="back_to_menu", icon_custom_emoji_id="5960671702059848143")]
    ])

def premium_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=" Оформить NodeLink PREMIUM", callback_data="get_premium", icon_custom_emoji_id="5440841102871517055", style="success")],
        [InlineKeyboardButton(text=" Назад", callback_data="back_to_menu", icon_custom_emoji_id="5960671702059848143")]
    ])

def premium_buy_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=" Telegram Stars", callback_data="telegram_stars_pay", icon_custom_emoji_id="5438496463044752972", style="success")],
        [InlineKeyboardButton(text=" CryptoPay", callback_data="crypto_pay", icon_custom_emoji_id="5361543877599724417", style="primary")],
        [InlineKeyboardButton(text=" Ручная оплата", url="https://t.me/s_narzimurodov", callback_data="admin_pay", icon_custom_emoji_id="5445353829304387411", style="danger")],
        [InlineKeyboardButton(text=" Назад", callback_data="premium", icon_custom_emoji_id="5960671702059848143")]
    ])


async def callback_instruction(callback: CallbackQuery):
    await callback.answer()
    try:
        await callback.message.delete()
    except Exception:
        pass
    await callback.message.answer(
        '<tg-emoji emoji-id=\"5287684458881756303\">🤖</tg-emoji> В этом боте ты можешь зарабатывать коины на <b>HolyWorld Lite,</b>просто приглашая друзей или выполняя лёгкие задания!\n\n'

'<tg-emoji emoji-id=\"5271604874419647061\">🔗</tg-emoji> <b>Как пригласить друга и получить монеты?</b>\n\n'

'<tg-emoji emoji-id=\"5886583490434044162\">👆</tg-emoji> Нажми кнопку <b>«Реферальная ссылка»</b> в меню бота (/menu).\n'
'<tg-emoji emoji-id=\"6037622221625626773\">➡️</tg-emoji> Отправь свою ссылку друзьям или размещай её в группах и каналах.\n'
'<tg-emoji emoji-id=\"5769289093221454192\">🔗</tg-emoji> Когда человек перейдёт по твоей ссылке <b>и пригласит своего друга,</b> ты получишь <b>5 монеты,</b> которые можно потратить в магазине.\n\n'

'<tg-emoji emoji-id=\"5334882760735598374\">📝</tg-emoji> <b>Как получить монеты за выполнение заданий?</b>\n\n'

'<tg-emoji emoji-id=\"5884106131822875141\">👆</tg-emoji> Нажми кнопку <b>«Задания»</b> в меню (/menu).\n'
'<tg-emoji emoji-id=\"6033070647213560346\">🪟</tg-emoji> В открывшемся приложении выполняй задания и моментально получай монеты на свой баланс.',
        parse_mode="HTML",
        reply_markup=back_keyboard(),
    )

async def callback_help(callback: CallbackQuery):
    await callback.answer()
    try:
        await callback.message.delete()
    except Exception:
        pass
    await callback.message.answer(
        '<tg-emoji emoji-id="5443038326535759644">💬</tg-emoji> <b>Служба поддержки</b>\n\n'
        '<tg-emoji emoji-id="5436113877181941026">❓</tg-emoji> По всем вопросам и сделкам обращайтесь: @s_narzimurodov\n\n'
        '<tg-emoji emoji-id="5208573502046610594">🏪</tg-emoji> Мы готовы помочь вам 24/7!',
        parse_mode="HTML",
        reply_markup=back_keyboard(),
    )


async def callback_referral(callback: CallbackQuery, bot: Bot):
    await callback.answer()
    user = callback.from_user
    ref_link = f"https://t.me/{(await bot.get_me()).username}?start=ref_{user.id}"

    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT referral_count FROM users WHERE telegram_id = %s", (user.id,))
        row = cur.fetchone()
        referral_count = row["referral_count"] if row else 0

    text = (
        '<tg-emoji emoji-id="6028171274939797252">📦</tg-emoji> <b>Ваша реферальная ссылка</b>\n'
        f"<code>{ref_link}</code>\n\n"
        "▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        f'<tg-emoji emoji-id="6032594876506312598">👥</tg-emoji> <b>Рефералов:</b> {referral_count}\n\n'
        "Рефералы - это ваши друзья, приглашённые в бота через эту ссылку. "
        "Каждый новый реферал который пригласил своего реферала принесет вам +5 монет к балансу!"
    )

    try:
        await callback.message.delete()
    except Exception:
        pass
    await callback.message.answer(text, parse_mode="HTML", reply_markup=back_keyboard())


async def callback_premium(callback: CallbackQuery):
    await callback.answer()
    try:
        await callback.message.delete()
    except Exception:
        pass
    await callback.message.answer(
        '<tg-emoji emoji-id="5188481279963715781">🚀</tg-emoji> <b><u>NodeLink Premium — окупи подписку за один вечер!</u></b>\n'
        '<b>Представь: ты платишь всего 99 рублей за месяц и уже через 10 приглашённых друзей полностью возвращаешь эти деньги и выходишь в плюс!</b>\n\n'

        '<b><tg-emoji emoji-id="5217822164362739968">👑</tg-emoji> Что даёт Premium:</b>\n'
        '<b><tg-emoji emoji-id="5258354775757439405">➡️</tg-emoji>10 монет вместо 5 за каждого друга (а дальше — по 7.5 монет)</b>\n'
        '<b><tg-emoji emoji-id="5258354775757439405">➡️</tg-emoji>+50% к награде за все задания</b>\n'
        '<b><tg-emoji emoji-id="5258354775757439405">➡️</tg-emoji>Быстрый вывод — твои заказы обрабатываются в приоритете (выше в очереди у админа)</b>\n'
        '<b><tg-emoji emoji-id="5258354775757439405">➡️</tg-emoji>Красивая премиум-звезда перед именем (видно в ивенте и в списке друзей)</b>\n'
        '<b><tg-emoji emoji-id="5258354775757439405">➡️</tg-emoji>Кастомный тег в общем чате NodeLink</b>\n'
        '<b><tg-emoji emoji-id="5258354775757439405">➡️</tg-emoji>Полностью без рекламы — ничего не отвлекает от заработка</b>\n\n'

        '<b><tg-emoji emoji-id="5438496463044752972">⭐️</tg-emoji> А теперь самое вкусное:</b>\n'
        '<b>Многие пользователи приглашают 15+ рефералов в день! <tg-emoji emoji-id="5298822893923216866">😮</tg-emoji></b>\n'
        '<b>Это значит — ты не просто окупаешь подписку, а начинаешь зарабатывать гораздо больше, чем обычные юзеры. <tg-emoji emoji-id="5231200819986047254">📊</tg-emoji></b>\n'
        '<b>99 рублей → 10 друзей = подписка уже окупилась</b>\n'
        '<b><tg-emoji emoji-id="5424972470023104089">🔥</tg-emoji> А дальше — чистая прибыль + куча приятных бонусов каждый день.</b>\n'
        '<b>Хватит зарабатывать меньше, чем мог бы! <tg-emoji emoji-id="5231005931550030290">💸</tg-emoji></b>\n'
        '<b><tg-emoji emoji-id="5451882707875276247">🕯</tg-emoji> Переходи на новый уровень прямо сейчас и начинай получать максимум от NodeLink.</b>\n',
        parse_mode="HTML",
        reply_markup=premium_keyboard(),
    )

async def callback_buy_premium(callback: CallbackQuery):
    await callback.answer()
    try:
        await callback.message.delete()
    except Exception:
        pass
    await callback.message.answer(
        '<tg-emoji emoji-id="5381975814415866082">👇</tg-emoji> <b>Выберите способ оплаты:</b>',
        parse_mode="HTML",
        reply_markup=premium_buy_keyboard(),
    )


async def callback_stars_pay(callback: CallbackQuery, bot: Bot):
    await callback.answer()
    user_id = callback.from_user.id
    try:
        await bot.send_invoice(
            chat_id=callback.message.chat.id,
            title="NodeLink Premium",
            description=f"Премиум на {PREMIUM_DAYS} дней — все привилегии и бонусы!",
            payload=f"premium_stars_{user_id}",
            currency="XTR",
            prices=[LabeledPrice(label=f"Premium {PREMIUM_DAYS} дней", amount=PREMIUM_PRICE_STARS)],
        )
    except Exception as e:
        logger.error("callback_stars_pay error: %s", e)
        await callback.message.answer("<tg-emoji emoji-id=\"5465665476971471368\">❌</tg-emoji> Ошибка при создании счёта, попробуйте позже.")


async def callback_crypto_pay(callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    if not CRYPTO_PAY_TOKEN:
        await callback.message.answer("<tg-emoji emoji-id=\"5465665476971471368\">❌</tg-emoji> CryptoPay не найден, обратитесь в поддержку.")
        return
    try:
        payload = f"premium_crypto_{user_id}"
        url = f"{CRYPTO_PAY_API}/createInvoice"
        resp = http_requests.post(
            url,
            json={
                "asset": "USDT",
                "amount": PREMIUM_PRICE_USDT,
                "description": f"NodeLink Premium — {PREMIUM_DAYS} дней",
                "payload": payload,
                "paid_btn_name": "callback",
                "paid_btn_url": MINI_APP_URL,
                "allow_comments": False,
                "allow_anonymous": False,
            },
            headers={"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN},
            timeout=10,
        )
        result = resp.json()
        if result.get("ok"):
            invoice = result["result"]
            pay_url = invoice.get("pay_url") or invoice.get("bot_invoice_url")
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="<tg-emoji emoji-id=\"5427168083074628963\">💎</tg-emoji> Оплатить через CryptoBot", url=pay_url)]
            ])
            await callback.message.answer(
                f'<tg-emoji emoji-id=\"5427168083074628963\">💎</tg-emoji> <b>Счёт создан!</b>\n\nСумма: <b>{PREMIUM_PRICE_USDT} USDT</b>\nНажмите кнопку ниже для оплаты.',
                parse_mode="HTML",
                reply_markup=kb,
            )
        else:
            err = result.get("error", {})
            await callback.message.answer(f"<tg-emoji emoji-id=\"5465665476971471368\">❌</tg-emoji> Ошибка CryptoPay: {err.get('message', 'Неизвестная ошибка')}")
    except Exception as e:
        logger.error("callback_crypto_pay error: %s", e)
        await callback.message.answer("<tg-emoji emoji-id=\"5465665476971471368\">❌</tg-emoji> Ошибка соединения с CryptoPay, попробуйте позже.")

async def callback_back_to_menu(callback: CallbackQuery, bot: Bot):
    await callback.answer()
    try:
        await callback.message.delete()
    except Exception:
        pass
    await send_menu(bot, callback.message.chat.id)

async def cmd_menu(message: Message, bot: Bot):
    await send_menu(bot, message.chat.id)


async def cmd_app(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Открыть приложение", web_app=WebAppInfo(url=MINI_APP_URL))]
    ])
    await message.answer(
        '<tg-emoji emoji-id="5282843764451195532">📱</tg-emoji> Нажми кнопку ниже, чтобы открыть приложение:',
        parse_mode="HTML",
        reply_markup=kb,
    )


async def cmd_shop(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Открыть Магазин", web_app=WebAppInfo(url=MINI_APP_URL + "?section=shop"))]
    ])
    await message.answer(
        '<tg-emoji emoji-id="5208573502046610594">🏪</tg-emoji> Нажми кнопку ниже, чтобы открыть Магазин:',
        parse_mode="HTML",
        reply_markup=kb,
    )


async def cmd_tasks(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Открыть Задания", web_app=WebAppInfo(url=MINI_APP_URL + "?section=tasks"))]
    ])
    await message.answer(
        '<tg-emoji emoji-id="5193177581888755275">📝</tg-emoji> Нажми кнопку ниже, чтобы открыть Задания:',
        parse_mode="HTML",
        reply_markup=kb,
    )


async def cmd_event(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Открыть Ивент", web_app=WebAppInfo(url=MINI_APP_URL + "?section=event"))]
    ])
    await message.answer(
        '<tg-emoji emoji-id="5462927083132970373">🎉</tg-emoji> Нажми кнопку ниже, чтобы открыть Ивент:',
        parse_mode="HTML",
        reply_markup=kb,
    )


async def cmd_invite(message: Message, bot: Bot):
    user = message.from_user
    ref_link = f"https://t.me/{(await bot.get_me()).username}?start=ref_{user.id}"

    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT referral_count FROM users WHERE telegram_id = %s", (user.id,))
        row = cur.fetchone()
        referral_count = row["referral_count"] if row else 0

    text = (
        '<tg-emoji emoji-id="6028171274939797252">📦</tg-emoji> <b>Ваша реферальная ссылка</b>\n'
        f"<code>{ref_link}</code>\n\n"
        "▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        f'<tg-emoji emoji-id="6032594876506312598">👥</tg-emoji> <b>Рефералов:</b> {referral_count}\n\n'
        "Рефералы - это ваши друзья, приглашённые в бота через эту ссылку. "
        "Каждый новый реферал который пригласил своего реферала принесет вам +5 монет к балансу!"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=back_keyboard())


async def cmd_premium(message: Message):
    await message.answer(
        '<tg-emoji emoji-id="5188481279963715781">🚀</tg-emoji> <b><u>NodeLink Premium — окупи подписку за один вечер!</u></b>\n'
        '<b>Представь: ты платишь всего 99 рублей за месяц и уже через 10 приглашённых друзей полностью возвращаешь эти деньги и выходишь в плюс!</b>\n\n'
        '<b><tg-emoji emoji-id="5217822164362739968">👑</tg-emoji> Что даёт Premium:</b>\n'
        '<b><tg-emoji emoji-id="5258354775757439405">➡️</tg-emoji>10 монет вместо 5 за каждого друга (а дальше — по 7.5 монет)</b>\n'
        '<b><tg-emoji emoji-id="5258354775757439405">➡️</tg-emoji>+50% к награде за все задания</b>\n'
        '<b><tg-emoji emoji-id="5258354775757439405">➡️</tg-emoji>Быстрый вывод — твои заказы обрабатываются в приоритете (выше в очереди у админа)</b>\n'
        '<b><tg-emoji emoji-id="5258354775757439405">➡️</tg-emoji>Красивая премиум-звезда перед именем (видно в ивенте и в списке друзей)</b>\n'
        '<b><tg-emoji emoji-id="5258354775757439405">➡️</tg-emoji>Кастомный тег в общем чате NodeLink</b>\n'
        '<b><tg-emoji emoji-id="5258354775757439405">➡️</tg-emoji>Полностью без рекламы — ничего не отвлекает от заработка</b>\n\n'
        '<b><tg-emoji emoji-id="5438496463044752972">⭐️</tg-emoji> А теперь самое вкусное:</b>\n'
        '<b>Многие пользователи приглашают 15+ рефералов в день! <tg-emoji emoji-id="5298822893923216866">😮</tg-emoji></b>\n'
        '<b>Это значит — ты не просто окупаешь подписку, а начинаешь зарабатывать гораздо больше, чем обычные юзеры. <tg-emoji emoji-id="5231200819986047254">📊</tg-emoji></b>\n'
        '<b>99 рублей → 10 друзей = подписка уже окупилась</b>\n'
        '<b><tg-emoji emoji-id="5424972470023104089">🔥</tg-emoji> А дальше — чистая прибыль + куча приятных бонусов каждый день.</b>\n'
        '<b>Хватит зарабатывать меньше, чем мог бы! <tg-emoji emoji-id="5231005931550030290">💸</tg-emoji></b>\n'
        '<b><tg-emoji emoji-id="5451882707875276247">🕯</tg-emoji> Переходи на новый уровень прямо сейчас и начинай получать максимум от NodeLink.</b>\n',
        parse_mode="HTML",
        reply_markup=premium_keyboard(),
    )


async def cmd_instruction(message: Message):
    await message.answer(
        '<tg-emoji emoji-id="5287684458881756303">🤖</tg-emoji> В этом боте ты можешь зарабатывать коины на <b>HolyWorld Lite,</b>просто приглашая друзей или выполняя лёгкие задания!\n\n'
        '<tg-emoji emoji-id="5271604874419647061">🔗</tg-emoji> <b>Как пригласить друга и получить монеты?</b>\n\n'
        '<tg-emoji emoji-id="5886583490434044162">👆</tg-emoji> Нажми кнопку <b>«Реферальная ссылка»</b> в меню бота (/menu).\n'
        '<tg-emoji emoji-id="6037622221625626773">➡️</tg-emoji> Отправь свою ссылку друзьям или размещай её в группах и каналах.\n'
        '<tg-emoji emoji-id="5769289093221454192">🔗</tg-emoji> Когда человек перейдёт по твоей ссылке <b>и пригласит своего друга,</b> ты получишь <b>5 монеты,</b> которые можно потратить в магазине.\n\n'
        '<tg-emoji emoji-id="5334882760735598374">📝</tg-emoji> <b>Как получить монеты за выполнение заданий?</b>\n\n'
        '<tg-emoji emoji-id="5884106131822875141">👆</tg-emoji> Нажми кнопку <b>«Задания»</b> в меню (/menu).\n'
        '<tg-emoji emoji-id="6033070647213560346">🪟</tg-emoji> В открывшемся приложении выполняй задания и моментально получай монеты на свой баланс.',
        parse_mode="HTML",
        reply_markup=back_keyboard(),
    )


async def cmd_help(message: Message):
    await message.answer(
        '<tg-emoji emoji-id="5443038326535759644">💬</tg-emoji> <b>Служба поддержки</b>\n\n'
        '<tg-emoji emoji-id="5436113877181941026">❓</tg-emoji> По всем вопросам и сделкам обращайтесь: @s_narzimurodov\n\n'
        '<tg-emoji emoji-id="5208573502046610594">🏪</tg-emoji> Мы готовы помочь вам 24/7!',
        parse_mode="HTML",
        reply_markup=back_keyboard(),
    )


async def send_menu(bot: Bot, chat_id: int):
    try:
        await bot.send_photo(
            chat_id=chat_id,
            photo=MENU_P,
            caption=MENU,
            parse_mode="HTML",
            reply_markup=build_menu_keyboard(),
        )
    except TelegramAPIError as e:
        logger.error("Ошибка отправки меню: %s", e)
        await bot.send_message(
            chat_id=chat_id,
            text=MENU,
            parse_mode="HTML",
            reply_markup=build_menu_keyboard(),
        )


async def send_start_menu(bot: Bot, chat_id: int):
    try:
        await bot.send_photo(
            chat_id=chat_id,
            photo=MENU_PHOTO,
            caption=MENU_TEXT,
            parse_mode="HTML",
            reply_markup=build_menu_keyboard(),
        )
    except TelegramAPIError as e:
        logger.error("Ошибка отправки стартового меню: %s", e)
        await bot.send_message(
            chat_id=chat_id,
            text=MENU_TEXT,
            parse_mode="HTML",
            reply_markup=build_menu_keyboard(),
        )


def run_flask():
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


async def run_bot():
    global BOT_USERNAME

    if not BOT_TOKEN:
        logger.warning("BOT_TOKEN не задан, бот не запущен")
        return

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()

    try:
        bot_info = await bot.get_me()
        BOT_USERNAME = bot_info.username
        logger.info("Bot username: %s", BOT_USERNAME)
    except Exception as e:
        logger.error("Failed to get bot username: %s", e)

    # Drop any leftover webhook/session from previous instance
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("Webhook deleted, starting polling...")
    except Exception as e:
        logger.warning("delete_webhook failed: %s", e)

    await asyncio.sleep(2)

    dp.message.register(cmd_start, CommandStart())
    dp.message.register(cmd_menu, Command("menu"))
    dp.message.register(cmd_app, Command("app"))
    dp.message.register(cmd_shop, Command("shop"))
    dp.message.register(cmd_tasks, Command("tasks"))
    dp.message.register(cmd_event, Command("event"))
    dp.message.register(cmd_invite, Command("invite"))
    dp.message.register(cmd_premium, Command("premium"))
    dp.message.register(cmd_instruction, Command("instruction"))
    dp.message.register(cmd_help, Command("help"))
    dp.callback_query.register(callback_check_sub, F.data == "check_sub")
    dp.callback_query.register(callback_instruction, F.data == "instruction")
    dp.callback_query.register(callback_help, F.data == "help")
    dp.callback_query.register(callback_referral, F.data == "referral")
    dp.callback_query.register(callback_premium, F.data == "premium")
    dp.callback_query.register(callback_back_to_menu, F.data == "back_to_menu")
    dp.callback_query.register(callback_buy_premium, F.data == "get_premium")
    dp.callback_query.register(callback_stars_pay, F.data == "telegram_stars_pay")
    dp.callback_query.register(callback_crypto_pay, F.data == "crypto_pay")
    dp.pre_checkout_query.register(callback_pre_checkout)
    dp.message.register(callback_successful_payment, F.successful_payment)
    dp.message.register(handle_any_message)

    await dp.start_polling(bot)


def main():
    # Init DB first
    try:
        init_db()
    except Exception as e:
        logger.error("DB init failed: %s", e)

    # Start Flask in background thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask запущен на порту %s", int(os.environ.get("PORT", 5000)))

    # Start referral cleanup thread
    cleanup_thread = threading.Thread(target=referral_cleanup_loop, daemon=True)
    cleanup_thread.start()
    logger.info("Referral cleanup thread started")

    # Start weekly event loop thread
    event_thread = threading.Thread(target=event_loop, daemon=True)
    event_thread.start()
    logger.info("Weekly event loop thread started")

    # Start Telegram bot
    asyncio.run(run_bot())


if __name__ == "__main__":
    main()
