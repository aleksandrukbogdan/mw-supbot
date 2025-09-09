import sqlite3
import os
import asyncio
import aiosqlite
import logging
from logger import logger

log = logger.get_logger('database')

# Проверяем, запущено ли приложение в окружении сервера (например, Amvera),
# где есть смонтированная папка /data для постоянного хранения.
if os.path.isdir('/data'):
    # Мы на сервере: используем абсолютный путь к смонтированной папке.
    DB_FOLDER = '/data'
else:
    # Мы на локальной машине: используем папку data внутри проекта.
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    DB_FOLDER = os.path.join(SCRIPT_DIR, 'data')

DB_NAME = 'users.db'
DB_PATH = os.path.join(DB_FOLDER, DB_NAME)

# Убедимся, что папка для БД существует.
# На сервере она должна быть смонтирована, локально — будет создана.
os.makedirs(DB_FOLDER, exist_ok=True)

def _initialize_db_sync():
    """Синхронная функция для инициализации БД (для run_in_executor)."""
    print(f"!!! АБСОЛЮТНЫЙ ПУТЬ К БАЗЕ ДАННЫХ: {os.path.abspath(DB_PATH)} !!!")
    conn = sqlite3.connect(DB_PATH, timeout=15)
    cursor = conn.cursor()
    
    # Создаем таблицу, если она не существует
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            fio TEXT
        )
    ''')
    
    # Проверяем, существует ли колонка fio, и добавляем ее, если нет
    cursor.execute("PRAGMA table_info(users)")
    columns = [column[1] for column in cursor.fetchall()]
    if 'fio' not in columns:
        cursor.execute('ALTER TABLE users ADD COLUMN fio TEXT')
    if 'username' not in columns:
        cursor.execute('ALTER TABLE users ADD COLUMN username TEXT')

    conn.commit()
    conn.close()

async def initialize_db():
    """Инициализирует базу данных и создает таблицы, если они не существуют."""
    db_dir = os.path.dirname(DB_PATH)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
        print(f"!!! СОЗДАНА ДИРЕКТОРИЯ ДЛЯ БАЗЫ ДАННЫХ: {db_dir} !!!")
    
    print(f"!!! АБСОЛЮТНЫЙ ПУТЬ К БАЗЕ ДАННЫХ: {os.path.abspath(DB_PATH)} !!!")
    
    conn = await aiosqlite.connect(DB_PATH)
    try:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                fio TEXT,
                username TEXT
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS topics (
                key TEXT PRIMARY KEY,
                thread_id INTEGER
            )
        ''')

        await conn.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        
        await conn.commit()
    finally:
        await conn.close()

def _get_or_create_user_sync(user_id: int) -> str | None:
    conn = sqlite3.connect(DB_PATH, timeout=15)
    cursor = conn.cursor()

    # Добавляем пользователя, игнорируя, если он уже существует
    cursor.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))

    # Получаем ФИО
    cursor.execute("SELECT fio FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()

    conn.commit()
    conn.close()

    if result and result[0]:
        return result[0]
    return None

async def get_or_create_user(user_id: int) -> str | None:
    """Асинхронно получает ФИО пользователя или создает нового, если его нет."""
    return await asyncio.to_thread(_get_or_create_user_sync, user_id)

def _set_user_fio_sync(user_id: int, fio: str):
    conn = sqlite3.connect(DB_PATH, timeout=15)
    cursor = conn.cursor()
    
    cursor.execute("UPDATE users SET fio = ? WHERE user_id = ?", (fio, user_id))
    
    conn.commit()
    conn.close()

async def set_user_fio(user_id: int, fio: str):
    """Асинхронно устанавливает или обновляет ФИО для пользователя."""
    await asyncio.to_thread(_set_user_fio_sync, user_id, fio)

def _get_user_fio_sync(user_id: int) -> str | None:
    """Синхронно получает ФИО пользователя из БД."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT fio FROM users WHERE user_id = ?", (user_id,))
        result = cursor.fetchone()
        return result[0] if result and result[0] else None

async def get_user_fio(user_id: int) -> str | None:
    """Асинхронно получает ФИО пользователя из БД."""
    return await asyncio.to_thread(_get_user_fio_sync, user_id)

def _get_all_users_sync():
    conn = sqlite3.connect(DB_PATH, timeout=15)
    cursor = conn.cursor()
    
    cursor.execute("SELECT user_id FROM users")
    # Преобразуем список кортежей [(id1,), (id2,)] в простой список [id1, id2]
    users = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    return users

async def get_all_users():
    """Асинхронно возвращает список всех ID пользователей из базы данных."""
    return await asyncio.to_thread(_get_all_users_sync)

def _delete_user_sync(user_id: int):
    """Синхронно удаляет пользователя из БД."""
    conn = sqlite3.connect(DB_PATH, timeout=15)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()

async def delete_user(user_id: int):
    """Асинхронно удаляет пользователя из базы данных."""
    await asyncio.to_thread(_delete_user_sync, user_id)

def _set_user_username_sync(user_id: int, username: str):
    conn = sqlite3.connect(DB_PATH, timeout=15)
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET username = ? WHERE user_id = ?", (username, user_id))
    conn.commit()
    conn.close()

async def set_user_username(user_id: int, username: str):
    await asyncio.to_thread(_set_user_username_sync, user_id, username)

def _get_user_username_sync(user_id: int) -> str | None:
    conn = sqlite3.connect(DB_PATH, timeout=15)
    cursor = conn.cursor()
    cursor.execute("SELECT username FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    conn.close()
    if result and result[0]:
        return result[0]
    return None

async def get_user_username(user_id: int) -> str | None:
    return await asyncio.to_thread(_get_user_username_sync, user_id) 

async def set_topic_id(topic_key: str, thread_id: int):
    """Сохраняет или обновляет thread_id для системного топика."""
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute(
                "INSERT INTO topics (key, thread_id) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET thread_id = excluded.thread_id",
                (topic_key, thread_id)
            )
            await conn.commit()
            log.info(f"ID топика для '{topic_key}' сохранен/обновлен: {thread_id}")
    except aiosqlite.Error as e:
        log.error(f"Ошибка при сохранении ID топика '{topic_key}': {e}")

async def get_all_topic_ids() -> dict:
    """Возвращает словарь со всеми сохраненными ID топиков."""
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT key, thread_id FROM topics")
            rows = await cursor.fetchall()
            log.info(f"Загружено {len(rows)} ID системных топиков из БД.")
            return {row[0]: row[1] for row in rows}
    except aiosqlite.Error as e:
        log.error(f"Ошибка при загрузке ID топиков из БД: {e}")
        return {} 

async def delete_all_topics() -> None:
    """Удаляет таблицу топиков из базы данных, если она существует."""
    conn = await aiosqlite.connect(DB_PATH)
    try:
        await conn.execute('DROP TABLE IF EXISTS topics')
        await conn.commit()
        log.info("Таблица 'topics' успешно удалена.")
    except Exception as e:
        log.error(f"Ошибка при удалении таблицы 'topics': {e}")
    finally:
        await conn.close() 

async def set_setting(key: str, value: str):
    """Сохраняет или обновляет значение для указанного ключа в настройках."""
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, str(value))  # Убедимся, что значение всегда строка
            )
            await conn.commit()
            log.info(f"Настройка '{key}' сохранена/обновлена: {value}")
    except aiosqlite.Error as e:
        log.error(f"Ошибка при сохранении настройки '{key}': {e}")

async def get_setting(key: str) -> str | None:
    """Возвращает значение для указанного ключа из настроек."""
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT value FROM settings WHERE key = ?", (key,))
            row = await cursor.fetchone()
            if row:
                log.info(f"Загружена настройка '{key}': {row[0]}")
                return row[0]
            else:
                log.info(f"Настройка '{key}' не найдена в БД.")
                return None
    except aiosqlite.Error as e:
        log.error(f"Ошибка при загрузке настройки '{key}' из БД: {e}")
        return None 