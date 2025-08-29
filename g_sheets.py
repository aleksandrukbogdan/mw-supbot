import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import os
import logging
import asyncio
from logger import logger

log = logger.get_logger('g_sheets')

# Определяем абсолютный путь к директории, где находится этот скрипт
script_dir = os.path.dirname(os.path.abspath(__file__))
# Составляем полный путь к файлу credentials.json
credentials_path = os.path.join(script_dir, "credentials.json")

# Глобальный кэш для объекта рабочего листа и лок для асинхронного доступа
_worksheet_cache = None
_worksheet_lock = asyncio.Lock()

# Словарь с часами на решение по каждому уровню приоритета.
# Вы можете изменить эти значения при необходимости.
SLA_HOURS = {
    'Критичный': 1.5,
    'Высокий': 8,
    'Средний': 72,
    'Низкий': 168 
}

def _connect_and_get_worksheet_sync():
    """Синхронная функция для подключения к Google Sheets. Вызывается только при необходимости."""
    log.info("Попытка подключения к Google Sheets")
    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        if not os.path.exists(credentials_path):
            log.error(f"Файл credentials.json не найден по пути: {credentials_path}")
            return None
            
        creds = Credentials.from_service_account_file(credentials_path, scopes=scopes)
        client = gspread.authorize(creds)
        log.info("Успешная авторизация в Google Sheets API")
        
        sheet_name = os.getenv("GOOGLE_SHEET_NAME")
        if not sheet_name:
            log.error("Необходимо установить переменную окружения GOOGLE_SHEET_NAME")
            return None

        spreadsheet = client.open(sheet_name)
        worksheet = spreadsheet.sheet1
        
        log.info("Таблица успешно открыта")

        # Проверяем и обновляем заголовки
        headers = [
            "Номер", "Статус обращения", "Приоритет", "ID Пользователя", "Тип", "ФИО", "Логин", "Площадка", "Сообщение", "Фото (File ID)", 
            "Время получения обращения", "Время взятия в работу", 
            "Передача на 2 линию", "Передача на 3 Линию",
            "Время на 1 линии", "Время на 2 линии", "Время на 3 линии",
            "Время закрытия обращения", "Время решения", "SLA (Время на решение)", "Соответствие SLA", "SLA-уведомление отправлено"
        ]
        
        current_headers = worksheet.row_values(1) if worksheet.get_all_values() else []

        if len(current_headers) < len(headers):
            worksheet.update('A1', [headers])
            log.info("Заголовки таблицы успешно обновлены")
        
        return worksheet

    except Exception as e:
        log.error(f"Ошибка при подключении к Google Sheets: {e}")
        log.exception("Полный стек ошибки:")
        return None

async def get_worksheet():
    """
    Асинхронно получает объект рабочего листа, используя кэширование.
    Инициализирует соединение при первом вызове.
    """
    global _worksheet_cache
    if _worksheet_cache is not None:
        return _worksheet_cache

    async with _worksheet_lock:
        # Повторная проверка внутри лока на случай, если другой поток уже инициализировал
        if _worksheet_cache is not None:
            return _worksheet_cache
        
        worksheet = await asyncio.to_thread(_connect_and_get_worksheet_sync)
        if worksheet:
            _worksheet_cache = worksheet
        return worksheet


def _add_feedback_sync(worksheet, user_id, feedback_type, fio, username, platform, message, photo_id):
    log.info(f"Добавление обращения в Google Sheets для user_id={user_id}, type={feedback_type}")
    try:
        current_time = datetime.now().strftime("%H:%M:%S %d.%m.%Y")
        new_row = ["", "Зарегистрировано", "", str(user_id), feedback_type, fio, username or "", platform, message, photo_id, current_time]
        new_row.extend([""] * 11)
        worksheet.append_row(new_row)
        # Теперь ищем строку по уникальным данным
        all_values = worksheet.get_all_values()
        for idx, row in enumerate(all_values[1:], 1):  # пропускаем заголовки
            if (
                row[3] == str(user_id)
                and row[4] == feedback_type
                and row[5] == fio
                and row[10] == current_time
            ):
                # Проставляем номер обращения в первый столбец
                worksheet.update_cell(idx + 1, 1, str(idx))
                return idx
        log.error("Не удалось найти только что добавленную строку для user_id=%s, feedback_type=%s", user_id, feedback_type)
        return None
    except Exception as e:
        log.error(f"Ошибка при добавлении записи в Google Sheets: {e}")
        log.exception("Полный стек ошибки:")
        return None

async def add_feedback(user_id, feedback_type, fio, username, platform, message, photo_id):
    """Асинхронно добавляет новую запись в Google Sheet и возвращает ее номер."""
    worksheet = await get_worksheet()
    if not worksheet:
        log.error("Не удалось получить доступ к рабочему листу для добавления отзыва")
        return None
    return await asyncio.to_thread(_add_feedback_sync, worksheet, user_id, feedback_type, fio, username, platform, message, photo_id)

def format_delta(delta):
    """Форматирует timedelta в строку ЧЧ:ММ:СС."""
    total_seconds = int(delta.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"

def _set_priority_and_sla_sync(worksheet, entry_id, priority):
    log.info(f"Установление приоритета {priority} и SLA для обращения #{entry_id}")
    try:
        row_number = int(entry_id) + 1
        
        COL_PRIORITY = 2
        COL_CREATION_TIME = 10
        COL_SLA = 19

        creation_time_str = worksheet.cell(row_number, COL_CREATION_TIME + 1).value
        if not creation_time_str:
            return
        
        creation_time = datetime.strptime(creation_time_str, "%H:%M:%S %d.%m.%Y")
        
        sla_hours = SLA_HOURS.get(priority)
        if sla_hours is None:
            return
            
        sla_deadline = creation_time + timedelta(hours=sla_hours)
        sla_deadline_str = sla_deadline.strftime("%H:%M:%S %d.%m.%Y")

        worksheet.update_cell(row_number, COL_PRIORITY + 1, priority)
        worksheet.update_cell(row_number, COL_SLA + 1, sla_deadline_str)

        log.info(f"Приоритет и SLA успешно установлены для обращения #{entry_id}")
    except Exception as e:
        log.error(f"Ошибка при установке приоритета для #{entry_id}: {e}")

async def set_priority_and_sla(entry_id, priority):
    """Асинхронно записывает приоритет и рассчитывает SLA."""
    worksheet = await get_worksheet()
    if not worksheet:
        log.error("Не удалось получить доступ к рабочему листу для установки приоритета")
        return
    await asyncio.to_thread(_set_priority_and_sla_sync, worksheet, entry_id, priority)

def _get_open_tickets_for_sla_check_sync(worksheet):
    log.info("Поиск открытых обращений для проверки sla")
    try:
        all_records = worksheet.get_all_records()
        log.info(f"Получено {len(all_records)} записей для проверки соответствия sla")

        open_tickets = []
        for record in all_records:
            number = record.get("Номер")
            status = record.get("Статус обращения")
            sla_time = record.get("SLA (Время на решение)")
            notification_sent = record.get("SLA-уведомление отправлено")
            priority = record.get("Приоритет")

            # Только критичные, не завершённые, с SLA, и без отправленного уведомления
            if status != "Завершено" and sla_time and priority == "Критичный" and not notification_sent:
                log.info(f"Найдено незавершённых критичных обращений #{number}")
                open_tickets.append(record)
        log.info(f"Всего найдено {len(open_tickets)} незавершённых критичных обращений")        
        return open_tickets
    except Exception as e:
        log.error(f"Ошибка при получении заявок для проверки SLA: {e}")
        return []

async def get_open_tickets_for_sla_check():
    """Асинхронно возвращает все строки, где статус не 'Завершено', SLA установлено, приоритет 'Критичный' и уведомление ещё не отправлено."""
    worksheet = await get_worksheet()
    if not worksheet:
        log.error("Не удалось получить доступ к рабочему листу для проверки SLA")
        return []
    return await asyncio.to_thread(_get_open_tickets_for_sla_check_sync, worksheet)

def _mark_sla_notification_sent_sync(worksheet, entry_id):
    try:
        row_number = int(entry_id) + 1
        col_index = 22 
        worksheet.update_cell(row_number, col_index, "Да")
        log.info(f"sla уведомление отмечено как отправленное для обращения #{entry_id}")
    except Exception as e:
         log.error(f"Ошибка при отметке SLA-уведомления для #{entry_id}: {e}")

async def mark_sla_notification_sent(entry_id):
    """Асинхронно отмечает, что уведомление по SLA было отправлено."""
    worksheet = await get_worksheet()
    if not worksheet:
        log.error("Не удалось получить доступ к рабочему листу для отметки SLA-уведомления")
        return
    await asyncio.to_thread(_mark_sla_notification_sent_sync, worksheet, entry_id)

def _record_action_sync(worksheet, entry_id, action, action_time, status=None):
    try:
        row_number = int(entry_id) + 1
        try:
            row_values = worksheet.row_values(row_number)
        except gspread.exceptions.CellNotFound:
             log.error(f"Не удалось найти строку для обращения #{entry_id}")
             return

        COL_STATUS = 1
        COL_DATE = 10
        COL_TAKEN_TIME = 11
        COL_TRANSFER_L2 = 12
        COL_TRANSFER_L3 = 13
        COL_DURATION_L1 = 14
        COL_DURATION_L2 = 15
        COL_DURATION_L3 = 16
        COL_CLOSE_TIME = 17
        COL_RESOLUTION_TIME = 18
        COL_SLA_TIME = 19
        COL_SLA_COMPLIANCE = 20
        EXPECTED_COLS = 22

        if len(row_values) < EXPECTED_COLS:
            row_values.extend([""] * (EXPECTED_COLS - len(row_values)))

        def parse_time(time_str):
            return datetime.strptime(time_str, "%H:%M:%S %d.%m.%Y") if time_str and isinstance(time_str, str) else None
        
        updates = []
        action_time_str = action_time.strftime("%H:%M:%S %d.%m.%Y")

        if status:
            updates.append({'range': gspread.utils.rowcol_to_a1(row_number, COL_STATUS + 1), 'values': [[status]]})

        if action == 'taken' and not row_values[COL_TAKEN_TIME]:
            updates.append({'range': gspread.utils.rowcol_to_a1(row_number, COL_TAKEN_TIME + 1), 'values': [[action_time_str]]})
        
        elif action == 'transfer_l2' and not row_values[COL_TRANSFER_L2]:
            updates.append({'range': gspread.utils.rowcol_to_a1(row_number, COL_TRANSFER_L2 + 1), 'values': [[action_time_str]]})
            if not row_values[COL_TAKEN_TIME]:
                updates.append({'range': gspread.utils.rowcol_to_a1(row_number, COL_TAKEN_TIME + 1), 'values': [[action_time_str]]})
            if not row_values[COL_DURATION_L1]:
                creation_time = parse_time(row_values[COL_DATE])
                if creation_time:
                    duration = action_time - creation_time
                    updates.append({'range': gspread.utils.rowcol_to_a1(row_number, COL_DURATION_L1 + 1), 'values': [[format_delta(duration)]]})

        elif action == 'transfer_l3' and not row_values[COL_TRANSFER_L3]:
            updates.append({'range': gspread.utils.rowcol_to_a1(row_number, COL_TRANSFER_L3 + 1), 'values': [[action_time_str]]})
            if not row_values[COL_TAKEN_TIME]:
                updates.append({'range': gspread.utils.rowcol_to_a1(row_number, COL_TAKEN_TIME + 1), 'values': [[action_time_str]]})
            if not row_values[COL_DURATION_L1]:
                creation_time = parse_time(row_values[COL_DATE])
                if creation_time:
                    duration = action_time - creation_time
                    updates.append({'range': gspread.utils.rowcol_to_a1(row_number, COL_DURATION_L1 + 1), 'values': [[format_delta(duration)]]})
            if not row_values[COL_DURATION_L2]:
                transfer_l2_time = parse_time(row_values[COL_TRANSFER_L2])
                if transfer_l2_time:
                    duration = action_time - transfer_l2_time
                    updates.append({'range': gspread.utils.rowcol_to_a1(row_number, COL_DURATION_L2 + 1), 'values': [[format_delta(duration)]]})
        
        elif action == 'closed' and not row_values[COL_CLOSE_TIME]:
            updates.append({'range': gspread.utils.rowcol_to_a1(row_number, COL_CLOSE_TIME + 1), 'values': [[action_time_str]]})
            
            creation_time = parse_time(row_values[COL_DATE])
            if creation_time and not row_values[COL_RESOLUTION_TIME]:
                duration = action_time - creation_time
                updates.append({'range': gspread.utils.rowcol_to_a1(row_number, COL_RESOLUTION_TIME + 1), 'values': [[format_delta(duration)]]})

            # Расчет времени на последней линии
            transfer_l3_time = parse_time(row_values[COL_TRANSFER_L3])
            transfer_l2_time = parse_time(row_values[COL_TRANSFER_L2])
            taken_time = parse_time(row_values[COL_TAKEN_TIME])

            if transfer_l3_time and not row_values[COL_DURATION_L3]:
                duration_l3 = action_time - transfer_l3_time
                updates.append({'range': gspread.utils.rowcol_to_a1(row_number, COL_DURATION_L3 + 1), 'values': [[format_delta(duration_l3)]]})
            elif transfer_l2_time and not row_values[COL_DURATION_L2]:
                duration_l2 = action_time - transfer_l2_time
                updates.append({'range': gspread.utils.rowcol_to_a1(row_number, COL_DURATION_L2 + 1), 'values': [[format_delta(duration_l2)]]})
            elif taken_time and not row_values[COL_DURATION_L1]:
                duration_l1 = action_time - taken_time
                updates.append({'range': gspread.utils.rowcol_to_a1(row_number, COL_DURATION_L1 + 1), 'values': [[format_delta(duration_l1)]]})

            sla_time = parse_time(row_values[COL_SLA_TIME])
            if sla_time:
                compliance = "В срок" if action_time <= sla_time else "Просрочено"
                updates.append({'range': gspread.utils.rowcol_to_a1(row_number, COL_SLA_COMPLIANCE + 1), 'values': [[compliance]]})

        if updates:
            worksheet.batch_update(updates)
            log.info(f"Действие '{action}' успешно записано в Google Sheets для обращения #{entry_id}")
            
    except Exception as e:
        log.error(f"Ошибка при записи действия для #{entry_id}: {e}")
        log.exception("Полный стек ошибки:")

async def record_action(entry_id, action, action_time, status=None):
    """Асинхронно записывает действие, его время и вычисляет длительность этапов."""
    worksheet = await get_worksheet()
    if not worksheet:
        log.error("Не удалось получить доступ к рабочему листу для записи действия")
        return
    await asyncio.to_thread(_record_action_sync, worksheet, entry_id, action, action_time, status)
