import logging
import os
import sys
import asyncio
from datetime import datetime
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, Message, ReplyKeyboardMarkup, InputMediaPhoto, InputMediaDocument, BotCommand, BotCommandScopeChat
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    JobQueue,
)
import html
from g_sheets import add_feedback, record_action, set_priority_and_sla, get_open_tickets_for_sla_check, mark_sla_notification_sent
from database import (
    initialize_db, get_all_users, get_user_fio, set_user_fio, 
    get_or_create_user, delete_user, set_user_username, get_user_username,
    set_topic_id, get_all_topic_ids, delete_all_topics
)
from logger import logger

# Логируем версию Python при старте
log = logger.get_logger('main')
log.info(f"Запуск на Python версии: {sys.version}")

# Загрузка переменных окружения
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")
ADMIN_USER_IDS = [int(admin_id) for admin_id in os.getenv("ADMIN_USER_IDS", "").split(',') if admin_id]
SLA_NOTIFICATION_USER_IDS = [int(user_id) for user_id in os.getenv("SLA_NOTIFICATION_USER_IDS", "").split(',') if user_id]

ADMIN_CHAT_ID_STR = os.getenv("ADMIN_CHAT_ID")
ADMIN_CHAT_ID = None
if ADMIN_CHAT_ID_STR:
    try:
        ADMIN_CHAT_ID = int(ADMIN_CHAT_ID_STR)
        log.info(f"Загружен ADMIN_CHAT_ID: {ADMIN_CHAT_ID}")
    except ValueError:
        log.error(f"Неверный ADMIN_CHAT_ID в файле .env: '{ADMIN_CHAT_ID_STR}'. ID должен быть числом.")
else:
    log.warning("Переменная ADMIN_CHAT_ID не установлена в файле .env.")

TOPIC_NAMES = {
    "dashboard": "🕹️ Панель управления",
    "l1_requests": "Линия 1",
    "l2_support": "Линия 2",
    "l3_billing": "Линия 3",
}

# Состояния для ConversationHandler
# Основной диалог
CHOOSING, PLATFORM, FEEDBACK, AWAITING_PHOTO = range(4)
# Состояние для регистрации
REG_AWAITING_FIO = range(4, 5)
# Диалог рассылки
CHOOSING_DIGEST_CONTENT, AWAITING_DIGEST_TEXT, AWAITING_DIGEST_DOCUMENT, CONFIRM_DIGEST = range(5, 9)
# Клавиатура для постоянного меню
persistent_keyboard = [["📝 Создать новое обращение"]]
persistent_markup = ReplyKeyboardMarkup(persistent_keyboard, resize_keyboard=True)

# Клавиатура выбора типа обращения
reply_keyboard = [
    [InlineKeyboardButton("🐛 Отправить баг", callback_data="bug")],
    [InlineKeyboardButton("💡 Отправить предложение", callback_data="feature")],
    [InlineKeyboardButton("🚧 Проблема с доступом", callback_data="access_issue")],
    [InlineKeyboardButton("📞 Получить консультацию", callback_data="consultation")],
]
markup = InlineKeyboardMarkup(reply_keyboard)

# Список площадок
PLATFORMS = [
    "УЛЛЧ",
    "БНКМ",
    "ЮКМ",
    "Анастасьевка",
    "Джелюмкен",
    "Смидович",
    "Кирга",
    "Вернебелое",
    "Новобурейское",
    "Заречное",
    "Усть-Перра",
    "КС6-Хабаровская",
    "КС-Тосненская",
    "ВСК(Волхов-Сегежа)",
    "УКПГ-45"
]

# Создаем клавиатуру для выбора площадки
def get_platform_keyboard():
    keyboard = []
    # Размещаем по 2 кнопки в ряд
    for i in range(0, len(PLATFORMS), 2):
        row = []
        row.append(InlineKeyboardButton(PLATFORMS[i], callback_data=f"platform_{PLATFORMS[i]}"))
        if i + 1 < len(PLATFORMS):
            row.append(InlineKeyboardButton(PLATFORMS[i + 1], callback_data=f"platform_{PLATFORMS[i + 1]}"))
        keyboard.append(row)
    return InlineKeyboardMarkup(keyboard)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Начинает диалог, при необходимости регистрирует пользователя."""
    user = update.message.from_user
    logger.set_context(update)
    log.info(f"Начало работы с пользователем {user.id}")

    fio = await get_or_create_user(user.id) # Проверяем и создаем пользователя за один вызов
    log.debug(f"Получено ФИО пользователя: {fio}")

    if fio:
        logger.set_context(update)
        log.info(f"Пользователь обратился к боту")
        context.user_data["fio"] = fio
        await update.message.reply_text(
            f"👋 С возвращением, {fio}!\n\n"
            "Я помогу вам сообщить о проблеме или предложить идею.\n"
            "Нажмите кнопку '📝 Создать новое обращение' ниже или воспользуйтесь командой /new_ticket.",
            reply_markup=persistent_markup,
        )
        return ConversationHandler.END
    else:
        await update.message.reply_text(
            "👋 Привет!\n"
            "Добро пожаловать в бота поддержки METASFERA Welding.\n\n"
            "Я помогу вам сообщить о проблеме, предложить идею или рассказать о сложностях с доступом. "
            "Чтобы начать, мне нужно узнать ваше ФИО.\n\n"
            "Пожалуйста, введите ваше ФИО:",
        )
        logger.set_context(update)
        log.info(f"Новый пользователь начал регистрацию")
        return REG_AWAITING_FIO

async def register_fio(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Сохраняет ФИО нового пользователя."""
    try:
        user = update.message.from_user
        fio = update.message.text
        await set_user_fio(user.id, fio)
        if user.username:
            await set_user_username(user.id, user.username)
            context.user_data["username"] = user.username
        context.user_data["fio"] = fio

        await update.message.reply_text(
            f"✅ Спасибо, {fio}! Вы успешно зарегистрированы.\n\n"
            "Теперь вы можете отправлять обращения. Для этого нажмите кнопку МЕНЮ и выберите 'Создать новое обращение', либо пропишите команду /new_ticket",
            reply_markup=persistent_markup,
        )
        logger.set_context(update)
        log.info(f"Новый пользователь зарегестрировался")
    except Exception as e:
        logger.set_context(update) 
        log.error(f"Ошибка при регистрации пользователя: {e}")
        await update.message.reply_text(
            "❌ Произошла непредвиденная ошибка при регистрации. Пожалуйста, попробуйте снова позже или свяжитесь с администратором."
        )
    return ConversationHandler.END

async def start_new_ticket(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Начинает новый диалог по кнопке 'Новое обращение' или команде /new_ticket."""
    user_id = update.message.from_user.id
    fio = await get_user_fio(user_id)
    username = update.message.from_user.username
    logger.set_context(update)
    log.info(f"Начало создания нового обращения для пользователя {user_id}")
    
    if username:
        await set_user_username(user_id, username)
        context.user_data["username"] = username
        log.debug(f"Обновлен username пользователя: {username}")
    
    # Эта проверка на случай, если пользователь удалил и снова запустил бота без /start
    if not fio:
        await update.message.reply_text(
            "Мы не смогли найти ваши данные. Пожалуйста, пройдите регистрацию, отправив команду /start."
        )
        logger.set_context(update)
        log.warning(f"Незарегестрированный пользователь попытался создать запрос")
        return ConversationHandler.END # Остаемся в том же состоянии

    context.user_data['fio'] = fio
    
    await update.message.reply_text(
        "💬 Чем можем помочь? Выберите тип обращения:",
        reply_markup=markup
    )
    return CHOOSING

async def button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает нажатие на кнопку и предлагает выбрать площадку."""
    query = update.callback_query
    await query.answer()
    choice = query.data
    logger.set_context(update)
    log.info(f"Пользователь выбрал тип обращения: {choice}")

    if choice == 'bug':
        user_choice_text = "Баг"
    elif choice == 'feature':
        user_choice_text = "Предложение"
    elif choice == 'access_issue':
        user_choice_text = "Проблема с доступом"
    elif choice == 'consultation':
        user_choice_text = "Консультация"
    else:
        user_choice_text = "Обращение"
    context.user_data["choice"] = user_choice_text

    # ФИО уже известно, сразу запрашиваем площадку
    await query.edit_message_text(
        text="Пожалуйста, выберите площадку:",
        reply_markup=get_platform_keyboard()
    )
    
    return PLATFORM

async def handle_platform_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает выбор площадки и запрашивает описание проблемы/предложения."""
    query = update.callback_query
    await query.answer()
    
    # Получаем название площадки из callback_data
    platform = query.data.replace("platform_", "")
    context.user_data["platform"] = platform
    logger.set_context(update)
    log.info(f"Пользователь выбрал площадку: {platform}")
    
    choice = context.user_data["choice"]
    if choice == "Баг":
        feedback_type_text = "опишите баг"
    elif choice == "Предложение":
        feedback_type_text = "опишите ваше предложение"
    elif choice == "Проблема с доступом":
        feedback_type_text = "опишите проблему (например, 'не могу зайти', 'страницы загружаются очень медленно')"
    elif choice == "Консультация":
        feedback_type_text = "опишите, по какому вопросу вам нужна консультация"
    else:
        feedback_type_text = "опишите ваш вопрос"

    await query.edit_message_text(
        f"Выбрана площадка: {platform}\n\n"
        f"Отлично! Теперь, пожалуйста, подробно {feedback_type_text}."
    )
    return FEEDBACK

async def get_feedback_and_ask_for_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Сохраняет текст отзыва и предлагает отправить фото."""
    context.user_data["feedback_text"] = update.message.text
    context.user_data["photo_ids"] = [] # Инициализируем список для ID фото
    context.user_data["albums"] = {} # Для обработки медиагрупп
    logger.set_context(update)
    log.info("Получен текст обращения пользователя")
    
    keyboard = [
        [InlineKeyboardButton("Пропустить (без фото)", callback_data="skip_photo")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "Спасибо. Теперь отправьте одно или несколько фото.\n"
        "После каждой фотографии я буду предлагать завершить создание обращения.\n"
        "Если фото не требуется, нажмите 'Пропустить'.",
        reply_markup=reply_markup
    )
    
    return AWAITING_PHOTO

async def process_album(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправляет одно сообщение после получения всех фото в медиагруппе."""
    job = context.job
    chat_id, media_group_id, user_id = job.data
    
    # Достаем user_data для конкретного пользователя
    user_data = context.application.user_data.get(user_id, {})
    
    if 'albums' not in user_data or media_group_id not in user_data['albums']:
        return # Группа уже обработана или не существует
        
    photos = user_data['albums'][media_group_id]['photos']
    log.info(f"Найдено {len(photos)} фото в альбоме")
    
    # Объединяем фото из альбома с основным списком
    if "photo_ids" not in user_data:
        user_data["photo_ids"] = []
    user_data["photo_ids"].extend(photos)
    log.info(f"Добавлено {len(photos)} фото к основному списку (всего: {len(user_data['photo_ids'])})")
    
    # Очищаем данные по этому альбому
    del user_data['albums'][media_group_id]

    keyboard = [[InlineKeyboardButton("✅ Готово, больше фото нет", callback_data="finish_photos")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await context.bot.send_message(
        chat_id=chat_id,
        text=f"Альбом из {len(photos)} фото добавлен (всего: {len(user_data['photo_ids'])}). Отправьте еще или нажмите 'Готово'.",
        reply_markup=reply_markup
    )

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает получение фото, группируя их в альбомы при необходимости."""
    message = update.message

    # Если фото пришло как часть медиагруппы (альбома)
    if message.media_group_id:

        media_group_id = message.media_group_id
        photo_id = message.photo[-1].file_id
        
        # Инициализируем хранилище альбомов, если его нет
        if 'albums' not in context.user_data:
            context.user_data['albums'] = {}
        
        # Если это первое фото в альбоме, создаем для него запись
        if media_group_id not in context.user_data['albums']:
            context.user_data['albums'][media_group_id] = {
                'photos': [],
                'job_name': f"album_{media_group_id}"
            }
        
        # Добавляем фото в альбом
        context.user_data['albums'][media_group_id]['photos'].append(photo_id)
        
        job_name = context.user_data['albums'][media_group_id]['job_name']
        
        # Удаляем предыдущий таймер, если он был, чтобы сбросить отсчет
        # Это нужно, чтобы дождаться всех фото из альбома
        existing_jobs = context.job_queue.get_jobs_by_name(job_name)
        for job in existing_jobs:
            job.schedule_removal()
            
        # Устанавливаем таймер на 1 секунду. Если за это время придет еще фото,
        # таймер сбросится. Если нет - вызовется process_album.
        context.job_queue.run_once(
            process_album, 
            1, 
            data=(message.chat_id, media_group_id, message.from_user.id), 
            name=job_name
        )

    # Если фото пришло одно (не в альбоме)
    else:
        photo_file_id = message.photo[-1].file_id
        if "photo_ids" not in context.user_data:
            context.user_data["photo_ids"] = []
        context.user_data["photo_ids"].append(photo_file_id)

        keyboard = [[InlineKeyboardButton("✅ Готово, больше фото нет", callback_data="finish_photos")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await message.reply_text(
            f"Фото добавлено (всего: {len(context.user_data['photo_ids'])}). Отправьте следующее или нажмите 'Готово'.",
            reply_markup=reply_markup
        )
    return AWAITING_PHOTO # Остаемся в том же состоянии

async def finish_photos_and_save(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Завершает добавление фото и сохраняет обращение."""
    query = update.callback_query
    await query.answer()
    logger.set_context(update)
    log.info("Пользователь завершил добавление фото")
    # Просто вызываем final_save, который возьмет фото из user_data
    return await final_save(update, context)

async def final_save(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Собирает все данные, создает тикет в виде топика, отправляет уведомления."""
    user_data = context.user_data
    user = update.callback_query.from_user if update.callback_query else update.message.from_user
    logger.set_context(update)
    log.info(f"Завершение создания обращения для пользователя {user.id}")

    # Собираем все данные
    feedback_type = user_data["choice"]
    fio = user_data["fio"]
    platform = user_data["platform"]
    message_text = user_data["feedback_text"]
    photo_ids = user_data.get("photo_ids", [])
    username = user.username or user_data.get("username")
    if username:
        await set_user_username(user.id, username)
    else:
        username = await get_user_username(user.id)

    try:
        # Integration Point: Сохраняем в Google Sheets и получаем ID
        first_photo_id = photo_ids[0] if photo_ids else ""
        new_entry_id = await add_feedback(user.id, feedback_type, fio, username, platform, message_text, first_photo_id)
        
        if new_entry_id is not None:
            entry_id_str = str(new_entry_id)
            reply_text = f"✅ Спасибо! Ваше обращение №{html.escape(entry_id_str)} было успешно создано."
            
            # Integration Point: Устанавливаем SLA
            if feedback_type == "Консультация":
                await set_priority_and_sla(entry_id_str, "Средний")
            
            # --- Новая логика с топиками ---
            if ADMIN_CHAT_ID:
                try:
                    # 1. Создаем новый топик для тикета
                    topic_title = f"[New] Ticket #{entry_id_str} from @{username or fio}"
                    ticket_topic = await context.bot.create_forum_topic(chat_id=ADMIN_CHAT_ID, name=topic_title)
                    ticket_topic_id = ticket_topic.message_thread_id
                    
                    # Сохраняем связь user_id -> ticket_topic_id
                    context.bot_data.setdefault('user_ticket_topics', {})[user.id] = ticket_topic_id
                    context.bot_data.setdefault('topic_ticket_info', {})[ticket_topic_id] = {'user_id': user.id, 'entry_id': entry_id_str, 'fio': fio, 'username': username, 'status': 'new', 'assignee': None, 'topic_id': ticket_topic_id, 'feedback_type': feedback_type}


                    # 2. Отправляем полную информацию в новый топик с кнопками приоритета
                    admin_message_lines = [
                        f"🚨 <b>Новое обращение #{entry_id_str}</b> 🚨", "---",
                        f"👤 <b>От:</b> {html.escape(fio)}" + (f" (@{html.escape(username)})" if username else ""),
                        f"🔧 <b>Тип:</b> {html.escape(feedback_type)}", f"📍 <b>Площадка:</b> {html.escape(platform)}", "---",
                        "<b>Сообщение:</b>", f"{html.escape(message_text)}"
                    ]
                    admin_message = "\n".join(admin_message_lines)
                    
                    priority_keyboard = [
                        InlineKeyboardButton("Критичный", callback_data=f"priority_Критичный_{entry_id_str}_{user.id}_{ticket_topic_id}"),
                        InlineKeyboardButton("Высокий", callback_data=f"priority_Высокий_{entry_id_str}_{user.id}_{ticket_topic_id}")
                    ]
                    priority_keyboard2 = [
                        InlineKeyboardButton("Средний", callback_data=f"priority_Средний_{entry_id_str}_{user.id}_{ticket_topic_id}"),
                        InlineKeyboardButton("Низкий", callback_data=f"priority_Низкий_{entry_id_str}_{user.id}_{ticket_topic_id}")
                    ]
                    admin_buttons_markup = InlineKeyboardMarkup([priority_keyboard, priority_keyboard2])
                    
                    if photo_ids:
                        media = [InputMediaPhoto(media=pid) for pid in photo_ids]
                        if len(media) > 1:
                            await context.bot.send_media_group(chat_id=ADMIN_CHAT_ID, message_thread_id=ticket_topic_id, media=media)
                        else:
                            await context.bot.send_photo(chat_id=ADMIN_CHAT_ID, message_thread_id=ticket_topic_id, photo=photo_ids[0])

                    await context.bot.send_message(
                        chat_id=ADMIN_CHAT_ID, message_thread_id=ticket_topic_id,
                        text=admin_message, parse_mode='HTML', reply_markup=admin_buttons_markup
                    )

                    # 3. Отправляем уведомление в L1 (БЕЗ кнопки "Взять в работу")
                    l1_topic_id = context.bot_data.get("l1_requests_topic_id")
                    if l1_topic_id:
                        log.info(f"Найден ID топика L1: {l1_topic_id}. Отправка уведомления для тикета #{entry_id_str}...")
                        try:
                            chat_link = f"https://t.me/c/{str(ADMIN_CHAT_ID).replace('-100', '')}"
                            ticket_url = f"{chat_link}/{ticket_topic_id}"
                            l1_summary = (f"🆕 <b>Новый тикет #{entry_id_str}</b> от @{username or fio}\n"
                                          f"<b>Тип:</b> {feedback_type}\n"
                                          f"<b>Приоритет:</b> <i>Не установлен</i>\n"
                                          f"<a href='{ticket_url}'>➡️ Перейти к тикету для установки приоритета</a>")
                            
                            l1_message = await context.bot.send_message(
                                chat_id=ADMIN_CHAT_ID, message_thread_id=l1_topic_id,
                                text=l1_summary, parse_mode='HTML', disable_web_page_preview=True
                            )
                            # Сохраняем ID сообщения в L1 для последующего обновления
                            context.bot_data.setdefault('l1_messages', {})[entry_id_str] = l1_message.message_id
                            log.info(f"Уведомление для тикета #{entry_id_str} успешно отправлено в L1 (message_id: {l1_message.message_id}).")
                        except Exception as e:
                            log.error(f"Не удалось отправить сообщение в топик L1 ({l1_topic_id}) для тикета #{entry_id_str}: {e}", exc_info=True)
                    else:
                        log.warning("Не найден 'l1_requests_topic_id' в bot_data. Уведомление в L1 не будет отправлено.")

                    # 4. Обновляем Dashboard
                    await update_dashboard(context.application)

                except Exception as e:
                    log.error(f"Не удалось отправить уведомление в чат админов по новой логике: {e}", exc_info=True)
                    reply_text += "\n\n⚠️ Не удалось уведомить администраторов. Пожалуйста, свяжитесь с ними напрямую."


        else:
            reply_text = "❌ Произошла ошибка при записи вашего сообщения. Пожалуйста, попробуйте еще раз позже."
            log.error("Ошибка при записи в Google Sheets: add_feedback вернул None")
            
    except Exception as e:         
        reply_text = "❌ Произошла ошибка при записи вашего сообщения. Пожалуйста, попробуйте еще раз позже."
        log.error(f"Неожиданная ошибка при сохранении обращения: {e}", exc_info=True)

    # Отвечаем либо на сообщение, либо на колбэк
    if update.callback_query:
        await update.callback_query.edit_message_text(reply_text)
    else:
        await update.message.reply_text(reply_text)
    log.info(f"Обращение завершено для пользователя {user.id}")
    user_data.clear()
    
    return ConversationHandler.END

async def skip_photo_and_save(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Пропускает шаг с фото и сохраняет обращение."""
    logger.set_context(update)
    log.info("Пропуск шага с фото и сохранение обращения")
    await update.callback_query.answer()
    return await final_save(update, context)

async def take_ticket(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает взятие тикета в работу из топика L1."""
    query = update.callback_query
    await query.answer()
    
    admin_user = query.from_user
    admin_identifier = f"@{admin_user.username}" if admin_user.username else admin_user.full_name
    logger.set_context(update)
    log.info(f"Администратор {admin_identifier} берет в работу тикет.")

    try:
        _, ticket_topic_id, entry_id, user_id_str = query.data.split('_')
        ticket_topic_id = int(ticket_topic_id)
        user_id = int(user_id_str)
    except (ValueError, IndexError) as e:
        log.error(f"Ошибка парсинга callback_data для 'take_ticket': {query.data}, {e}")
        await query.message.reply_text("Произошла внутренняя ошибка при обработке запроса.")
        return

    # Обновляем статус тикета
    ticket_info = context.bot_data.get('topic_ticket_info', {}).get(ticket_topic_id)
    if ticket_info:
        ticket_info['status'] = 'in_progress'
        ticket_info['assignee'] = admin_identifier
    else:
        log.warning(f"Не найдена информация для тикета в топике {ticket_topic_id}")
        await query.message.reply_text("Не удалось найти информацию о тикете. Возможно, он был удален.", show_alert=True)
        return
        
    # 1. Удаляем сообщение из L1
    try:
        await query.message.delete()
        log.info(f"Сообщение о тикете #{entry_id} удалено из L1.")
    except Exception as e:
        log.warning(f"Не удалось удалить сообщение из L1 для тикета #{entry_id}: {e}")

    # 2. Переименовываем топик тикета
    try:
        ticket_info = context.bot_data.get('topic_ticket_info', {}).get(ticket_topic_id, {})
        username = ticket_info.get('username', 'user')
        fio = ticket_info.get('fio', '')
        new_topic_name = f"[In Progress - {admin_identifier}] Ticket #{entry_id} from @{username or fio}"
        await context.bot.edit_forum_topic(chat_id=ADMIN_CHAT_ID, message_thread_id=ticket_topic_id, name=new_topic_name)
        log.info(f"Топик для тикета #{entry_id} переименован.")
    except Exception as e:
        log.error(f"Не удалось переименовать топик для тикета #{entry_id}: {e}")

    # 3. Обновляем Dashboard
    await update_dashboard(context.application)

    # 4. Integration Point: Обновляем Google Sheets
    try:
        await record_action(entry_id, 'taken', datetime.now(), status=f"В работе у {admin_identifier}")
        log.info(f"Статус тикета #{entry_id} обновлен в Google Sheets.")
    except Exception as e:
        log.error(f"Не удалось обновить Google Sheets для тикета #{entry_id}: {e}")

    # 5. Уведомляем пользователя
    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=f"⚙️ Ваше обращение №{entry_id} было взято в работу оператором ({admin_identifier})."
        )
        log.info(f"Уведомление отправлено пользователю {user_id} о взятии тикета в работу.")
    except Exception as e:
        log.error(f"Не удалось уведомить пользователя {user_id} о взятии тикета в работу: {e}")

    # 6. Добавляем кнопки управления в топик
    transfer_keyboard = [
        InlineKeyboardButton("2️⃣ На 2 линию", callback_data=f"transfer_l2_{entry_id}_{user_id}_{ticket_topic_id}"),
        InlineKeyboardButton("3️⃣ На 3 линию", callback_data=f"transfer_l3_{entry_id}_{user_id}_{ticket_topic_id}")
    ]
    close_keyboard = [
        InlineKeyboardButton("❌ Закрыть задачу", callback_data=f"close_ticket_{entry_id}_{user_id}_{ticket_topic_id}")
    ]
    control_markup = InlineKeyboardMarkup([transfer_keyboard, close_keyboard])
    await context.bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        message_thread_id=ticket_topic_id,
        text="Панель управления тикетом:",
        reply_markup=control_markup
    )

async def take_escalated_ticket(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает взятие в работу эскалированного тикета из топиков L2/L3."""
    query = update.callback_query
    await query.answer()
    
    admin_user = query.from_user
    admin_identifier = f"@{admin_user.username}" if admin_user.username else admin_user.full_name
    logger.set_context(update)

    try:
        _, _, ticket_topic_id_str, entry_id, user_id_str = query.data.split('_')
        ticket_topic_id = int(ticket_topic_id_str)
        user_id = int(user_id_str)
    except (ValueError, IndexError) as e:
        log.error(f"Ошибка парсинга callback_data для 'take_escalated_ticket': {query.data}, {e}")
        await query.message.reply_text("Произошла внутренняя ошибка при обработке запроса.")
        return
    
    log.info(f"Администратор {admin_identifier} берет в работу эскалированный тикет #{entry_id}.")

    # Обновляем статус тикета
    ticket_info = context.bot_data.get('topic_ticket_info', {}).get(ticket_topic_id)
    if ticket_info:
        ticket_info['status'] = 'in_progress'
        ticket_info['assignee'] = admin_identifier
    else:
        log.warning(f"Не найдена информация для тикета в топике {ticket_topic_id}")
        await query.message.reply_text("Не удалось найти информацию о тикете. Возможно, он был удален.", show_alert=True)
        return

    # Удаляем сообщение из L2/L3
    try:
        await query.message.delete()
        log.info(f"Сообщение об эскалированном тикете #{entry_id} удалено.")
    except Exception as e:
        log.warning(f"Не удалось удалить сообщение об эскалации для тикета #{entry_id}: {e}")

    # Переименовываем топик
    try:
        username = ticket_info.get('username', 'user')
        fio = ticket_info.get('fio', '')
        new_topic_name = f"[In Progress - {admin_identifier}] Ticket #{entry_id} from @{username or fio}"
        await context.bot.edit_forum_topic(chat_id=ADMIN_CHAT_ID, message_thread_id=ticket_topic_id, name=new_topic_name)
        log.info(f"Топик для тикета #{entry_id} переименован.")
    except Exception as e:
        log.error(f"Не удалось переименовать топик для тикета #{entry_id}: {e}")

    # Обновляем Dashboard
    await update_dashboard(context.application)

    # Уведомляем пользователя
    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=f"⚙️ Ваше обращение №{entry_id} было взято в работу оператором L2/L3 ({admin_identifier})."
        )
        log.info(f"Уведомление отправлено пользователю {user_id} о взятии тикета в работу.")
    except Exception as e:
        log.error(f"Не удалось уведомить пользователя {user_id} о взятии тикета в работу: {e}")

    # Добавляем кнопки управления в топик
    transfer_keyboard = [
        InlineKeyboardButton("2️⃣ На 2 линию", callback_data=f"transfer_l2_{entry_id}_{user_id}_{ticket_topic_id}"),
        InlineKeyboardButton("3️⃣ На 3 линию", callback_data=f"transfer_l3_{entry_id}_{user_id}_{ticket_topic_id}")
    ]
    close_keyboard = [
        InlineKeyboardButton("❌ Закрыть задачу", callback_data=f"close_ticket_{entry_id}_{user_id}_{ticket_topic_id}")
    ]
    control_markup = InlineKeyboardMarkup([transfer_keyboard, close_keyboard])
    await context.bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        message_thread_id=ticket_topic_id,
        text="Панель управления тикетом:",
        reply_markup=control_markup
    )

async def transfer_to_line(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает эскалацию тикета на L2 или L3."""
    query = update.callback_query
    await query.answer()

    admin_user = query.from_user
    admin_identifier = f"@{admin_user.username}" if admin_user.username else admin_user.full_name
    logger.set_context(update)
    
    try:
        _, line, entry_id, user_id_str, ticket_topic_id_str = query.data.split('_')
        line_number = line[1:] # l2 -> 2
        user_id = int(user_id_str)
        ticket_topic_id = int(ticket_topic_id_str)
    except (ValueError, IndexError) as e:
        log.error(f"Ошибка парсинга callback_data для 'transfer_to_line': {query.data}, {e}")
        await query.message.reply_text("Произошла внутренняя ошибка.")
        return

    log.info(f"Администратор {admin_identifier} эскалирует тикет #{entry_id} на линию {line_number}")

    # Обновляем статус тикета
    ticket_info = context.bot_data.get('topic_ticket_info', {}).get(ticket_topic_id)
    if ticket_info:
        ticket_info['status'] = f"escalated_l{line_number}"
        # При эскалации убираем назначенного, так как он теперь в общей очереди
        ticket_info['assignee'] = None 
    else:
        log.warning(f"Не найдена информация для тикета в топике {ticket_topic_id} при эскалации")

    # 1. Отправляем уведомление в соответствующий топик (L2/L3)
    target_topic_key = f"l{line_number}_support_topic_id"
    target_topic_id = context.bot_data.get(target_topic_key)
    
    if target_topic_id:
        try:
            chat_link = f"https://t.me/c/{str(ADMIN_CHAT_ID).replace('-100', '')}"
            ticket_url = f"https://t.me/c/{str(ADMIN_CHAT_ID).replace('-100', '')}/{ticket_topic_id}"

            ticket_info = context.bot_data.get('topic_ticket_info', {}).get(ticket_topic_id, {})
            username = ticket_info.get('username', 'user')
            fio = ticket_info.get('fio', '')

            escalation_summary = (
                f"❗️ <b>Эскалация на L{line_number}</b>\n"
                f"Тикет #{entry_id} от @{username or fio}\n"
                f"Передал: {admin_identifier}\n"
                f"<a href='{ticket_url}'>➡️ Перейти к тикету</a>"
            )

            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                message_thread_id=target_topic_id,
                text=escalation_summary,
                parse_mode='HTML',
                disable_web_page_preview=True
            )
            log.info(f"Уведомление об эскалации тикета #{entry_id} отправлено в топик L{line_number}")
        except Exception as e:
            log.error(f"Не удалось отправить уведомление об эскалации в топик L{line_number}: {e}")
    else:
        log.warning(f"Не найден ID топика для L{line_number} ({target_topic_key})")

    # 2. Обновляем Dashboard
    await update_dashboard(context.application)

    # 3. Integration Point: Обновляем Google Sheets
    try:
        status = f"Передано на {line_number} линию"
        await record_action(entry_id, f'transfer_l{line_number}', datetime.now(), status=status)
        log.info(f"Статус тикета #{entry_id} обновлен в Google Sheets на '{status}'.")
    except Exception as e:
        log.error(f"Не удалось обновить Google Sheets для тикета #{entry_id} при эскалации: {e}")
        
    # 4. Уведомляем пользователя
    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=f"⚙️ Ваше обращение №{entry_id} было передано на линию поддержки L{line_number} для дальнейшего рассмотрения."
        )
        log.info(f"Уведомление об эскалации отправлено пользователю {user_id}.")
    except Exception as e:
        log.error(f"Не удалось уведомить пользователя {user_id} об эскалации: {e}")

    # 5. Обновляем сообщение в самом тикете, СОХРАНЯЯ кнопки
    await query.edit_message_text(
        text=query.message.text + f"\n\n---\n➡️ <b>Тикет эскалирован на L{line_number}</b> администратором {admin_identifier}.",
        reply_markup=query.message.reply_markup # Сохраняем исходные кнопки
    )

async def set_priority(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает нажатие кнопок приоритета, что равносильно взятию тикета в работу."""
    query = update.callback_query
    await query.answer()
    
    admin_user = query.from_user
    admin_identifier = f"@{admin_user.username}" if admin_user.username else admin_user.full_name
 
    try:
        _prefix, priority, entry_id, user_id_str, ticket_topic_id_str = query.data.split('_')
        user_id = int(user_id_str)
        ticket_topic_id = int(ticket_topic_id_str)
    except (ValueError, IndexError) as e:
        log.error(f"Ошибка парсинга callback_data для 'set_priority': {query.data}, {e}")
        await query.message.reply_text("Произошла внутренняя ошибка при обработке запроса.")
        return
 
    log.info(f"Администратор {admin_identifier} устанавливает приоритет '{priority}' и берет в работу тикет #{entry_id}")
 
    # 1. Проверяем, не взят ли тикет уже в работу
    ticket_info = context.bot_data.get('topic_ticket_info', {}).get(ticket_topic_id)
    if not ticket_info:
        log.warning(f"Не найдена информация для тикета в топике {ticket_topic_id} при установке приоритета.")
        await query.edit_message_text("Не удалось найти данные по этому тикету. Возможно, он уже обработан.", reply_markup=None)
        return
    
    if ticket_info.get('status') == 'in_progress':
        await query.answer(f"Тикет уже в работе у {ticket_info.get('assignee', 'другого оператора')}.", show_alert=True)
        return
         
    # 2. Обновляем данные тикета в bot_data
    ticket_info['status'] = 'in_progress'
    ticket_info['assignee'] = admin_identifier
    ticket_info['priority'] = priority
 
    # 3. Обновляем Google Sheets (приоритет и статус/ответственный)
    try:
        await set_priority_and_sla(entry_id, priority)
        await record_action(entry_id, 'taken', datetime.now(), status=f"В работе у {admin_identifier}")
        log.info(f"Google Sheets обновлен для тикета #{entry_id}: приоритет={priority}, ответственный={admin_identifier}")
    except Exception as e:
        log.error(f"Не удалось обновить Google Sheets для тикета #{entry_id}: {e}")
        await query.message.reply_text(f"⚠️ Ошибка обновления Google Sheets: {e}")
        # Не прерываем, чтобы остальная логика работала
 
    # 4. Переименовываем топик
    try:
        username = ticket_info.get('username', 'user')
        fio = ticket_info.get('fio', '')
        new_topic_name = f"[In Progress - {admin_identifier}] Ticket #{entry_id} from @{username or fio}"
        await context.bot.edit_forum_topic(chat_id=ADMIN_CHAT_ID, message_thread_id=ticket_topic_id, name=new_topic_name)
    except Exception as e:
        log.error(f"Не удалось переименовать топик {ticket_topic_id}: {e}")
 
    # 5. Удаляем уведомление из L1
    l1_message_id = context.bot_data.get('l1_messages', {}).pop(entry_id, None)
    l1_topic_id = context.bot_data.get("l1_requests_topic_id")
    if l1_message_id and l1_topic_id:
        try:
            await context.bot.delete_message(chat_id=ADMIN_CHAT_ID, message_id=l1_message_id)
            log.info(f"Уведомление для тикета #{entry_id} удалено из L1.")
        except Exception as e:
            log.warning(f"Не удалось удалить уведомление из L1 для тикета #{entry_id}: {e}")
    else:
        log.warning(f"Не найден ID сообщения L1 для тикета #{entry_id}, не могу удалить.")
 
    # 6. Уведомляем пользователя
    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=f"⚙️ Ваше обращение №{entry_id} было взято в работу оператором ({admin_identifier})."
        )
    except Exception as e:
        log.error(f"Не удалось уведомить пользователя {user_id}: {e}")
 
    # 7. Обновляем Dashboard
    await update_dashboard(context.application)
 
    # 8. Редактируем исходное сообщение в топике тикета
    try:
        original_text = query.message.text
        new_text = original_text + f"\n\n---\n⭐️ <b>Приоритет:</b> {html.escape(priority)}\n" \
                        f"⭐️ <b>Ответственный:</b> {html.escape(admin_identifier)}\n"
        await query.edit_message_text(text=new_text, parse_mode='HTML', reply_markup=None)
    except Exception as e:
        log.error(f"Не удалось отредактировать сообщение в топике тикета {ticket_topic_id}: {e}")
 
    # 9. Добавляем кнопки управления в топик
    transfer_keyboard = [
        InlineKeyboardButton("2️⃣ На 2 линию", callback_data=f"transfer_l2_{entry_id}_{user_id}_{ticket_topic_id}"),
        InlineKeyboardButton("3️⃣ На 3 линию", callback_data=f"transfer_l3_{entry_id}_{user_id}_{ticket_topic_id}")
    ]
    close_keyboard = [
        InlineKeyboardButton("❌ Закрыть задачу", callback_data=f"close_ticket_{entry_id}_{user_id}_{ticket_topic_id}")
    ]
    control_markup = InlineKeyboardMarkup([transfer_keyboard, close_keyboard])
    await context.bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        message_thread_id=ticket_topic_id,
        text="Панель управления тикетом:",
        reply_markup=control_markup
    )

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отменяет текущий диалог."""
    logger.set_context(update)
    log.info("Отмена текущего диалога")
    await update.message.reply_text("Действие отменено.")
    context.user_data.clear()
    return ConversationHandler.END

# Создание рассылки дайджеста

DIGEST_CHOOSE = [
    [InlineKeyboardButton("Добавить текст ✍", callback_data="add_text")],
    [InlineKeyboardButton("Добавить фото 📷", callback_data="add_photo")],
    [InlineKeyboardButton("Добавить документы 📂", callback_data="add_document")],
    [InlineKeyboardButton("Предпросмотр и рассылка 👀", callback_data="preview_and_send")]
]

async def start_digest(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Начинает диалог создания дайджеста."""
    user_id = update.message.from_user.id
    logger.set_context(update) 
     
    if user_id not in ADMIN_USER_IDS:
        log.info("Пользователь попытался начать создание дайджеста не имея прав на это.")
        await update.message.reply_text("У вас нет прав для выполнения этой команды.")
        return ConversationHandler.END

    log.info("Пользователь начал создание дайджеста.")

    if 'digest_content_text' not in context.user_data:
        context.user_data['digest_content_text'] = []
    if 'digest_content_photos' not in context.user_data:
        context.user_data['digest_content_photos'] = []
    if 'digest_content_documents' not in context.user_data:
        context.user_data['digest_content_documents'] = []

    reply_markup = InlineKeyboardMarkup(DIGEST_CHOOSE)

    await update.message.reply_text(
        "Что вы хотите добавить в дайджест?",
        reply_markup=reply_markup
    )
    return CHOOSING_DIGEST_CONTENT

async def choose_content_type(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """ Обрабатывает выбор админа текст/фото/документ/предпросмотр """
    query = update.callback_query
    logger.set_context(update)
    if query is None:
        log.warning("choose_content_type вызван без CallbackQuery объекта.")
        return ConversationHandler.END
    
    await query.answer()

    log.info("Пользователь выбрал: %s", query.data)

    if query.data == "add_text":
        await query.edit_message_text("Отправьте текст для дайджеста.")
        return AWAITING_DIGEST_TEXT
    elif query.data == "add_photo":
        done_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Готово, больше фото нет", callback_data="finish_photos")]])
        await query.edit_message_text(
            "Отправьте фото для дайджеста. Вы можете отправить несколько фото или альбом. Когда закончите, нажмите 'Готово'.",
            reply_markup=done_keyboard
        )
        return AWAITING_PHOTO    
    elif query.data == "add_document":
        done_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Готово, больше документов нет", callback_data="finish_documents")]])
        await query.edit_message_text(
            "Отправьте документы для дайджеста. Когда закончите, нажмите 'Готово'.",
            reply_markup=done_keyboard
        )
        return AWAITING_DIGEST_DOCUMENT
    elif query.data == "preview_and_send":
        await query.edit_message_text("Подготовка предпросмотра...")
        return await show_digest_preview(update, context)
    
    logger.set_context(update)
    
    log.warning("Неизвестный callback_data: %s от пользователя", query.data )
    await query.edit_message_text("Неизвестный выбор. Диалог завершен.")
    return ConversationHandler.END

async def receive_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Сохраняет полученный текст и возвращает к выбору контента."""
    logger.set_context(update)
    
    if update.message.text is None or update.message.text =="":
        log.warning("receive_text вызван без текстового сообщения." )
        await update.message.reply_text("Пожалуйста, отправьте именно текст.")
        return AWAITING_DIGEST_TEXT
    
    text_content = update.message.text
    context.user_data['digest_content_text'].append(text_content)
    log.info("Пользователь добавил текст в дайджест." )

    reply_markup = InlineKeyboardMarkup(DIGEST_CHOOSE)

    await update.message.reply_text(
        "Текст добавлен в дайджест. Что вы хотите добавить ещё?",
        reply_markup=reply_markup
    )
    return CHOOSING_DIGEST_CONTENT

async def digest_media_save(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Завершает процесс добавления фото, сохраняет их и возвращает к выбору контента."""
    query = update.callback_query
    logger.set_context(update)
    
    if query is None:
        log.warning("Вызван без CallbackQuery объекта." )
        return ConversationHandler.END

    await query.answer()

    raw_photo_ids = context.user_data.pop('photo_ids', [])
    if 'digest_content_photos' not in context.user_data:
        context.user_data['digest_content_photos'] = []

    if not raw_photo_ids:
        await query.edit_message_text("Вы не добавили ни одного фото. Что вы хотите добавить ещё?")
        log.info("Пользователь не добавил ни одного фото в дайджест.")
    else:
        for file_id_str in raw_photo_ids:
            context.user_data['digest_content_photos'].append({'file_id': file_id_str})
        
        log.info("Пользователь добавил %d фото в дайджест.", len(raw_photo_ids) )
        await query.edit_message_text(f"{len(raw_photo_ids)} фото добавлено в дайджест. Что вы хотите добавить ещё?")

    reply_markup = InlineKeyboardMarkup(DIGEST_CHOOSE)
    await context.bot.send_message(query.message.chat.id, "Выберите действие:", reply_markup=reply_markup)
    return CHOOSING_DIGEST_CONTENT

async def process_document_album(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправляет одно сообщение после получения всех документов в медиагруппе."""
    job = context.job
    chat_id, media_group_id, user_id = job.data
    user_data = context.application.user_data.get(user_id, {})
    
    if 'document_albums' not in user_data or media_group_id not in user_data['document_albums']:
        return    
    documents_in_album = user_data['document_albums'][media_group_id]['documents']
    
    if "document_ids" not in user_data:
        user_data["document_ids"] = []

    user_data["document_ids"].extend(documents_in_album)
    del user_data['document_albums'][media_group_id]
    log.debug(f"Добавлено {len(documents_in_album)} документов из альбома {media_group_id}")

    keyboard = [[InlineKeyboardButton("✅ Готово, больше документов нет", callback_data="finish_documents")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await context.bot.send_message(
        chat_id=chat_id,
        text=f"Альбом из {len(documents_in_album)} документов добавлен (всего: {len(user_data['document_ids'])}). Отправьте еще или нажмите 'Готово'.",
        reply_markup=reply_markup
    )

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает получение документа, группируя их в альбомы при необходимости."""
    message = update.message

    if not message.document:
        log.warning("Получено сообщение без документа")
        await message.reply_text("Это не документ. Пожалуйста, отправьте документ или нажмите 'Готово'.")
        return AWAITING_DIGEST_DOCUMENT # Остаемся в текущем состоянии или возвращаемся к выбору

    file_id = message.document.file_id
    document_info = {'file_id': file_id}

    # Если документ пришел как часть медиагруппы (альбома)
    if message.media_group_id:
        media_group_id = message.media_group_id
        if 'document_albums' not in context.user_data:
            context.user_data['document_albums'] = {}
        if media_group_id not in context.user_data['document_albums']:
            context.user_data['document_albums'][media_group_id] = {
                'documents': [],
                'job_name': f"document_album_{media_group_id}"
            }
            log.info(f"Создан новый альбом документов {media_group_id}")
            
        context.user_data['document_albums'][media_group_id]['documents'].append(document_info)
        job_name = context.user_data['document_albums'][media_group_id]['job_name']
        # Удаляем предыдущий таймер, если он был, чтобы сбросить отсчет
        existing_jobs = context.job_queue.get_jobs_by_name(job_name)
        for job in existing_jobs:
            job.schedule_removal()
            
        # Устанавливаем таймер на 1 секунду. Если за это время придет еще документ, таймер сбросится. Если нет - вызовется process_document_album.
        context.job_queue.run_once(
            process_document_album, 
            1, 
            data=(message.chat_id, media_group_id, message.from_user.id), 
            name=job_name
        )

    # Если документ пришел один (не в альбоме)
    else:
        log.info("Документ получен отдельно (не в альбоме)")
        if "document_ids" not in context.user_data:
            context.user_data["document_ids"] = []
        context.user_data["document_ids"].append(document_info)
        log.info(f"Добавлен отдельный документ. Всего документов: {len(context.user_data['document_ids'])}")

        keyboard = [[InlineKeyboardButton("✅ Готово, больше документов нет", callback_data="finish_documents")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await message.reply_text(
            f"Документ добавлен (всего: {len(context.user_data['document_ids'])}). Отправьте следующий или нажмите 'Готово'.",
            reply_markup=reply_markup
        )
    return AWAITING_DIGEST_DOCUMENT

async def digest_document_save(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Завершает процесс добавления документов, сохраняет их и возвращает к выбору контента."""
    query = update.callback_query
    logger.set_context(update)
    
    if query is None:
        log.warning("Вызван без CallbackQuery объекта." )
        return ConversationHandler.END

    await query.answer()

    documents_to_add = context.user_data.pop('document_ids', [])
    log.info(f"Получено {len(documents_to_add)} документов для добавления")

    if 'digest_content_documents' not in context.user_data:
        context.user_data['digest_content_documents'] = []

    if not documents_to_add:
        await query.edit_message_text("Вы не добавили ни одного документа. Что вы хотите добавить ещё?")
    else:
        context.user_data['digest_content_documents'].extend(documents_to_add)
        logger.set_context(update)
        log.info("Пользователь добавил %d документов.", len(documents_to_add) )
        await query.edit_message_text(f"{len(documents_to_add)} документов добавлено в дайджест. Что вы хотите добавить ещё?")

    # Возвращаемся к выбору контента
    reply_markup = InlineKeyboardMarkup(DIGEST_CHOOSE)
    await context.bot.send_message(query.message.chat.id, "Выберите действие:", reply_markup=reply_markup)
    return CHOOSING_DIGEST_CONTENT

async def show_digest_preview(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """ Показывает предпросмотр дайджеста, группируя по типам контента. """
    text_items = context.user_data.get('digest_content_text', [])
    photo_items = context.user_data.get('digest_content_photos', [])
    document_items = context.user_data.get('digest_content_documents', [])

    admin_chat_id = update.effective_chat.id
    logger.set_context(update)
    log.info("Пользователь запросил предпросмотр дайджеста." )

    if not text_items and not photo_items and not document_items:
        await context.bot.send_message(admin_chat_id, "Дайджест пуст. Добавьте контент перед предпросмотром.")
        # Возвращаемся к выбору контента
        reply_markup = InlineKeyboardMarkup(DIGEST_CHOOSE)
        await context.bot.send_message(admin_chat_id, "Что вы хотите добавить в дайджест?", reply_markup=reply_markup)
        return CHOOSING_DIGEST_CONTENT

    await context.bot.send_message(admin_chat_id, "Вот как будет выглядеть дайджест:")

    # 1. Отправляем весь текст
    if text_items:
        full_text_content = "\n\n".join(text_items)
        # Разделяем на части, если текст слишком длинный для одного сообщения Telegram
        text_chunks = [full_text_content[i:i + 4096] for i in range(0, len(full_text_content), 4096)]
        log.info(f"Текст разбит на {len(text_chunks)} частей для отправки. Начало отправки")
        for chunk in text_chunks:
            try:
                await context.bot.send_message(admin_chat_id, chunk)
                await asyncio.sleep(0.01)
            except Exception as e:
                log.error("Ошибка при отправке текстового блока предпросмотра пользователю: %s", e )
                await context.bot.send_message(admin_chat_id, f"Ошибка при показе текстового блока предпросмотра: {e}")

    # 2. Отправляем все фотографии медиагруппами
    if photo_items:
        current_media_group = []
        log.info(f"Начало отправки {len(photo_items)} фото")
        for i, photo_item in enumerate(photo_items):
            current_media_group.append(InputMediaPhoto(media=photo_item['file_id']))
            if (len(current_media_group) == 10) or (i == len(photo_items) - 1):
                try:
                    await context.bot.send_media_group(admin_chat_id, current_media_group, read_timeout=20, write_timeout=20)
                    await asyncio.sleep(0.01)
                except Exception as e:
                    log.error("Ошибка при отправке фото медиагруппы предпросмотра пользователю: %s", e )
                    await context.bot.send_message(admin_chat_id, f"Ошибка при показе фото медиагруппы предпросмотра: {e}")
                current_media_group = []

    # 3. Отправляем все документы медиагруппами
    if document_items:
        current_document_group = []
        log.info(f"Начало отправки {len(document_items)} документов")
        for i, doc_item in enumerate(document_items):
            current_document_group.append(InputMediaDocument(media=doc_item['file_id']))
            if len(current_document_group) == 10 or (i == len(document_items) - 1):
                try:
                    await context.bot.send_media_group(admin_chat_id, current_document_group, read_timeout=20, write_timeout=20)
                    await asyncio.sleep(0.01)
                except Exception as e:
                    log.error("Ошибка при отправке документов медиагруппы предпросмотра пользователю: %s", e )
                    await context.bot.send_message(admin_chat_id, f"Ошибка при показе документов медиагруппы предпросмотра: {e}")
                current_document_group = []

    all_users = await get_all_users()
    user_count = len([user_id for user_id in all_users if user_id not in ADMIN_USER_IDS])
    log.info(f"Дайджест будет отправлен {user_count} пользователям")

    keyboard = [
        [InlineKeyboardButton("✅ Да, отправить", callback_data="confirm_broadcast")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_broadcast")],
        [InlineKeyboardButton("❗ Дополнить дайджест ❗", callback_data="add_something_to_digest")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await context.bot.send_message(
        admin_chat_id,
        f"Дайджест будет отправлен {user_count} пользователям. Подтверждаете рассылку?",
        reply_markup=reply_markup
    )
    return CONFIRM_DIGEST

async def handle_broadcast_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает подтверждение или отмену рассылки после предпросмотра."""
    query = update.callback_query
    logger.set_context(update)
    log.info("handle_broadcast_conversation вызван. query.data: %s", query.data )

    if query is None:
        log.warning("handle_broadcast_confirmation вызван без CallbackQuery объекта." )
        return ConversationHandler.END

    await query.answer()

    if query.data == "confirm_broadcast":
        log.info("Пользователь подтвердил рассылку дайджеста")
        await query.edit_message_text("Начинаю рассылку дайджеста...")
        await send_final_digest(update, context)
        return ConversationHandler.END
    elif query.data == "add_something_to_digest":
        log.info("Пользователь решил дополнить дайджест")
        await query.edit_message_text("Рассылка приостановлена. Вы можете добавить ещё контент.")
        reply_markup = InlineKeyboardMarkup(DIGEST_CHOOSE)
        await context.bot.send_message(query.message.chat.id, "Что вы хотите добавить в дайджест?", reply_markup=reply_markup)
        return CHOOSING_DIGEST_CONTENT
    elif query.data == "cancel_broadcast":
        log.info("Пользователь отменил рассылку дайджеста")
        await query.edit_message_text("Рассылка отменена.")
        await cancel_digest_creation(update, context)
        return ConversationHandler.END

async def send_final_digest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отправляет собранный дайджест всем пользователям, группируя фото и документы в альбомы."""

    text_items = context.user_data.get('digest_content_text', [])
    photo_items = context.user_data.get('digest_content_photos', [])
    document_items = context.user_data.get('digest_content_documents', [])

    admin_chat_id = update.effective_chat.id
    log.info(f"Начало финальной рассылки дайджеста. Текст: {len(text_items)}, фото: {len(photo_items)}, документы: {len(document_items)}")

    if not text_items and not photo_items and not document_items:
        log.warning("Попытка отправить пустой дайджест." )
        await context.bot.send_message(admin_chat_id, "Дайджест пуст. Ничего не отправлено.")
        return

    all_users = await get_all_users()
    users_to_broadcast = [user_id for user_id in all_users if user_id not in ADMIN_USER_IDS]
    log.info(f"Всего пользователей для рассылки: {len(users_to_broadcast)}")

    success_count = 0
    fail_count = 0
    total_users_to_send = len(users_to_broadcast)

    await context.bot.send_message(admin_chat_id, f"Начинаю рассылку дайджеста для {total_users_to_send} пользователей...")

    for user_id in users_to_broadcast:
        user_send_successful = True
        try:
            # 1. Отправляем весь текст
            if text_items:
                full_text_content = "\n\n".join(text_items)
                text_chunks = [full_text_content[i:i + 4096] for i in range(0, len(full_text_content), 4096)]
                for chunk in text_chunks:
                    try:
                        await context.bot.send_message(user_id, chunk)
                        await asyncio.sleep(0.05)
                    except Exception as e:
                        log.error("Ошибка при отправке текстового блока пользователю %s: %s", user_id, e )
                        user_send_successful = False
                        break
                await asyncio.sleep(0.1)

            # 2. Отправляем все фотографии медиагруппами
            if photo_items and user_send_successful: # Продолжаем, только если не было ошибок ранее для этого пользователя
                current_media_group = []
                for i, photo_item in enumerate(photo_items):
                    current_media_group.append(InputMediaPhoto(media=photo_item['file_id']))
                    if (len(current_media_group) == 10) or (i == len(photo_items) - 1):
                        try:
                            await context.bot.send_media_group(user_id, current_media_group, read_timeout=20, write_timeout=20)
                            await asyncio.sleep(0.05)
                        except Exception as e:
                            log.error("Ошибка при отправке фото пользователю %s: %s", user_id, e )
                            user_send_successful = False
                            break
                        current_media_group = []
                await asyncio.sleep(0.1)

            # 3. Отправляем все документы медиагруппами
            if document_items and user_send_successful: 
                current_document_group = []
                for i, doc_item in enumerate(document_items):
                    current_document_group.append(InputMediaDocument(media=doc_item['file_id']))
                    if len(current_document_group) == 10 or (i == len(document_items) - 1):
                        try:
                            await context.bot.send_media_group(user_id, current_document_group, read_timeout=20, write_timeout=20)
                            await asyncio.sleep(0.05)
                        except Exception as e:
                            log.error("Ошибка при отправке документов пользователю %s: %s", user_id, e )
                            user_send_successful = False
                            break
                        current_document_group = []
                await asyncio.sleep(0.1)

        except Exception as e:
            log.error("Общая ошибка при отправке дайджеста пользователю %s: %s", user_id, e )
            user_send_successful = False
            
        if user_send_successful:
            success_count += 1
        else:
            fail_count += 1
            log.warning(f"Не удалось отправить дайджест пользователю {user_id}")
            
        await asyncio.sleep(0.2) # Общая задержка между отправками разным пользователям

    await context.bot.send_message(
        admin_chat_id,
        f"Рассылка дайджеста завершена!\n"
        f"Успешно отправлено {success_count} пользователям.\n"
        f"Не удалось отправить {fail_count} пользователям."
    )
    log.info("Рассылка дайджеста завершена. Успешно: %d, Ошибок: %d.", success_count, fail_count )

    # Очищаем данные дайджеста после рассылки
    if 'digest_content_text' in context.user_data:
        del context.user_data['digest_content_text']
    if 'digest_content_photos' in context.user_data:
        del context.user_data['digest_content_photos']
    if 'digest_content_documents' in context.user_data:
        del context.user_data['digest_content_documents']
    return ConversationHandler.END

async def cancel_digest_creation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отменяет создание дайджеста и очищает временные данные."""
    if update.message:
        await update.message.reply_text("Создание дайджеста отменено.")
    elif update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text("Создание дайджеста отменено.")
    
    logger.set_context(update)
    log.info("Пользователь отменил создание дайджеста." )
    
    if 'digest_content_text' in context.user_data:
        del context.user_data['digest_content_text']
    if 'digest_content_photos' in context.user_data:
        del context.user_data['digest_content_photos']
    if 'digest_content_documents' in context.user_data:
        del context.user_data['digest_content_documents']
    return ConversationHandler.END

async def get_photo_by_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправляет фото по его file_id."""
    logger.set_context(update)
    user_id = update.message.from_user.id
    log.info(f"Запрос фото по ID от пользователя {user_id}")
    if user_id not in ADMIN_USER_IDS:
        log.warning(f"Пользователь {user_id} без прав пытается получить фото")
        await update.message.reply_text("У вас нет прав для выполнения этой команды.")
        return

    if not context.args:
        log.warning("Не указан ID фото")
        await update.message.reply_text("Пожалуйста, укажите ID фото. Пример: /get_photo <photo_id>")
        return

    photo_id = context.args[0]

    try:
        await context.bot.send_photo(chat_id=user_id, photo=photo_id)
        log.info(f"Фото с ID {photo_id} успешно отправлено")
    except Exception as e:
        
        log.error(f"Ошибка при отправке фото по ID {photo_id}: {e}" )
        await update.message.reply_text("Не удалось найти или отправить фото. Убедитесь, что ID корректен.")

async def unhandled_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает текст от пользователя, направляя его в активный топик тикета."""
    user = update.message.from_user
    user_id = user.id
    logger.set_context(update)

    # Проверяем, есть ли у пользователя активный тикет
    active_ticket_topic_id = context.bot_data.get('user_ticket_topics', {}).get(user_id)
    
    if active_ticket_topic_id and ADMIN_CHAT_ID:
        log.info(f"Получено сообщение от пользователя {user_id} для активного тикета в топике {active_ticket_topic_id}")
        try:
            # Пересылаем сообщение в соответствующий топик
            await context.bot.forward_message(
                chat_id=ADMIN_CHAT_ID,
                from_chat_id=user_id,
                message_id=update.message.message_id,
                message_thread_id=active_ticket_topic_id
            )
            # await update.message.reply_text("✅ Ваше сообщение доставлено оператору.")
        except Exception as e:
            log.error(f"Не удалось переслать сообщение от пользователя {user_id} в топик {active_ticket_topic_id}: {e}")
            await update.message.reply_text("❌ Произошла ошибка при отправке вашего сообщения. Пожалуйста, попробуйте еще раз.")
        return

    # Если активного тикета нет, предлагаем создать новый
    log.info(f"Получено сообщение от пользователя {user_id} без активного тикета.")
    await update.message.reply_text(
        "Для начала общения, пожалуйста, создайте новое обращение.",
        reply_markup=persistent_markup
    )

async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает неизвестные команды."""
    await update.message.reply_text(
        "🤷‍♂️ Неизвестная команда. Я не знаю, что с этим делать.\n\n"
        "Используйте команду /start, чтобы начать заново, или кнопку '📝 Создать новое обращение'."
    )

async def close_ticket(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает закрытие тикета, архивирует топик и обновляет все системы."""
    query = update.callback_query
    await query.answer()

    admin_user = query.from_user
    admin_identifier = f"@{admin_user.username}" if admin_user.username else admin_user.full_name
    logger.set_context(update)

    try:
        _, _, entry_id, user_id_str, ticket_topic_id_str = query.data.split('_')
        user_id = int(user_id_str)
        ticket_topic_id = int(ticket_topic_id_str)
    except (ValueError, IndexError) as e:
        log.error(f"Ошибка парсинга callback_data для 'close_ticket': {query.data}, {e}")
        await query.message.reply_text("Произошла внутренняя ошибка.")
        return

    log.info(f"Администратор {admin_identifier} закрывает тикет #{entry_id}")

    # 1. Integration Point: Обновляем Google Sheets и останавливаем SLA
    try:
        await record_action(entry_id, 'closed', datetime.now(), status="Завершено")
        log.info(f"Статус тикета #{entry_id} обновлен на 'Завершено' в Google Sheets.")
    except Exception as e:
        log.error(f"Не удалось обновить Google Sheets для тикета #{entry_id} при закрытии: {e}")
        await query.message.reply_text(f"⚠️ Ошибка при обновлении Google Sheets: {e}")
        # Не продолжаем, если не смогли записать в GS
        return

    # 2. Удаляем сообщение из Dashboard
    dashboard_message_id = context.bot_data.get('dashboard_messages', {}).get(entry_id)
    if dashboard_message_id:
        try:
            await context.bot.delete_message(chat_id=ADMIN_CHAT_ID, message_id=dashboard_message_id)
            log.info(f"Сообщение о тикете #{entry_id} удалено из дашборда.")
        except Exception as e:
            log.warning(f"Не удалось удалить сообщение ({dashboard_message_id}) из дашборда для тикета #{entry_id}: {e}")
    
    # 3. Уведомляем пользователя
    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=f"✅ Ваше обращение №{entry_id} было закрыто. Если у вас возникнут новые вопросы, пожалуйста, создайте новое обращение."
        )
        log.info(f"Уведомление о закрытии отправлено пользователю {user_id}.")
    except Exception as e:
        log.error(f"Не удалось уведомить пользователя {user_id} о закрытии: {e}")

    # 4. Архивируем топик
    try:
        original_topic_info = context.bot_data.get('topic_ticket_info', {}).get(ticket_topic_id)
        original_name = f"Ticket #{entry_id} from @{original_topic_info.get('username', 'user')}" # Формируем базовое имя
        closed_topic_name = f"[Closed] {original_name}"
        
        await context.bot.edit_forum_topic(
            chat_id=ADMIN_CHAT_ID,
            message_thread_id=ticket_topic_id,
            name=closed_topic_name
        )
        # Отправляем финальное сообщение перед закрытием
        await context.bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            message_thread_id=ticket_topic_id,
            text=f"---\n✅ Тикет закрыт администратором {admin_identifier}."
        )
        await context.bot.close_forum_topic(chat_id=ADMIN_CHAT_ID, message_thread_id=ticket_topic_id)
        log.info(f"Топик {ticket_topic_id} для тикета #{entry_id} закрыт и архивирован.")
    except Exception as e:
        log.error(f"Не удалось закрыть/архивировать топик {ticket_topic_id}: {e}")
        
    # 5. Очищаем все связанные данные
    context.bot_data.get('user_ticket_topics', {}).pop(user_id, None)
    context.bot_data.get('topic_ticket_info', {}).pop(ticket_topic_id, None)
    context.bot_data.get('dashboard_messages', {}).pop(entry_id, None)
    log.info(f"Временные данные для тикета #{entry_id} очищены.")
    
    # Удаляем кнопки из сообщения
    await query.message.delete()

async def handle_admin_reply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает сообщения администраторов в топиках тикетов и пересылает их пользователям."""
    logger.set_context(update)
    message = update.message
    
    if not message or not message.is_topic_message or not ADMIN_CHAT_ID or message.chat_id != int(ADMIN_CHAT_ID):
        return

    thread_id = message.message_thread_id
    
    # Игнорируем сообщения в системных топиках
    system_topic_ids = {
        context.bot_data.get("dashboard_topic_id"),
        context.bot_data.get("l1_requests_topic_id"),
        context.bot_data.get("l2_support_topic_id"),
        context.bot_data.get("l3_billing_topic_id"),
    }
    if thread_id in system_topic_ids:
        return # Это системный топик, не для пересылки

    ticket_info = context.bot_data.get('topic_ticket_info', {}).get(thread_id)
    
    if ticket_info:
        user_id = ticket_info.get('user_id')
        entry_id = ticket_info.get('entry_id')
        
        if not user_id:
            log.warning(f"Не найден user_id для топика {thread_id}")
            return

        try:
            # Пересылаем сообщение пользователя
            # Копируем, чтобы выглядело как прямое сообщение от бота, а не форвард
            await context.bot.copy_message(
                chat_id=user_id,
                from_chat_id=ADMIN_CHAT_ID,
                message_id=message.message_id
            )
            log.info(f"Сообщение от администратора отправлено пользователю {user_id} для тикета #{entry_id}")
            # Можно добавить тихое подтверждение в топик, что сообщение доставлено
            # await message.reply_text("✅ Отправлено пользователю.", quote=False)
        except Exception as e:
            log.error(f"Не удалось отправить сообщение пользователю {user_id}: {e}", exc_info=True)
            try:
                await message.reply_text(f"❌ Не удалось доставить сообщение пользователю. Ошибка: {e}", quote=True)
            except Exception as e_reply:
                log.error(f"Не удалось даже отправить сообщение об ошибке в чат: {e_reply}")

async def check_sla_breaches(context: ContextTypes.DEFAULT_TYPE):
    """Проверяет просроченные заявки и отправляет уведомления."""
    logger.set_context()
    log.info("Запуск проверки SLA..." )
    breached_tickets = []
    open_tickets = await get_open_tickets_for_sla_check()
    now = datetime.now()

    for ticket in open_tickets:
        try:
            sla_time_str = ticket.get("SLA (Время на решение)")
            if sla_time_str:
                sla_time = datetime.strptime(sla_time_str, "%H:%M:%S %d.%m.%Y")
                if now > sla_time:
                    breached_tickets.append(ticket)
        except (ValueError, TypeError) as e:
            logger.set_context()
            log.error(f"Ошибка парсинга времени SLA для заявки #{ticket.get('Номер')}: {e}" )
            continue

    if not breached_tickets:
        log.info("Просроченных заявок не найдено." )
        return
    
    if breached_tickets:
        log.info(f"Количество просроченных заявок:{len(breached_tickets)}")
    for ticket in breached_tickets:
        entry_id = ticket.get('Номер')
        logger.set_context()
        log.warning(f"Обнаружена просроченная заявка #{entry_id}" )
        
        # Формируем сообщение
        message_lines = [
            f"❗️ <b>Просрочена заявка!</b> ❗️",
            f"\n<b>Номер обращения:</b> {html.escape(str(entry_id))}",
            f"<b>От:</b> {html.escape(ticket.get('ФИО'))}",
            f"<b>Приоритет:</b> {html.escape(ticket.get('Приоритет'))}",
            f"<b>Статус:</b> {html.escape(ticket.get('Статус обращения'))}",
            f"\n<b>Суть обращения:</b>\n{html.escape(ticket.get('Сообщение'))}",
            f"\n<b>История работы:</b>",
            f"  - Получено: {html.escape(ticket.get('Время получения обращения'))}",
            f"  - Взято в работу: {html.escape(ticket.get('Время взятия в работу', 'Нет'))}",
            f"  - Передано на 2 линию: {html.escape(ticket.get('Передача на 2 линию', 'Нет'))}",
            f"  - Передано на 3 линию: {html.escape(ticket.get('Передача на 3 Линию', 'Нет'))}",
            f"\n<b>Крайний срок по SLA:</b> {html.escape(ticket.get('SLA (Время на решение)'))}"
        ]
        
        message = "\n".join(filter(None, message_lines))

        # Отправляем уведомления
        if not SLA_NOTIFICATION_USER_IDS:
            logger.set_context()
            log.warning("SLA_NOTIFICATION_USER_IDS не установлен. Уведомление не будет отправлено." )
        else:
            for user_id in SLA_NOTIFICATION_USER_IDS:
                try:
                    await context.bot.send_message(chat_id=user_id, text=message, parse_mode='HTML')
                except Exception as e:
                    logger.set_context()
                    log.error(f"Не удалось отправить SLA-уведомление пользователю: {e}" )
            
        # Отмечаем, что уведомление отправлено
        await mark_sla_notification_sent(entry_id)
        log.info(f"Заявка №{entry_id} помечена как уведомленная")

async def delete_me(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Удаляет данные пользователя из базы данных."""
    user_id = update.message.from_user.id
    logger.set_context(update)
    log.info(f"Запрос на удаление данных пользователя {user_id}")
    try:
        await delete_user(user_id)
        log.info(f"Данные пользователя {user_id} успешно удалены")
        await update.message.reply_text(
            "✅ Ваши данные были успешно удалены. "
            "Чтобы снова начать пользоваться ботом, пожалуйста, отправьте команду /start для регистрации."
        )
    except Exception as e:
        logger.set_context(update)
        log.error(f"Ошибка при удалении пользователя: {e}", exc_info=True )
        await update.message.reply_text(
            "❌ Произошла ошибка при удалении ваших данных. Пожалуйста, попробуйте снова позже."
        )

async def setup_admin_group_topics(application: Application) -> None:
    """Проверяет, создает и/или загружает ID обязательных топиков в чате админов."""
    bot = application.bot
    logger.set_context()
    log.info("Настройка топиков в группе администраторов...")

    if not ADMIN_CHAT_ID:
        log.warning("ADMIN_CHAT_ID не установлен. Пропуск создания топиков.")
        return

    try:
        chat_info = await bot.get_chat(ADMIN_CHAT_ID)
        if not chat_info.is_forum:
            log.error(f"Чат {ADMIN_CHAT_ID} не является форумом. Невозможно создать топики.")
            return
    except Exception as e:
        log.error(f"Не удалось получить информацию о чате {ADMIN_CHAT_ID}: {e}")
        return

    # Загружаем существующие ID из БД
    existing_topics = await get_all_topic_ids()
    application.bot_data.update(existing_topics)
    log.info(f"Загружены ID топиков из БД: {existing_topics}")

    missing_topics = False
    for key, name in TOPIC_NAMES.items():
        topic_key_in_db = f"{key}_topic_id"
        if topic_key_in_db not in existing_topics:
            missing_topics = True
            log.info(f"Топик '{name}' отсутствует в БД, попытка создания...")
            try:
                topic = await bot.create_forum_topic(chat_id=ADMIN_CHAT_ID, name=name)
                thread_id = topic.message_thread_id
                
                await set_topic_id(topic_key_in_db, thread_id)
                application.bot_data[topic_key_in_db] = thread_id
                log.info(f"Топик '{name}' успешно создан с thread_id: {thread_id} и сохранен в БД.")
                
                # Специальная логика для Dashboard при первом создании
                if key == "dashboard":
                    message_to_pin = await bot.send_message(
                        chat_id=ADMIN_CHAT_ID,
                        message_thread_id=thread_id,
                        text="📊 Панель управления тикетами. Новые обращения будут появляться здесь."
                    )
                    application.bot_data['dashboard_message_id'] = message_to_pin.message_id
                    await bot.pin_chat_message(
                        chat_id=ADMIN_CHAT_ID,
                        message_id=message_to_pin.message_id,
                        disable_notification=True
                    )
                    log.info(f"Сообщение в топике Dashboard создано (ID: {message_to_pin.message_id}) и закреплено.")
                    
            except Exception as e:
                # Вложенный try-except для обработки ошибок создания одного топика
                log.error(f"Не удалось создать топик '{name}': {e}", exc_info=True)

    if not missing_topics:
        log.info("Все системные топики уже существуют и загружены.")

async def post_init_setup(application: Application) -> None:
    """Выполняет настройку после инициализации бота (команды, топики)."""
    # Установка команд для меню
    user_commands = [
        BotCommand("start", "Перезапустить бота / Регистрация"),
        BotCommand("new_ticket", "Создать новое обращение"),
        BotCommand("delete_me", "Удалить мои данные"),
    ]
    await application.bot.set_my_commands(user_commands)

    admin_commands = user_commands + [
        BotCommand("start_digest", "Создать рассылку"),
        BotCommand("get_photo", "Получить фото по ID"),
        BotCommand("recreate_topics", "Пересоздать системные топики"),
    ]
    
    if ADMIN_USER_IDS:
        for admin_id in ADMIN_USER_IDS:
            try:
                await application.bot.set_my_commands(admin_commands, scope=BotCommandScopeChat(admin_id))
            except Exception as e:
                logger.set_context()
                log.warning(f"Не удалось установить команды для админа {admin_id}: {e}")

    # Настройка обязательных топиков в группе админов
    await setup_admin_group_topics(application)
    # Первоначальная отрисовка дашборда
    await update_dashboard(application)

async def update_dashboard(application: Application) -> None:
    """Собирает информацию о всех тикетах и обновляет сообщение-дашборд."""
    bot = application.bot
    bot_data = application.bot_data

    dashboard_topic_id = bot_data.get("dashboard_topic_id")
    dashboard_message_id = bot_data.get("dashboard_message_id")
    
    if not dashboard_topic_id or not dashboard_message_id or not ADMIN_CHAT_ID:
        log.warning("ID топика или сообщения дашборда не найдены, обновление пропущено.")
        return

    # 1. Собираем и группируем все активные тикеты
    all_tickets = list(bot_data.get('topic_ticket_info', {}).values())
    
    new_tickets = [t for t in all_tickets if t.get('status') == 'new']
    l2_tickets = [t for t in all_tickets if t.get('status') == 'escalated_l2']
    l3_tickets = [t for t in all_tickets if t.get('status') == 'escalated_l3']
    
    in_progress_tickets = {}
    for ticket in all_tickets:
        if ticket.get('status') == 'in_progress':
            assignee = ticket.get('assignee', 'Не назначен')
            if assignee not in in_progress_tickets:
                in_progress_tickets[assignee] = []
            in_progress_tickets[assignee].append(ticket)

    # 2. Формируем текст дашборда
    dashboard_lines = ["📊 <b>Панель управления тикетами</b>\n"]
    chat_link = f"https://t.me/c/{str(ADMIN_CHAT_ID).replace('-100', '')}"

    # Секция: Новые тикеты
    dashboard_lines.append("<b>📥 Новые тикеты (L1):</b>")
    if not new_tickets:
        dashboard_lines.append("  <i>Нет новых тикетов</i>")
    else:
        for ticket in sorted(new_tickets, key=lambda x: int(x['entry_id'])):
            ticket_url = f"{chat_link}/{ticket['topic_id']}"
            user_info = f"@{ticket['username']}" if ticket['username'] else ticket['fio']
            dashboard_lines.append(f"  - <a href='{ticket_url}'>Тикет #{ticket['entry_id']}</a> ({html.escape(ticket.get('feedback_type', ''))}) от {html.escape(user_info)}")
    dashboard_lines.append("")

    # Секция: Тикеты в работе
    dashboard_lines.append("<b>⚙️ Тикеты в работе:</b>")
    if not in_progress_tickets:
        dashboard_lines.append("  <i>Нет тикетов в работе</i>")
    else:
        for admin in sorted(in_progress_tickets.keys()):
            dashboard_lines.append(f"  - <b>Оператор: {html.escape(admin)}</b>")
            for ticket in sorted(in_progress_tickets[admin], key=lambda x: int(x['entry_id'])):
                ticket_url = f"{chat_link}/{ticket['topic_id']}"
                dashboard_lines.append(f"    - <a href='{ticket_url}'>Тикет #{ticket['entry_id']}</a> ({html.escape(ticket.get('feedback_type', ''))})")
    dashboard_lines.append("")
    
    # Секция: L2
    dashboard_lines.append("<b>🛠️ Эскалация (L2):</b>")
    if not l2_tickets:
        dashboard_lines.append("  <i>Нет тикетов</i>")
    else:
        for ticket in sorted(l2_tickets, key=lambda x: int(x['entry_id'])):
            ticket_url = f"{chat_link}/{ticket['topic_id']}"
            dashboard_lines.append(f"  - <a href='{ticket_url}'>Тикет #{ticket['entry_id']}</a>")
    dashboard_lines.append("")
    
    # Секция: L3
    dashboard_lines.append("<b>💰 Эскалация (L3):</b>")
    if not l3_tickets:
        dashboard_lines.append("  <i>Нет тикетов</i>")
    else:
        for ticket in sorted(l3_tickets, key=lambda x: int(x['entry_id'])):
            ticket_url = f"{chat_link}/{ticket['topic_id']}"
            dashboard_lines.append(f"  - <a href='{ticket_url}'>Тикет #{ticket['entry_id']}</a>")
    
    dashboard_text = "\n".join(dashboard_lines)

    # 3. Обновляем сообщение
    try:
        await bot.edit_message_text(
            text=dashboard_text,
            chat_id=ADMIN_CHAT_ID,
            message_id=dashboard_message_id,
            parse_mode='HTML',
            disable_web_page_preview=True
        )
    except Exception as e:
        log.error(f"Не удалось обновить дашборд: {e}", exc_info=True)

async def main() -> None:
    """Настраивает и запускает бота."""
    logger.set_context()
    log.info("Запуск бота")
    # Сначала асинхронно инициализируем БД, так как это нужно до запуска бота
    try:
        await initialize_db()
        log.info("База данных успешно инициализирована")
    except Exception as e:
        log.error(f"Ошибка инициализации базы данных: {e}")
        return

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        log.error("Необходимо установить переменную окружения TELEGRAM_BOT_TOKEN")
        return

    # ApplicationBuilder сам управляет JobQueue
    application = (
        Application.builder()
        .token(token)
        .post_init(post_init_setup)
        .connect_timeout(30)
        .read_timeout(30)
        .write_timeout(30)
        .build()
    )

    # Отдельный обработчик для регистрации
    registration_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            REG_AWAITING_FIO: [MessageHandler(filters.TEXT & ~filters.COMMAND, register_fio)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True
    )
    log.info("Обработчик регистрации создан")

    # Основной обработчик для создания обращений
    conv_handler = ConversationHandler(
        entry_points=[
            MessageHandler(filters.TEXT & filters.Regex("^📝 Создать новое обращение$"), start_new_ticket),
            CommandHandler("new_ticket", start_new_ticket)
        ],
        states={
            CHOOSING: [
                CallbackQueryHandler(button, pattern="^(bug|feature|access_issue|consultation)$"),
            ],
            PLATFORM: [
                CallbackQueryHandler(handle_platform_selection, pattern="^platform_")
            ],
            FEEDBACK: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_feedback_and_ask_for_photo)],
            AWAITING_PHOTO: [
                MessageHandler(filters.PHOTO, handle_photo),
                CallbackQueryHandler(skip_photo_and_save, pattern="^skip_photo$"),
                CallbackQueryHandler(finish_photos_and_save, pattern="^finish_photos$")
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True
    )
    log.info("Обработчик обращений создан")

    digest_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start_digest", start_digest)],
        states={
            CHOOSING_DIGEST_CONTENT: [
                CallbackQueryHandler(choose_content_type, pattern="^(add_text|add_photo|add_document|preview_and_send)$")
            ],
            AWAITING_DIGEST_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND | filters.Document.ALL | filters.PHOTO, receive_text)],
            AWAITING_PHOTO: [
                MessageHandler(filters.PHOTO | filters.TEXT & ~filters.COMMAND | filters.Document.ALL , handle_photo),
                CallbackQueryHandler(digest_media_save, pattern="^finish_photos$")
            ],
            AWAITING_DIGEST_DOCUMENT: [
                MessageHandler(filters.TEXT | filters.PHOTO | filters.Document.ALL & ~filters.COMMAND, handle_document), # Перехватываем все документы
                CallbackQueryHandler(digest_document_save, pattern="^finish_documents$") # Обрабатываем кнопку "Готово"
            ],
            CONFIRM_DIGEST: [
                CallbackQueryHandler(handle_broadcast_confirmation, pattern="^(confirm_broadcast|cancel_broadcast|add_something_to_digest)$")
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel_digest_creation)]
    )
    log.info("Обработчик дайджеста создан")

    application.add_handler(registration_handler)
    application.add_handler(conv_handler)
    application.add_handler(digest_conv_handler)
    application.add_handler(CommandHandler("get_photo", get_photo_by_id))
    application.add_handler(CommandHandler("delete_me", delete_me))
    application.add_handler(CommandHandler("recreate_topics", recreate_topics))
    application.add_handler(CallbackQueryHandler(take_ticket, pattern="^take_ticket_"))
    application.add_handler(CallbackQueryHandler(take_escalated_ticket, pattern="^take_escalated_"))
    application.add_handler(CallbackQueryHandler(transfer_to_line, pattern="^transfer_l[23]_"))
    application.add_handler(CallbackQueryHandler(set_priority, pattern="^priority_"))
    application.add_handler(CallbackQueryHandler(close_ticket, pattern="^close_ticket_"))
    application.add_handler(MessageHandler(
        filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND,
        handle_admin_reply
    ))
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE,
        unhandled_text
    ))
    # Этот обработчик должен быть одним из последних
    application.add_handler(MessageHandler(filters.COMMAND, unknown_command))
    
    # Запускаем фоновую проверку SLA
    application.job_queue.run_repeating(check_sla_breaches, interval=900, first=10)

    # Запускаем бота до принудительной остановки
    log.info("Бот готов к работе...")
    
    async with application:
        await application.start()
        await application.updater.start_polling()
        # Бесконечно ждем, пока не получим сигнал остановки (например, Ctrl+C)
        await asyncio.Event().wait()

async def recreate_topics(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Принудительно удаляет и пересоздает системные топики."""
    user_id = update.message.from_user.id
    if user_id not in ADMIN_USER_IDS:
        await update.message.reply_text("У вас нет прав для выполнения этой команды.")
        return

    log.info(f"Администратор {user_id} инициировал пересоздание топиков.")
    await update.message.reply_text("Начинаю процесс пересоздания топиков... Это может занять несколько секунд.")

    try:
        # 1. Удаляем все старые записи о топиках из БД
        await delete_all_topics()
        log.info("Все записи о топиках удалены из БД.")

        # 2. Очищаем bot_data от старых ID
        topic_keys_to_remove = list(TOPIC_NAMES.keys())
        topic_keys_to_remove.extend([f"{k}_topic_id" for k in TOPIC_NAMES.keys()])
        topic_keys_to_remove.append('dashboard_message_id')
        for key in topic_keys_to_remove:
            context.application.bot_data.pop(key, None)
        log.info("Данные о топиках в bot_data очищены.")

        # 3. Запускаем процедуру создания заново
        await setup_admin_group_topics(context.application)
        log.info("Процедура setup_admin_group_topics завершена.")
        
        # 4. Обновляем дашборд с новыми данными
        await update_dashboard(context.application)
        log.info("Дашборд обновлен.")

        await update.message.reply_text("✅ Системные топики и дэшборд успешно пересозданы!")
    except Exception as e:
        log.error(f"Ошибка при пересоздании топиков: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Произошла ошибка: {e}")


if __name__ == "__main__":
    asyncio.run(main())
