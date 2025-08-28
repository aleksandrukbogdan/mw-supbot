import logging
import sys
from typing import Optional
from telegram import Update

class BotLogger:
    def __init__(self):
        self._logger = logging.getLogger('bot')
        self._logger.setLevel(logging.INFO)
                
        file_handler = logging.FileHandler(filename="log_file.log", mode='a', encoding='utf-8')
        console_handler = logging.StreamHandler(sys.stdout)
        
        formatter = logging.Formatter('%(asctime)s|%(name)-12s|%(levelname)-7s|%(funcName)-36s|%(lineno)-4d|user_id=%(user_id)-18s|chat_id=%(chat_id)-20s|message_id=%(message_id)s|%(message)s')
        
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        self._logger.addHandler(file_handler)
        self._logger.addHandler(console_handler)
        self._context = {
            'user_id': 'SYSTEM',
            'chat_id': 'N/A',
            'message_id': 'N/A'
        }

    def set_context(self, update: Optional[Update] = None):
        """Обновляет контекст логирования на основе Telegram Update"""
        if update is None:
            self._context = {
                'user_id': 'SYSTEM',
                'chat_id': 'N/A',
                'message_id': 'N/A'
            }
            return
        
        try:
            if update.message:
                self._context = {
                    'user_id': update.effective_user.id,
                    'chat_id': update.effective_chat.id,
                    'message_id': update.message.message_id
                }
            elif update.callback_query:
                self._context = {
                    'user_id': update.callback_query.from_user.id,
                    'chat_id': update.callback_query.message.chat.id,
                    'message_id': update.callback_query.message.message_id
                }
        except Exception as e:
            self._logger.error(f"Ошибка установки контекста: {e}")

    def get_logger(self, module_name: str = None):
        """Возвращает логгер с текущим контекстом"""
        logger = logging.getLogger(f'bot.{module_name}' if module_name else 'bot')
        
        # Добавляем контекст ко всем записям
        old_factory = logging.getLogRecordFactory()
        
        def record_factory(*args, **kwargs):
            record = old_factory(*args, **kwargs)
            record.user_id = self._context['user_id']
            record.chat_id = self._context['chat_id']
            record.message_id = self._context['message_id']
            return record
        
        logging.setLogRecordFactory(record_factory)
        return logger

# Создаем глобальный экземпляр логгера
logger = BotLogger()