import os
from dotenv import load_dotenv
load_dotenv()
import json
import uuid
import logging
import redis
import yt_dlp
from rq import Queue
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler, MessageHandler, Filters, CallbackContext

logging.basicConfig(level=os.environ.get('LOG_LEVEL','INFO'))
log = logging.getLogger(__name__)

BOT_TOKEN = os.environ['BOT_TOKEN']
REDIS_URL = os.environ.get('REDIS_URL', 'redis://redis:6379/0')
RQ_QUEUE = os.environ.get('RQ_QUEUE', 'downloads')
CHOICE_TTL = 600
TMP_KEY_PREFIX = 'yt:choice:'

redis_conn = redis.from_url(REDIS_URL)
queue = Queue(RQ_QUEUE, connection=redis_conn)

updater = Updater(BOT_TOKEN, use_context=True)
disp = updater.dispatcher

def list_formats(url):
    opts = {'quiet': True}
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False)

    title = info.get('title', 'video')
    formats = info.get('formats', [])

    candidates = []
    candidates.append({'id': 'bestaudio', 'label': 'Аудио (bestaudio)', 'ext': 'm4a'})

    prefer_heights = [144, 240, 360, 480, 720, 1080]
    seen = set()
    for f in formats:
        h = f.get('height')
        fid = f.get('format_id')
        ext = f.get('ext')
        size = f.get('filesize') or f.get('filesize_approx')
        if h and h in prefer_heights and h not in seen:
            label = f"{h}p {ext}"
            if size:
                label += f" ({round(size/1024/1024,1)}MB)"
            candidates.append({'id': fid, 'label': label, 'ext': ext})
            seen.add(h)

    candidates.append({'id': 'best', 'label': 'Лучшее качество', 'ext': None})
    return title, candidates

def cmd_start(update: Update, context: CallbackContext):
    update.message.reply_text('Привет! Пришли ссылку — я соберу доступные форматы.')

def on_message(update: Update, context: CallbackContext):
    text = (update.message.text or '').strip()
    if not text or 'http' not in text:
        update.message.reply_text('Пришли ссылку на видео.')
        return

    url = text.split()[0]
    msg = update.message.reply_text('Собираю форматы...')

    try:
        title, candidates = list_formats(url)
    except Exception as e:
        log.exception('list_formats failed')
        update.message.reply_text('Не удалось получить форматы: %s' % e)
        return

    key = TMP_KEY_PREFIX + str(uuid.uuid4())
    payload = {'url': url, 'title': title, 'candidates': candidates}
    redis_conn.setex(key, CHOICE_TTL, json.dumps(payload))

    kb = []
    for idx, c in enumerate(candidates):
        kb.append([InlineKeyboardButton(c['label'], callback_data=f"choice:{key}:{idx}")])

    update.message.reply_text(f'Форматы для <b>{title}</b>:', parse_mode='HTML', reply_markup=InlineKeyboardMarkup(kb))

def callback_choice(update: Update, context: CallbackContext):
    q = update.callback_query
    q.answer()
    data = q.data
    try:
        _, key, idx = data.split(':')
        idx = int(idx)
    except Exception:
        q.edit_message_text('Неправильные данные.')
        return

    raw = redis_conn.get(key)
    if not raw:
        q.edit_message_text('Время выбора истекло. Пришли ссылку заново.')
        return

    payload = json.loads(raw)
    url = payload['url']
    title = payload.get('title')
    candidates = payload['candidates']

    if idx < 0 or idx >= len(candidates):
        q.edit_message_text('Неверный формат.')
        return

    fmt = candidates[idx]
    from worker.tasks import process_job
    job = queue.enqueue(process_job, q.message.chat_id, q.from_user.id, url, fmt['id'], title)
    pos = len(queue)
    q.edit_message_text(f'Добавлено в очередь. Текущая позиция: {pos}.')

def main():
    disp.add_handler(CommandHandler('start', cmd_start))
    disp.add_handler(MessageHandler(Filters.text & ~Filters.command, on_message))
    disp.add_handler(CallbackQueryHandler(callback_choice))

    updater.start_polling()
    updater.idle()

if __name__ == '__main__':
    main()
