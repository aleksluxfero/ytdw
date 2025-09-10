import os
import json
import shutil
import tempfile
import logging
import requests
from pathlib import Path
import yt_dlp
import psycopg2
import redis
from rq import get_current_job
from pyrogram import Client

logging.basicConfig(level=os.environ.get('LOG_LEVEL','INFO'))
log = logging.getLogger(__name__)

BOT_TOKEN = os.environ['BOT_TOKEN']
API_ID = int(os.environ.get('API_ID', '0'))
API_HASH = os.environ.get('API_HASH', '')
PYRO_SESSION = os.environ.get('PYRO_SESSION_NAME', 'mtproto_session')
PYRO_WORKDIR = os.environ.get('PYRO_WORKDIR', 'sessions')
REDIS_URL = os.environ.get('REDIS_URL', 'redis://redis:6379/0')
DB_URL = os.environ.get('DATABASE_URL', 'postgresql://ytdlp:ytdlp@db:5432/ytdlp')
TMP_DIR_BASE = os.environ.get('TMP_DIR', '/tmp/ytdlp')

os.makedirs(TMP_DIR_BASE, exist_ok=True)

def get_file_id(url, format_id):
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT file_id FROM downloads WHERE url=%s AND format_id=%s", (url, format_id))
            row = cur.fetchone()
            return row[0] if row else None

def save_file_id(url, format_id, file_id):
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO downloads(url, format_id, file_id) VALUES (%s,%s,%s) ON CONFLICT (url, format_id) DO NOTHING", (url, format_id, file_id))
            conn.commit()

def bot_send_message(chat_id, text):
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'
    payload = {'chat_id': chat_id, 'text': text, 'parse_mode': 'HTML'}
    r = requests.post(url, json=payload)
    try:
        return r.json()
    except Exception:
        return None

def bot_edit_message(chat_id, message_id, text):
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/editMessageText'
    payload = {'chat_id': chat_id, 'message_id': message_id, 'text': text, 'parse_mode': 'HTML'}
    requests.post(url, json=payload)

def bot_send_document_via_botapi_by_file_id(chat_id, file_id, caption=None):
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendDocument'
    payload = {'chat_id': chat_id, 'document': file_id}
    if caption:
        payload['caption'] = caption
    r = requests.post(url, json=payload)
    try:
        return r.json()
    except Exception:
        return None

def bot_send_document_via_botapi_file(chat_id, file_path, caption=None):
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendDocument'
    with open(file_path, 'rb') as f:
        files = {'document': f}
        data = {'chat_id': chat_id}
        if caption:
            data['caption'] = caption
        r = requests.post(url, data=data, files=files)
    try:
        return r.json()
    except Exception:
        return None

def human_size(nbytes):
    for unit in ['B','KB','MB','GB','TB']:
        if nbytes < 1024.0:
            return f"{nbytes:.1f}{unit}"
        nbytes /= 1024.0
    return f"{nbytes:.1f}PB"

def process_job(chat_id, user_id, url, format_id='best', title=None):
    job = get_current_job()
    log.info('Job start: %s %s %s %s', job.id if job else None, chat_id, user_id, url)

    cached = get_file_id(url, format_id)
    if cached:
        bot_send_message(chat_id, 'Файл найден в кэше, отправляю...')
        resp = bot_send_document_via_botapi_by_file_id(chat_id, cached, caption=title)
        if resp and resp.get('ok'):
            log.info('Sent cached file_id for %s', url)
            return
        else:
            log.warning('Failed to send cached file_id, продолжу загрузку (возможно file_id устарел)')

    tmpdir = Path(tempfile.mkdtemp(prefix='ytdlp_', dir=TMP_DIR_BASE))
    outtmpl = str(tmpdir / '%(title).200s.%(id)s.%(ext)s')

    status_msg = bot_send_message(chat_id, 'Начинаю загрузку...')
    status_mid = None
    if status_msg and status_msg.get('result'):
        status_mid = status_msg['result']['message_id']

    def ydl_hook(d):
        try:
            if d.get('status') == 'downloading':
                downloaded = d.get('downloaded_bytes', 0)
                total = d.get('total_bytes') or d.get('total_bytes_estimate') or 0
                if total:
                    pct = int(downloaded * 100 / total)
                    text = f"Скачивание: {pct}% ({human_size(downloaded)} / {human_size(total)})"
                else:
                    text = f"Скачано: {human_size(downloaded)}"
                if status_mid:
                    bot_edit_message(chat_id, status_mid, text)
            elif d.get('status') == 'finished':
                if status_mid:
                    bot_edit_message(chat_id, status_mid, 'Загрузка завершена. Обрабатываю...')
        except Exception:
            log.exception('hook')

    ydl_opts = {
        'format': format_id,
        'outtmpl': outtmpl,
        'noplaylist': True,
        'progress_hooks': [ydl_hook],
        'merge_output_format': 'mp4',
        'quiet': True,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)

        files = list(tmpdir.glob('*'))
        if not files:
            bot_send_message(chat_id, 'Не удалось найти скачанный файл.')
            shutil.rmtree(tmpdir, ignore_errors=True)
            return

        files = sorted(files, key=lambda p: p.stat().st_size, reverse=True)
        file_path = files[0]
        size = file_path.stat().st_size
        log.info('Downloaded file: %s (%s)', file_path, human_size(size))

        if size <= 50 * 1024 * 1024:
            if status_mid:
                bot_edit_message(chat_id, status_mid, f'Отправляю через Bot API ({human_size(size)})...')
            resp = bot_send_document_via_botapi_file(chat_id, str(file_path), caption=title)
            if resp and resp.get('ok'):
                try:
                    fid = resp['result']['document']['file_id']
                    save_file_id(url, format_id, fid)
                except Exception:
                    log.exception('save file_id failed')
            else:
                log.warning('Bot API send failed: %s', resp)

        else:
            if status_mid:
                bot_edit_message(chat_id, status_mid, f'Файл {human_size(size)} — отправляю через MTProto...')

            app = Client(PYRO_SESSION, api_id=API_ID, api_hash=API_HASH, workdir=PYRO_WORKDIR)

            def progress(current, total):
                try:
                    pct = int(current * 100 / total) if total else 0
                    text = f'Загрузка в Telegram: {pct}% ({human_size(current)} / {human_size(total)})'
                    if status_mid:
                        bot_edit_message(chat_id, status_mid, text)
                except Exception:
                    log.exception('progress')

            with app:
                msg = app.send_document(chat_id, str(file_path), caption=title, progress=progress)
                try:
                    fid = None
                    if msg and msg.document:
                        fid = msg.document.file_id
                    if fid:
                        save_file_id(url, format_id, fid)
                except Exception:
                    log.exception('save file_id pyrogram')

        bot_send_message(chat_id, 'Готово ✅')

    except Exception as e:
        log.exception('process_job failed')
        bot_send_message(chat_id, f'Ошибка: {e}')

    finally:
        try:
            shutil.rmtree(tmpdir)
        except Exception:
            pass

if __name__ == '__main__':
    pass
