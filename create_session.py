import os
from dotenv import load_dotenv
load_dotenv()
from pyrogram import Client

API_ID = int(os.environ.get('API_ID'))
API_HASH = os.environ.get('API_HASH')
SESSION = os.environ.get('PYRO_SESSION_NAME', 'mtproto_session')
WORKDIR = os.environ.get('PYRO_WORKDIR', 'sessions')

if __name__ == '__main__':
    print('API_ID и API_HASH берутся из окружения.')
    print('При первом запуске нужно ввести номер и код из Telegram.')
    with Client(SESSION, api_id=API_ID, api_hash=API_HASH, workdir=WORKDIR) as app:
        me = app.get_me()
        print('Авторизация успешна для', me.username or me.first_name)
        print('Сессия сохранена в', WORKDIR)
