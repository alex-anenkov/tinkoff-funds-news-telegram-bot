import telebot
from telebot import types
import sqlite3
from datetime import date
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import threading
import yaml
from retrying import retry

TOKEN = None
ADMIN_CHAT_ID = None
DOMAIN_NAME = 'https://www.tinkoff.ru'
SUPPORTED_TICKERS = 'TRUR TUSD TEUR TBEU TSPX TECH TSOX TPAS TMOS TEUS TBRU TIPO TGLD TRRE TSPV TGRN TFNX TSST TEMS TCBR TBUY TBIO TRAI'

with open('config.yaml', 'r') as stream:
    try:
        config = yaml.safe_load(stream)
        TOKEN = config['token']
        ADMIN_CHAT_ID = config['admin_chat_id']
    except yaml.YAMLError as e:
        print(e)

bot = telebot.TeleBot(TOKEN)
db = sqlite3.connect('main.db', check_same_thread=False)
db.execute('VACUUM')
db_cursor = db.cursor()

def extract_args(arg):
    return arg.split()[1:]

def create_users_db():
    db_cursor.execute('CREATE TABLE IF NOT EXISTS Users (chat_id INTEGER PRIMARY KEY UNIQUE, user_name TEXT, reg_date DATE, \
        TRUR_ticker BOOLEAN, \
        TUSD_ticker BOOLEAN,  \
        TEUR_ticker BOOLEAN,  \
        TBEU_ticker BOOLEAN,  \
        TSPX_ticker BOOLEAN,  \
        TECH_ticker BOOLEAN,  \
        TSOX_ticker BOOLEAN,  \
        TPAS_ticker BOOLEAN,  \
        TMOS_ticker BOOLEAN,  \
        TEUS_ticker BOOLEAN,  \
        TBRU_ticker BOOLEAN,  \
        TIPO_ticker BOOLEAN,  \
        TGLD_ticker BOOLEAN,  \
        TRRE_ticker BOOLEAN,  \
        TSPV_ticker BOOLEAN,  \
        TGRN_ticker BOOLEAN,  \
        TFNX_ticker BOOLEAN,  \
        TSST_ticker BOOLEAN,  \
        TEMS_ticker BOOLEAN,  \
        TCBR_ticker BOOLEAN,  \
        TBUY_ticker BOOLEAN,  \
        TBIO_ticker BOOLEAN,  \
        TRAI_ticker BOOLEAN  \
        )')

def create_news_db():
    db_cursor.execute('CREATE TABLE IF NOT EXISTS News \
        (link STRING PRIMARY KEY UNIQUE, title STRING, announce STRING, date DATETIME, ticker STRING)')

def create_db():
    create_users_db()
    create_news_db()
    db.commit()

def clear_users_db():
    db_cursor.execute('DELETE FROM Users',)
    db.commit()
    print('We have deleted', db_cursor.rowcount, 'records from Users table')

def clear_news_db():
    db_cursor.execute('DELETE FROM News',)
    db.commit()
    print('We have deleted', db_cursor.rowcount, 'records from News table')

def delete_user_from_db(chat_id):
    db_cursor.execute('DELETE FROM Users WHERE chat_id = ?', (str(chat_id),))
    if db_cursor.rowcount > 0:
        print(f'User {chat_id} deleted')
    db.commit()

def print_users_table():
    db_cursor.execute('SELECT * FROM Users ORDER BY user_name LIMIT 100')
    results = db_cursor.fetchall()
    print(results)

def print_news_table():
    db_cursor.execute('SELECT * FROM News ORDER BY date LIMIT 100')
    results = db_cursor.fetchall()
    print(results)

def insert_new_user_in_db(chat_id, user_name: str):
    db_cursor.execute('INSERT OR IGNORE INTO Users (chat_id, user_name, reg_date) VALUES (?, ?, ?)',
        (str(chat_id), user_name, str(date.today())))
    if db_cursor.rowcount > 0:
        print(f'Insert new user {chat_id}, {user_name}')
    db.commit()

def set_user_ticker(chat_id, ticker: str, value):
    db_ticker_name = f'{ticker.upper()}_ticker'
    db_cursor.execute(f'UPDATE Users SET {db_ticker_name} = ? WHERE chat_id = ?', (bool(value), str(chat_id),))
    db.commit()
    if db_cursor.rowcount > 0:
        return True
    return False

def is_supported_ticker(ticker: str):
    if SUPPORTED_TICKERS.upper().find(ticker.upper()) != -1:
        return True
    return False

def send_news_message(chat_id, title: str, announce: str, link: str, ticker: str):
    print(f'Send to {chat_id}')
    msg = f'<b>{title}</b>\n\n'
    if announce is not None and announce != '':
        msg += f'{announce}\n\n'
    msg += f'#{ticker.upper()}'
    if 'дивиденды' in title:
        msg += ' #дивиденды'
    reply_markup=types.InlineKeyboardMarkup([
        [types.InlineKeyboardButton(text='Open link', url=DOMAIN_NAME + link)],
    ])
    bot.send_message(chat_id, text=msg, parse_mode='html', reply_markup=reply_markup)

def broadcast_news(user_list, title: str, announce: str, link: str, ticker: str):
    if user_list is None:
        return
    for chat_id in user_list:
        send_news_message(chat_id, title, announce, link, ticker)

def get_user_list_for_news_broadcast(ticker: str):
    db_ticker_name = f'{ticker.upper()}_ticker'
    db_cursor.execute(f'SELECT chat_id FROM Users WHERE {db_ticker_name} = 1')
    return db_cursor.fetchone()

def handle_news(titles, announces, links, ticker: str):
    count = min(len(titles), len(links))
    for i in range(count):
        title = titles[i].get_text()
        announce = ''
        if len(announces) > 0:
            announce = announces[i].get_text()
        link = links[i]['href']
        today_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        db_cursor.execute('INSERT OR IGNORE INTO News (link, title, announce, date, ticker) VALUES (?, ?, ?, ?, ?)',
            (link, title, announce, today_date, ticker))
        if db_cursor.rowcount > 0:
            print(f'New: {title}')
            user_list = get_user_list_for_news_broadcast(ticker)
            broadcast_news(user_list, title, announce, link, ticker)
    db.commit()

def retry_if_connection_error(exception):
    print('Trying send request again...')
    return isinstance(exception, ConnectionError)

# if exception retry with 30 second wait  
@retry(retry_on_exception=retry_if_connection_error, wait_fixed=30000)
def safe_request(url, **kwargs):
    return requests.get(url, **kwargs)

def parse_web_and_insert_to_db(ticker: str):
    url = f'{DOMAIN_NAME}/invest/etfs/{ticker}/news/'
    page = safe_request(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    wrappers = soup.select('div[class*="NewsItem__wrapper_"]')
    for w in wrappers:
        titles = w.select('div[class*="NewsItem__title_"]')
        announces = w.select('div[class*="NewsItem__announce_"]')
        links = w.select('div[class*="Link-module__link_"], a[href]')
        handle_news(titles, announces, links, ticker)
    # print_news_table()

def update_news():
    tickers = SUPPORTED_TICKERS.split(' ')
    for ticker in tickers:
        parse_web_and_insert_to_db(ticker)

def update_news_async(f_stop):
    update_news()
    if not f_stop.is_set():
        # call update_news_async() again in 1800 seconds
        threading.Timer(1800, update_news_async, [f_stop]).start()

@bot.message_handler(commands=['start'])
def handle_start_msg(msg):
    bot.send_message(msg.chat.id, f'Привет, {msg.chat.first_name}')
    insert_new_user_in_db(msg.chat.id, msg.chat.first_name)
    print_users_table()

@bot.message_handler(commands=['update'])
def handle_update_msg(msg):
    if msg.chat.id != ADMIN_CHAT_ID:
        return
    update_news()

@bot.message_handler(commands=['stop'])
def handle_stop_msg(msg):
    bot.send_message(msg.chat.id, f'Пока, {msg.chat.first_name}')
    delete_user_from_db(msg.chat.id)

@bot.message_handler(commands=['add'])
def handle_add_msg(msg):
    args = extract_args(msg.text)
    if len(args) > 0:
        ticker=args[0]
        if not is_supported_ticker(ticker):
            bot.send_message(msg.chat.id, f'Неизвестный тикер {ticker}\nСписок поддерживаемых тикеров: {SUPPORTED_TICKERS}')
            return
        if set_user_ticker(msg.chat.id, ticker, 1):
            bot.send_message(msg.chat.id, 'Список тикеров обновлен')
    else:
        bot.send_message(msg.chat.id, 'Для добавления тикера используйте команду /add <ticker>')

@bot.message_handler(commands=['remove'])
def handle_remove_msg(msg):
    args = extract_args(msg.text)
    if len(args) > 0:
        ticker=args[0]
        if not is_supported_ticker(ticker):
            bot.send_message(msg.chat.id, f'Неизвестный тикер {ticker}\nСписок поддерживаемых тикеров: {SUPPORTED_TICKERS}')
            return
        if set_user_ticker(msg.chat.id, ticker, 0):
            bot.send_message(msg.chat.id, 'Список тикеров обновлен')
        else:
            bot.send_message(msg.chat.id, f'Неизвестный тикер {ticker}\nСписок поддерживаемых тикеров: {SUPPORTED_TICKERS}')
    else:
        bot.send_message(msg.chat.id, 'Для удаления тикера используйте команду /remove <ticker>')

@bot.message_handler(commands=['help'])
def handle_help_msg(msg):
    text = 'Команды:\n'
    text += '/add <тикер>\n'
    text += '/remove <тикер>\n\n'
    text += f'Список поддерживаемых тикеров: {SUPPORTED_TICKERS}'
    bot.send_message(msg.chat.id, text)

@bot.message_handler(commands=['clear'])
def handle_clear_msg(msg):
    if msg.chat.id != ADMIN_CHAT_ID:
        return

    args = extract_args(msg.text)
    if len(args) <= 0:
        return
    if args[0] == 'users':
        clear_users_db()
        bot.send_message(msg.chat.id, 'Users database cleared')
    if args[0] == 'news':
        clear_news_db()
        bot.send_message(msg.chat.id, 'News database cleared')

assert sqlite3.threadsafety == 1, 'Database is not threadsafe'

create_db()

f_stop = threading.Event()
# start calling update_news_async now and every 1800 sec thereafter
update_news_async(f_stop)

print('bot online')
bot.infinity_polling(timeout=10, long_polling_timeout=5)

f_stop.set()
db.close()
