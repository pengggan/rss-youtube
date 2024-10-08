import os
import asyncio
import aiohttp
import aiomysql
import json
import logging
from feedparser import parse
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 从环境变量中获取敏感信息
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
ALLOWED_CHAT_IDS = os.getenv("ALLOWED_CHAT_IDS").split(",")

TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

# RSS 源列表
RSS_FEEDS = [
    'https://rsshub.app/zaobao/znews/china', # 联合早报
   # 'https://rsshub.app/fortunechina',
    'https://rsshub.app/bilibili/hot-search', # bilibili
    'https://rss.mifaw.com/articles/5c8bb11a3c41f61efd36683e/5c91d2e23882afa09dff4901', # 36氪 - 24小时热榜
    'https://rss.mifaw.com/articles/5c8bb11a3c41f61efd36683e/5cac99a7f5648c90ed310e18', # 微博热搜
    'https://rss.mifaw.com/articles/5c8bb11a3c41f61efd36683e/5cf92d7f0cc93bc69d082608', # 百度热搜榜
   # 'https://blog.090227.xyz/atom.xml',
    'https://www.freedidi.com/feed', # 零度解说
   # 'https://rsshub.app/guancha/headline',
    'https://rsshub.app/zaobao/znews/china', # 联合早报
   # 'http://blog.caixin.com/feed',
   # 'https://rss.mifaw.com/articles/5c8bb11a3c41f61efd36683e/5ca0144af6f83a0a176acfd6',
    'https://36kr.com/feed',
    # 添加更多 RSS 源
]

async def fetch_feed(session, feed, retries=3, delay=10):
    for attempt in range(retries):
        try:
            async with session.get(feed, timeout=60) as response:
                response.raise_for_status()
                content = await response.read()
                return parse(content)
        except Exception as e:
            logging.error(f"Error fetching {feed}: Attempt {attempt + 1} of {retries}: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(delay)
            else:
                logging.error(f"Failed to fetch {feed} after {retries} attempts.")
                return None

async def send_message(session, chat_id, text):
    payload = {
        'chat_id': chat_id,
        'text': text,
        'parse_mode': 'Markdown'
    }
    try:
        async with session.post(TELEGRAM_API_URL, json=payload) as response:
            response.raise_for_status()
            logging.info(f"Message sent to {chat_id}: {text}")
    except Exception as e:
        logging.error(f"Error sending message to {chat_id}: {e}")

async def process_feed(session, feed, sent_entries, connection):
    feed_data = await fetch_feed(session, feed)
    if feed_data is None:
        return []

    new_entries = []
    for entry in feed_data.entries:
        if entry.link not in sent_entries:
            message = f"*{entry.title}*\n{entry.link}"
            for chat_id in ALLOWED_CHAT_IDS:
                await send_message(session, chat_id, message)
            new_entries.append(entry.link)
            await save_sent_entry_to_db(connection, entry.link)
            sent_entries.add(entry.link)
            await asyncio.sleep(5)
    return new_entries

async def connect_to_db():
    try:
        connection = await aiomysql.connect(
            host=DB_HOST,
            db=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return connection
    except Exception as e:
        logging.error(f"Error while connecting to MySQL: {e}")
        return None

async def load_sent_entries_from_db(connection):
    try:
        async with connection.cursor() as cursor:
            await cursor.execute("SELECT data FROM sent_entries")
            rows = await cursor.fetchall()
            return {json.loads(row[0])['url'] for row in rows}
    except Exception as e:
        logging.error(f"Error fetching sent entries: {e}")
        return set()

async def save_sent_entry_to_db(connection, url):
    try:
        async with connection.cursor() as cursor:
            await cursor.execute("INSERT IGNORE INTO sent_entries (data) VALUES (%s)", (json.dumps({"url": url}),))
            await connection.commit()
            logging.info(f"Saved sent entry: {url}")
    except Exception as e:
        logging.error(f"Error saving sent entry: {e}")

async def main():
    connection = await connect_to_db()
    if connection is None:
        logging.error("Failed to connect to the database. Exiting.")
        return

    sent_entries = await load_sent_entries_from_db(connection)
    new_entries = []

    async with aiohttp.ClientSession() as session:
        tasks = [process_feed(session, feed, sent_entries, connection) for feed in RSS_FEEDS]
        results = await asyncio.gather(*tasks)
        for result in results:
            if result:
                new_entries.extend(result)

    connection.close()

if __name__ == "__main__":
    asyncio.run(main())
