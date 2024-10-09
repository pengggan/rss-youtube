import os
import asyncio  
import aiohttp
import aiomysql
import logging
import datetime  # 导入 datetime 模块
from feedparser import parse
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 初始化日志记录器
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# RSS 源列表
RSS_FEEDS = [
    'https://rsshub.app/zaobao/znews/china', # 联合早报
   # 'https://rsshub.app/fortunechina',
   # 'https://rsshub.app/bilibili/hot-search', # bilibili
   # 'https://rss.mifaw.com/articles/5c8bb11a3c41f61efd36683e/5c91d2e23882afa09dff4901', # 36氪 - 24小时热榜
   # 'https://rss.mifaw.com/articles/5c8bb11a3c41f61efd36683e/5cac99a7f5648c90ed310e18', # 微博热搜
   # 'https://rss.mifaw.com/articles/5c8bb11a3c41f61efd36683e/5cf92d7f0cc93bc69d082608', # 百度热搜榜
   # 'https://blog.090227.xyz/atom.xml',
    'https://www.freedidi.com/feed', # 零度解说
   # 'https://rsshub.app/guancha/headline',
   # 'http://blog.caixin.com/feed',
   # 'https://rss.mifaw.com/articles/5c8bb11a3c41f61efd36683e/5ca0144af6f83a0a176acfd6',
    # 添加更多 RSS 源
]

# 从环境变量中获取配置
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ALLOWED_CHAT_IDS = os.getenv("ALLOWED_CHAT_IDS").split(",")  # 如果有多个聊天 ID，用逗号分隔
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

async def fetch_feed(session, feed):
    try:
        async with session.get(feed, timeout=60) as response:
            response.raise_for_status()
            content = await response.read()
            return parse(content)
    except Exception as e:
        logging.error(f"Error fetching {feed}: {e}")
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
        # 获取 subject 和 url，检查是否为 None
        subject = entry.title if entry.title else None  # 确保这里不会为 None
        url = entry.link if entry.link else None  # 确保这里不会为 None
        message_id = f"{subject}_{url}" if subject and url else None  # 如果都为 None，将 message_id 设置为 None

        # 检查是否已发送
        if (url, subject, message_id) not in sent_entries:
            message = f"*{entry.title}*\n{entry.link}"
            for chat_id in ALLOWED_CHAT_IDS:
                await send_message(session, chat_id, message)
            new_entries.append((url, subject, message_id))
            
            # 获取当前时间
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # 使用当前时间替换 url、subject 和 message_id 如果它们为 None
            await save_sent_entry_to_db(connection, url if url else current_time, subject if subject else current_time, message_id if message_id else current_time)
            sent_entries.add((url if url else current_time, subject if subject else current_time, message_id if message_id else current_time))
            await asyncio.sleep(6)  # 等待6秒，避免API限制
            
    return new_entries

async def connect_to_db():
    try:
        connection = await aiomysql.connect(
            host=os.getenv("DB_HOST"),
            db=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        return connection
    except Exception as e:
        logging.error(f"Error while connecting to MySQL: {e}")
        return None

async def load_sent_entries_from_db(connection):
    try:
        async with connection.cursor() as cursor:
            await cursor.execute("SELECT url, subject, message_id FROM sent_rss")
            rows = await cursor.fetchall()
            return {(row[0], row[1], row[2]) for row in rows}
    except Exception as e:
        logging.error(f"Error fetching sent entries: {e}")
        return set()

async def save_sent_entry_to_db(connection, url, subject, message_id):
    try:
        async with connection.cursor() as cursor:
            await cursor.execute(
                "INSERT IGNORE INTO sent_rss (url, subject, message_id) VALUES (%s, %s, %s)", 
                (url, subject, message_id)
            )
            await connection.commit()
            logging.info(f"Saved sent entry: {url}, {subject}, {message_id}")
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
