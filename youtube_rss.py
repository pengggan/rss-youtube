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
TELEGRAM_BOT_YOUTUBE = os.getenv("TELEGRAM_BOT_YOUTUBE")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
ALLOWED_CHAT_IDS = os.getenv("ALLOWED_CHAT_IDS").split(",")

TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_YOUTUBE}/sendMessage"

# RSS 源列表
RSS_FEEDS = [
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCUNciDq-y6I6lEQPeoP-R5A', # 苏恒观察
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCMtXiCoKFrc2ovAGc1eywDg', # 一休
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCNiJNzSkfumLB7bYtXcIEmg', # 真的很博通
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCii04BCvYIdQvshrdNDAcww', # 悟空的日常
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCJMEiNh1HvpopPU3n9vJsMQ', # 理科男士
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCYjB6uufPeHSwuHs8wovLjg', # 中指通
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCSs4A6HYKmHA2MG_0z-F0xw', # 李永乐老师
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCZDgXi7VpKhBJxsPuZcBpgA', # 可恩Ke En
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCSYBgX9pWGiUAcBxjnj6JCQ', # 郭正亮頻道
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCbCCUH8S3yhlm7__rhxR2QQ', # 不良林
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCXkOTZJ743JgVhJWmNV8F3Q', # 寒國人
    'https://www.youtube.com/feeds/videos.xml?channel_id=UC000Jn3HGeQSwBuX_cLDK8Q', # 我是柳傑克
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCG_gH6S-2ZUOtEw27uIS_QA', # 7Car小七車觀點
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCJ5rBA0z4WFGtUTS83sAb_A', # POP Radio聯播網 官方頻道
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCiwt1aanVMoPYUt_CQYCPQg', # 全球大視野
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCQoagx4VHBw3HkAyzvKEEBA', # 科技共享<
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

async def process_feed(session, feed, sent_youtube, connection):
    feed_data = await fetch_feed(session, feed)
    if feed_data is None:
        return []

    new_entries = []
    for entry in feed_data.entries:
        if entry.link not in sent_youtube:
            message = f"*{entry.title}*\n{entry.link}"
            for chat_id in ALLOWED_CHAT_IDS:
                await send_message(session, chat_id, message)
            new_entries.append(entry.link)
            await save_sent_entry_to_db(connection, entry.link)
            sent_youtube.add(entry.link)
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

async def load_sent_youtube_from_db(connection):
    try:
        async with connection.cursor() as cursor:
            await cursor.execute("SELECT data FROM sent_youtube")
            rows = await cursor.fetchall()
            return {json.loads(row[0])['url'] for row in rows}
    except Exception as e:
        logging.error(f"Error fetching sent entries: {e}")
        return set()

async def save_sent_entry_to_db(connection, url):
    try:
        async with connection.cursor() as cursor:
            await cursor.execute("INSERT IGNORE INTO sent_youtube (data) VALUES (%s)", (json.dumps({"url": url}),))
            await connection.commit()
            logging.info(f"Saved sent entry: {url}")
    except Exception as e:
        logging.error(f"Error saving sent entry: {e}")

async def main():
    connection = await connect_to_db()
    if connection is None:
        logging.error("Failed to connect to the database. Exiting.")
        return

    sent_youtube = await load_sent_youtube_from_db(connection)
    new_entries = []

    async with aiohttp.ClientSession() as session:
        tasks = [process_feed(session, feed, sent_youtube, connection) for feed in RSS_FEEDS]
        results = await asyncio.gather(*tasks)
        for result in results:
            if result:
                new_entries.extend(result)

    connection.close()

if __name__ == "__main__":
    asyncio.run(main())
