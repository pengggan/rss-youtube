import os
import json
import requests
from feedparser import parse

RSS_FEEDS = [
    'https://rsshub.app/guancha/headline',
    'https://rsshub.app/zaobao/znews/china',
    'https://www.freedidi.com/feed',
    'https://p3terx.com/feed',
    'https://sspai.com/feed',
    'https://www.digihubs.xyz/feeds/posts/default?alt=rss',
    'https://blog.090227.xyz/atom.xml',
    'https://hunsh.net/atom.xml',
    'https://rsshub.app/youtube/playlist/PLRQMDFCUMjJW_R29PyDKbILE2Nj6mC3X3',
    'https://rsshub.app/youtube/playlist/PLvrTMNP6Iw6oTPlmRHvjAWiCeQpwHK6yG',
    'https://www.youtube.com/@bulianglin/videos',
    'https://www.youtube.com/@IamJackLiu/videos',
    'https://www.youtube.com/@TchLiyongle/videos',
    'https://www.youtube.com/@%E4%B8%AD%E6%8C%87%E9%80%9A/videos',
    'https://rsshub.app/fortunechina',
    'http://blog.caixin.com/feed',    
    'http://news.stockstar.com/rss/xml.aspx?file=xml/stock/2.xml',
    'https://36kr.com/feed',
    'https://www.huxiu.com/rss/0.xml',
    'https://www.chinanews.com.cn/rss/finance.xml',
    'http://cn.reuters.com/rssfeed/cnintlbiznews',
    'https://xueqiu.com/hots/topic/rss',
    'https://qks.sufe.edu.cn/J/CJYJ/RSS/CN',
    'https://www.economist.com/sections/china/rss.xml',
    'https://rsshub.app/youtube/playlist/PLRQMDFCUMjJW_R29PyDKbILE2Nj6mC3X3',
    'https://rsshub.app/youtube/playlist/PLvrTMNP6Iw6oTPlmRHvjAWiCeQpwHK6yG',
    'https://www.youtube.com/@bulianglin/videos',
    'https://www.youtube.com/@IamJackLiu/videos',
    'https://www.youtube.com/@TchLiyongle/videos',
    'https://www.youtube.com/@%E4%B8%AD%E6%8C%87%E9%80%9A/videos',
    # 添加更多 RSS 源
]

TELEGRAM_API_URL = f"https://api.telegram.org/bot{os.environ['TELEGRAM_BOT_TOKEN']}/sendMessage"

def load_sent_entries():
    try:
        with open('sent_entries.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return []

def save_sent_entries(entries):
    with open('sent_entries.json', 'w') as f:
        json.dump(entries, f)

def send_message(chat_id, text):
    payload = {
        'chat_id': chat_id,
        'text': text,
        'parse_mode': 'Markdown'
    }
    requests.post(TELEGRAM_API_URL, json=payload)

def main():
    sent_entries = load_sent_entries()
    new_entries = []

    for feed in RSS_FEEDS:
        feed_data = parse(feed)
        for entry in feed_data.entries:
            if entry.link not in sent_entries:
                message = f"*{entry.title}*\n{entry.link}"
                send_message(os.environ['TELEGRAM_CHAT_ID'], message)
                new_entries.append(entry.link)

    sent_entries.extend(new_entries)
    save_sent_entries(sent_entries)

if __name__ == "__main__":
    main()
