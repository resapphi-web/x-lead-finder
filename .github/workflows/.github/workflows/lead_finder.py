import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
from datetime import datetime, timedelta

# -------------------------------
# Настройки
# -------------------------------
BEARER_TOKEN = os.getenv("X_BEARER_TOKEN")  # X API ключ
SHEET_FILE = "path_to_your_service_account.json"  # credentials
LEADS_SHEET_NAME = "leads"
KEYWORDS_SHEET_NAME = "keywords"
MAX_LEADS_PER_DAY = 50
MAX_CAPPERS = 30

# -------------------------------
# Google Sheets
# -------------------------------
scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name(SHEET_FILE, scope)
client = gspread.authorize(creds)

leads_sheet = client.open("YourGoogleSheetName").worksheet(LEADS_SHEET_NAME)
keywords_sheet = client.open("YourGoogleSheetName").worksheet(KEYWORDS_SHEET_NAME)

# Получаем уже существующих пользователей
existing_usernames = set(leads_sheet.col_values(2))  # B колонка: twitter_username

# Получаем ключевые слова
keywords_data = keywords_sheet.get_all_records()
search_queries = {}
for row in keywords_data:
    search_queries.setdefault(row['type'].lower(), []).append(row['keyword'])

# -------------------------------
# Функции для X API
# -------------------------------
headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}

def search_tweets(query, max_results=50):
    url = "https://api.twitter.com/2/tweets/search/recent"
    params = {
        "query": f'({query}) lang:en -is:retweet',
        "tweet.fields": "author_id,created_at",
        "max_results": min(max_results, 100)
    }
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code != 200:
        print("Error:", resp.status_code, resp.text)
        return []
    return resp.json().get("data", [])

def get_user_info(user_id):
    url = f"https://api.twitter.com/2/users/{user_id}"
    params = {
        "user.fields": "username,description,public_metrics,url,verified,created_at"
    }
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code != 200:
        return None
    return resp.json().get("data")

# -------------------------------
# Фильтрация
# -------------------------------
def is_valid_user(user, type_):
    # followers filter
    followers = user['public_metrics']['followers_count']
    if type_ == 'capper' and followers < 50000:
        return False

    # ссылка в профиле
    profile_url = user.get('url')
    if not profile_url:
        return False

    return True

# -------------------------------
# Основной процесс
# -------------------------------
cappers_added = 0
other_added = 0
total_added = 0

today_str = datetime.utcnow().strftime("%Y-%m-%d")

for type_, keywords in search_queries.items():
    for kw in keywords:
        if total_added >= MAX_LEADS_PER_DAY:
            break

        tweets = search_tweets(kw, max_results=50)

        for tweet in tweets:
            if total_added >= MAX_LEADS_PER_DAY:
                break

            user_id = tweet['author_id']
            user_info = get_user_info(user_id)
            if not user_info:
                continue

            username = user_info['username']
            if username in existing_usernames:
                continue

            # last tweet date filter
            tweet_date = datetime.strptime(tweet['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            if tweet_date < datetime.utcnow() - timedelta(days=60):
                continue

            # фильтруем
            if not is_valid_user(user_info, type_):
                continue

            # считаем лимиты
            if type_ == 'capper':
                if cappers_added >= MAX_CAPPERS:
                    continue
                cappers_added += 1
            else:
                other_added += 1

            total_added += 1

            # добавляем в Google Sheet
            leads_sheet.append_row([
                today_str,
                username,
                f"https://x.com/{username}",
                type_,
                user_info['public_metrics']['followers_count'],
                tweet_date.strftime("%Y-%m-%d"),
                user_info.get('description', ''),
                user_info.get('url', ''),
                '',  # website_domain
                '',  # email
                ''   # notes
            ])
            existing_usernames.add(username)
            print(f"Added {type_}: {username}")

print("Done. Total added:", total_added)
