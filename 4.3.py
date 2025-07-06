from pymongo import MongoClient
import json
import os
from datetime import datetime
from turtledemo.clock import current_day
from pprint import pprint

# Подключение к MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["my_database"]
collection = db["user_events"]

# Создаем архивную коллекцию
archived_users = db['archived_users']

# Список документов
data = [
    {
        "user_id": 123,
        "event_type": "purchase",
        "event_time": datetime(2024, 1, 20, 10, 0, 0),
        "user_info": {
            "email": "user1@example.com",
            "registration_date": datetime(2023, 12, 1, 10, 0, 0)
        }
    },
    {
        "user_id": 124,
        "event_type": "login",
        "event_time": datetime(2024, 1, 21, 9, 30, 0),
        "user_info": {
            "email": "user2@example.com",
            "registration_date": datetime(2023, 12, 2, 12, 0, 0)
        }
    },
    {
        "user_id": 125,
        "event_type": "signup",
        "event_time": datetime(2024, 1, 19, 14, 15, 0),
        "user_info": {
            "email": "user3@example.com",
            "registration_date": datetime(2023, 12, 3, 11, 45, 0)
        }
    },
    {
        "user_id": 126,
        "event_type": "purchase",
        "event_time": datetime(2024, 1, 20, 16, 0, 0),
        "user_info": {
            "email": "user4@example.com",
            "registration_date": datetime(2023, 12, 4, 9, 0, 0)
        }
    },
    {
        "user_id": 127,
        "event_type": "login",
        "event_time": datetime(2024, 1, 22, 10, 0, 0),
        "user_info": {
            "email": "user5@example.com",
            "registration_date": datetime(2023, 12, 5, 10, 0, 0)
        }
    },
    {
        "user_id": 128,
        "event_type": "signup",
        "event_time": datetime(2024, 1, 22, 11, 30, 0),
        "user_info": {
            "email": "user6@example.com",
            "registration_date": datetime(2023, 12, 6, 13, 0, 0)
        }
    },
    {
        "user_id": 129,
        "event_type": "purchase",
        "event_time": datetime(2024, 1, 23, 15, 0, 0),
        "user_info": {
            "email": "user7@example.com",
            "registration_date": datetime(2023, 12, 7, 8, 0, 0)
        }
    },
    {
        "user_id": 130,
        "event_type": "login",
        "event_time": datetime(2024, 1, 23, 16, 45, 0),
        "user_info": {
            "email": "user8@example.com",
            "registration_date": datetime(2023, 12, 8, 10, 0, 0)
        }
    },
    {
        "user_id": 131,
        "event_type": "purchase",
        "event_time": datetime(2024, 1, 24, 12, 0, 0),
        "user_info": {
            "email": "user9@example.com",
            "registration_date": datetime(2023, 12, 9, 14, 0, 0)
        }
    },
    {
        "user_id": 132,
        "event_type": "signup",
        "event_time": datetime(2024, 1, 24, 18, 30, 0),
        "user_info": {
            "email": "user10@example.com",
            "registration_date": datetime(2023, 12, 10, 10, 0, 0)
        }
    }
]

# Заливка данных в коллекцию
collection.insert_many(data)

# Вводим временные отрезки
from dateutil.relativedelta import relativedelta
today = datetime.today().strftime("%Y-%m-%d")
one_month_ago = datetime.now() - relativedelta(months=1)
fourteen_days_ago = datetime.now() - relativedelta(days=14)

# Создаем список пользователей для архивации
archiving_users = []

# Находим искомых пользователей и добаляем их в список
for user in collection.find({"user_info.registration_date": {"$lt": one_month_ago}, "event_time": {"$lt": fourteen_days_ago} }, {} ):
    archiving_users.append(user)

# Создаем список пользователей для отчета
archiving_users_ids = []
for user in archiving_users:
    archiving_users_ids.append(user["user_id"])

# Создаем отчет
report = {
    "date": today,
    "archiving_users_count": len(archiving_users),
    "archiving_users_ids": archiving_users_ids
}
file_name = os.path.join(f"{today}.json")
with open(file_name, "w") as file:
    json.dump(report, file, indent=2)

# Переносим пользователей в архив
archived_users.insert_many(archiving_users)

#Удаляем пользователей из основной коллекции
collection.delete_many({"user_info.registration_date": {"$lt": one_month_ago}, "event_time": {"$lt": fourteen_days_ago} })

