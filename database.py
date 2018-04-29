import json
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime


def connect_mongo():
        try:
                client = MongoClient('mongodb://root:root@ds155577.mlab.com:55577/geo_coord_db')
                print("Server connected")
                return client
        except ConnectionFailure:
                print ("Server not available")
                return None


def insert_to_mongo(data):
    print("testing")
    log_details = data

    src_lat_dict = {}
    src_long_dict = {}

    src_lat_dict.update({'src_lat': log_details['src_lat']})

    src_long_dict.update({'src_long': log_details['src_long']})

    for value in log_details['data']:
        user_dict = {}
        user_dict.update(value)
        user_dict.update(src_long_dict)
        user_dict.update(src_lat_dict)
        date_time = datetime.now().strftime('%Y%m%d%H%M%S')
        user_dict.update({'date_time': date_time})
        client = connect_mongo()
        db = client.get_default_database()
        coord_table = db['coord_table']
        print(db)
        coord_table.insert(user_dict)
