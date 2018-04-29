import json
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime


def connect_mongo(data):
        try:
                client = MongoClient('mongodb://root:root@ds155577.mlab.com:55577/geo_coord_db')
                print("Server connected")
                client.server_info()
                db = client.get_default_database()
                coord_db = db['coord_table']
                coord_db.insert_one(data)
        except ConnectionFailure:
                print ("Server not available")
                return None


def insert_to_mongo(data):
    print("testing")
    log_details = data

    user_dict = {}

    for value in log_details:
            if value == 'data':
                     for sub_value in log_details[value]:
                            for details in sub_value:
                                    sub_value_dict = { details: sub_value[details]}
                                    user_dict.update( sub_value_dict )
            elif value == 'src_lat':
                    src_lat_dict = {'src_lat' : log_details[value]}
                    user_dict.update(src_lat_dict)
            elif value == 'src_long':
                    src_long_dict = {'src_long' : log_details[value]}
                    user_dict.update(src_long_dict )

    date_time = datetime.now().strftime('%Y%m%d%H%M%S')
    user_dict.update({'date_time': date_time})
    connect_mongo(user_dict)
