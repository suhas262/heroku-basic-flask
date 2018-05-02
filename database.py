import json
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime
import pymongo



def connect_mongo():
        try:
                client = MongoClient('mongodb://root:root@ds155577.mlab.com:55577/geo_coord_db')
                print("Server connected")
                return client
        except ConnectionFailure:
                print ("Server not available")
                return None

def get_max_sequence_dict():
    client = connect_mongo()
    db = client.get_default_database()
    seq_table = db['sequence_table']
    num1 = 0
    for val in seq_table.find().sort('seq_no', pymongo.DESCENDING):
        num = ((val['seq_no']))
        num1 = int(num) + 1
        # print(num1)
        break
    seq_table.insert_one({"seq_no": num1})
    # print(num1)
    return num1


def insert_to_mongo(data):
    print("testing")
    log_details = data

    src_lat_dict = {}
    src_long_dict = {}

    src_lat_dict.update({'src_lat': log_details['src_lat']})

    src_long_dict.update({'src_long': log_details['src_long']})
    seq_dict = get_max_sequence_dict()
    # print(seq_dict)

    # next_sequence_no, sequence_no = get_max_sequence()
    seq_dict1 = {"sequence_no": seq_dict}

    # client = connect_mongo()
    # db = client.get_default_database()
    #
    # seq_table = db['sequence_table']


    for value in log_details['data']:
        user_dict = {}
        user_dict.update(value)
        user_dict.update(src_long_dict)
        user_dict.update(src_lat_dict)
        date_time = datetime.now().strftime('%Y%m%d%H%M%S')
        user_dict.update({'date_time': date_time})
        user_dict.update(seq_dict1)
        # print(user_dict)
        client = connect_mongo()
        db = client.get_default_database()
        # seq_table = db['se']
        # seq_table.insert(seq_dict)
        coord_table = db['coord_table']
        # print(db)
        coord_table.insert_one(user_dict)

