import json
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime
import pymongo
import math


def connect_mongo():
        try:
                client = MongoClient('mongodb://root:root@ds155577.mlab.com:55577/geo_coord_db')
                print("Server connected")
                return client
        except ConnectionFailure:
                print ("Server not available")
                return None

def get_max_sequence_dict(bool):
    client = connect_mongo()
    db = client.get_default_database()
    seq_table = db['sequence_table']
    num1 = 0
    for val in seq_table.find().sort('seq_no', pymongo.DESCENDING):
        num = ((val['seq_no']))
        num1 = int(num) + 1
        # print(num1)
        break
    if bool:
        seq_table.insert_one({"seq_no": num1})
    # print(num1)
    # return 17
    return num1


def insert_to_mongo(data):
    print("testing")
    log_details = data

    src_lat_dict = {}
    src_long_dict = {}

    src_lat_dict.update({'src_lat': log_details['src_lat']})

    src_long_dict.update({'src_long': log_details['src_long']})
    seq_dict = get_max_sequence_dict(1)
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


def get_count_phone_percentage():
    client = connect_mongo()
    db = client.get_default_database()
    coord_table = db['coord_table']
    val = get_max_sequence_dict(0)
    # print(val)
    prev_val = val - 1
    coord_table_count = coord_table.aggregate(
        [
            {"$match": {"sequence_no": val - 1}},
            {"$group": {
                "_id": {"brand": "$brand"},
                "count": {
                    "$sum": 1
                }}}
        ]
    )
    sequence_no_count = coord_table.find(
        {"sequence_no": prev_val}
    ).count()

    brand_percentage_list = []
    print(sequence_no_count)

    brand_count_dict = {}
    for value in coord_table_count:
        # temp_count_val = val['count']
        # print(temp_count_val)

        temp_val = {value['_id']['brand']: value['count']}
        brand_count_dict.update(temp_val)

    i = 0
    for k, v in brand_count_dict.items():
        brand = k
        percentage = math.floor((v * 100) / sequence_no_count)
        temp_list_brand1 = {"percentage": percentage, "brand": brand}

        brand_percentage_list.append(temp_list_brand1)

    return brand_percentage_list


def get_distance():
    client = connect_mongo()
    db = client.get_default_database()
    coord_table = db['coord_table']
    val = get_max_sequence_dict(0)

    prev_val = val - 1
    coord_table_distance = coord_table.aggregate(
        [
            {"$match": {"sequence_no": prev_val}},
            {"$group": {
                "_id": {"distance": {"$ceil":"$distance"}},
                "count": {
                    "$sum": 1
                }}}
        ]
)

    coord_dist_dict = []
    distance_dict_count = {}
    distance_limit_count = 0

    for value in coord_table_distance:
        temp_distance = value['_id']['distance']
        temp_count = value['count']

        if temp_distance > 9:
            distance_limit_count += 1
            continue

        temp_val = {temp_distance: temp_count}
        distance_dict_count.update(temp_val)

    if distance_limit_count:
        distance_dict_count.update({"'>9'": distance_limit_count})

    for k,v in distance_dict_count.items():
        distance = k
        count = v
        temp_list_distance = { "distance" : distance,"count":count}
        coord_dist_dict.append(temp_list_distance)

    return coord_dist_dict