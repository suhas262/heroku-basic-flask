import json
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime
import pymongo
import math
from operator import itemgetter

def connect_mongo():
        try:
                client = MongoClient('mongodb://root:root@ds155577.mlab.com:55577/geo_coord_db')
                # print("Server connected")
                return client
        except ConnectionFailure:
                print ("Server not available")
                return None

def get_coord_table():
    client = connect_mongo()
    db = client.get_default_database()
    coord_table = db['coord_table']
    return coord_table

def get_sequence_table():
    client = connect_mongo()
    db = client.get_default_database()
    seq_table = db['sequence_table']
    return seq_table

def get_sequence_number_data_distance(seq_no):
    coord_table = get_coord_table()
    coord_table_distance = coord_table.aggregate(
        [
            {"$match": {"sequence_no": seq_no}},
            {"$group": {
                "_id": {"distance": {"$ceil": "$distance"}},
                "count": {
                    "$sum": 1
                }}}
        ]
    )
    return coord_table_distance

def get_sequence_number_data_brand(seq_no):
    coord_table = get_coord_table()
    print(seq_no)
    coord_table_count = coord_table.aggregate(
        [
            {"$match": {"sequence_no": seq_no}},
            {"$group": {
                "_id": {"brand": "$brand"},
                "count": {
                    "$sum": 1
                }}}

        ]
    )
    return coord_table_count

def get_sequence_number(seq_no):
    if seq_no is None:
        seq_no = get_max_sequence_dict() - 1
        return seq_no

    seq_no = int(seq_no)
    return seq_no
    # return 17


def get_sequence_number_count(seq_no = None):

    sequence_no_count = get_coord_table().find(
        {"sequence_no": get_sequence_number(seq_no)}
    ).count()

    return sequence_no_count

def get_max_sequence_dict():

    seq_table = get_sequence_table()
    num1 = 0
    for val in seq_table.find().sort('seq_no', pymongo.DESCENDING):
        num = ((val['seq_no']))
        num1 = int(num) + 1
        # print(num1)
        break


    # return 17
    return num1


def get_records(seq_no):
        seq_no = get_sequence_number(seq_no)
        coord_table = get_coord_table()
        coord_table_record = coord_table.find(
            {
                "sequence_no": seq_no
            }
        )

        coord_table_dict = []
        for value in coord_table_record:
            temp_record_list = {}
            for record in value:
                if record != '_id' and record != 'sequence_no':
                    # temp_list = { record : value[record]}
                    temp_record_list.update(({record: value[record]}))
                    # coord_table_dict.append(temp_list)
            coord_table_dict.append(temp_record_list)
        return coord_table_dict

def insert_to_mongo(data):
    print("testing")
    log_details = data

    src_lat_dict = {}
    src_long_dict = {}

    src_lat_dict.update({'src_lat': log_details['src_lat']})

    src_long_dict.update({'src_long': log_details['src_long']})
    seq_dict = get_max_sequence_dict()
    seq_table = get_sequence_table()

    seq_table.insert_one({"seq_no": seq_dict})
    print(seq_dict)

    seq_dict1 = {"sequence_no": seq_dict}



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


def get_count_phone_percentage(seq_no = None):
    seq_no = get_sequence_number(seq_no)
    coord_table_count = get_sequence_number_data_brand(seq_no)
    sequence_no_count = get_sequence_number_count(seq_no)

    brand_percentage_list = []
    print(sequence_no_count)
    brand_count_dict = {}

    for value in coord_table_count:
        temp_val = {value['_id']['brand']: value['count']}
        brand_count_dict.update(temp_val)

    for k, v in brand_count_dict.items():
        brand = k
        percentage = math.floor((v * 100) / sequence_no_count)
        temp_list_brand1 = {"percentage": percentage, "brand": brand}
        brand_percentage_list.append(temp_list_brand1)

    brand_percentage_list = sorted(brand_percentage_list, key=itemgetter('percentage'), reverse=True)
    length_list = len(brand_percentage_list)

    if length_list >10:
        sum_index = 0
        counter = 0
        for index in range(10,length_list):
            sum_index += brand_percentage_list[index]['percentage']
            counter += 1
        del brand_percentage_list[:-counter]
        brand_percentage_list.append({ "Others": sum_index })

    return brand_percentage_list


def get_distance(seq_no = None):
    seq_no = get_sequence_number(seq_no)
    print(seq_no)

    coord_table_distance = get_sequence_number_data_distance(seq_no)

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


def get_brand_distribution(seq_no, latest = False):
    if latest:
        return get_count_phone_percentage(get_sequence_number(get_max_sequence_dict()-1))
    return  get_count_phone_percentage(seq_no)

def get_nearby_wifi_count(seq_no = None, latest = False):
    if latest:
        return get_distance(get_sequence_number(get_max_sequence_dict()-1))
    return get_distance(seq_no)


def get_latest_records(seq_no=None):
    return get_records(seq_no)
