from flask import Flask, request, jsonify
import database as dbConn
import json

app = Flask(__name__)

@app.route('/')
def homepage():
    return "here"


@app.route("/brand")
def brand_details():
    brand_percentage_details = dbConn.get_count_phone_percentage()
    return jsonify({"data":brand_percentage_details})


@app.route("/distance")
def distance_details():
    distance_details = dbConn.get_distance()
    return jsonify({"name":"bar chart data","data":distance_details})

@app.route("/brandDistribution/<sequence_id>")
def brand_distribution(sequence_id):
    sequence_id = sequence_id
    brand_distribution_data = dbConn.get_brand_distribution(sequence_id,False)
    return jsonify({"sequence":sequence_id,"data":brand_distribution_data})

@app.route("/NearbyWiFiDevicesCount/<sequence_id>")
def nearby_wifi_devices_count(sequence_id):

    wifi_details = dbConn.get_nearby_wifi_count(sequence_id,False)
    return jsonify({"sequence":sequence_id,"data":wifi_details})

@app.route("/latestNearbyWiFiDevicesCount/")
def latest_nearby_wifi_devices_count():
    wifi_details = dbConn.get_nearby_wifi_count(None, False)
    max_sequence_id = dbConn.get_max_sequence_dict() - 1
    return jsonify({"sequence": max_sequence_id, "data": wifi_details})


@app.route("/uploadlog/",methods = ['GET', 'POST', 'DELETE'])
def getPostDataUploadLog():
    if request.method == 'POST':

        content = request.json

        dbConn.insert_to_mongo(content)

        return jsonify({
            "status":"OK",
            "message":"successful"
        })

if __name__ == '__main__':
    app.run(debug=True, use_reloader=True)

