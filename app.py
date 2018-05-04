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

