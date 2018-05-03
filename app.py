from flask import Flask, request, jsonify
import database as dbConn
import json

app = Flask(__name__)

@app.route('/')
def homepage():
    return "here"


@app.route("/uploadlog/",methods = ['GET', 'POST', 'DELETE'])
def getPostDataUploadLog():
    if request.method == 'POST':
        try:
            content = request.json
        except ValueError:
            return jsonify({
                "status":"Fail"
            })
        dbConn.insert_to_mongo(content)

        return jsonify({
            "status":"OK",
            "message":"successful"
        })

if __name__ == '__main__':
    app.run(debug=True, use_reloader=True)

