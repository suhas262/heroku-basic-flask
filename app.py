from flask import Flask, request
import database as dbConn

app = Flask(__name__)

@app.route('/')
def homepage():
    return "here"


@app.route("/uploadlog/",methods = ['GET', 'POST', 'DELETE'])
def getPostDataUploadLog():
    if request.method == 'POST':
        content = request.json
        print("test")
        dbConn.insert_to_mongo(content)
        return "OK"

if __name__ == '__main__':
    app.run(debug=True, use_reloader=True)

