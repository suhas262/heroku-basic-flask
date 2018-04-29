from flask import Flask, request
from datetime import datetime
app = Flask(__name__)

@app.route('/')
def homepage():

    return "OK"


@app.route("/uploadlog/",methods = ['GET', 'POST', 'DELETE'])
def getPostDataUploadLog():
    if request.method == 'POST':
        content = request.data
        return "OK"

if __name__ == '__main__':
    app.run(debug=True, use_reloader=True)

