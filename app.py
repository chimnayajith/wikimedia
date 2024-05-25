from flask import Flask
from datetime import datetime
from crontab import CronTab

app = Flask(__name__)

@app.route('/fetch/', methods = ['GET'])
def fetch():
	return "Hello, world!"

app.run(port=3000)