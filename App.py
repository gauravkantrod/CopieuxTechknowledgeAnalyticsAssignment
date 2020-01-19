from flask import Flask
from flask_pymongo import PyMongo, MongoClient
from flask import jsonify, request
from bson.json_util import dumps
import json
from datetime import datetime
import time
from Employee import Employee

app = Flask(__name__)

client = MongoClient("mongodb://127.0.0.1:27017")
db = client.techknowledgeAPI
employees = db.Employees

@app.route('/')
@app.route('/index')
def app_check():
    return "Application is up and running."

@app.route("/employees", methods=['GET'])
def get_all_data():
    chunk_number = request.args.get('chunk')
    if chunk_number == '0':
        return "Chunk number is 0."

    if chunk_number is not None:
        end_emp_code = 20*int(chunk_number)
        start_emp_code = 20*int(chunk_number)-19

        emp_chunk_lst = []
        for i in range(start_emp_code, end_emp_code+1):
            emp_id = "E"+str(i)
            for emp_data in employees.find({'employee_code':emp_id}):
                emp_data.pop('_id', None)
                emp_chunk_lst.append(emp_data)

        if len(emp_chunk_lst) == 0:
            return "No employee data found."
        else:
            return dumps(emp_chunk_lst)

    data_without_waltzz = []
    data_with_waltzz = []
    data_with_less_than_14_days = []
    count = 1
    for d in employees.find():
        count += 1
        d.pop('_id', None)
        emp_time = d['date']

        present_timestamp = milliseconds = int(round(time.time() * 1000))

        dt_obj = datetime.strptime(emp_time,'%d/%m/%Y %H:%M:%S')
        emp_timestamp = dt_obj.timestamp() * 1000
        time_since_updation = int(present_timestamp - emp_timestamp)

        if time_since_updation >  1209600000:
            if d['department'] != "Waltzz":
                data_without_waltzz.append(d)
        elif d['department'] == "Waltzz":
            data_with_waltzz.append(d)
        elif d['department'] != "Waltzz" and time_since_updation < 1209600000:
            data_with_less_than_14_days.append(d)

    data_without_waltzz = sorted(data_without_waltzz, key=lambda i: i['score'], reverse=True)

    expected_response = []
    len_w = len(data_without_waltzz)
    for i in range(1, count):
        if i%5 != 0:
            if len(data_without_waltzz) != 0:
                expected_response.append(data_without_waltzz.pop(0))
        else:
            if len(data_with_waltzz) != 0:
                expected_response.append(data_with_waltzz.pop(0))
            if len(data_with_waltzz) != 0:
                expected_response.append(data_with_waltzz.pop(0))
            if len(data_with_less_than_14_days) != 0:
                expected_response.append(data_with_less_than_14_days.pop(0))
            if len(data_with_less_than_14_days) != 0:
                expected_response.append(data_with_less_than_14_days.pop(0))

    return dumps(expected_response)


@app.route('/insert_data', methods=['POST'])
def insert_data():
    data = request.json
    employees.insert_many(data)

    return 'OK'

def get_timesptamp_millis():
    current_time = time.time()
    time_millis = int(round(current_time * 1000))

    return time_millis

if __name__ == '__main__':
    app.run(debug=True)