import sys
from flask import Flask,request,jsonify
import json
from datetime import datetime
import os


local_model_path = "best.pt"
#app = Flask(__name__)

folder_path = 'D:/backup/importantes/pythoncodigos/projeto_bolt_bolsa2/MLSpikeDetector/DATA/META'
def error_line(message):
    print(f'❌❌❌❌❌❌')
    print(message)


def get_prediction_status(file_path):
    # Define the file path
   
    try:
        # Read the existing JSON data
        with open(file_path, 'r') as file:
            data = json.load(file)
            print(data)

    except FileNotFoundError:
        # If the file doesn't exist, return "Not ready"
        error_line("get_prediction_status not found")
        return "Not ready"

    # Check if there are any predictions in the data
    if data:
        latest_prediction_time_str = data.get('prediction_time')
        latest_prediction_str = data.get('prediction')
        print("prediction_time ",latest_prediction_time_str)
        print("prediction ",latest_prediction_str )
        if latest_prediction_time_str and latest_prediction_str:
            # Convert prediction time string to datetime object
            latest_prediction_time = datetime.strptime(latest_prediction_time_str, "%Y-%m-%d %H:%M:%S")
            latest_prediction_time = latest_prediction_time.strftime("%Y-%m-%d %H:%M:%S")
            # Compare the prediction time with the current time
            current_time = datetime.now()
            current_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

            print("current_time ",current_time)
            #  current_time < latest_prediction_time
            if current_time < latest_prediction_time:
                # If the current time is greater than the prediction time, return the prediction
                return latest_prediction_str
            else:
                # If the current time is not greater than the prediction time, return "Not ready"
                print(1)
                return "Not ready"
        else:
            # If prediction data is incomplete, return "Not ready"
            print(2)
            return "Not ready"
    else:
        # If there are no predictions in the data, return "Not ready"
        print(3)
        return "Not ready"

def get_val(val,request_json,request_args):
        if request_json and val in request_json:
            x = request_json[val]
        elif request_args and val in request_args:
            x = request_args[val]
        else:
            x = val
        return x

def deleting_files(folder_path):
    if not os.path.exists(folder_path):
        print(f"A pasta '{folder_path}' não existe.")
        return

    try:
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            if os.path.isfile(file_path):
                try:
                    os.remove(file_path)
                    print(f"Arquivo   deletado: {file_path}")
                except OSError as e:
                    print(f"Erro ao deletar '{file_path}': {e}")
    except OSError as e:
        print(f"Erro ao acessar a pasta '{folder_path}': {e}")

#@app.route("/predict", methods = ['GET'])

def read_prediction():
    """
    This function reads the prediction from data.json and returns the prediction as a response
    """
    try:
        symbol = request.args.get("symbol")
        Timeframe = request.args.get("Timeframe")
        print("Symbol ", symbol)
        print("Timeframe Number ", Timeframe)
        file_path = "./"+symbol+Timeframe+".json"
        print("The file path is ",file_path)
        prediction = get_prediction_status(file_path)
        print("The prediction is ",prediction)
        if prediction is not None:
            
            return jsonify(prediction)
            
        else:
            # If prediction is None, return an appropriate error response
            return jsonify({"error": f'An error occurred'})  

    except Exception as e:
        error_line(e)
        print("read_prediction")
        # If an exception occurs, return an appropriate error response
        return jsonify({"error": f'An error occurred : {e}'})

#options menu

while True:
    option = input('0 - EXIT\n\n'
                   '1 - Start Forecast\n\n'
                   '2 - UPDATE DATABASE\n\n'
                   '3 - CREATE DATABASE\n\n'
                   
                   'OPTION: ')
    match option:
        case '0':
            deleting_files(folder_path)
            print('EXITING THE PROGRAM...')
            exit()
        case '1':
            sticker = input('Enter the name of the sticker: ')
            time = input(int('What is the timeframe? Ex.: 1, 5, 15'))
            time2 = input(str('Minutes or Hours?'))
        case _:
            print('INVALID OPTION!')


# if __name__ == '__main__':

#     app.run(debug=True)
