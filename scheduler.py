import sched
import sys
import time
import MetaTrader5 as mt5
from datetime import datetime
import pandas as pd
from save_data import authenticate_to_mt5,create_folder, wamaitha_account, wamaitha_password, wamaitha_server
# from make_predictions import load_model, local_model_path
import json
import cv2
from datetime import datetime,timedelta
import numpy as np
from ultralyticsplus import YOLO,render_result
from sklearn.linear_model import Ridge
from sklearn.preprocessing import MinMaxScaler
import joblib

bars = 300
images_folder_for_symbol=f'LIVEDATA'
local_model_path_yolov = "best.pt"
local_model_path_ridge = "modelo_ridge.pkl"
symbol =''
time_schedule = 0
file_path = ''
scheduler = sched.scheduler(time.time, time.sleep)
def load_model_yolov(model_path):
    """
    Loads the  model for use in our project
    @returns model
    """
    try:
        print('Loading model ...')
        model = YOLO(model_path)
        # set model parameters
        model.overrides['conf'] = 0.25  # NMS confidence threshold
        model.overrides['iou'] = 0.45  # NMS IoU threshold
        model.overrides['agnostic_nms'] = False  # NMS class-agnostic
        model.overrides['max_det'] = 5  # maximum number of detections per image
        return model
    except Exception as e:
        error_line(e)

def load_model_ridge():

    model = joblib.load('modelo_ridge.pkl')
    return model

def error_line(message):
    print(f'❌❌❌❌❌❌')
    print(message)


def get_prediction(model_yolov, model_ridge,df,bars,images_folder=images_folder_for_symbol):
    """"
    Use the data retreived from the live data, draw the bars on a graph, then save the file to the images folder.
    
    @params model: The YOLO model to be used for prediction
    @params df: The YOLO model to be used for prediction
    @params bars: The YOLO model to be used for prediction
    @params images_folder: The YOLO model to be used for prediction
    """  
    try:

        height = 500
        width = 500        
        # Normalise high and low
        columns_to_normalize = ['high','low']
        # Min-Max scaling only on selected columns
        df[columns_to_normalize] = (df[columns_to_normalize] - df[columns_to_normalize].min()) / (df[columns_to_normalize].max() - df[columns_to_normalize].min())
        normalized_high_values = df['high'].values
        normalized_low_values = df['low'].values
        print("Normalized high ", normalized_high_values[0])
        # Calculate scaling factors for the 'High' and 'Low' values
        scaled_high_values = (normalized_high_values * (height-20)).astype(np.float32)
        scaled_low_values = (normalized_low_values * (height-20)).astype(np.float32)
        # Scale the values to fit within the image height
        scaling_factor = 0.9 # Adjust as needed to fit the graph within the image
        scaled_high_values *= scaling_factor
        scaled_low_values *= scaling_factor
        print("Scaled high values ",scaled_high_values[0])
        start_candle, end_candle = 0, bars
        graph = np.zeros((height, width, 3), dtype=np.uint8)
        graph.fill(255)  # Fill with white
        x = 1 # starting x coordinate
        thickness = 3 # thickness of the lines
        candle_width = 2  # Adjust the candlestick width as needed

        # plot each point 
        for i in range(start_candle,end_candle): 
            # Calculate rectangle coordinates for the high and low values
            high_y1 = height - 20 - int(scaled_high_values[i - 1])
            high_y2 = height - 20 - int(scaled_high_values[i])
            low_y1 = height - 20 - int(scaled_low_values[i - 1])
            low_y2 = height - 20 - int(scaled_low_values[i])
            # Determine the minimum and maximum y-coordinates for the rectangle
            y_min = min(high_y1, high_y2, low_y1, low_y2)
            y_max = max(high_y1, high_y2, low_y1, low_y2)
            # Determine if the candlestick is bullish or bearish
            if df['open'][i] <  df['close'][i]:
                color = (0, 0, 255)  # Bullish (red but using blue)
            else:
                color = (0, 255, 0)  # Bearish (green)
            # Draw rectangle for the candlestick (in red for high values, green for low values)
            cv2.rectangle(graph, (x - candle_width // 2, y_min), (x + candle_width // 2, y_max), color, thickness) 
            x += 1

        results = model_yolov.predict(graph, verbose=False)
        current_preds = []
        for result in results:
            print("********************************")
            for box in result.boxes:
                print("********************************")
                class_id = int(box.data[0][-1])
                print("Class ",model_yolov.names[class_id])
                current_preds.append(model_yolov.names[class_id])
        print(f'The current boxes for this chart are {current_preds} with the last prediction being {current_preds[0]}')
        render = render_result(model=model_yolov, image=graph, result=results[0])
        # Assuming 'render' contains the PIL Image returned by render_result
        render_np = np.array(render)  # Convert PIL Image to numpy array
        # Convert RGB to BGR (OpenCV uses BGR color order)
        render_np = cv2.cvtColor(render_np, cv2.COLOR_RGB2BGR)
        # Create the file name with sequency number
        filename = "graph.jpg"
        # Figure out the local path
        output_path = f'./{images_folder}/{filename}'
        # Save your sample
        cv2.imwrite(output_path, render_np)
        # Increment counters
        start_candle, end_candle = start_candle + 1, end_candle + 1
        
        up = 0
        down = 0
        
        for pred in current_preds:
            if pred == 'up':
                up +=1
            else:
                down +=1

            if up == down:
                forecast_yolov = current_preds[0]
            elif up > down:
                forecast_yolov = 'up'
            else:
                forecast_yolov = 'down'
        
        df = df.drop('spread', axis=1)
        df = df.drop('high', axis=1)
        df = df.drop('tick_volume', axis=1)
        df = df.drop('low', axis=1)
        df = df.drop('time', axis=1)
        df = df.drop('real_volume', axis=1)
        sc = MinMaxScaler(feature_range= (0,1))
        X = sc.fit_transform(df)
        X = X[-1]
        X = X.reshape(1, -1)
        
        # print("Normalizei os dados")
        # print (f'DEBUG x {X}')
        predicted_price = model_ridge.predict(X)
        # print(f'predicted_price {predicted_price}')
        predicted_price = (predicted_price[0,1] - X[0,1]) / X[0,1]
        predicted_price *= 100

        return forecast_yolov, predicted_price    
               
    except Exception as e:
        print("get_prediction")
        error_line(e)

def get_data(symbol):
    """
    @param symbol: The asset we need to get the data from
    @return df: Pandas dataframe
    """
    try:
        timeframe = mt5.TIMEFRAME_M1
        raw_data_from_mt5 = mt5.copy_rates_from_pos(str(symbol),timeframe, 0 , bars)
        df = pd.DataFrame(raw_data_from_mt5)
        return df
   
    except Exception as e:
        print("get_data")
        error_line(e)


def update_json_file(prediction):
    """
    @params prediction: the prediction to write to the file
    """
    try:
        try:
            with open(file_path, 'r') as file:
                print("-----------------",file_path)
                data = json.load(file)
        except FileNotFoundError:
            data = {} # If the file doesn't exist, create an empty dictionary
        # Update the data with new values
        prediction_time = (datetime.now() + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        data['prediction_time'] = prediction_time  # Renaming 'current_time' to 'prediction_time'
        data['prediction'] = prediction

        # Write the updated data back to the JSON file
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)

    except Exception as e:
         print("update_json_file")
         error_line(e) 


def make_prediction():
    """
    This function authenticates to MT5 , loads the YOLO model, gets the data from mt5 and makes a prediction.The prediction is written to a file for later use.
    """
    try:
      
        print("Making prediction")
        authenticate_to_mt5(wamaitha_account,wamaitha_password,wamaitha_server)
        model_yolov = load_model_yolov(local_model_path_yolov)
        model_ridge = load_model_ridge()
        df = get_data(symbol)
        prediction, predicted_price = get_prediction(model_yolov, model_ridge, df,bars)
        # print("Prediction is ",prediction)
        # update_json_file(prediction, predicted_price)
        
        if predicted_price > 0 and prediction == 'up':
            update_json_file(prediction)
            print(f"Prediction is: {prediction}")
        elif predicted_price < 0 and prediction == 'down':
            update_json_file(prediction)
            print(f"Prediction is: {prediction}")
        else:
            print("The script failed to create a reliable prediction")

    except Exception as e:
         print("make_prediction")
         error_line(e) 


def repeat_task():
    """
    Schedules the make_prediction() function to run every {time_schedule}
    
    """
    try:
        print("Time schedule ",time_schedule)
        scheduler.enter(time_schedule, 1, make_prediction, ())
        scheduler.enter(time_schedule, 1, repeat_task, ())
    except Exception as e:
         print("repeat_task")
         error_line(e) 

if __name__ == "__main__":
    # Check if the correct number of arguments is provided
    print(sys.argv, " -  ",len(sys.argv))
    if len(sys.argv) <= 2:
        print('Exiting!')
        sys.exit(1)
    
    if str(sys.argv[3]) == 'hours' or str(sys.argv[3]) == 'hour':
       time_schedule = int(sys.argv[2])/1 * 60 * 60

    if str(sys.argv[3]) == 'minutes' or str(sys.argv[3]) == 'minute':
       time_schedule = int(sys.argv[2])/60 * 60 * 60

    if str(sys.argv[3]) == 'day':
       time_schedule = int(sys.argv[2])* 60 * 60 * 60

    if str(sys.argv[3]) == 'month':
       time_schedule = int(sys.argv[2])* 60 * 30 * 60 * 60

    if str(sys.argv[3]) == 'week':
       time_schedule = int(sys.argv[2])* 60 * 7 * 60 * 60
    
    
    symbol = str(sys.argv[-1])
    print("The time is ",time_schedule, " for the ",symbol)
    symbol_without_spaces = symbol.replace(" ", "")
    timeframe = (str(sys.argv[2]) + str(sys.argv[3])).replace(" ", "")
    file_path = symbol_without_spaces+timeframe+".json"
    print(file_path)
    make_prediction()
    repeat_task()
    scheduler.run()


