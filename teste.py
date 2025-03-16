import os
import subprocess
from scheduler import get_data
from sklearn.linear_model import Ridge
import joblib
from sklearn.model_selection import cross_val_score
import pandas as pd
import numpy as np
import MetaTrader5 as mt5
from sklearn.preprocessing import MinMaxScaler
import joblib

folder_path = 'D:/backup/importantes/pythoncodigos/projeto_bolt_bolsa2/MLSpikeDetector/DATA/META'

import os

file_path = 'D:\\backup\\importantes\\pythoncodigos\\projeto_bolt_bolsa2\\scheduler.py'

if os.path.exists(file_path):
    with open(file_path, 'r') as file:
        # Faça algo com o arquivo
        pass
else:
    print(f"O arquivo {file_path} não foi encontrado.")

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

while True:
    option = input('0 - EXIT\n\n'
                   '1 - START FORECAST\n\n'
                                      
                   'OPTION: ')
    match option:
        case '0':
            deleting_files(folder_path)
            print('EXITING THE PROGRAM...')
            exit()
        case '1':
            symbol = input('Enter the name of the sticker: \n')
            time = int(input('What is the timeframe? Ex.: 1, 5, 15 \n'))
            time2 = str(input('Minutes or Hours?\n'))
            command = f'python scheduler.py python {time} {time2} {symbol}'

            ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
            venv_path = os.path.join(ROOT_DIR, '.venv', 'Scripts', 'activate.bat')
            subprocess.Popen(f'cmd /c "{venv_path} && {command}"', shell=True)

            print("Ainda continuo executando!!!")
        case '2':
            symbol = input('Enter the name of the sticker: \n')
            df = get_data(symbol)
            df['time'] = pd.to_datetime(df['time'], unit='s')
            df['target'] = df[['close']].shift(-1)
            df = df.drop('spread', axis=1)
            df = df.drop('high', axis=1)
            df = df.drop('tick_volume', axis=1)
            df = df.drop('low', axis=1)
            sc = MinMaxScaler(feature_range= (0,1))
            scaled_df = sc.fit_transform(df.drop(columns='time'))

            #x_alvo = df.loc[:, ['open', 'close']].to_numpy()
            x_alvo = scaled_df[:, :2]
            X_alvo = x_alvo[-1]
            X_alvo = X_alvo.reshape(1, -1)
            X_alvo = X_alvo[:, ~np.isnan(X_alvo).any(axis=0)]
            df = df.dropna()
                       
            # print(scaled_df)
            # print('--------------')
            # print(F'X ALVO {X_alvo}')
            X = scaled_df[:, :2]
            y = scaled_df[:, 2:]

            split = int(0.7 * (len(X)))
            X_train = X[:split]
            y_train = y[:split]
            X_test = X[split:]
            y_test = y[split:]

            model_carregado = joblib.load('modelo_ridge.pkl')

            
            predicted_price = model_carregado.predict(X_alvo)

            # model = Ridge(alpha=0.01)
            # model.fit(X_train, y_train)
            # predicted_price = model.predict(X_alvo)
            #print(F'PREVISAO {predicted_price}')
            valorizacao = (predicted_price[0,1] - X_alvo[0,1]) / X_alvo[0,1]
            valorizacao *= 100

            # scores = cross_val_score(model, X, y, cv=5, scoring='r2')
            # scor = scores.mean()
            # print(f'Taxa de acerto: {scor}')
            print(f'Valorização prevista em %: {valorizacao}' )
        
        case '3':
            time = '2024-08-30'
            mt5.initialize()
            date_from = pd.Timestamp(time)
            rates = mt5.copy_rates_from('ETHUSD', mt5.TIMEFRAME_M1, date_from, 3000)
            if rates is not None:
                
                df = pd.DataFrame(rates)
                df['time'] = pd.to_datetime(df['time'], unit='s')

            target_df = df
            target_df['target'] = df[['close']].shift(-1)
            target_df = target_df.drop('spread', axis=1)
            target_df = target_df.drop('high', axis=1)
            target_df = target_df.drop('tick_volume', axis=1)
            target_df = target_df.drop('low', axis=1)
            target_df = target_df.dropna()

            sc = MinMaxScaler(feature_range= (0,1))
            scaled_df = sc.fit_transform(target_df.drop(columns='time'))
            
            X = scaled_df[:, :2]
            y = scaled_df[:, 2:]

            split = int(0.7 * (len(X)))
            X_train = X[:split]
            y_train = y[:split]
            X_test = X[split:]
            y_test = y[split:]

            model = Ridge(alpha=0.01)
            model.fit(X_train, y_train)
            
            scores = cross_val_score(model, X, y, cv=5, scoring='r2')
            scor = scores.mean()

            joblib.dump(model, 'modelo_ridge.pkl')
            print(f'Taxa de acerto: {scor}')
        case _:
            print('INVALID OPTION!')