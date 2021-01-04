import pandas as pd
from sklearn import preprocessing
from keras.layers.core import Dense, Activation, Dropout
from keras.layers.recurrent import LSTM
from keras.models import Sequential

def predict(dict):
    to_dics = {}
    for key, value in dict.items():
        to_dics[key] = [value]
    df = pd.DataFrame.from_dict(to_dics)
    dff = df[['response_time', 'event_start_ms', 'event_start_epoc']]
    min_max_scaler = preprocessing.StandardScaler()
    np_scaled = min_max_scaler.fit_transform(dff[['response_time', 'event_start_ms', 'event_start_epoc']])
    dff = pd.DataFrame(np_scaled)
    x_test = dff.values
    y_test = dff[0].values
    # Building the model
    model = Sequential()
    model.add(LSTM(100, input_dim=3, return_sequences=True))
    model.add(Dropout(0.2))
    model.add(LSTM(100, return_sequences=False))
    model.add(Dropout(0.2))
    model.add(Dense(6))
    model.add(Activation('linear'))
    model.compile(loss='mae', optimizer='Adam')
    model.load_weights("model.h5")
    model.compile(loss='mae', optimizer='Adam', metrics=['accuracy'])
    score = model.evaluate(x_test, y_test, verbose=0)
    if score < 0.5:
        return 0
    else:
        return 1


data = {'response_time': 17.6937940120697,
        'event_start_ms': 804,
        'event_start_epoc': 1540169485,
        'year': 2018,
        'month': 10,
        'day': 22}

