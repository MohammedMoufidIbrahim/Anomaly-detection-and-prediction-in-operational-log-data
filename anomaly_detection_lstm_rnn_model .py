# -*- coding: utf-8 -*-
"""Anomaly_Detection_LSTM_RNN_Model.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1Ub4adSCHvqAXI1yz_OZP2GJvVw5zK8r6

##1. Download the dataset
"""

#To upload file from local system
from google.colab import files
uploaded = files.upload()

"""## 2. Import the needed 3rd party packages"""

# Commented out IPython magic to ensure Python compatibility.
#Importing Required Libraries
import pandas as pd
import numpy as np
import matplotlib
import seaborn as sns
sns.set(color_codes=True)
import matplotlib.pyplot as plt
# %matplotlib inline
import seaborn
import matplotlib.dates as md
from sklearn import preprocessing
from keras.layers.core import Dense, Activation, Dropout
from keras.layers.recurrent import LSTM
from keras.models import Sequential
from keras.optimizers import Adam
import time
import datetime
from datetime import datetime

#Reading file from local system
df = pd.read_csv("BTFEEDleftmenu - 100.csv")

df.head()

# Calculating the timestamp
df ['timestamp'] = df['event_start_epoc'].apply(lambda ts: datetime.fromtimestamp(ts))
df.head()

# Formating the data into required format
df['hours'] = df['timestamp'].dt.hour
df['daylight'] = ((df['hours'] >= 7) & (df['hours'] <= 22)).astype(int)
df['DayOfTheWeek'] = df['timestamp'].dt.dayofweek
df['WeekDay'] = (df['DayOfTheWeek'] < 5).astype(int)

# Detecting the working days and the holydays
df['daytype'] = df['WeekDay']*2 + df['daylight']
Weekend_Night = df.loc[df['daytype'] == 0, 'month']
Weekend_Light = df.loc[df['daytype'] == 1, 'month']
Weekday_Night = df.loc[df['daytype'] == 2, 'month']
Weekday_Light = df.loc[df['daytype'] == 3, 'month']

df.head()

"""## 3.  Visualize the data"""

# plot the respond time corsponding to the timestamp
figsize=(20,12)
df.plot(x='timestamp', y='response_time', figsize=figsize, title='Time series - Webshop latency data')
plt.grid()
plt.show()

# Visualizing the formatted data
figsize=(10,5)
fig, ax = plt.subplots(figsize=figsize)
a_heights, a_bins = np.histogram(Weekend_Night)
b_heights, b_bins = np.histogram(Weekend_Light, bins=a_bins)
c_heights, c_bins = np.histogram(Weekday_Night, bins=a_bins)
d_heights, d_bins = np.histogram(Weekday_Light, bins=a_bins)
width = (a_bins[1] - a_bins[0])/6
ax.bar(a_bins[:-1], a_heights*100/Weekend_Night.count(), width=width, facecolor='blue', label='Weekend Night')
ax.bar(b_bins[:-1]+width, (b_heights*100/Weekend_Light.count()), width=width, facecolor='green', label ='Weekend Light')
ax.bar(c_bins[:-1]+width*2, (c_heights*100/Weekday_Night.count()), width=width, facecolor='red', label ='Weekday Night')
ax.bar(d_bins[:-1]+width*3, (d_heights*100/Weekday_Light.count()), width=width, facecolor='black', label ='Weekday Light')
plt.legend()
plt.show()

"""The above histogram shows that the Logs are comparatively more stable during Week Days in the daylights, and extremely high between October and December especially at the weekends"""

# Visualizing the log according to the frequency in the month
plt.figure(figsize= (12,6))
plt.ylabel('Month')
plt.xlabel('Operational log')
df['month'].value_counts().plot(kind = 'barh', grid = True)
plt.show()

"""## 4. Preparing the data for LSTM model

### 4.1 MinMax scaler for scaling the data
"""

data_n = df[['response_time', 'event_start_ms','event_start_epoc']]
min_max_scaler = preprocessing.StandardScaler()
np_scaled = min_max_scaler.fit_transform(data_n[['response_time', 'event_start_ms','event_start_epoc']])
data_n = pd.DataFrame(np_scaled)

data_n.head()

"""### 4.2 Spliting the Dataset

we define the datasets for training and testing our neural network. To do this, we perform a simple split where we train on the first part of the dataset, which represents normal operational logs. We then test on the remaining part of the dataset that contains the anomaly logs.
"""

#Important parameters and training/Test size
prediction_time = 1 
testdatasize = 8000
unroll_length = 50
testdatacut = testdatasize + unroll_length  + 1

#Training data
x_train = data_n[0:-prediction_time-testdatacut].values
y_train = data_n[prediction_time:-testdatacut  ][0].values

#Test data
x_test = data_n[0-testdatacut:-prediction_time].values
y_test = data_n[prediction_time-testdatacut:  ][0].values

def unroll(data,sequence_length=24):
    result = []
    for index in range(len(data) - sequence_length):
        result.append(data[index: index + sequence_length])
    return np.asarray(result)

#Adapt the datasets for the sequence data shape
x_train = unroll(x_train,unroll_length)
x_test  = unroll(x_test,unroll_length)
y_train = y_train[-x_train.shape[0]:]
y_test  = y_test[-x_test.shape[0]:]

#Shape of the data
print("x_train", x_train.shape)
print("y_train", y_train.shape)
print("x_test", x_test.shape)
print("y_test", y_test.shape)

"""## 5. Building the LSTM model"""

model = Sequential()

model.add(LSTM(100,input_dim=x_train.shape[-1], return_sequences=True))
model.add(Dropout(0.2))

model.add(LSTM(100, return_sequences=False))
model.add(Dropout(0.2))

model.add(Dense(6))
model.add(Activation('linear'))

start = time.time()
model.compile(loss='mae', optimizer='Adam')
print('compilation time : {}'.format(time.time() - start))
model.summary()

"""### 5.1 Training the model"""

model.fit(x_train, y_train, batch_size=3028, epochs=50, validation_split=0.1)

#Visualizing training and validaton loss
plt.figure(figsize = (10, 5))
plt.plot(model.history.history['loss'], label = 'Loss')
plt.plot(model.history.history['val_loss'], Label = 'Val_Loss')
plt.xlabel('Epochs')
plt.ylabel('Loss')
plt.grid()
plt.legend()

"""### 5.2 Saving the model weights into HDF5"""

# Serialize weights to HDF5
model.save_weights("model.h5")
print("Saved model to disk")

"""### 5.3 Difference between prediction and test data"""

# The list of difference between prediction and test data
loaded_model = model
diff=[]
ratio=[]
pred = loaded_model.predict(x_test)
for i in range(len(y_test)):
    pr = pred[i][0]
    ratio.append((y_test[i]/pr)-1)
    diff.append(abs(y_test[i]- pr))

# Plotting the reality, Test data
plt.figure(figsize = (15, 8))
plt.plot(y_test,color='blue', label='Test Data')
plt.legend(loc='upper left')
plt.grid()
plt.legend()

# Plotting the prediction and the reality
plt.figure(figsize = (15, 8))
plt.plot(pred,color='red', label='Prediction')
plt.legend(loc='upper left')
plt.grid()
plt.legend()

"""## 6. Amonaly detection"""

# Anomaly estimated population
outliers_fraction = 0.01

#Pick the most distant prediction/reality data points as anomalies
diff = pd.Series(diff)
number_of_outliers = int(outliers_fraction*len(diff))
threshold = diff.nlargest(number_of_outliers).min()

#Data with anomaly label
test = (diff >= threshold).astype(int)
complement = pd.Series(0, index=np.arange(len(data_n)-testdatasize))
df['anomaly'] = complement.append(test, ignore_index='True')
print(df['anomaly'].sum())
print('threshold', threshold)

# Visualizing anomalies
plt.figure(figsize=(15,10))
aa = df.loc[df['anomaly'] == 1, ['timestamp', 'response_time']] #anomaly
plt.plot(df['timestamp'], df['response_time'], color='blue')
plt.scatter(aa['timestamp'],aa['response_time'], color='red', label = 'Anomaly')
plt.grid()
plt.legend()

# Visualize the data
plt.figure(figsize=(15,10))
aa.plot(x ='timestamp',y='response_time', color='red', title = 'Anomaly')
plt.grid()
plt.legend()

