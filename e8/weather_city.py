import pandas as pd
import numpy as np
import sys
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import KNeighborsClassifier

data_file_labelled = pd.read_csv(sys.argv[1])
data_file_unlabelled = pd.read_csv(sys.argv[2])

def get_X_data(d):
    X_data=d.loc[:,'tmax-01':'snwd-12']
    return X_data

def get_Y_data(d):      
    Y_data= d['city']
    return Y_data

X_data = get_X_data(data_file_labelled)
Y_data = get_Y_data(data_file_labelled)

def get_unlabelled_data(d):
     data = d.loc[:,'tmax-01':'snwd-12']
     return data

unlabelled_data = get_unlabelled_data(data_file_unlabelled)

X_train, X_test, y_train, y_test = train_test_split(X_data, Y_data)

model = make_pipeline(StandardScaler(),KNeighborsClassifier(n_neighbors=12))

model.fit(X_train, y_train)

predictions = model.predict(unlabelled_data)
print("model_score = ", round(model.score(X_test, y_test), 4))
data = pd.DataFrame({'truth': y_test, 'prediction': model.predict(X_test)})
pd.Series(predictions).to_csv(sys.argv[3], index=False, header=False)