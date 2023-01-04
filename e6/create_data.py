import time
import pandas as pd
from implementations import all_implementations
import numpy as np

def get_csv_data(sorted_data):
    data = pd.DataFrame(sorted_data, columns=['qs1', 'qs2', 'qs3', 'qs4', 'qs5', 'merge1', 'partition_sort'])
    return data

def get_random_integers():
    return np.random.randint(5000,size = 10000)

final_csv_data = []
for i in range(50):
    column_data = []
    random_array = get_random_integers()
    for sort in all_implementations:
        st = time.time()
        res = sort(random_array)
        en = time.time()
        total_time = en - st
        column_data.append(total_time)
    final_csv_data.append(column_data)

data = get_csv_data(final_csv_data)
data.to_csv('data.csv', index=False)