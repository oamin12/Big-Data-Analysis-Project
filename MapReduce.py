'''
CopyRight: 2024 Property of Omar Amin & Kimo Yasser. All Rights Reserved.

this file is the map reduce class that will be used to implement the map reduce algorithm
on the dataset, used for CMPS451 course project.

'''

# Importing the required libraries
import pandas as pd
import numpy as np
import os
import threading
from datetime import datetime


# MapReduce 
def read_dataset(file_path):
    '''
    Function to read the dataset from the file path.
    '''

    # Read the dataset
    dataset = pd.read_csv(file_path)
    return dataset

def map_function(dataset):
    '''
    Function to apply the map function on the dataset.
    '''

    prices = [(0 + 50 * i, 50 + 50 * i) for i in range(0, 40)]

    # Apply the map function on the dataset
    dataset["price_category"] = dataset['price'].apply(lambda x: [i for i in range(len(prices)) if prices[i][0] <= int(x) <= prices[i][1]][0])
    return dataset

def mapping(dataset, number_of_mappers):
    '''
    Function to apply the map function on the dataset.
    '''
    dataset_parts = np.array_split(dataset, number_of_mappers)

    threads = []
    for part in dataset_parts:
        t = threading.Thread(target=map_function, args=(part,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    return pd.concat(dataset_parts)  # Concatenate parts back together

# Test mapping
now = datetime.now()

dataset = read_dataset('Dataset/Airbnb_Data1.csv')
dataset = mapping(dataset, 5)

print(datetime.now() - now, "It took to map the dataset")
#print(dataset['price_category'].head())


# Reduce Function
#the reduce function will count the number of listings in each price category

#global list of counts for each price category initialized to 0
counts = [0 for i in range(40)]



def reduce_function(dataset):
    '''
    Function to apply the reduce function on the dataset.
    '''

    # Apply the reduce function on the dataset
    for row in dataset['price_category']:
        counts[row] += 1


def reducing(dataset, number_of_reducers):
    '''
    Function to apply the reduce function on the dataset.
    '''
    dataset_parts = np.array_split(dataset, number_of_reducers)

    threads = []
    for part in dataset_parts:
        t = threading.Thread(target=reduce_function, args=(part,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    return counts  # Concatenate parts back together

# Test reducing
now = datetime.now()

counts = reducing(dataset, 100)

print(datetime.now() - now, "It took to reduce the dataset")

print(counts)

#test to see if the implemented reducer will get the same values are pandas groupby
dataset = read_dataset('Dataset/Airbnb_Data1.csv')
dataset = map_function(dataset)

print(dataset.groupby('price_category').size().values)
print(counts)
print(np.array_equal(dataset.groupby('price_category').size().values, counts))

