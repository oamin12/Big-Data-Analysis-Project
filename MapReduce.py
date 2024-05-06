'''
CopyRight: 2024 _ Property of Omar Amin & Kimo Yasser. All Rights Reserved.

this file is the map reduce class that will be used to implement the map reduce algorithm
on the dataset, used for CMPS451 course project.

'''

# Importing the required libraries
import pandas as pd
import numpy as np
import os
import threading

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

    stringo = '''0-50, 51-100, 101-150, 151-200, 201-250, 251-300, 301-350, 351-400, 401-450, 451-500, 501-550, 551-600, 601-650, 650-700, 701-750, 751-800, 801-850, 851-900, 901-950, 951-1000, 1001-1050, 1051-1100, 1101-1150, 1151-1200, 1201-1250, 1251-1300, 1301-1350, 1351-1400, 1401-1450, 1451-1500, 1501-1550, 1551-1600, 1601-1650, 1651-1700, 1701-1750, 1751-1800, 1801-1850, 1851-1900, 1901-1950, 1951-2000'''

    prices = stringo.split(',')
    
    prices = [(int(price.split('-')[0]), int(price.split('-')[1])) for price in prices]

    # Apply the map function on the dataset
    dataset["price_category"] = dataset['price'].apply(lambda x: [i for i in range(len(prices)) if prices[i][0] <= int(x) <= prices[i][1]][0])
    print(dataset['price_category'])
    return dataset


def mapping(dataset, number_of_mappers):
    '''
    Function to apply the map function on the dataset.
    '''
    #we will use 5 mappers to split the dataset into 5 parts each has 20% of the dataset 
    #and then apply the map function on each part.
    #split the dataset into 5 parts
    dataset_parts = np.array_split(dataset, number_of_mappers)

    #repeat the first row in each part to keep the columns names
    dataset_parts = [pd.concat([dataset_parts[i].iloc[0:1], dataset_parts[i]]) for i in range(len(dataset_parts))]



    #use thredding to apply the map function on each part
    threads = []

    print(type(dataset_parts[0]),"ANA HENAAAAAAAAAAAAAAAAAAAAA")
    for part in dataset_parts:

        #convert the part to a pandas dataframe
        t = threading.Thread(target=map_function, args=(part,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    return dataset

#test mapping

dataset = read_dataset('Dataset/Airbnb_Data1.csv')

dataset = mapping(dataset,2)

#print head of the dataset
print(dataset['price_category'])







