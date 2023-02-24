# -*- coding: utf-8 -*-
"""
Created on Sun Jan 29 11:25:15 2023

@author: rober
"""

import os
import pandas as pd
import pprint

pd.set_option('display.max_rows', 20)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 7)


'''
Configure AQI dataframe
 - Create dataframe
 - Define location of datasets
 - Import datasets into dataframe
 - Prepend headers with aqi
 - Remove all entries where state is not California
'''

aqi_dataframe = pd.DataFrame()

aqi_datafile_path = "data/aqi/"
aqi_datafiles =  [aqi_datafile_path + i for i in os.listdir(aqi_datafile_path) if i.endswith('.csv')]


for aqi_datafile in aqi_datafiles:
    aqi_dataframe = aqi_dataframe.append(pd.read_csv(aqi_datafile))

aqi_headers = list(aqi_dataframe.columns)
aqi_headers = ['aqi_' + header for header in aqi_headers]
aqi_dataframe.columns = aqi_headers

aqi_dataframe = aqi_dataframe[aqi_dataframe['aqi_State'] == 'California']



'''
Configure Asthma dataframe
 - Create dataframe
 - Define location of datasets
 - Import datasets into dataframe
 - Prepend headers with asthma
'''

asthma_dataframe = pd.DataFrame()

asthma_datafile_path = "data/asthma/"
asthma_datafiles =  [asthma_datafile_path + i for i in os.listdir(asthma_datafile_path) if i.endswith('.csv')]

for asthma_datafile in asthma_datafiles:
    asthma_dataframe = asthma_dataframe.append(pd.read_csv(asthma_datafile))

asthma_headers = list(asthma_dataframe.columns)
asthma_headers = ['asthma_' + header for header in asthma_headers]
asthma_dataframe.columns = asthma_headers

'''

Configure Project dataframe
 - Create dataframe
 - Merge Asthma data and append AQI headers

'''

project_dataframe = pd.DataFrame()

project_dataframe = pd.merge(asthma_dataframe,aqi_dataframe, on=None, how='left', left_index=True, right_index=True)


'''

Conditionally merge AQI dataframe into Project dataframe
 - For each row in project dataframe append aqi dataframe values where county and year match

'''
for idx,row in project_dataframe.iterrows():
    matched_data = aqi_dataframe.loc[
        (aqi_dataframe['aqi_County'] == row['asthma_COUNTY']) &
        (aqi_dataframe['aqi_Year'] == row['asthma_YEAR'])
        ]

    if (matched_data.size == 0):
        print("No AQI data for ({}) - ({})".format(row['asthma_COUNTY'], row['asthma_YEAR']))
    else:
        for index, value in matched_data.items():
            project_dataframe.at[idx,index] = value.item()


#print(project_dataframe)
project_dataframe.to_csv('test_output.csv', index=False)

    
