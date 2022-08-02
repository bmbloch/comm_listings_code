import csv
import pandas as pd
import numpy as np
import sys
import random
import itertools
import math
import time
from datetime import datetime
from pathlib import Path
import matplotlib.pyplot as plt
import re
import os
from os import listdir
from os.path import isfile, join
from datetime import date
pd.set_option('display.max_rows',  1000)
pd.set_option('display.max_columns', None)
pd.options.display.float_format = '{:.3f}'.format
from IPython.core.display import display, HTML
import logging
import warnings
warnings.filterwarnings('ignore')
from IPython.display import Audio
from os import listdir
from os.path import isfile, join
import os
import s3fs
import multiprocessing as mp
import seaborn as sns
from pandas.api.types import CategoricalDtype
import io
from datetime import datetime, timezone
import boto3
import base64
from botocore.exceptions import ClientError
import json
from datetime import timedelta

curryr = 2022
currmon = 7
use_reis_yr = True # Set this to true if want to override the RDMA year built and month built values with Foundation values
update_umix = True # Set this to true if want to also update the umixfamily.txt file in addition to the metro log files

print("Initiating process for {}m{}".format(curryr, currmon))

logger = logging.getLogger()

log_file_name = 'DL_Log_Apt.log'

if len(logger.handlers) > 0:
    logger.handlers.pop()
if len(logger.handlers) > 0:
        logger.handlers.pop()
fhandler = logging.FileHandler(log_file_name, mode='w')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fhandler.setFormatter(formatter)
logger.addHandler(fhandler)
logger.setLevel(logging.INFO)
logging.getLogger('charset_normalizer').setLevel(logging.FATAL)
logger.propagate = False
error = False

def read_logs(): 
        
    logging.info("Loading and appending historical logs for Apt...")
    logging.info('\n')
    
    count = 0
    df = pd.DataFrame()

    dir_list = [f for f in listdir('/home/central/square/data/apt/download/') if isfile(join('/home/central/square/data/apt/download/', f))]
    
    file_list = [x for x in dir_list if len(x.split('/')[-1].split('.log')[0]) == 2]
    
    for path in file_list:
        if count % 10 == 0 and count > 0:
            logging.info("Loading {:,} of {:,} total logs".format(count, len(file_list)))
        count += 1
        
        file_read = '/home/central/square/data/apt/download/' + path
        
        temp = pd.read_csv(file_read, sep=',', encoding = 'utf-8',  na_values= "", keep_default_na = False)
        if len(temp.columns) == 1:
            temp = pd.read_csv(file_read, sep='\t', encoding = 'utf-8',  na_values= "", keep_default_na = False)

        df = df.append(temp, ignore_index=True)

    df.columns= df.columns.str.strip().str.lower()
    logging.info('\n')
    print("Apt Logs Loaded")

    return df

df_in = pd.read_csv('/home/central/square/data/zzz-bb-test2/python/catylist_snapshots/InputFiles/apt_view_{}m{}.csv'.format(curryr, currmon), na_values="", keep_default_na=False, parse_dates=['completed_date_full'], infer_datetime_format=True)
print("survey view file dimensions: {}".format(df_in.shape))
live_subs = pd.read_csv('/home/central/square/data/zzz-bb-test2/python/catylist_snapshots/InputFiles/apt_live_subs.csv', na_values= "", keep_default_na = False)
valid_aptdata = pd.read_csv('/home/central/square/data/zzz-bb-test2/python/catylist_snapshots/InputFiles/aptdata_valid_ids.csv', na_values= "", keep_default_na = False)
valid_aptdata['id'] = 'A' + valid_aptdata['id'].astype(str)
log_in = read_logs()
print("log file dimensions: {}".format(log_in.shape))

is_structural = ['propname', 'metcode', 'subid', 'address', 'city', 'county', 'state', 'zip', 'x', 'y', 'flrs', 'year', 
                 'utilities', 'amenities', 'month', 'status', 'renov', 'type2', 'units0', 'units1', 'units2', 'units3', 
                 'units4', 'totunits', 'cptv', 'size0', 'size1', 'size2', 'size3', 'size4', 'avgsize', 'estunit', 
                 'fipscode']

df_in['survey_legacy_data_source'] = np.where((df_in['survey_legacy_data_source'].isnull() == True), '', df_in['survey_legacy_data_source'])

test = df_in.copy()
test['survdate_d'] = pd.to_datetime(test['survdate'])
if datetime.today().weekday() != 0:
    delta_val = 1
else:
    delta_val = 3
test = test[(test['survey_legacy_data_source'] == '') & (test['survdate_d'] == datetime.strftime(date.today() - timedelta(days = delta_val), '%Y-%m-%d'))][['property_source_id', 'survdate']]
if len(test) == 0:
    print("There are no incremental surveys for {}".format(datetime.strftime(date.today() - timedelta(days = delta_val), '%m/%d/%Y')))
del test

df = df_in.copy()
drop_log = pd.DataFrame()

df = df.rename(columns={'first_year': 'year', 'first_month': 'month'})

df['property_reis_rc_id'] = df['property_reis_rc_id_an']
df['property_reis_rc_id'] = np.where((df['property_reis_rc_id'].str[0] == 'L'), 'A'+ df['property_reis_rc_id'].str[1:], df['property_reis_rc_id'])
df['property_reis_rc_id'] = np.where((df['property_reis_rc_id'].str[-1] == '-'), df['property_reis_rc_id'].str[:-1], df['property_reis_rc_id'])

df['property_source_id'] = np.where((df['property_source_id'].isnull() == True), df['property_reis_rc_id'], df['property_source_id'])

df['property_source_id'] = df['property_source_id'].astype(str)

if len(df[df['property_source_id'] == '']) > 0:
    print("There are properties without an id")

nan_to_string = ['property_source_id', 'property_reis_rc_id', 'property_reis_property_id', 'property_er_to_foundation_ids_list', 'propname', 'metcode',
                 'address', 'city', 'county', 'state', 'status', 'type2', 'category', 'subcategory', 'housing_type',
                 'student_housing_type', 'reis_type_secondary']
for x in nan_to_string:
    df[x] = np.where((df[x].isnull() == True), '', df[x])

to_lower = ['status', 'category', 'subcategory', 'housing_type', 'student_housing_type', 'reis_type_secondary']
for x in to_lower:
    df[x] = df[x].str.lower()
    
conv_float = ['ren0', 'ren1', 'ren2', 'ren3', 'ren4', 'size0', 'size1', 'size2', 'size3', 'size4', 'vac0', 'vac1', 'vac2',
              'vac3', 'vac4', 'free_rent']

for col in conv_float:
    df[col] = df[col].astype(float)

df['zip'] = df['zip'].astype(str)
df['zip'] = np.where((df['zip'].str.replace('.0', '').str.isdigit() == False), np.nan, df['zip'])
df['zip'] = df['zip'].astype(float)

df['fipscode'] = np.where(df['fipscode'].str.isdigit() == False, np.nan, df['fipscode'])
df['fipscode'] = df['fipscode'].astype(float)

df['free_rent'] = round(df['free_rent'],3)

test = log_in.copy()
test['in_log'] = 1
test = test[test['survdate'].isnull() == False]
test['property_reis_rc_id'] = 'A' + test['id'].astype(str)
df = df.join(test.drop_duplicates('property_reis_rc_id').set_index('property_reis_rc_id')[['in_log']], on='property_reis_rc_id')


display(pd.DataFrame(df.groupby('survey_legacy_data_source')['property_source_id'].count()).rename(index={'survey_legacy_data_source': 'survey_source'}, columns={'property_source_id': 'count_rows'}))

print("Initial row count: {:,}".format(len(df)))
print('Initial unique property count: {:,}'.format(len(df.drop_duplicates('property_source_id'))))
df['survdate_d'] = pd.to_datetime(df['survdate'])
temp = df.copy()
temp['property_reis_rc_id'] = np.where((temp['property_reis_rc_id'] == '') & (temp['property_er_to_foundation_ids_list'].str[0] == 'A'), temp['property_er_to_foundation_ids_list'].str.split(',').str[0], temp['property_reis_rc_id'])
temp['count_links'] = temp.groupby('property_reis_rc_id')['property_source_id'].transform('nunique')
temp['count_links'] = np.where((temp['property_reis_rc_id'] == '') & (temp['property_er_to_foundation_ids_list'].str[0] != 'A'), 0, temp['count_links'])
temp['count_test'] = temp[temp['survey_legacy_data_source'] == 'REIS_RC_Apt'].groupby('property_source_id')['property_source_id'].transform('count')
temp['count_test'] = temp.groupby('property_source_id')['count_test'].bfill()
temp['count_test'] = temp.groupby('property_source_id')['count_test'].ffill()
temp['count_early'] = temp[(temp['survdate_d'] < '06/01/2022') & (temp['survey_legacy_data_source'] == '')].groupby('property_source_id')['property_source_id'].transform('count')
temp['count_early'] = temp.groupby('property_source_id')['count_early'].bfill()
temp['count_early'] = temp.groupby('property_source_id')['count_early'].ffill()
temp['count'] = temp.groupby('property_source_id')['property_source_id'].transform('count')
temp = temp[((temp['survey_legacy_data_source'] == 'REIS_RC_Apt') & (temp['count'] == temp['count_test'])) | ((temp['survey_legacy_data_source'] == '') & (temp['survdate_d'] < '06/01/2022') & (temp['count'] == temp['count_early']))]
temp['reason'] = 'No legacy log historical rows, and RDMA survey not true incremental survey'
drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason', 'count_links']], ignore_index=True)
del temp
df = df[(df['survey_legacy_data_source'].isin(['Foundation', 'ApartmentData.com'])) | ((df['survdate_d'] >= '06/01/2022') & (df['survey_legacy_data_source'] == ''))]
print('Property count after removing test surveys: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

df = df.join(valid_aptdata.drop_duplicates('id').set_index('id')[['valid']], on='property_reis_rc_id')
df['count_apt'] = df[df['survey_legacy_data_source'] == 'ApartmentData.com'].groupby('property_source_id')['property_source_id'].transform('count')
df['count_apt'] = df.groupby('property_source_id')['count_apt'].bfill()
df['count_apt'] = df.groupby('property_source_id')['count_apt'].ffill()
df['count_apt'] = df['count_apt'].fillna(0)
df['valid'] = np.where((df['count_apt'] == 0) | (df['property_reis_rc_id'].str[0] != 'A') | (df['in_log'].isnull() == True), 1, df['valid'])
df['valid'] = df['valid'].fillna(0)
temp = df.copy()
temp['property_reis_rc_id'] = np.where((temp['property_reis_rc_id'] == '') & (temp['property_er_to_foundation_ids_list'].str[0] == 'A'), temp['property_er_to_foundation_ids_list'].str.split(',').str[0], temp['property_reis_rc_id'])
temp['count_links'] = temp.groupby('property_reis_rc_id')['property_source_id'].transform('nunique')
temp['count_links'] = np.where((temp['property_reis_rc_id'] == '') & (temp['property_er_to_foundation_ids_list'].str[0] != 'A'), 0, temp['count_links'])
temp['count'] = temp.groupby('property_source_id')['property_source_id'].transform('count')
temp = temp[(temp['survey_legacy_data_source'] == 'ApartmentData.com') & (temp['count'] == temp['count_apt']) & (temp['valid'] == 0) & (temp['property_reis_rc_id'].str[0] == 'A')]
temp['reason'] = 'Aptdata.com survey deemed not publishable'
drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason', 'count_links']], ignore_index=True)
del temp
df = df[(df['survey_legacy_data_source'] != 'ApartmentData.com') | (df['valid'] == 1) | (df['property_reis_rc_id'].str[0] != 'A')]
print('Property count after removing non published aptdata.com surveys: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

display(pd.DataFrame(df.groupby('survey_legacy_data_source')['property_source_id'].count()).rename(index={'survey_legacy_data_source': 'survey_source'}, columns={'property_source_id': 'count_rows'}))

df['reis_record'] = np.where((df['property_reis_rc_id'] == '') | (df['property_reis_rc_id'].str[0] != 'A'), False, True)
df['nc_eligible'] = np.where((df['year'] >= curryr - 1) & (df['month'].isnull() == False) & ((df['property_reis_rc_id'].str[0] == 'A') | (df['property_reis_rc_id'].str[0].isnull() == True)), True, False)
temp = df.copy()
temp = temp[(temp['reis_record'] == False) & (temp['nc_eligible'] == False)]
temp['reason'] = 'Property is not linked to a REIS Apartment record'
drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
del temp
df = df[(df['reis_record']) | (df['nc_eligible'])]
print('Property count after removing properties not linked to REIS ID: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

temp = df.copy()
temp = temp[((temp['year'] > curryr) & (temp['year'].isnull() == False)) | ((temp['year'] == curryr) & (temp['month'] > currmon) & (temp['month'].isnull() == False))]
temp['reason'] = 'Property has year built in the future'
drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
del temp
df = df[(df['year'] < curryr) | (df['year'].isnull() == True) | ((df['year'] == curryr) & ((df['month'] <= currmon) | (df['month'].isnull() == True)))]
print('Property count after removing props with future year builts: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

print("Put this in once we are confident that data is clean")
# temp = df.copy()
# temp = temp[(temp['category'] != 'multifamily') & ((temp['subcategory'] != 'mixed_use') | (temp['totunits'].isnull() == True)) & (temp['property_source_id'] != temp['property_reis_rc_id'])]
# temp['reason'] = 'property no longer multifamily'
# drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
# del temp
# df = df[(df['category'] == 'multifamily') | ((df['subcategory'] =='mixed_use') & (df['totunits'] > 0)) | (df['property_source_id'] == df['property_reis_rc_id'])]
# print('Property count after removing non RDMA multifamily properties: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

if len(df[df['survdate'].isnull() == True]) > 0:
    print("There are rows that are missing a survey date")

df['count'] = df.groupby(['property_source_id', 'survdate'])['property_source_id'].transform('count')
df['dupe_surv_check'] = np.where((df['count'] > 1), 1, 0)
if len(df[(df['dupe_surv_check'] == 1) & (df['survey_legacy_data_source'] == 'Foundation')]) > 500 or len(df[(df['dupe_surv_check'] == 1) & (df['survey_legacy_data_source'] != 'Foundation')]):
    print("There are duplicate surveys")

df['count_source'] = df.groupby(['property_source_id', 'survdate'])['survey_legacy_data_source'].transform('nunique')
df['dupe_source_check'] = np.where((df['count_source'] > 1), 1, 0)
if len(df[df['dupe_source_check'] == 1]) > 0:
    print("There are surveys on the same date for different sources")

df['completed_date_full'] = np.where((df['survey_legacy_data_source'] == 'Foundation'), np.datetime64('NaT'), df['completed_date_full'])
df.sort_values(by=['property_source_id', 'completed_date_full', 'ren0', 'ren1', 'ren2', 'ren3', 'ren4', 'vac0', 'vac1', 'vac2', 'vac3', 'vac4'], ascending=[True, False, False, False, False, False, False, False, False, False, False, False], inplace=True)
df['count_no_ts'] = df.groupby(['property_source_id', 'survdate'])['property_source_id'].transform('count')
df['cumcount_id'] = df.groupby('property_source_id')['property_source_id'].transform('cumcount')
df = df[(df['cumcount_id'] == 0) | (df['count_no_ts'] == 1) | (df['survey_legacy_data_source'] == 'Foundation')]
print('Property count after removing incremental surveys that occurred on the same day: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

for col in ['buildings_condominiumized_flag']:
    df[col] = np.where((df[col] == 'Y'), 1, 0)

df['section_8_housing'] = np.where((df['section_8_housing'].isnull() == True), 0, df['section_8_housing'])

df['totunits'] = np.where((df['mr_units'].isnull() == False), df['mr_units'], df['totunits'])
df['mixed_income'] = np.where((df['housing_type'].isin(['affordable', 'age_restricted']) & (df['mr_units'] > 40)), True, False)   

temp = df.copy()
temp = temp[temp['section_8_housing'] == 1]
temp['reason'] = 'Property is Section 8'
drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
del temp
df = df[(df['section_8_housing'] == 0)]
print('Property count after removing section 8 properties: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

temp = df.copy()
temp = temp[(temp['buildings_condominiumized_flag'] == 1) & (temp['in_log'].isnull() == True)]
temp['reason'] = 'Property is Condo'
drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
del temp
df = df[(df['buildings_condominiumized_flag'] == 0) | (df['in_log'] == 1)]
print('Property count after removing condo properties: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

temp = df.copy()
temp = temp[(temp['housing_type'].isin(['affordable', 'age_restricted'])) & (temp['mixed_income'] == False)]
temp['reason'] = 'Property is Affordable Housing'
drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
del temp
df = df[~(df['housing_type'].isin(['affordable', 'age_restricted'])) | (df['mixed_income'])]    
print('Property count after removing non mixed income affordable properties: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

temp = df.copy()
temp = temp[temp['housing_type'] == 'student']
temp['reason'] = 'Property is Student Housing'
drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
del temp
df = df[(df['housing_type'] != 'student')]
print('Property count after removing student properties: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

temp = df.copy()
temp = temp[(temp['in_log'].isnull() == True) & ((temp['year'] < curryr - 1) | (temp['month'].isnull() == True))]
temp['reason'] = 'Property is linked to REIS record that is not in the log. Possible Aff issue'
drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
del temp
df = df[(df['in_log'] == 1) | ((df['year'] >= curryr - 1) & (df['month'].isnull() == False))]
print('Property count after removing properties linked to REIS ID not in the log: {:,}'.format(len(df.drop_duplicates('property_source_id'))))
print("Can take this last drop out after market rate units are reliably filled in, and will let IAG props come in with incrementals")

temp = df.copy()
temp = temp[(temp['property_reis_rc_id'] == '') & (temp['year'] >= curryr - 1) & (temp['in_log'].isnull() == True)]

test = log_in.copy()
test['id'] = test['id'].astype(str)
log_ids = list(test[test['survdate'].isnull() == False].drop_duplicates('id')['id'])

del test

drop_list = []
for index, row in temp.iterrows():
    if row['property_er_to_foundation_ids_list'] != '':
        er_ids = row['property_er_to_foundation_ids_list'].split(',')

        for er_id in er_ids:
            if er_id[0] == 'A':
                if er_id[1:] in log_ids:
                    drop_list.append(row['property_source_id'])
                    break

temp = df.copy()
temp = temp[(temp['property_source_id'].isin(drop_list))]
temp['reason'] = 'Property potential new construction, but ER link indicates property is already in the log'
drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)

df = df[~df['property_source_id'].isin(drop_list)]
del temp
print('Property count after removing potential nc properties that likely already exist in the log: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

temp = df.copy()
temp = temp[(temp['property_reis_rc_id'] == '') & (temp['year'] >= curryr - 1) & (temp['in_log'].isnull() == True) & (temp['totunits'].isnull() == True)]
temp['reason'] = 'Net new NC property but no total units'
drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
temp['drop_this'] = 1
df = df.join(temp.drop_duplicates('property_source_id').set_index('property_source_id')[['drop_this']], on='property_source_id')
df = df[df['drop_this'].isnull() == True]
del temp
print('Property count after removing potential nc properties without total units: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

temp = df.copy()
temp = temp.drop_duplicates('property_source_id')
temp['count_links'] = temp.groupby('property_reis_rc_id')['property_source_id'].transform('count')
df = df.join(temp.drop_duplicates('property_reis_rc_id').set_index('property_reis_rc_id')[['count_links']], on='property_reis_rc_id')
df['count_links'] = df['count_links'].fillna(1)
df['mult_link_check'] = np.where((df['count_links'] > 1) & (df['property_reis_rc_id'] != ''), 1, 0)
del temp

df['count_inc'] = df[df['survey_legacy_data_source'] == ''].groupby('property_source_id')['property_source_id'].transform('count')
df['count'] = df.groupby('property_source_id')['property_source_id'].transform('count')
temp = df.copy()
temp = temp[(temp['count_inc'] == temp['count']) & (temp['mult_link_check'] == 1)]
temp['reason'] = 'Property linked to multiple Catylist skittles'
drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
del temp
df = df[(df['mult_link_check'] == 0) | (df['survey_legacy_data_source'].isin(['Foundation', 'ApartmentData.com']))]
print('Property count after removing incrementals linked to multiple catylist skittles: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

temp = df.copy()
temp = temp[temp['mult_link_check'] == 1]
temp['count_source_link'] = temp.groupby(['property_reis_rc_id', 'property_source_id'])['property_source_id'].transform('count')
temp['max_source_link'] = temp.groupby('property_reis_rc_id')['count_source_link'].transform('max')
temp = temp[temp['count_source_link'] == temp['max_source_link']]

df = df.join(temp.drop_duplicates('property_reis_rc_id').set_index('property_reis_rc_id').rename(columns={'property_source_id': 'mult_property_source_id'})[['mult_property_source_id']], on='property_reis_rc_id')
del temp
df['property_source_id'] = np.where((df['mult_property_source_id'].isnull() == False), df['mult_property_source_id'], df['property_source_id'])
print('Property count unifying mult prop links: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

temp = log_in.copy()
temp = temp[['id'] + is_structural]
for col in temp.columns:
    if col == 'id':
        continue
    temp[col] = np.where((temp[col] == -1)| (temp[col] == '-1'), np.nan, temp[col])
    temp.rename(columns={col: 'f_' + col}, inplace=True)
temp['property_reis_rc_id'] = 'A' + temp['id'].astype(str)
df = df.join(temp.drop_duplicates('property_reis_rc_id').set_index('property_reis_rc_id')[['f_' + x for x in is_structural]], on='property_reis_rc_id')
del temp
for col in is_structural:
    df[col] = np.where((df['valid'] == 0) & (df['in_log'] == 1), df['f_' + col], df[col])
df['mr_units'] = np.where((df['valid'] == 0) & (df['in_log'] == 1), df['totunits'], df['mr_units'])
df['non_comp_units'] = np.where((df['valid'] == 0) & (df['in_log'] == 1), np.nan, df['non_comp_units'])
temp = df.copy()
temp = temp[(temp['valid'] == 0) & ((temp['nc_eligible'] == False) | (temp['property_reis_rc_id'] != ''))]
temp['reason'] = 'Property is not publishable aptdata.com record, and is not in the log'
drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
del temp
df = df[(df['valid'] == 1) | (df['in_log'] == 1) | ((df['nc_eligible']) & (df['property_reis_rc_id'] == ''))]
print("Property count after removing surveys that are not valid aptdata.com properties and have no umix in the legacy file: {:,}".format(len(df.drop_duplicates('property_source_id'))))
df = df.drop(['f_' + x for x in is_structural], axis=1)


test = log_in.copy()
test['id'] = 'A' + test['id'].astype(str)
log_cols = []
for x in test.columns:
    if x not in is_structural:
        continue
    log_cols.append('f_' + x)
    test.rename(columns={x: 'f_' + x}, inplace=True)
df = df.join(test.drop_duplicates('id').set_index('id')[log_cols], on='property_reis_rc_id')
del test

for col in is_structural:
    df[col] = np.where((df['mult_link_check'] == 1) & (df['survey_legacy_data_source'] == 'Foundation') & (df['count_apt'] == 0), df['f_' + col], df[col])
    
nan_to_string = ['propname', 'metcode', 'address', 'city', 'county', 'state', 'status', 'type2']
for x in nan_to_string:
    df[x] = np.where((df[x].isnull() == True), '', df[x])
    
df = df.drop(log_cols, axis=1)    

test = df.copy()
test = test[test['survey_legacy_data_source'] == 'ApartmentData.com']
log_cols = []
for x in log_in.columns:
    if x not in is_structural:
        continue
    log_cols.append('f_' + x)
    test.rename(columns={x: 'f_' + x}, inplace=True)
df = df.join(test.drop_duplicates('property_reis_rc_id').set_index('property_reis_rc_id')[log_cols], on='property_reis_rc_id')
del test

df['count_apt'] = df[df['survey_legacy_data_source'] == 'ApartmentData.com'].groupby('property_source_id')['property_source_id'].transform('count')
df['count_apt'] = df.groupby('property_source_id')['count_apt'].bfill()
df['count_apt'] = df.groupby('property_source_id')['count_apt'].ffill()

for col in is_structural:
    df[col] = np.where((df['mult_link_check'] == 1) & (df['count_apt'] > 0), df['f_' + col], df[col])
    
nan_to_string = ['propname', 'metcode', 'address', 'city', 'county', 'state', 'status', 'type2']
for x in nan_to_string:
    df[x] = np.where((df[x].isnull() == True), '', df[x])

temp = df.copy()
temp = temp[(temp['metcode'] == '')]
temp['reason'] = 'Property has no metcode'
drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
del temp

df = df[df['metcode'] != '']

print('Property count after removing properties with no metro: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

df['subid_temp'] = np.where((df['subid'].isnull() == True), '', df['subid'])
df['geo_ident'] = df['metcode'] + '/' + df['subid_temp'].astype(str)
temp = df.copy()
test = log_in.copy()
test['metcode'] = np.where((test['metcode'].isnull() == True), '', test['metcode'])
test = test[test['metcode'] != '']
test['subid'] = np.where((test['subid'].isnull() == True), '', test['subid'])
test = test[test['subid'] != '']
test['geo_ident'] = test['metcode'] + '/' + test['subid'].astype(str)
temp = temp[~(temp['geo_ident'].isin(test['geo_ident'].unique())) & ((temp['subid'].isnull() == False) | (~temp['metcode'].isin(log_in['metcode'].unique())))]
temp['reason'] = 'Property linked to metcode subid combination that does not exist for apartment'
drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
del temp


df = df[(df['geo_ident'].isin(test['geo_ident'].unique())) | ((df['subid'].isnull() == True) & (df['metcode'].isin(log_in['metcode'].unique())))]
del test
print('Property count after removing properties with metro sub combos that are not valid apt combos: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

if use_reis_yr:
    test = log_in.copy()
    test['id'] = 'A' + test['id'].astype(str)
    test = test.rename(columns={'year': 'f_year', 'month': 'f_month'})
    df = df.drop(['f_year', 'f_month'], axis=1)
    df = df.join(test.drop_duplicates('id').set_index('id')[['f_year', 'f_month']], on='property_reis_rc_id')
    df[((df['year'] >= curryr - 3) | (df['f_year'] >= curryr - 3)) & ((df['year'] != df['f_year']) | (df['month'] != df['f_month'])) & (df['f_year'].isnull() == False)].drop_duplicates('property_reis_rc_id')[['property_source_id', 'property_reis_rc_id', 'year', 'month', 'f_year', 'f_month']].to_csv('/home/central/square/data/zzz-bb-test2/python/catylist_snapshots/OutputFiles/apt/year_built_diffs_{}m{}.csv'.format(curryr, currmon), index=False)
    df['month'] = np.where(((df['year'] >= curryr - 3) | (df['f_year'] >= curryr - 3)) & (df['f_year'].isnull() == False), df['f_month'], df['month'])
    df['year'] = np.where(((df['year'] >= curryr - 3) | (df['f_year'] >= curryr - 3)) & (df['f_year'].isnull() == False), df['f_year'], df['year'])
    del test
df['renov'] = np.where((df['renov'].isnull() == False) & (df['renov'] < df['year']), np.nan, df['renov'])

for col in df.columns:
    if col in is_structural or col == 'property_source_id':
        df[col] = np.where((df[col] == ''), np.nan, df[col])
        df[col] = df.groupby('property_source_id')[col].bfill()
        df[col] = df.groupby('property_source_id')[col].ffill()
        df['count'] = df.groupby('property_source_id')[col].transform('nunique')
        if len(df[df['count'] > 1]) > 0:
            print("Structural inconsistency for {} at {:,} properties".format(col, len(df[df['count'] > 1].drop_duplicates('property_source_id'))))
            display(df[df['count'] > 1].sort_values(by=['property_source_id'], ascending=[True])[['property_source_id', 'property_reis_rc_id', col, 'survey_legacy_data_source', 'mult_link_check']].drop_duplicates(col).head(2))          

df[(df['in_log'].isnull() == True) & (df['year'] >= curryr - 1) & (df['property_reis_rc_id'] == '')].drop_duplicates('property_source_id')[['property_source_id', 'property_er_to_foundation_ids_list', 'metcode', 'subid', 'year', 'month', 'totunits', 'mr_units']].to_csv('/home/central/square/data/zzz-bb-test2/python/catylist_snapshots/OutputFiles/apt/new_nc_{}m{}.csv'.format(curryr, currmon), index=False)

test = log_in.copy()
temp = df.copy()
temp['in_view'] = 1
test['property_reis_rc_id'] = 'A' + test['id'].astype(str)
test = test.join(temp.drop_duplicates('property_reis_rc_id').set_index('property_reis_rc_id')[['in_view']], on='property_reis_rc_id')
test = test.join(drop_log.drop_duplicates(['property_reis_rc_id']).set_index('property_reis_rc_id')[['reason']], on='property_reis_rc_id')
if len(test[(test['in_view'].isnull() == True) & (test['realyr'].isnull() == False) & (test['reason'] == '')]) > 0:
    display(test[(test['in_view'].isnull() == True) & (test['realyr'].isnull() == False) & (test['reason'] == '')].drop_duplicates('property_reis_rc_id')[['property_reis_rc_id', 'reason']])

if len(temp[(temp['in_log'].isnull() == True) & (temp['year'] < curryr - 1)]) > 0:
    display(temp[(temp['in_log'].isnull() == True)].drop_duplicates('property_reis_rc_id')[['property_source_id', 'property_reis_rc_id', 'housing_type', 'type2', 'year']])
del test
del temp

temp = log_in.copy()
temp['count_rows_log'] = temp.groupby('metcode')['id'].transform('count')
temp = temp.drop_duplicates('metcode')

temp1 = df.copy()
temp1 = temp1[temp1['survey_legacy_data_source'] == 'Foundation']
temp1['count_rows_df'] = temp1.groupby('metcode')['property_source_id'].transform('count')
temp1 = temp1.drop_duplicates('metcode')

temp = temp.join(temp1.set_index('metcode')[['count_rows_df']], on='metcode')
temp['diff'] = temp['count_rows_df'] - temp['count_rows_log']
temp['perc_diff'] = abs((temp['count_rows_df'] - temp['count_rows_log'])) / temp['count_rows_log']
if temp[(temp['count_rows_log'] > 3000) | (temp['count_rows_df'] == 0)]['perc_diff'].max() > 0.1 or temp['perc_diff'].max() > 0.5:
    print("There is a significant difference in historical rows between the legacy download and the preprocessed logs")
temp[['metcode', 'count_rows_log', 'count_rows_df', 'diff', 'perc_diff']].sort_values(by=['perc_diff'], ascending=[False]).to_csv('/home/central/square/data/zzz-bb-test2/python/catylist_snapshots/OutputFiles/apt/diff_log_report_{}m{}.csv'.format(curryr, currmon), index=False)

del temp1

df['survdate'] = pd.to_datetime(df['survdate']).dt.strftime('%m/%d/%Y')
df['count'] = df[df['survey_legacy_data_source'] != 'ApartmentData.com'].groupby('property_source_id')['property_source_id'].transform('count')
df['count'] = df.groupby('property_source_id')['count'].bfill()
df['count'] = df.groupby('property_source_id')['count'].ffill()
df['survdate'] = np.where((df['year'] >= curryr - 1) & (df['property_reis_rc_id'] == '') & (df['count'] == 1), '{}/15/{}'.format(currmon, curryr), df['survdate'])
df['realyr'] = np.where((df['year'] >= curryr - 1) & (df['property_reis_rc_id'] == '') & (df['count'] == 1), curryr, df['realyr'])
df['realqtr'] = np.where((df['year'] >= curryr - 1) & (df['property_reis_rc_id'] == '') & (df['count'] == 1), np.ceil(currmon / 3), df['realqtr'])

df['catylist_id'] = np.where((df['property_reis_rc_id'] == '') & (df['property_source_id'].str.isdigit()), 'a' + df['property_source_id'], df['property_source_id'])
df['id'] = np.where((df['property_reis_rc_id'] != ''), df['property_reis_rc_id'].str[1:], df['catylist_id'])
df = df[list(log_in.columns) + ['property_reis_rc_id', 'property_source_id']]

df = df.join(live_subs[live_subs['subid'] == 90].drop_duplicates('metcode').set_index('metcode').rename(columns={'subid': 'tertiary_sub'})[['tertiary_sub']], on='metcode')
df['subid'] = np.where((df['tertiary_sub'] == 90), df['tertiary_sub'], df['subid'])

display(pd.DataFrame(drop_log.groupby('reason')['property_source_id'].count()).rename(columns={'property_source_id': 'count'}).sort_values(by=['count'], ascending=[False]))
test = log_in.copy()
test['property_reis_rc_id'] = 'A' + test['id'].astype(str)
test['in_log'] = 1
test1 = test.copy()
test1['count'] = test1.groupby('property_reis_rc_id')['property_reis_rc_id'].transform('count')
test1 = test1[(test1['count'] == 1) & (test1['ren0'] == -1) & (test1['ren1'] == -1) & (test1['ren2'] == -1) & (test1['ren3'] == -1) & (test1['ren4'] == -1) & (test1['vac0'] == -1) & (test1['vac1'] == -1) & (test1['vac2'] == -1) & (test1['vac3'] == -1) & (test1['vac4'] == -1) & (test1['avail'] == -1) & (test1['free_rent'] == -1)]
test1['has_surv'] = 0
drop_log = drop_log.join(test.drop_duplicates('property_reis_rc_id').set_index('property_reis_rc_id')[['in_log']], on='property_reis_rc_id')
drop_log = drop_log.join(test1.drop_duplicates('property_reis_rc_id').set_index('property_reis_rc_id')[['has_surv']], on='property_reis_rc_id')
drop_log['has_surv'] = np.where((drop_log['has_surv'].isnull() == True) & (drop_log['in_log'] == 1), 1, drop_log['has_surv'])
drop_log['has_surv'] = np.where((drop_log['has_surv'].isnull() == True) & (drop_log['in_log'].isnull() == True), 0, drop_log['has_surv'])
drop_log['in_log'] = drop_log['in_log'].fillna(0)
drop_log['in_log'] = np.where((drop_log['property_reis_rc_id'] == ''), 0, drop_log['in_log'])
temp = df.copy()
temp['in_snap'] = 1
drop_log = drop_log.join(temp.drop_duplicates('property_reis_rc_id').set_index('property_reis_rc_id')[['in_snap']], on='property_reis_rc_id')
drop_log['in_snap'] = drop_log['in_snap'].fillna(0)
drop_log['in_snap'] = np.where((drop_log['property_reis_rc_id'] == ''), 0, drop_log['in_snap'])
drop_log.to_csv('/home/central/square/data/zzz-bb-test2/python/catylist_snapshots/OutputFiles/apt/drop_log_{}m{}.csv'.format(curryr, currmon), index=False)

del test
del test1

df.drop_duplicates('property_reis_rc_id')[['property_source_id', 'property_reis_rc_id']].to_csv('/home/central/square/data/zzz-bb-test2/python/catylist_snapshots/OutputFiles/apt/property_ids.csv', index=False)
df = df.drop(['property_reis_rc_id'], axis=1)

for met in log_in['metcode'].unique():
    logging.info('Saving log for {}'.format(met))
    path = '/home/central/square/data/apt/download/test2022/{}.log'.format(met.lower())
    df[df['metcode'] == met].to_csv(r'{}'.format(path), header=df.columns, index=None, sep=',', mode='w')

if update_umix:

    print("Updating Umixfamily...")

    df_survs = df.copy()
    drop_log_survs = drop_log.copy()

    df_in = pd.read_csv('/home/central/square/data/zzz-bb-test2/python/catylist_snapshots/InputFiles/umix_family_view_{}m{}.csv'.format(curryr, currmon), na_values="", keep_default_na=False)
    print("umix view file dimensions: {}".format(df_in.shape))
    umix_in = pd.read_csv('/home/central/vc/mfp/finaltoit/anytime/umixfamily.txt', sep='\t', header=0, na_values= "", keep_default_na = False)
    umix_in.columns= umix_in.columns.str.strip().str.lower()
    umix_in['spacetype'] = umix_in['spacetype'].str.replace('bath', 'Bath')
    print("umix file dimensions: {}".format(umix_in.shape))

    df_in['survey_legacy_data_source'] = np.where((df_in['survey_legacy_data_source'].isnull() == True), '', df_in['survey_legacy_data_source'])
    df_in['survey_legacy_data_source'] = np.where((df_in['survey_legacy_data_source'].str.contains('Foundation') == True), 'Foundation', df_in['survey_legacy_data_source'])

    conv_float = ['units', 'sqftavg', 'normalized_rent_by_month_unit_average_amount', 'vacant', 'unit_detail_full_baths',
                'unit_detail_half_baths', 'unit_detail_three_quarter_baths', 'unit_detail_one_quarter_baths', 
                'renthigh', 'rentlow', 'unit_detail_combined_room_config_beds', 'is_section_8_housing_flag']
    for col in conv_float:
        df_in[col] = df_in[col].astype(float)

    if df_in['normalized_rent_by_month_unit_average_amount'].max() > 15000:
        display(df_in[df_in['normalized_rent_by_month_unit_average_amount'] > 15000].drop_duplicates(['property_source_id', 'spacetype']).sort_values(by=['normalized_rent_by_month_unit_average_amount'], ascending=[False]).head(10)[['property_source_id', 'property_reis_rc_id_an', 'normalized_rent_by_month_unit_average_amount', 'survdate', 'survey_legacy_data_source']].rename(columns={'property_source_id': 'c_id', 'property_reis_rc_id_an': 'r_id', 'normalized_rent_by_month_unit_average_amount': 'avgrent', 'survey_legacy_data_source': 'surv_source'}))
    
    test = df_in.copy()
    test['survdate_d'] = pd.to_datetime(test['survdate'])
    if datetime.today().weekday() != 0:
        delta_val = 1
    else:
        delta_val = 3
    test = test[(test['survey_legacy_data_source'] == '') & (test['survdate_d'] == datetime.strftime(date.today() - timedelta(days = delta_val), '%Y-%m-%d'))][['property_source_id', 'survdate']]
    if len(test) == 0:
        print("There are no incremental surveys for {}".format(datetime.strftime(date.today() - timedelta(days = delta_val), '%m/%d/%Y')))
    del test

    is_structural = ['selectcode', 'selectcodeshortdesc', 'propertytype', 'propertytypeshortdesc', 'bedrooms', 'bathrooms',
                'spacetype', 'units', 'sqftavg']

    df = df_in.copy()
    drop_log = pd.DataFrame()

    df['spacetype'] = np.where((df['spacetype'].isnull() == True), '', df['spacetype'])

    df = df.rename(columns={'foundation_propertytype': 'propertytype', 'unit_detail_combined_room_config_beds': 'bedrooms', 'first_year': 'year', 'first_month': 'month', 'survey_completed_datetime': 'completed_date_full'})

    df['property_reis_rc_id'] = df['property_reis_rc_id_an']
    df['property_reis_rc_id'] = np.where((df['property_reis_rc_id'].str[0] == 'L'), 'A'+ df['property_reis_rc_id'].str[1:], df['property_reis_rc_id'])
    df['property_reis_rc_id'] = np.where((df['property_reis_rc_id'].str[-1] == '-'), df['property_reis_rc_id'].str[:-1], df['property_reis_rc_id'])

    df['property_source_id'] = np.where((df['property_source_id'].isnull() == True), df['property_reis_rc_id'], df['property_source_id'])

    df['property_source_id'] = df['property_source_id'].astype(str)

    if len(df[df['property_source_id'] == '']) > 0:
        print("There are properties without an id")

    nan_to_string = ['property_source_id', 'property_reis_rc_id', 'property_er_to_foundation_ids_list', 'category',
                    'subcategory', 'reis_sector', 'property_geo_msa_code', 'housing_type']
    for x in nan_to_string:
        df[x] = np.where((df[x].isnull() == True), '', df[x])
        
    to_lower = ['category', 'subcategory', 'housing_type']
    for x in to_lower:
        df[x] = df[x].str.lower()
        
    df['normalized_rent_by_month_unit_average_amount'] = round(df['normalized_rent_by_month_unit_average_amount'], 0)
        
    df['has_half_bath'] = np.where((df['unit_detail_half_baths'].isnull() == False) & (df['unit_detail_half_baths'] > 0), True, False)
    df['has_one_quarter_bath'] = np.where((df['unit_detail_one_quarter_baths'].isnull() == False) & (df['unit_detail_one_quarter_baths'] > 0), True, False)
    df['has_three_quarter_bath'] = np.where((df['unit_detail_three_quarter_baths'].isnull() == False) & (df['unit_detail_three_quarter_baths'] > 0), True, False)
    df['bathrooms'] = np.where((df['unit_detail_full_baths'].isnull() == False) & (df['has_half_bath']), df['unit_detail_full_baths'] + 0.5, np.nan)
    df['bathrooms'] = np.where((df['unit_detail_full_baths'].isnull() == False) & (df['has_one_quarter_bath']), df['unit_detail_full_baths'] + 0.25, df['bathrooms'])
    df['bathrooms'] = np.where((df['unit_detail_full_baths'].isnull() == False) & (df['has_three_quarter_bath']), df['unit_detail_full_baths'] + 0.75, df['bathrooms'])
    df['bathrooms'] = np.where((df['unit_detail_full_baths'].isnull() == True) & (df['has_half_bath']), 0.5, df['bathrooms'])
    df['bathrooms'] = np.where((df['unit_detail_full_baths'].isnull() == True) & (df['has_one_quarter_bath']), 0.25, df['bathrooms'])
    df['bathrooms'] = np.where((df['unit_detail_full_baths'].isnull() == True) & (df['has_three_quarter_bath']), 0.75, df['bathrooms'])
    df['bathrooms'] = np.where((df['unit_detail_full_baths'].isnull() == False) & (df['has_half_bath'] == False) & (df['has_one_quarter_bath'] == False) & (df['has_three_quarter_bath'] == False), df['unit_detail_full_baths'], df['bathrooms'])

    test = umix_in.copy()
    test['in_umix'] = 1
    test = test[test['surveydate'].isnull() == False]
    df = df.join(test.drop_duplicates('propertyid').set_index('propertyid')[['in_umix']], on='property_reis_rc_id')
    del test

    display(pd.DataFrame(df.groupby('survey_legacy_data_source')['property_source_id'].count()).rename(index={'survey_legacy_data_source': 'survey_source'}, columns={'property_source_id': 'count_rows'}))

    print("Initial row count: {:,}".format(len(df)))
    print('Initial unique property count: {:,}'.format(len(df.drop_duplicates('property_source_id'))))
    df['survdate_d'] = pd.to_datetime(df['survdate'])
    temp = df.copy()
    temp['property_reis_rc_id'] = np.where((temp['property_reis_rc_id'] == '') & (temp['property_er_to_foundation_ids_list'].str[0] == 'A'), temp['property_er_to_foundation_ids_list'].str.split(',').str[0], temp['property_reis_rc_id'])
    temp['count_links'] = temp.groupby('property_reis_rc_id')['property_source_id'].transform('nunique')
    temp['count_links'] = np.where((temp['property_reis_rc_id'] == '') & (temp['property_er_to_foundation_ids_list'].str[0] != 'A'), 0, temp['count_links'])
    temp['count_test'] = temp[temp['survey_legacy_data_source'] == 'REIS_RC_Apt'].groupby('property_source_id')['property_source_id'].transform('count')
    temp['count_test'] = temp.groupby('property_source_id')['count_test'].bfill()
    temp['count_test'] = temp.groupby('property_source_id')['count_test'].ffill()
    temp['count_early'] = temp[(temp['survdate_d'] < '06/01/2022') & (temp['survey_legacy_data_source'] == '')].groupby('property_source_id')['property_source_id'].transform('count')
    temp['count_early'] = temp.groupby('property_source_id')['count_early'].bfill()
    temp['count_early'] = temp.groupby('property_source_id')['count_early'].ffill()
    temp['count'] = temp.groupby('property_source_id')['property_source_id'].transform('count')
    temp = temp[((temp['survey_legacy_data_source'] == 'REIS_RC_Apt') & (temp['count'] == temp['count_test'])) | ((temp['survey_legacy_data_source'] == '') & (temp['survdate_d'] < '06/01/2022') & (temp['count'] == temp['count_early']))]
    temp['reason'] = 'No legacy historical rows, and RDMA survey not true incremental survey'
    drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason', 'count_links']], ignore_index=True)
    del temp
    df = df[(df['survey_legacy_data_source'].isin(['Foundation', 'ApartmentData.com'])) | ((df['survdate_d'] >= '06/01/2022') & (df['survey_legacy_data_source'] == ''))]
    print('Property count after removing test surveys: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

    df = df.join(valid_aptdata.drop_duplicates('id').set_index('id')[['valid']], on='property_reis_rc_id')
    df['count_apt'] = df[df['survey_legacy_data_source'] == 'ApartmentData.com'].groupby('property_source_id')['property_source_id'].transform('count')
    df['count_apt'] = df.groupby('property_source_id')['count_apt'].bfill()
    df['count_apt'] = df.groupby('property_source_id')['count_apt'].ffill()
    df['count_apt'] = df['count_apt'].fillna(0)
    df['valid'] = np.where((df['count_apt'] == 0) | (df['property_reis_rc_id'].str[0] != 'A') | (df['in_umix'].isnull() == True), 1, df['valid'])
    df['valid'] = df['valid'].fillna(0)
    temp = df.copy()
    temp['property_reis_rc_id'] = np.where((temp['property_reis_rc_id'] == '') & (temp['property_er_to_foundation_ids_list'].str[0] == 'A'), temp['property_er_to_foundation_ids_list'].str.split(',').str[0], temp['property_reis_rc_id'])
    temp['count_links'] = temp.groupby('property_reis_rc_id')['property_source_id'].transform('nunique')
    temp['count_links'] = np.where((temp['property_reis_rc_id'] == '') & (temp['property_er_to_foundation_ids_list'].str[0] != 'A'), 0, temp['count_links'])
    temp['count'] = temp.groupby('property_source_id')['property_source_id'].transform('count')
    temp = temp[(temp['survey_legacy_data_source'] == 'ApartmentData.com') & (temp['count'] == temp['count_apt']) & (temp['valid'] == 0) & (temp['property_reis_rc_id'].str[0] == 'A')]
    temp['reason'] = 'Aptdata.com survey deemed not publishable'
    drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason', 'count_links']], ignore_index=True)
    del temp
    df = df[(df['survey_legacy_data_source'] != 'ApartmentData.com') | (df['valid'] == 1) | (df['property_reis_rc_id'].str[0] != 'A')]
    print('Property count after removing non published aptdata.com surveys: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

    display(pd.DataFrame(df.groupby('survey_legacy_data_source')['property_source_id'].count()).rename(index={'survey_legacy_data_source': 'survey_source'}, columns={'property_source_id': 'count_rows'}))

    df['reis_record'] = np.where((df['property_reis_rc_id'] == '') | (df['property_reis_rc_id'].str[0] != 'A'), False, True)
    df['nc_eligible'] = np.where((df['year'] >= curryr - 1) & (df['month'].isnull() == False) & ((df['property_reis_rc_id'].str[0] == 'A') | (df['property_reis_rc_id'].str[0].isnull() == True)), True, False)
    temp = df.copy()
    temp = temp[(temp['reis_record'] == False) & (temp['nc_eligible'] == False)]
    temp['reason'] = 'Property is not linked to a REIS Apartment record'
    drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
    del temp
    df = df[(df['reis_record']) | (df['nc_eligible'])]
    print('Property count after removing properties not linked to REIS ID: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

    temp = df.copy()
    temp = temp[((temp['year'] > curryr) & (temp['year'].isnull() == False)) | ((temp['year'] == curryr) & (temp['month'] > currmon) & (temp['month'].isnull() == False))]
    temp['reason'] = 'Property has year built in the future'
    drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
    del temp
    df = df[(df['year'] < curryr) | (df['year'].isnull() == True) | ((df['year'] == curryr) & ((df['month'] <= currmon) | (df['month'].isnull() == True)))]
    print('Property count after removing props with future year builts: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

    df['sum_units'] = df.drop_duplicates(['property_source_id', 'spacetype']).groupby('property_source_id')['units'].transform('sum')
    df['sum_units'] = df.groupby('property_source_id')['sum_units'].bfill()
    df['sum_units'] = df.groupby('property_source_id')['sum_units'].ffill()
    df['has_units'] = np.where(((df['sum_units'] > 0)) | (df['occupancy_number_of_units'] > 0), True, False)
    print("Put this in once we are confident that data is clean")
    # temp = df.copy()
    # temp = temp[(temp['category'] != 'multifamily') & ((temp['subcategory'] != 'mixed_use') | (temp['has_units'] == False)) & (temp['property_source_id'] != temp['property_reis_rc_id'])]
    # temp['reason'] = 'property no longer multifamily'
    # drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
    # del temp
    # df = df[(df['category'] == 'multifamily') | ((df['subcategory'] =='mixed_use') & (df['has_units'])) | (df['property_source_id'] == df['property_reis_rc_id'])]
    # print('Property count after removing non RDMA multifamily properties: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

    if len(df[df['survdate'].isnull() == True]) > 0:
        print("There are rows that are missing a survey date")

    df['count'] = df.groupby(['property_source_id', 'survdate', 'spacetype'])['property_source_id'].transform('count')
    df['dupe_surv_check'] = np.where((df['count'] > 1), 1, 0)
    if len(df[(df['dupe_surv_check'] == 1) & (df['survey_legacy_data_source'] == 'Foundation')]) > 500 or len(df[(df['dupe_surv_check'] == 1) & (df['survey_legacy_data_source'] != 'Foundation')]):
        print("There are duplicate surveys")

    df['count_source'] = df.groupby(['property_source_id', 'survdate', 'spacetype'])['survey_legacy_data_source'].transform('nunique')
    df['dupe_source_check'] = np.where((df['count_source'] > 1), 1, 0)
    if len(df[df['dupe_source_check'] == 1]) > 0:
        print("There are surveys on the same date for different sources")

    df['completed_date_full'] = np.where((df['survey_legacy_data_source'] == 'Foundation'), np.datetime64('NaT'), df['completed_date_full'])
    df.sort_values(by=['property_source_id', 'completed_date_full'], ascending=[True, False], inplace=True)
    df['count_no_ts'] = df.groupby(['property_source_id', 'survdate', 'spacetype'])['property_source_id'].transform('count')
    df['cumcount_id'] = df.groupby(['property_source_id', 'spacetype'])['property_source_id'].transform('cumcount')
    df = df[(df['cumcount_id'] == 0) | (df['count_no_ts'] == 1) | (df['survey_legacy_data_source'] == 'Foundation')]
    print('Property count after removing incremental surveys that occurred on the same day: {:,}'.format(len(df.drop_duplicates('property_source_id'))))


    for col in ['buildings_condominiumized_flag']:
        df[col] = np.where((df[col] == 'Y'), 1, 0)

    df['is_section_8_housing_flag'] = np.where((df['is_section_8_housing_flag'].isnull() == True), 0, df['is_section_8_housing_flag'])

    temp = df.copy()
    temp = temp[temp['is_section_8_housing_flag'] == 1]
    temp['reason'] = 'Property is Section 8'
    drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
    del temp
    df = df[(df['is_section_8_housing_flag'] == 0)]
    print('Property count after removing section 8 properties: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

    temp = df.copy()
    temp = temp[(temp['buildings_condominiumized_flag'] == 1) & (temp['in_umix'].isnull() == True)]
    temp['reason'] = 'Property is Condo'
    drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
    del temp
    df = df[(df['buildings_condominiumized_flag'] == 0) | (df['in_umix'] == 1)]
    print('Property count after removing condo properties: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

    temp = df.copy()
    temp = temp[(temp['housing_type'].isin(['affordable', 'age_restricted']))]
    temp['reason'] = 'Property is Affordable Housing'
    drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
    del temp
    df = df[~(df['housing_type'].isin(['affordable', 'age_restricted']))]    
    print('Property count after removing affordable properties: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

    temp = df.copy()
    temp = temp[temp['housing_type'] == 'student']
    temp['reason'] = 'Property is Student Housing'
    drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
    del temp
    df = df[(df['housing_type'] != 'student')]
    print('Property count after removing student properties: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

    temp = df.copy()
    temp = temp[(temp['in_umix'].isnull() == True) & ((temp['year'] < curryr - 1) | (temp['month'].isnull() == True))]
    temp['reason'] = 'Property is linked to REIS record that is not in legacy umix. Possible Aff issue'
    drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
    del temp
    df = df[(df['in_umix'] == 1) | ((df['year'] >= curryr - 1) & (df['month'].isnull() == False))]
    print('Property count after removing properties linked to REIS ID not in legacy umix: {:,}'.format(len(df.drop_duplicates('property_source_id'))))
    print("Can take this last drop out after market rate units are reliably filled in, and will let IAG props come in with incrementals")

    temp = df.copy()
    temp = temp[(temp['property_reis_rc_id'] == '') & (temp['year'] >= curryr - 1) & (temp['in_umix'].isnull() == True)]

    test = umix_in.copy()
    umix_ids = list(test[test['surveydate'].isnull() == False].drop_duplicates('propertyid')['propertyid'])
    del test

    drop_list = []
    for index, row in temp.iterrows():
        if row['property_er_to_foundation_ids_list'] != '':
            er_ids = row['property_er_to_foundation_ids_list'].split(',')

            for er_id in er_ids:
                if er_id[0] == 'A':
                    if er_id in umix_ids:
                        drop_list.append(row['property_source_id'])
                        break
    
    temp = df.copy()
    temp = temp[(temp['property_source_id'].isin(drop_list))]
    temp['reason'] = 'Property potential new construction, but ER link indicates property is already in umix'
    drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
    del temp

    df = df[~df['property_source_id'].isin(drop_list)]
    print('Property count after removing potential nc properties that likely already exist in the log: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

    temp = df.copy()
    temp = temp[(temp['property_reis_rc_id'] == '') & (temp['year'] >= curryr - 1) & (temp['in_umix'].isnull() == True) & (temp['has_units'] == False)]
    temp['reason'] = 'Net new NC property but no total units'
    drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
    temp['drop_this'] = 1
    df = df.join(temp.drop_duplicates('property_source_id').set_index('property_source_id')[['drop_this']], on='property_source_id')
    df = df[df['drop_this'].isnull() == True]
    del temp
    print('Property count after removing potential nc properties without total units: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

    temp = df.copy()
    temp = temp.drop_duplicates('property_source_id')
    temp['count_links'] = temp.groupby('property_reis_rc_id')['property_source_id'].transform('count')
    df = df.join(temp.drop_duplicates('property_reis_rc_id').set_index('property_reis_rc_id')[['count_links']], on='property_reis_rc_id')
    df['count_links'] = df['count_links'].fillna(1)
    df['mult_link_check'] = np.where((df['count_links'] > 1) & (df['property_reis_rc_id'] != ''), 1, 0)
    del temp

    df['count_inc'] = df[df['survey_legacy_data_source'].isin(['', 'ApartmentData.com'])].groupby('property_source_id')['property_source_id'].transform('count')
    df['count'] = df.groupby('property_source_id')['property_source_id'].transform('count')
    temp = df.copy()
    temp = temp[(temp['count_inc'] == temp['count']) & (temp['mult_link_check'] == 1)]
    temp['reason'] = 'Property linked to multiple Catylist skittles'
    drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
    del temp
    df = df[(df['mult_link_check'] == 0) | (df['survey_legacy_data_source'].isin(['Foundation']))]
    print('Property count after removing incrementals linked to multiple catylist skittles: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

    temp = df.copy()
    temp = temp[temp['mult_link_check'] == 1]
    temp['count_source_link'] = temp.groupby(['property_reis_rc_id', 'property_source_id'])['property_source_id'].transform('count')
    temp['max_source_link'] = temp.groupby('property_reis_rc_id')['count_source_link'].transform('max')
    temp = temp[temp['count_source_link'] == temp['max_source_link']]

    df = df.join(temp.drop_duplicates('property_reis_rc_id').set_index('property_reis_rc_id').rename(columns={'property_source_id': 'mult_property_source_id'})[['mult_property_source_id']], on='property_reis_rc_id')
    df['property_source_id'] = np.where((df['mult_property_source_id'].isnull() == False), df['mult_property_source_id'], df['property_source_id'])
    del temp
    print('Property count unifying mult prop links: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

    temp = umix_in.copy()
    temp = temp[['propertyid', 'spacetype', 'units', 'sqftavg']]
    temp = temp.rename(columns={'units': 'f_units', 'sqftavg': 'f_sqftavg'})
    temp['identity'] = temp['propertyid'] + '/' + temp['spacetype']
    temp['space_in_umix'] = 1
    df['identity'] = df['property_reis_rc_id'] + '/' + df['spacetype']
    df = df.join(temp.drop_duplicates('identity').set_index('identity')[['f_units', 'f_sqftavg', 'space_in_umix']], on='identity')
    del temp
    for col in ['units', 'sqftavg']:
        df[col] = np.where((df['valid'] == 0) & (df['space_in_umix'] == 1), df['f_' + col], df[col])

    temp = df.copy()
    temp['in_umix_sum'] = temp.groupby('property_source_id')['space_in_umix'].transform('sum')
    temp = temp[(temp['valid'] == 0) & (temp['in_umix_sum'] == 0) & ((temp['nc_eligible'] == False) | (temp['property_reis_rc_id'] != ''))]
    temp['reason'] = 'Property is not publishable aptdata.com record, and has no umixes that exist in legacy file'
    drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
    del temp
    df = df[(df['valid'] == 1) | (df['space_in_umix'] == 1) | ((df['nc_eligible']) & (df['property_reis_rc_id'] == ''))]
    print("Property count after removing surveys that are not valid aptdata.com properties and have no umix in the legacy file: {:,}".format(len(df.drop_duplicates('property_source_id'))))
    df = df.drop(['f_units', 'f_sqftavg', 'space_in_umix'], axis=1)

    df['ident'] = df['property_reis_rc_id'] + '/' + df['spacetype']
    test = umix_in.copy()
    test['ident']= test['propertyid']+ '/' + test['spacetype']
    umix_cols = []
    for x in test.columns:
        if x not in is_structural:
            continue
        umix_cols.append('f_' + x)
        test.rename(columns={x: 'f_' + x}, inplace=True)
    df = df.join(test.drop_duplicates('ident').set_index('ident')[umix_cols], on='ident')
    del test

    for col in is_structural:
        df[col] = np.where((df['mult_link_check'] == 1) & (df['survey_legacy_data_source'] == 'Foundation') & (df['count_apt'] == 0), df['f_' + col], df[col])
        
        
    df = df.drop(umix_cols, axis=1)    

    test = df.copy()
    test = test[test['survey_legacy_data_source'] == 'ApartmentData.com']
    umix_cols = []
    for x in umix_in.columns:
        if x not in is_structural + ['market_rate_units']:
            continue
        umix_cols.append('f_' + x)
        test.rename(columns={x: 'f_' + x}, inplace=True)
    df = df.join(test.drop_duplicates('property_reis_rc_id').set_index('property_reis_rc_id')[umix_cols], on='property_reis_rc_id')
    del test

    df['count_apt'] = df[df['survey_legacy_data_source'] == 'ApartmentData.com'].groupby('property_source_id')['property_source_id'].transform('count')
    df['count_apt'] = df.groupby('property_source_id')['count_apt'].bfill()
    df['count_apt'] = df.groupby('property_source_id')['count_apt'].ffill()

    for col in is_structural:
        df[col] = np.where((df['mult_link_check'] == 1) & (df['count_apt'] > 0), df['f_' + col], df[col])

    temp = log_in.copy()
    temp['property_reis_rc_id'] = 'A' + temp['id'].astype(str)
    df = df.join(temp.drop_duplicates('property_reis_rc_id').set_index('property_reis_rc_id')[['metcode', 'subid']], on='property_reis_rc_id')
    del temp
    df['metcode'] = np.where((df['property_geo_msa_code'] != ''), df['property_geo_msa_code'], df['metcode'])
    df['metcode'] = np.where((df['metcode'].isnull() == True), '', df['metcode'])
    df['subid'] = np.where((df['property_geo_subid'].isnull() == False), df['property_geo_subid'], df['subid'])
    temp = df.copy()
    temp = temp[(temp['metcode'] == '')]
    temp['reason'] = 'Property has no metcode'
    drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
    del temp

    df = df[df['metcode'] != '']

    print('Property count after removing properties with no metro: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

    df['subid_temp'] = np.where((df['subid'].isnull() == True), '', df['subid'])
    df['geo_ident'] = df['metcode'] + '/' + df['subid_temp'].astype(str)
    temp = df.copy()
    test = log_in.copy()
    test['metcode'] = np.where((test['metcode'].isnull() == True), '', test['metcode'])
    test = test[test['metcode'] != '']
    test['subid'] = np.where((test['subid'].isnull() == True), '', test['subid'])
    test = test[test['subid'] != '']
    test['geo_ident'] = test['metcode'] + '/' + test['subid'].astype(str)
    temp = temp[~(temp['geo_ident'].isin(test['geo_ident'].unique())) & ((temp['subid'].isnull() == False) | (~temp['metcode'].isin(log_in['metcode'].unique())))]
    temp['reason'] = 'Property linked to metcode subid combination that does not exist for apartment'
    drop_log = drop_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'property_reis_rc_id', 'reason']], ignore_index=True)
    del temp

    df = df[(df['geo_ident'].isin(test['geo_ident'].unique())) | ((df['subid'].isnull() == True) & (df['metcode'].isin(log_in['metcode'].unique())))]
    del test
    print('Property count after removing properties with metro sub combos that are not valid apt combos: {:,}'.format(len(df.drop_duplicates('property_source_id'))))

    print("dont need this after AS fixes upstream")
    df['count_dupes'] = df.groupby(['property_source_id', 'spacetype', 'survdate', 'survey_legacy_data_source'])['property_source_id'].transform('count')
    temp = df.copy()
    temp.sort_values(by=['property_source_id', 'survdate_d'], ascending=[True, False], inplace=True)
    temp = temp.drop_duplicates('property_source_id')
    temp['ident'] = temp['property_source_id'] + '/' + temp['survdate_d'].astype(str)
    temp['mr_surv'] = 1
    df['ident'] = df['property_source_id'] + '/' + temp['survdate_d'].astype(str)
    df = df.join(temp.drop_duplicates('ident').set_index('ident')[['mr_surv']], on='ident')
    del temp
    df['units_dupe'] = df[(df['survey_legacy_data_source'] != 'Foundation') & (df['count_dupes'] > 1) & (df['mr_surv'] == 1)].groupby(['property_source_id', 'spacetype', 'survdate'])['units'].transform('sum', min_count=1)
    df['sqftavg_dupe'] = df[(df['survey_legacy_data_source'] != 'Foundation') & (df['count_dupes'] > 1) & (df['mr_surv'] == 1)].groupby(['property_source_id', 'spacetype', 'survdate'])['sqftavg'].transform('mean')
    df['units_dupe'] = df.groupby(['property_source_id', 'spacetype'])['units_dupe'].bfill()
    df['units_dupe'] = df.groupby(['property_source_id', 'spacetype'])['units_dupe'].ffill()
    df['sqftavg_dupe'] = df.groupby(['property_source_id', 'spacetype'])['sqftavg_dupe'].bfill()
    df['sqftavg_dupe'] = df.groupby(['property_source_id', 'spacetype'])['sqftavg_dupe'].ffill()
    df['units'] = np.where((df['units_dupe'].isnull() == False), df['units_dupe'], df['units'])
    df['sqftavg'] = np.where((df['sqftavg_dupe'].isnull() == False), df['sqftavg_dupe'], df['sqftavg'])
    df['normalized_rent_by_month_unit_average_amount_dupe'] = df[(df['survey_legacy_data_source'] != 'Foundation') & (df['count_dupes'] > 1)].groupby(['property_source_id', 'spacetype', 'survdate'])['normalized_rent_by_month_unit_average_amount'].transform('mean')
    df['vacant_dupe'] = df[(df['survey_legacy_data_source'] != 'Foundation') & (df['count_dupes'] > 1)].groupby(['property_source_id', 'spacetype', 'survdate'])['vacant'].transform('sum', min_count=1)
    df['normalized_rent_by_month_unit_average_amount'] = np.where((df['normalized_rent_by_month_unit_average_amount_dupe'].isnull() == False), df['normalized_rent_by_month_unit_average_amount_dupe'], df['normalized_rent_by_month_unit_average_amount'])
    df['vacant'] = np.where((df['vacant_dupe'].isnull() == False), df['vacant_dupe'], df['vacant'])
    
    for col in df.columns:
        if col in is_structural or col == 'property_source_id':
            df[col] = np.where((df[col] == ''), np.nan, df[col])
            df[col] = df.groupby(['property_source_id', 'spacetype'])[col].bfill()
            df[col] = df.groupby(['property_source_id', 'spacetype'])[col].ffill()
            df['count'] = df.groupby(['property_source_id', 'spacetype'])[col].transform('nunique')
            if len(df[df['count'] > 1]) > 0:
                print("Structural inconsistency for {} at {:,} properties".format(col, len(df[df['count'] > 1].drop_duplicates('property_source_id'))))
                display(df[df['count'] > 1].sort_values(by=['property_source_id', 'spacetype'], ascending=[True, True])[['property_source_id', 'property_reis_rc_id', col, 'survey_legacy_data_source', 'mult_link_check', 'spacetype']].drop_duplicates(col).head(2))  

    test = umix_in.copy()
    temp = df.copy()
    temp['in_view'] = 1
    test = test.join(temp.drop_duplicates('property_reis_rc_id').set_index('property_reis_rc_id')[['in_view']], on='propertyid')
    test = test.join(drop_log.drop_duplicates(['property_reis_rc_id']).set_index('property_reis_rc_id')[['reason']], on='propertyid')
    if len(test[(test['in_view'].isnull() == True) & (test['reason'] == '')]) > 0:
        display(test[(test['in_view'].isnull() == True) & (test['reason'] == '')].drop_duplicates('propertyid')[['propertyid', 'reason']])

    if len(temp[(temp['in_umix'].isnull() == True) & (temp['year'] < curryr - 1)]) > 0:
        display(temp[(temp['in_umix'].isnull() == True)].drop_duplicates('property_reis_rc_id')[['property_source_id', 'property_reis_rc_id', 'housing_type', 'type2', 'year']])
    del test
    del temp

    temp = umix_in.copy()
    test = log_in.copy()
    test['propertyid'] = 'A' + test['id'].astype(str)
    temp = temp.join(test.drop_duplicates('propertyid').set_index('propertyid')[['metcode']], on='propertyid')
    temp['count_rows_umix'] = temp.groupby('metcode')['propertyid'].transform('count')
    temp = temp.drop_duplicates('metcode')

    temp1 = df.copy()
    temp1 = temp1[temp1['survey_legacy_data_source'] == 'Foundation']
    temp1['count_rows_df'] = temp1.groupby('metcode')['property_source_id'].transform('count')
    temp1 = temp1.drop_duplicates('metcode')

    temp = temp.join(temp1.set_index('metcode')[['count_rows_df']], on='metcode')
    temp['diff'] = temp['count_rows_df'] - temp['count_rows_umix']
    temp['perc_diff'] = abs((temp['count_rows_df'] - temp['count_rows_umix'])) / temp['count_rows_umix']
    if temp[(temp['count_rows_umix'] > 6000) | (temp['count_rows_df'] == 0)]['perc_diff'].max() > 0.1 or temp['perc_diff'].max() > 0.5:
        print("There is a significant difference in historical rows between the legacy umix and the preprocessed umix")
    temp[['metcode', 'count_rows_umix', 'count_rows_df', 'diff', 'perc_diff']].sort_values(by=['perc_diff'], ascending=[False]).to_csv('/home/central/square/data/zzz-bb-test2/python/catylist_snapshots/OutputFiles/umix/diff_log_report_{}m{}.csv'.format(curryr, currmon), index=False)

    del test
    del temp

    df['survdate'] = pd.to_datetime(df['survdate']).dt.strftime('%m/%d/%Y')
    df['count'] = df[df['survey_legacy_data_source'] != 'ApartmentData.com'].groupby('property_source_id')['property_source_id'].transform('count')
    df['count'] = df.groupby('property_source_id')['count'].bfill()
    df['count'] = df.groupby('property_source_id')['count'].ffill()
    df['survdate'] = np.where((df['year'] >= curryr - 1) & (df['property_reis_rc_id'] == '') & (df['count'] == 1), '{}/15/{}'.format(currmon, curryr), df['survdate'])
    df['surveyquarter'] = np.where((df['year'] >= curryr - 1) & (df['property_reis_rc_id'] == '') & (df['count'] == 1), "{} - Q{}".format(curryr, np.ceil(currmon/3)), df['surveyquarter'])

    df['catylist_id'] = np.where((df['property_reis_rc_id'] == '') & (df['property_source_id'].str.isdigit()), 'a' + df['property_source_id'], df['property_source_id'])
    df['propertyid'] = np.where((df['property_reis_rc_id'] != ''), df['property_reis_rc_id'], df['catylist_id'])
    df = df.rename(columns={'survdate': 'surveydate', 'normalized_rent_by_month_unit_average_amount': 'averagerent'})
    df = df[list(umix_in.columns) + ['property_reis_rc_id', 'property_source_id']]        

    display(pd.DataFrame(drop_log.groupby('reason')['property_source_id'].count()).rename(columns={'property_source_id': 'count'}).sort_values(by=['count'], ascending=[False]))
    test = umix_in.copy()
    test['in_umix'] = 1
    test1 = test.copy()
    test1['count'] = test1.groupby('propertyid')['propertyid'].transform('count')
    test1 = test1[(test1['count'] == 1) & (test1['averagerent'].isnull() == True) & (test1['vacant'].isnull() == True)]
    test1['has_surv'] = 0
    drop_log = drop_log.join(test.drop_duplicates('propertyid').set_index('propertyid')[['in_umix']], on='property_reis_rc_id')
    drop_log = drop_log.join(test1.drop_duplicates('propertyid').set_index('propertyid')[['has_surv']], on='property_reis_rc_id')
    drop_log['has_surv'] = np.where((drop_log['has_surv'].isnull() == True) & (drop_log['in_umix'] == 1), 1, drop_log['has_surv'])
    drop_log['has_surv'] = np.where((drop_log['has_surv'].isnull() == True) & (drop_log['in_umix'].isnull() == True), 0, drop_log['has_surv'])
    drop_log['in_umix'] = drop_log['in_umix'].fillna(0)
    drop_log['in_umix'] = np.where((drop_log['property_reis_rc_id'] == ''), 0, drop_log['in_umix'])
    temp = df.copy()
    temp['in_snap'] = 1
    drop_log = drop_log.join(temp.drop_duplicates('property_reis_rc_id').set_index('property_reis_rc_id')[['in_snap']], on='property_reis_rc_id')
    drop_log['in_snap'] = drop_log['in_snap'].fillna(0)
    drop_log['in_snap'] = np.where((drop_log['property_reis_rc_id'] == ''), 0, drop_log['in_snap'])

    drop_log.to_csv('/home/central/square/data/zzz-bb-test2/python/catylist_snapshots/OutputFiles/umix/drop_log_{}m{}.csv'.format(curryr, currmon), index=False) 

    del temp
    del test

    df.drop_duplicates('property_reis_rc_id')[['property_source_id', 'property_reis_rc_id']].to_csv('/home/central/square/data/zzz-bb-test2/python/catylist_snapshots/OutputFiles/umix/property_ids.csv', index=False)

    leg_sel_codes = umix_in.copy()
    leg_sel_codes = leg_sel_codes.drop_duplicates('propertyid')
    leg_sel_codes = leg_sel_codes[['propertyid', 'selectcode']]

    log_undup = log_in.drop_duplicates('id').copy()
    log_undup = log_undup[['id']]
    log_undup['id_join'] = 'A' + log_undup['id'].astype(str)
    log_undup['in_leg_log'] = 1
    umix_undup = umix_in.drop_duplicates('propertyid').copy()
    umix_undup = umix_undup[['propertyid']]
    umix_undup = umix_undup.rename(columns={'propertyid': 'id_join'})
    umix_undup['in_leg_umix'] = 1

    del df_in
    del log_in
    del umix_in
    del valid_aptdata
    del live_subs

    df = df.drop(['property_reis_rc_id'], axis=1)
    path = '/home/central/vc/mfp/finaltoit/anytime/umix_test_bb/umixfamily.txt'
    df.to_csv(r'{}'.format(path), header=df.columns, index=None, sep=',', mode='w')


    df_survs['id_join'] = np.where((df_survs['id'].str.isdigit()), 'A' + df_survs['id'].astype(str), df_survs['property_source_id'])
    df_survs['in_surv'] = 1
    df['in_umix'] = 1
    df['id_join'] = np.where((df['propertyid'].str[1:] == df['property_source_id']) | (df['propertyid'] == df['property_source_id']), df['property_source_id'], df['propertyid'])
    df = df.join(df_survs.drop_duplicates('id_join').set_index('id_join')[['in_surv']], on='id_join')
    df = df.join(log_undup.drop_duplicates('id_join').set_index('id_join')[['in_leg_log']], on='id_join')
    df_survs = df_survs.join(df.drop_duplicates('id_join').set_index('id_join')[['in_umix']], on='id_join')
    df_survs = df_survs.join(umix_undup.drop_duplicates('id_join').set_index('id_join')[['in_leg_umix']], on='id_join')

    temp = df.copy()
    temp = temp[temp['in_surv'].isnull() == True]
    temp['has_rent'] = temp.groupby('propertyid')['averagerent'].transform('sum', min_count=1)
    temp['has_vac'] = temp.groupby('propertyid')['vacant'].transform('sum', min_count=1)
    temp = temp[(temp['has_rent'].isnull() == False) | (temp['has_vac'].isnull() == False)]
    temp = temp.join(leg_sel_codes.set_index('propertyid').rename(columns={'selectcode': 'l_selectcode'}), on='propertyid')
    temp = temp[(~temp['l_selectcode'].isin(['IAA', 'IAG', 'D', 'Q', 'S', 'EC', 'X', 'IAZ', 'IAU', 'I', 'T', 'IAR', 'E', 'IAT', 'IAK', 'IAI', 'B', 'P', 'TAX', 'NC']) | (temp['l_selectcode'].isnull() == True))]
    print("{:,} ids with live select codes that made it into umix that did not make it into the logs".format(len(temp.drop_duplicates('id_join'))))
    if len(temp) > 0:
        temp = temp.join(drop_log_survs.drop_duplicates('property_source_id').set_index('property_source_id')[['reason']], on='property_source_id')
    temp.drop_duplicates('id_join')[['property_source_id', 'propertyid', 'reason', 'in_leg_log']].to_csv('/home/central/square/data/zzz-bb-test2/python/catylist_snapshots/OutputFiles/umix/in_umix_not_log.csv', index=False)
    del temp

    temp1 = df_survs.copy()
    temp1 = temp1[temp1['in_umix'].isnull() == True]
    print("{:,} ids made it into logs that did not make it into umix".format(len(temp1.drop_duplicates('id_join'))))
    if len(temp1) > 0:
        temp1 = temp1.join(drop_log.drop_duplicates('property_source_id').set_index('property_source_id')[['reason']], on='property_source_id')
    temp1.drop_duplicates('id_join')[['property_source_id', 'id_join', 'reason', 'in_leg_umix']].rename(columns={'id_join': 'propertyid'}).to_csv('/home/central/square/data/zzz-bb-test2/python/catylist_snapshots/OutputFiles/umix/in_log_not_umix.csv', index=False)
    del temp1  