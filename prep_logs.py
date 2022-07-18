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
pd.set_option('display.max_columns', 100)
pd.options.display.float_format = '{:.3f}'.format
from IPython.core.display import display, HTML
import logging
import warnings
warnings.filterwarnings('ignore')
from IPython.display import Audio
from os import listdir
from os.path import isfile, join
import os
import multiprocessing as mp
import decimal

import boto3
import base64
from botocore.exceptions import ClientError
import json

def get_home():
    if os.name == "nt": return "//odin/reisadmin/central/square/data/zzz-bb-test2/python/catylist_snapshots"
    else: return "/home/central/square/data/zzz-bb-test2/python/catylist_snapshots"


class PrepareLogs:
    
    def __init__(self, sector_map, space_map, legacy_only, include_cons, use_rc_id, 
                 use_reis_sub, use_mult, curryr, currmon, live_load):

        self.home = get_home()
        self.legacy_only = legacy_only
        self.include_cons = include_cons
        self.use_rc_id = use_rc_id
        self.use_reis_sub = use_reis_sub
        self.use_mult = use_mult
        self.sector_map = sector_map
        self.space_map = space_map
        self.curryr = curryr
        self.currmon = currmon
        self.live_load = live_load
        
    def get_secret(self):
    
        # If you need more information about configurations or implementing the sample code, visit the AWS docs:   
        # https://aws.amazon.com/developers/getting-started/python/

        secret_name = "arn:aws:secretsmanager:us-east-1:006245041337:secret:development/cre/db/redshift/cre_developer-VtZhap"
        region_name = "us-east-1"
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )
        # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
        # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        # We rethrow the exception by default.
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'DecryptionFailureException':
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InternalServiceErrorException':
                # An error occurred on the server side.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                # You provided a parameter value that is not valid for the current state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
        else:
            # Decrypts secret using the associated KMS key.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
            else:
                decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        return get_secret_value_response

    def load_incrementals(self, sector, type_dict_all, rename_dict_all, consistency_dict, load, test_data_in=pd.DataFrame()):
        if load:
            if self.home[0:2] != 's3':
                print("Ensure d_prop.csv input file has been refreshed!!!")
            if self.home[0:2] == 's3' and self.live_load:
                logging.info('Querying View...')
                logging.info('\n')
                credentials = self.get_secret()
                username = json.loads(credentials['SecretString'])['username']
                password = json.loads(credentials['SecretString'])['password']
                host = json.loads(credentials['SecretString'])['host']
                conn = redshift_connector.connect(
                     host=host,
                     database='dev',
                     user=username,
                     password=password
                  )
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM consumption.v_catylisturt_listing_to_econ")
                test_data_in: pd.DataFrame = cursor.fetch_dataframe()
                test_data_in.replace([None], np.nan, inplace=True)
                conn.close()
                
                conv_decimal = list(test_data_in.loc[:,test_data_in.iloc[0].apply(type)==decimal.Decimal].columns)
                for col in conv_decimal:
                    test_data_in[col] = test_data_in[col].astype(float)

            else:
                if self.home[0:2] == 's3':
                    print('This is not a live load!!!')
                test_data_in = pd.read_csv('{}/InputFiles/listings_{}m{}.csv'.format(self.home, self.curryr, self.currmon), na_values= "", keep_default_na = False, dtype={'property_geo_subid_list': 'object'})
            logging.info("Incrementals data has {:,} unique listings and {:,} unique properties".format(len(test_data_in.drop_duplicates('listed_space_id')), len(test_data_in.drop_duplicates('property_source_id'))))
            logging.info('\n')

            test_data_in['property_source_id'] = np.where((test_data_in['property_source_id'].isnull() == True), '', test_data_in['property_source_id'])
            test_data_in['property_source_id'] = test_data_in['property_source_id'].astype(str)
        
            if len(test_data_in[(test_data_in['foundation_ids_list'].isnull() == False) & (test_data_in['foundation_ids_list'] != '')]) == 0:
                logging.info('All ER Links Missing')
                logging.info('\n')
                self.stop = True
            else:
                self.stop = False
                
            self.first = True
        else:
            self.first = False
            
        self.sector = sector
        try:
            self.type_dict = type_dict_all[sector]
        except:
            self.type_dict = type_dict_all[list(type_dict_all.keys())[0]]
            logging.info("{} has no entry in the types dict, need to add one. Will use {} for now to evaluate what columns we need to add or remove".format(sector.title(), list(type_dict_all.keys())[0].title()))
            logging.info('\n')
        try:
            self.rename_dict = rename_dict_all[sector]
        except:
            self.rename_dict = rename_dict_all[list(rename_dict_all.keys())[0]]
            logging.info("{} has no entry in the rename dict, need to add one. Will use {} for now to evaluate what columns we need to rename".format(sector.title(), list(type_dict_all.keys())[0].title()))
            logging.info('\n')
        self.orig_cols = list(self.type_dict.keys())
        if self.sector == "ret":
            self.consistency_dict = consistency_dict[sector]
        else:
            self.consistency_dict = {}
        dtypes = {}
        type_list = [val['type'] for key, val in self.type_dict.items() if 'type' in val]
        col_list = self.type_dict.keys()
        for k, v in zip(col_list, type_list):
            dtypes[k] = v    
        self.dtypes = dtypes

        if self.home[0:2] == 's3':
            self.prefix = 's3://'
            self.log_root = 's3://ma-cre-development-data-hub/foundation_extracts/{}/'.format(sector)
            self.ph_root = 's3://ma-cre-development-sandbox/data-analytics/bb/log_preprocess/InputFiles/ph_logs/'.format(sector)
        else:
            self.prefix = ''
            self.log_root = '/home/central/square/data/{}/download/'.format(sector)
            self.ph_root = '/home/central/square/data/{}/download/'.format(sector)
            
        self.drop_log = pd.DataFrame()
        self.logic_log = pd.DataFrame()

        return test_data_in, self.stop

    def filter_incrementals(self, test_data_in):

        filt = test_data_in.copy()

        filt['orig_surv_date'] = filt['surv_date']

        filt['surv_yr'] = np.where((filt['surv_yr'] == '') | (filt['surv_yr'].isnull() == True), self.curryr, filt['surv_yr'])
        filt['surv_qtr'] = np.where((filt['surv_qtr'] == '') | (filt['surv_qtr'].isnull() == True), np.ceil(self.currmon / 3), filt['surv_qtr'])
        filt['surv_date'] = pd.to_datetime(filt['surv_date'])
        filt['surv_date'] = filt['surv_date'].fillna(pd.Timestamp("{}/{}/{}".format(self.currmon, '15', self.curryr)))

        # Remove asking rents and transaction rents if no listings at the property were surveyed in this period
        # This will allow sq to move rents for listings that simply remain available for long periods of time
        # But we need to retain the rent even if outside the tight month survey window if other rents for spaces at the property were verified in the tight window, so the proeprty level aggregation is not thrown off
        if self.currmon != 1:
            prior_mon = self.currmon - 1
            prior_yr = self.curryr
        else:
            prior_mon = 12
            prior_yr = self.curryr - 1

        temp = filt.copy()
        temp['survyr'] = temp['surv_date'].dt.year
        temp['survmon'] = temp['surv_date'].dt.month
        temp['survday'] = temp['surv_date'].dt.day
        temp['reis_yr'] = np.where((temp['survmon'] == 12) & (temp['survday'] > 15), temp['survyr'] + 1, temp['survyr'])
        temp['reis_mon'] = np.where((temp['survmon'] == 12) & (temp['survday'] > 15), 1, temp['survmon'])
        temp['reis_mon'] = np.where((temp['survday'] > 15) & (temp['survmon'] < 12), temp['survmon'] + 1, temp['reis_mon'])
        temp['reis_qtr'] = np.ceil(temp['reis_mon'] / 3)
        temp = temp[(temp['reis_yr'] == self.curryr) & (temp['reis_mon'] == self.currmon)]
        temp = temp[(temp['lease_asking_rent_min_amt'].isnull() == False) | (temp['lease_asking_rent_max_amt'].isnull() == False) | (temp['lease_transaction_rent_price_min_amt'].isnull() == False) | (temp['lease_transaction_rent_price_max_amt'].isnull() == False)]
        temp['rent_in_month'] = True
        temp = temp.drop_duplicates('property_source_id')
        filt = filt.join(temp.set_index('property_source_id')[['rent_in_month']], on='property_source_id')
        filt['rent_in_month'] = np.where((filt['rent_in_month'].isnull() == True), False, filt['rent_in_month'])
        for col in ['lease_asking_rent_min_amt', 'lease_asking_rent_max_amt', 'lease_transaction_rent_price_min_amt', 'lease_transaction_rent_price_max_amt']:
            filt[col].mask((filt['surv_date'].lt("{}/{}/{}".format(prior_mon, '16', prior_yr))) & (filt['rent_in_month'] == False), np.nan, inplace=True)
            filt[col].mask((filt['surv_date'].gt("{}/{}/{}".format(self.currmon, '15', self.curryr))) & (filt['rent_in_month'] == False), np.nan, inplace=True)
        for col in ['occupancy_expenses_amount_amount', 'lease_or_property_expenses_amount', 'property_real_estate_tax_amt', 'lease_transaction_freerentmonths', 'commission_description', 'commission_amount_percentage', 'lease_transaction_tenantimprovementallowancepsf_amount', 'occupancy_cam_amount_amount', 'lease_terms', 'lease_transaction_leasetermmonths']:
            filt[col].mask((filt['surv_date'].lt("{}/{}/{}".format(prior_mon, '16', prior_yr))), np.nan, inplace=True)
            filt[col].mask((filt['surv_date'].gt("{}/{}/{}".format(self.currmon, '15', self.curryr))), np.nan, inplace=True)
        
        
        filt['surv_yr'].mask((filt['surv_date'].lt("{}/{}/{}".format(prior_mon, '16', prior_yr))), self.curryr, inplace=True)
        filt['surv_qtr'].mask((filt['surv_date'].lt("{}/{}/{}".format(prior_mon, '16', prior_yr))), np.ceil(self.currmon / 3), inplace=True)
        filt['surv_date'].mask((filt['surv_date'].lt("{}/{}/{}".format(prior_mon, '16', prior_yr))), (pd.Timestamp("{}/{}/{}".format(self.currmon, '15', self.curryr))), inplace=True)

        filt['surv_yr'].mask((filt['surv_date'].gt("{}/{}/{}".format(self.currmon, '15', self.curryr))), self.curryr, inplace=True)
        filt['surv_qtr'].mask((filt['surv_date'].gt("{}/{}/{}".format(self.currmon, '15', self.curryr))), np.ceil(self.currmon / 3), inplace=True)
        filt['surv_date'].mask((filt['surv_date'].gt("{}/{}/{}".format(self.currmon, '15', self.curryr))), (pd.Timestamp("{}/{}/{}".format(self.currmon, '15', self.curryr))), inplace=True)
        
        
        filt['survyr'] = filt['surv_date'].dt.year
        filt['survmon'] = filt['surv_date'].dt.month
        filt['survday'] = filt['surv_date'].dt.day
        filt['reis_yr'] = np.where((filt['survmon'] == 12) & (filt['survday'] > 15), filt['survyr'] + 1, filt['survyr'])
        filt['reis_mon'] = np.where((filt['survmon'] == 12) & (filt['survday'] > 15), 1, filt['survmon'])
        filt['reis_mon'] = np.where((filt['survday'] > 15) & (filt['survmon'] < 12), filt['survmon'] + 1, filt['reis_mon'])
        filt['reis_qtr'] = np.ceil(filt['reis_mon'] / 3)

        filt = filt[((filt['reis_yr'] == self.curryr) & (filt['reis_mon'] == self.currmon))]

        filt['surv_qtr'] = filt['reis_qtr']
        
        filt['rent_basis'] = np.where((filt['rent_basis'].isnull() == True), '', filt['rent_basis'])
        filt['rent_basis'] = np.where((filt['lease_asking_rent_min_amt'].isnull() == True) & (filt['lease_asking_rent_max_amt'].isnull() == True) & (filt['lease_transaction_rent_price_min_amt'].isnull() == True) & (filt['lease_transaction_rent_price_max_amt'].isnull() == True), '', filt['rent_basis'])
        
        return filt

    def read_logs(self): 
        
        logging.info("Loading and appending historical logs for {}...".format(self.sector.title()))
        logging.info('\n')
        
        count = 0
        df = pd.DataFrame()
        if self.home[0:2] == 's3':
            fs = s3fs.S3FileSystem()
            dir_list = fs.ls(self.log_root)
        else:
            dir_list = [f for f in listdir(self.log_root) if isfile(join(self.log_root, f))]
        
        file_list = [x for x in dir_list if len(x.split('/')[-1].split('.log')[0]) == 2]
        
        for path in file_list:
            if count % 10 == 0 and count > 0:
                logging.info("Loading {:,} of {:,} total logs".format(count, len(file_list)))
            count += 1
            
            if self.home[0:2] == 's3':
                file_read = '{}{}'.format(self.prefix, path)
            else:
                file_read = self.log_root + path
            
            temp = pd.read_csv(file_read, sep=',', encoding = 'utf-8',  na_values= "", keep_default_na = False,
                               dtype=self.dtypes)
            if len(temp.columns) == 1:
                temp = pd.read_csv(file_read, sep='\t', encoding = 'utf-8',  na_values= "", keep_default_na = False,
                                  dtype=self.dtypes)

            df = df.append(temp, ignore_index=True)

        df.columns= df.columns.str.strip().str.lower()
        logging.info('\n')
        print("{} Logs Loaded".format(self.sector.title()))

        return df
    
    def check_column_schema(self, log):

        if self.sector == "ret":
            for key, value in self.consistency_dict.items():
                log.rename(columns={key: value}, inplace=True)
        
        test_dict = [x for x in log.columns if x not in self.type_dict.keys()]
        
        if len(test_dict) > 0:
            logging.info("Type dict is missing {}".format(test_dict))
            self.stop = True
        test_dict = [x for x in self.type_dict.keys() if x not in log.columns]
        if len(test_dict) > 0:
            logging.info('\n')
            logging.info("Type dict does not need {}".format(test_dict))
            self.stop=True
            
        if self.sector == "ret":
            log['realid'] = log['realid'].astype(str)
            for col in log.columns:
                if col in ['a_size', 'n_size']:
                    log[col] = np.where((log[col] == -1), 0, log[col])
                else:
                    log[col] = np.where((log[col] == -1), np.nan, log[col])
                    log[col] = np.where((log[col] == -1), np.nan, log[col])
                  
            log['max_size'] = log.groupby('realid')['tot_size'].transform('max')
            temp = log.copy()
            temp = temp.drop_duplicates(['realid', 'n_size'])
            temp['tot_n_size'] = temp.groupby('realid')['n_size'].transform('sum', min_count=1)
            temp = temp.drop_duplicates('realid')
            log = log.join(temp.set_index('realid')[['tot_n_size']], on='realid')
            log['n_size'] = np.where((log['tot_n_size'].isnull() == False), log['tot_n_size'], log['n_size'])
            temp = log.copy()
            temp = temp.drop_duplicates(['realid', 'a_size'])
            temp['tot_a_size'] = temp.groupby('realid')['a_size'].transform('sum', min_count=1)
            temp = temp.drop_duplicates('realid')
            log = log.join(temp.set_index('realid')[['tot_a_size']], on='realid')
            log['a_size'] = np.where((log['tot_a_size'].isnull() == False), log['tot_a_size'], log['a_size'])
            temp = log.copy()
            temp['tot_size'] = np.where((temp['tot_size'] == temp['a_size'] + temp['n_size']), temp['a_size'] + temp['n_size'], np.nan)
            temp = temp[temp['tot_size'].isnull() == False]
            temp = temp.drop_duplicates('realid')
            log = log.join(temp.set_index('realid').rename(columns={'tot_size': 'tot_size_use'})[['tot_size_use']], on='realid')
            log['tot_size'] = np.where((log['tot_size_use'].isnull() == False), log['tot_size_use'], log['tot_size'])
            log['tot_size'] = np.where((log['max_size'].isnull() == False) & (log['tot_size_use'].isnull() == True), log['max_size'], log['tot_size'])
            log = log.drop(['max_size', 'tot_n_size', 'tot_a_size', 'tot_size_use'], axis=1)
                             
        log['state'] = np.where((log['state'] == '--'), '', log['state'])

        return log, self.stop
        
    def read_ph_logs(self): 
        
        logging.info("Loading and appending historical ph logs for {}...".format(self.sector.title()))
        logging.info('\n')
        
        if self.home[0:2] == 's3':
            fs = s3fs.S3FileSystem()
            dir_list = fs.ls(self.ph_root)
        else:
            dir_list = [f for f in listdir(self.ph_root) if isfile(join(self.ph_root, f))]
        file_list = [x for x in dir_list if len(x.split('/')[-1].split('.log')[0]) == 4 and x.split('/')[-1].split('.log')[0][0:2] == 'PH']
        
        
        count = 0
        df = pd.DataFrame()
        for path in file_list:
            if count % 10 == 0 and count > 0:
                logging.info("Loading {:,} of {:,} total logs".format(count, len(file_list)))
            count += 1
            
            if self.home[0:2] == 's3':
                file_read = '{}{}'.format(self.prefix, path)
            else:
                file_read = self.ph_root + path
            
            temp = pd.read_csv(file_read, sep=',', encoding = 'utf-8',  na_values= "", keep_default_na = False,
                               dtype=self.dtypes)
            if len(temp.columns) == 1:
                temp = pd.read_csv(file_read, sep='\t', encoding = 'utf-8',  na_values= "", keep_default_na = False,
                                  dtype=self.dtypes)

            df = df.append(temp, ignore_index=True)

        df.columns= df.columns.str.strip().str.lower()
        logging.info('\n')

        return df
    
    def map_met_to_state(self, log):
        
        met_state_dict = {}
        for met in log['metcode'].unique():
            met_state_dict[met] = list(log[log['metcode'] == met]['state'].unique())
        
        self.met_state_dict = met_state_dict
        
    def handle_case(self, test_data):
    
        test_data['property_reis_rc_id'] = np.where(test_data['property_reis_rc_id'] == 'None', '', test_data['property_reis_rc_id'])
        test_data['property_reis_rc_id'] = test_data['property_reis_rc_id'].str.replace('-', '')

        to_lower = ['buildings_building_status', 'availability_status', 'occupancy_type', 'category', 'subcategory',
                   'space_category', 'lease_asking_rent_price_period', 'lease_asking_rent_price_size', 'occupancy_expenses_period',
                   'occupancy_expenses_size', 'occupancy_cam_period', 'occupancy_cam_size', 'lease_transaction_rent_price_period', 
                   'lease_transaction_rent_price_size', 'street_address', 'lease_terms', 'space_floor', 'space_suite', 
                   'listed_space_title']
        
        for col in to_lower:
            test_data[col] = test_data[col].str.lower()
        
        null_to_string = ['buildings_building_status', 'occupancy_type', 'category', 'subcategory', 'space_category',
                          'retail_property_is_anchor_flag', 'property_source_id', 'property_reis_rc_id',
                          'foundation_ids_list', 'catylist_sector', 'property_geo_msa_list', 
                          'property_geo_subid_list', 'street_address', 'commission_description', 'lease_terms', 
                          'space_floor', 'space_suite', 'listed_space_title', 'state']
        
        for col in null_to_string:
            test_data[col] = np.where((test_data[col].isnull() == True), '', test_data[col])
        
        
        test_data['property_source_id'] = test_data['property_source_id'].astype(str)
        test_data['property_reis_rc_id'] = test_data['property_reis_rc_id'].astype(str)
        test_data['property_reis_rc_id'] = np.where((test_data['property_reis_rc_id'].str[0].isin(['I', 'O'])), test_data['property_reis_rc_id'].str[:-1], test_data['property_reis_rc_id'])
        
        test_data['businesspark'] = np.where((test_data['businesspark'].isnull() == True), '', test_data['businesspark'])
        test_data['businesspark'] = test_data['businesspark'].str.strip()
        test_data['zip'] = np.where((test_data['zip'].str.isdigit() == False), np.nan, test_data['zip'])
        test_data['zip'] = test_data['zip'].astype(float)
        test_data['zip_temp'] = np.where((test_data['zip'].isnull() == True), 999999999, test_data['zip'])
        test_data['zip_temp'] = test_data['zip_temp'].astype(int)
        test_data['park_identity'] = test_data['businesspark'] + '/' + test_data['zip_temp'].astype(str)
        test_data = test_data.drop(['zip_temp'], axis=1)
        
        test_data['count_unique_spaces'] = test_data.groupby('listed_space_id')['listed_space_id'].transform('count')
        if len(test_data[test_data['count_unique_spaces'] > 1]) > 0:
            logging.info("There are cases where there is more than one row per listed space")
            logging.info("\n")
        
        test_data = test_data.drop_duplicates('listed_space_id')
        
        # If a property has retail spaces but the category is not Retail, set the anchor flag to N if it is null
        test_data['retail_property_is_anchor_flag'] = np.where((test_data['category'] != 'retail') & (test_data['retail_property_is_anchor_flag'] == '') & (test_data['space_category'] == 'retail'), 'N', test_data['retail_property_is_anchor_flag'])
        
        test_data['mult_rc_tag'] = False
        test_data['count'] = test_data.groupby('property_reis_rc_id')['property_source_id'].transform('nunique')
        test_data['businesspark_temp'] = np.where((test_data['businesspark'] == ''), np.nan, test_data['businesspark'])
        test_data['businesspark_temp'] = test_data.groupby('property_reis_rc_id')['businesspark_temp'].bfill()
        test_data['businesspark_temp'] = test_data.groupby('property_reis_rc_id')['businesspark_temp'].ffill()
        test_data['count_bp'] = test_data.groupby('property_reis_rc_id')['businesspark_temp'].transform('nunique')
        temp = test_data.copy()
        temp = temp[(temp['count'] > 1) & ((temp['count_bp'] > 1) | (temp['businesspark'] == '')) & (temp['property_reis_rc_id'] != '') & (temp['property_reis_rc_id'].str[0] == self.sector_map[self.sector]['prefix'])]
        if len(temp) > 0 and self.use_rc_id:
            if self.first:
                logging.info("There are rc_ids that are linked to more than one unique Catylist property")
                logging.info("\n")
            temp['flag'] = 'Multiple Catylist properties linked to same RC ID'
            temp['c_value'] = temp['property_reis_rc_id']
            temp['status'] = 'flagged'
            self.logic_log = self.logic_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'flag', 'c_value', 'status']])
            if self.legacy_only:
                test_data['mult_rc_tag'] = np.where((test_data['count'] > 1) & ((test_data['count_bp'] > 1) | (test_data['businesspark'] == '')) & (temp['property_reis_rc_id'] != '') & (temp['property_reis_rc_id'].str[0] == self.sector_map[self.sector]['prefix']), True, test_data['mult_rc_tag'])
        if self.use_rc_id:
            test_data['property_reis_rc_id'] = np.where((test_data['count'] > 1) & ((test_data['count_bp'] > 1) | (test_data['businesspark'] == '')) & (test_data['property_reis_rc_id'] != ''), '', test_data['property_reis_rc_id'])
        
        test_data['avail_date_dt'] = pd.to_datetime(test_data['availability_availabledate'], errors='coerce')
        test_data['vac_date_dt'] = pd.to_datetime(test_data['availability_vacantdate'], errors='coerce')
        test_data['avail_vacdate_close'] = 0
        for col in ['avail_date_dt', 'vac_date_dt']:
            test_data['avail_vacdate_close'] = np.where((test_data['availability_vacant'] == 0) & (test_data[col].dt.year < self.curryr) & (test_data['availability_status'].isin(['available', 'pending'])), 1, test_data['avail_vacdate_close'])
            test_data['avail_vacdate_close'] = np.where((test_data['availability_vacant'] == 0) & (test_data[col].dt.month <= self.currmon + 1) & (test_data[col].dt.year == self.curryr) & (test_data['availability_status'].isin(['available', 'pending'])), 1, test_data['avail_vacdate_close'])
            test_data['avail_vacdate_close'] = np.where((test_data['availability_vacant'] == 0) & (test_data[col].dt.month == 1) & (test_data[col].dt.year == self.curryr + 1) & (self.currmon == 12) & (test_data['availability_status'].isin(['available', 'pending'])), 1, test_data['avail_vacdate_close'])
        test_data['avail_vacdate_close'] = np.where((test_data['avail_vacdate_close'] == 1) & (test_data['vac_date_dt'].dt.month > self.currmon + 1) & (test_data['vac_date_dt'].dt.year == self.curryr) & (test_data['availability_vacantdate'] != '') & (self.currmon != 12), 0, test_data['avail_vacdate_close'])
        test_data['avail_vacdate_close'] = np.where((test_data['avail_vacdate_close'] == 1) & (test_data['vac_date_dt'].dt.year > self.curryr) & (test_data['availability_vacantdate'] != '') & (self.currmon != 12), 0, test_data['avail_vacdate_close'])
        test_data['avail_vacdate_close'] = np.where((test_data['avail_vacdate_close'] == 1) & (test_data['vac_date_dt'].dt.year > self.curryr) & (test_data['availability_vacantdate'] != '') & (self.currmon == 12) & ((test_data['vac_date_dt'].dt.month > 1) | (test_data['vac_date_dt'].dt.year > self.curryr + 1)), 0, test_data['avail_vacdate_close'])
        
        test_data['space_size_available_leased'] = np.where((test_data['availability_status'].isin(['leased', 'withdrawn'])) | ((test_data['availability_vacant'] == 0) & (test_data['lease_sublease'] != 1) & (test_data['avail_vacdate_close'] == 0)), test_data['space_size_available'], np.nan)
        test_data['space_size_available'] = np.where((test_data['availability_status'].isin(['leased', 'withdrawn'])) | ((test_data['availability_vacant'] == 0) & (test_data['lease_sublease'] != 1) & (test_data['avail_vacdate_close'] == 0)), 0, test_data['space_size_available'])

        for col in ['lease_asking_rent_max_amt', 'lease_asking_rent_min_amt', 'lease_transaction_rent_price_max_amt', 'lease_transaction_rent_price_min_amt']:
            try:
                test_data['orig'] = test_data[col]
                test_data['was_blank'] = np.where((test_data[col].isnull() == True) | (test_data[col] == ''), True, False)
                test_data[col] = np.where((test_data[col].str.replace('.', '').str.isdigit() == False), np.nan, test_data[col])
                test_data[col] = np.where((test_data[col] == ''), np.nan, test_data[col])
                test_data[col] = test_data[col].astype(float)
                temp = test_data.copy()
                temp = temp[(temp['was_blank'] == False) & (temp[col].isnull() == True)]
                if len(temp) > 0:
                    logging.info("There are {:,} instances where rent values are not floats".format(len(temp)))
                    logging.info("\n")
                    temp['flag'] = 'Rent value is not float'
                    temp['c_value'] = temp['orig']
                    temp['status'] = 'dropped'
                    self.logic_log = self.logic_log.append(temp[['flag', 'property_source_id', 'listed_space_id', 'c_value', 'status']], ignore_index=True)
                test_data = test_data.drop(['was_blank', 'orig'],axis=1)
            except:
                False
                
        test_data['tot_size'] = np.where(test_data['tot_size'] == 0, np.nan, test_data['tot_size'])
        temp = test_data.copy()
        temp = temp[((temp['tot_size'].isnull() == True) | (temp['tot_size'] == '')) & (temp['category'] != 'land')]
        if len(temp) > 0 and self.first:
            logging.info("There are {:,} properties that do not have a total size".format(len(temp.drop_duplicates('property_source_id'))))
            logging.info("\n")
            
        return test_data
    
    def clean_comm(self, test_data):
        
        test_data['commission_description'] = test_data['commission_description'].str.strip()
        test_data['commission_description'] = test_data['commission_description'].str.lower()
        test_data['commission_description'] = test_data['commission_description'].str.replace('\*', '')
        test_data['comm_cleaned'] = ''

        test_data['temp'] = test_data['commission_description'].str.split(' ').str[0].str.strip()
        test_data['comm_cleaned'] = np.where((test_data['temp'].str.isdigit()), test_data['temp'], test_data['comm_cleaned'])
        test_data['comm_cleaned'] = np.where((test_data['temp'].str.replace('.', '').str.isdigit()), test_data['temp'], test_data['comm_cleaned'])
        test_data['comm_cleaned'] = np.where((test_data['temp'].str.replace('0.', '').str.isdigit()), test_data['temp'], test_data['comm_cleaned'])

        test_data['temp'] = test_data['commission_description'].str.split('/').str[0].str.strip()
        test_data['comm_cleaned'] = np.where((test_data['temp'].str.isdigit()), test_data['temp'], test_data['comm_cleaned'])
        test_data['comm_cleaned'] = np.where((test_data['temp'].str.replace('.', '').str.isdigit()), test_data['temp'], test_data['comm_cleaned'])
        test_data['comm_cleaned'] = np.where((test_data['temp'].str.replace('0.', '').str.isdigit()), test_data['temp'], test_data['comm_cleaned'])

        test_data['temp'] = test_data['commission_description'].str.split('%').str[0].str.strip()
        test_data['comm_cleaned'] = np.where((test_data['temp'].str.isdigit()), test_data['temp'], test_data['comm_cleaned'])
        test_data['comm_cleaned'] = np.where((test_data['temp'].str.replace('.', '').str.isdigit()), test_data['temp'], test_data['comm_cleaned'])
        test_data['comm_cleaned'] = np.where((test_data['temp'].str.replace('0.', '').str.isdigit()), test_data['temp'], test_data['comm_cleaned'])
        test_data['temp1'] = test_data['temp'].str.split('/').str[0].str.strip()
        test_data['comm_cleaned'] = np.where((test_data['temp1'].str.isdigit()), test_data['temp1'], test_data['comm_cleaned'])
        test_data['comm_cleaned'] = np.where((test_data['temp1'].str.replace('.', '').str.isdigit()), test_data['temp1'], test_data['comm_cleaned'])
        test_data['comm_cleaned'] = np.where((test_data['temp1'].str.replace('0.', '').str.isdigit()), test_data['temp1'], test_data['comm_cleaned'])

        test_data['comm_cleaned'] = np.where((test_data['comm_cleaned'] == ''), np.nan, test_data['comm_cleaned'])
        test_data['comm_cleaned'] = test_data['comm_cleaned'].astype(float)
        test_data['comm_cleaned'] = np.where((test_data['comm_cleaned'] > 10), np.nan, test_data['comm_cleaned'])

        test_data['comm_cleaned'] = np.where(((test_data['commission_description'].str.contains('sf') == True) | (test_data['commission_description'].str.contains('ft') == True)) & (test_data['comm_cleaned'] <= 1.5), np.nan, test_data['comm_cleaned'])
        test_data['comm_cleaned'] = np.where((test_data['commission_description'].str.split(' ').str[0] == '1/2') & (test_data['comm_cleaned'] <= 1.5), np.nan, test_data['comm_cleaned'])
        test_data['comm_cleaned'] = np.where((test_data['commission_description'].str.contains('month') == True) & (test_data['comm_cleaned'] <= 1.5), np.nan, test_data['comm_cleaned'])
        test_data['comm_cleaned'] = np.where((test_data['comm_cleaned'] < 0.06), test_data['comm_cleaned'] * 100, test_data['comm_cleaned'])
        
        test_data['commission_amount_percentage'] = np.where((test_data['commission_amount_percentage'].isnull() == False), test_data['commission_amount_percentage'], test_data['comm_cleaned'])

        return test_data
    
    def clean_lease_terms(self, test_data):
        
        test_data['lease_terms'] = np.where((test_data['lease_terms'].isnull() == True), '', test_data['lease_terms'])
        test_data['lease_terms'] = test_data['lease_terms'].str.lower().str.strip()
        test_data['lease_terms'] = test_data['lease_terms'].str.replace('to', '-')
        test_data['lease_terms'] = test_data['lease_terms'].str.replace(' -', '-').str.replace('- ', '-').str.replace('+', '').str.replace('minimum', '').str.replace('up to', '')
        test_data['lease_terms'] = test_data['lease_terms'].str.strip()
        test_data['part1'] = np.where((test_data['lease_terms'].str.split('-').str[0].str.strip().str.isdigit()), test_data['lease_terms'].str.split('-').str[0].str.strip(), np.nan)
        test_data['part2'] = np.where((test_data['lease_terms'].str.split('-').str[-1].str.split(' ').str[0].str.strip().str.isdigit()), test_data['lease_terms'].str.split('-').str[-1].str.split(' ').str[0].str.strip(), np.nan)
        test_data['part2'] = np.where((test_data['lease_terms'].str.split('-').str[-1].str.split(' ').str[0].str.strip().str[-3:] == 'yrs') & (test_data['lease_terms'].str.split('-').str[-1].str.split(' ').str[0].str.strip().str[:-3].str.isdigit()), test_data['lease_terms'].str.split('-').str[-1].str.split(' ').str[0].str.strip().str[:-3], test_data['part2'])
        test_data['lease_terms_cleaned'] = np.nan
        test_data['lease_terms_cleaned'] = np.where((test_data['part1'].isnull() == False) & (test_data['part2'].isnull() == False) & (test_data['lease_terms'].str.contains('month') == False), (test_data['part1'].astype(float) + test_data['part2'].astype(float)) / 2, test_data['lease_terms_cleaned'])
        test_data['lease_terms_cleaned'] = np.where((test_data['part1'].isnull() == False) & (test_data['part2'].isnull() == False) & (test_data['lease_terms'].str.contains('month') == True), ((test_data['part1'].astype(float)/ 12) + (test_data['part2'].astype(float) / 12)) / 2, test_data['lease_terms_cleaned'])
        test_data['lease_terms_cleaned'] = np.where((test_data['part1'].isnull() == False) & (test_data['part2'].isnull() == False) & (test_data['lease_terms_cleaned'] > 25) & (test_data['lease_terms'].str.contains('year') == False) & (test_data['lease_terms'].str.contains('yrs') == False), ((test_data['part1'].astype(float)/ 12) + (test_data['part2'].astype(float) / 12)) / 2, test_data['lease_terms_cleaned'])

        test_data['part1'] = np.where((test_data['lease_terms'].str.split(' ').str[0].str.isdigit()), test_data['lease_terms'].str.split(' ').str[0], np.nan)
        test_data['part1'] = np.where((test_data['lease_terms'].str.split(' ').str[0].str[:-3].str.isdigit()) & (test_data['lease_terms'].str[-3:] == 'yrs'), test_data['lease_terms'].str.split(' ').str[0].str[:-3], test_data['part1'])
        test_data['part2'] = True
        test_data['lease_terms_cleaned'] = np.where((test_data['lease_terms_cleaned'].isnull() == True) & (test_data['part1'].isnull() == False) & (test_data['part2']) & (test_data['lease_terms'].str.contains('month') == False), test_data['part1'].astype(float), test_data['lease_terms_cleaned'])
        test_data['lease_terms_cleaned'] = np.where((test_data['lease_terms_cleaned'].isnull() == True) & (test_data['part1'].isnull() == False) & (test_data['part2']) & (test_data['lease_terms'].str.contains('month') == True), test_data['part1'].astype(float) / 12, test_data['lease_terms_cleaned'])
        test_data['lease_terms_cleaned'] = np.where((test_data['part1'].isnull() == False) & (test_data['part2']) & (test_data['lease_terms_cleaned'] > 25) & (test_data['lease_terms'].str.contains('year') == False) & (test_data['lease_terms'].str.contains('yrs') == False), (test_data['part1'].astype(float)/ 12), test_data['lease_terms_cleaned'])
        test_data['lease_terms_cleaned'] = np.where((test_data['part1'].isnull() == False) & (test_data['part2']) & (test_data['lease_terms'].str.contains('month') == True) & ((test_data['lease_terms'].str.contains('year') == True) | (test_data['lease_terms'].str.contains('yrs') == True)), np.nan, test_data['lease_terms_cleaned'])

        test_data['lease_terms_cleaned'] = np.where((test_data['lease_sublease'] == 1) | (test_data['lease_terms'].str.contains('sub') == True) | (test_data['lease_terms_cleaned'] < 0.25) | (test_data['lease_terms'].str.contains('ground lease') == True), np.nan, test_data['lease_terms_cleaned'])
    
        test_data['lease_transaction_leasetermmonths'] = np.where((test_data['lease_transaction_leasetermmonths'].isnull() == False), test_data['lease_transaction_leasetermmonths'], test_data['lease_terms_cleaned'])
    
        return test_data
    
    def check_duplicate_catylist(self, test_data):
        
        temp = test_data.copy()
        temp = temp.drop_duplicates('property_source_id')
        temp = temp[(temp['y_lat'].isnull() == False) & (temp['x_long'].isnull() == False)]
        temp['y_lat_round'] = round(temp['y_lat'], 5)
        temp['x_long_round'] = round(temp['x_long'], 5)
        temp['count_distinct_geo'] = temp.groupby(['x_long_round', 'y_lat_round'])['property_source_id'].transform('count')
        temp = temp[temp['count_distinct_geo'] > 1]
        if len(temp) > 0:
            if self.first:
                logging.info("There are {:,} instances where unique Catylist IDs share the same geo coordinates".format(len(temp)))
                logging.info("\n")
            temp['flag'] = 'Multiple Unique Catylist IDs that share the same geo coordinates'
            temp['c_value'] = temp['y_lat'].astype(str) + ',' + temp['x_long'].astype(str)
            temp['status'] = 'flagged'
            self.logic_log = self.logic_log.append(temp[['flag', 'property_source_id', 'c_value', 'status']], ignore_index=True)
        
    def check_anchor_status(self, test_data):
        
        orig_len = len(self.logic_log)
        
        temp = test_data.copy()
        temp = temp[temp['category'] == 'retail']
        temp = temp[(temp['tot_size'].isnull() == False) & (temp['tot_size'] > 0) & (temp['retail_property_is_anchor_flag'] == '')]
        if len(temp) > 0:
            temp['flag'] = 'Property has no anchor status'
            temp['status'] = 'flagged'
            self.logic_log = self.logic_log.append(temp.drop_duplicates('property_source_id')[['flag', 'property_source_id', 'status']])
        
        
        temp = test_data.copy()
        temp = temp[temp['category'] == 'retail']
        temp['test1'] = temp['tot_size'] < 9300
        temp['test2'] = ((temp['occupancy_type'] == 'multi_tenant') | (temp['occupancy_type'] == '')) & (temp['tot_size'] < 18600) & (temp['rownum_buspark_retail_size_desc'] != 1)
        temp = temp[((temp['test1']) | (temp['test2'])) & (temp['retail_property_is_anchor_flag'] == "Y")]
        if len(temp) > 0:
            temp['flag'] = 'Anchor status incorrectly set as Y'
            temp['c_value'] = temp['retail_property_is_anchor_flag']
            temp['status'] = 'flagged'
            self.logic_log = self.logic_log.append(temp.drop_duplicates('property_source_id')[['flag', 'property_source_id', 'c_value', 'status']])
        
        temp = test_data.copy()
        temp = temp[temp['category'] == 'retail']
        temp['test1'] = (temp['tot_size'] >= 9300) & (temp['occupancy_type'] == 'single_tenant') 
        temp['test2'] = (temp['tot_size'] >= 18600)
        temp['test3'] = (temp['rownum_buspark_retail_size_desc'] == 1) & (temp['tot_size'] >= 9300)
        temp = temp[((temp['test1']) | (temp['test2']) | (temp['test3'])) & (temp['retail_property_is_anchor_flag'] == "N")]
        if len(temp) > 0:
            temp['flag'] = 'Anchor status incorrectly set as N'
            temp['c_value'] = temp['retail_property_is_anchor_flag']
            temp['status'] = 'flagged'
            self.logic_log = self.logic_log.append(temp.drop_duplicates('property_source_id')[['flag', 'property_source_id', 'c_value', 'status']])
        
        temp = test_data.copy()
        temp = temp[temp['category'] == 'retail']
        temp = temp[(temp['retail_property_is_anchor_flag'] == 'Y') & ((temp['anchor_within_business_park_size_sf'].isnull() == True) | (temp['anchor_within_business_park_size_sf'] == 0))]
        if len(temp) > 0:
            temp['flag'] = 'Property is anchor and has no anchor size'
            temp['status'] = 'flagged'
            self.logic_log = self.logic_log.append(temp.drop_duplicates('property_source_id')[['flag', 'property_source_id', 'status']])
            
        temp = test_data.copy()
        temp = temp[temp['category'] == 'retail']
        temp = temp[(temp['retail_property_is_anchor_flag'] == 'N') & ((temp['nonanchor_within_business_park_size_sf'].isnull() == True) | (temp['nonanchor_within_business_park_size_sf'] == 0))]
        if len(temp) > 0:
            temp['flag'] = 'Property is non anchor and has no non anchor size'
            temp['status'] = 'flagged'
            self.logic_log = self.logic_log.append(temp.drop_duplicates('property_source_id')[['flag', 'property_source_id', 'status']])
        
        temp = test_data.copy()
        temp = temp[temp['space_category'] == 'retail']
        temp = temp.drop_duplicates('property_source_id')
        temp = temp[(temp['nonanchor_within_business_park_size_sf']  == temp['anchor_within_business_park_size_sf']) & (temp['nonanchor_within_business_park_size_sf'].isnull() == False)]
        if len(temp) > 15:
            temp['flag'] = 'Anchor size equals non anchor size'
            temp['status'] = 'flagged'
            self.logic_log = self.logic_log.append(temp.drop_duplicates('property_source_id')[['flag', 'property_source_id', 'status']])
        
        temp = test_data.copy()
        temp = temp[temp['category'] == 'retail']
        temp = temp.drop_duplicates(['property_source_id', 'rownum_buspark_retail_size_desc'])
        temp['count'] = temp.groupby('property_source_id')['property_source_id'].transform('count')
        temp = temp[temp['count'] > 1]
        if len(temp) > 0:
            temp['flag'] = 'Property has inconsistent size ranking within business park across listings'
            temp['status'] = 'flagged'
            self.logic_log = self.logic_log.append(temp.drop_duplicates('property_source_id')[['flag', 'property_source_id', 'status']])
            
        temp = test_data.copy()
        temp = temp[temp['category'] == 'retail']
        temp['test1'] = ((temp['retail_property_is_anchor_flag'] == 'Y') & ((temp['nonanchor_within_business_park_size_sf'].isnull() == True) | (temp['nonanchor_within_business_park_size_sf'] == 0)) & (temp['anchor_within_business_park_size_sf'] != temp['business_park_retail_size_sf']) & (temp['anchor_within_business_park_size_sf'].isnull() == False))
        temp['test2'] = ((temp['retail_property_is_anchor_flag'] == 'N') & ((temp['anchor_within_business_park_size_sf'].isnull() == True) | (temp['anchor_within_business_park_size_sf'] == 0)) & (temp['nonanchor_within_business_park_size_sf'] != temp['business_park_retail_size_sf']) & (temp['nonanchor_within_business_park_size_sf'].isnull() == False))
        temp['test3'] = (temp['business_park_retail_size_sf'].isnull() == False)
        temp = temp[((temp['test1']) | (temp['test2'])) & (temp['test3'])]
        if len(temp) > 0:
            temp['flag'] = 'Property has only one type of space broken out, and it does not match total business park size'
            temp['status'] = 'flagged'
            self.logic_log = self.logic_log.append(temp.drop_duplicates('property_source_id')[['flag', 'property_source_id', 'status']])
        
        temp = test_data.copy()
        temp = temp[temp['category'] == 'retail']
        temp = temp[(temp['nonanchor_within_business_park_size_sf'] + temp['anchor_within_business_park_size_sf'] != temp['business_park_retail_size_sf']) & (temp['anchor_within_business_park_size_sf'].isnull() == False) & (temp['nonanchor_within_business_park_size_sf'].isnull() == False)]
        if len(temp) > 0:
            temp['flag'] = 'Anchor space and non anchor space do not add up to total business park size'
            temp['status'] = 'dropped'
            self.logic_log = self.logic_log.append(temp.drop_duplicates('property_source_id')[['flag', 'property_source_id', 'status']])
        
        temp = test_data.copy()
        temp['count_sizes'] = temp.groupby('park_identity')['business_park_retail_size_sf'].transform('nunique')
        temp = temp[temp['count_sizes'] > 1]
        temp['concat_props'] = temp.groupby('park_identity')['property_source_id'].transform(lambda x: ','.join(x))
        temp['concat_props'] = temp['concat_props'] + '/' + temp['businesspark']
        if len(temp) > 0:
            temp['flag'] = 'Inconsistent size of business park across properties'
            temp['c_value'] = temp['concat_props']
            temp['status'] = 'flagged'
            self.logic_log = self.logic_log.append(temp.drop_duplicates('park_identity')[['flag', 'property_source_id', 'c_value', 'status']])
        
        temp = test_data.copy()
        temp = temp[temp['category'] == 'retail']
        temp = temp[(temp['businesspark'] != '') & (temp['rownum_buspark_retail_size_desc'] > 0) & ((temp['business_park_retail_size_sf'] == 0) | (temp['business_park_retail_size_sf'].isnull() == True))]
        
        if len(self.logic_log) > orig_len:
            logging.info("\n")
            logging.info("There is an issue with anchor designation")
            logging.info("\n")
    
    def select_reis_id(self, undup, log, log_ids, batch):
    
        if batch == 1:
            logging.info("Choosing REIS ID Link...")
            logging.info("\n")
        
        undup = undup.drop_duplicates('property_source_id')
        undup = undup.reset_index(drop=True)
        decision = pd.DataFrame()
        
        used_ids = []
        
        for index, row in undup.iterrows():
            
            count = 0
            found_ids = []
            temp = {}
            prop_choice = ''
            msa_choice = ''
            sub_choice = np.nan
            er_check = True
            logic_flag = ''
            
            if row['foundation_ids_list'] == '' and row['property_reis_rc_id'] == '':
                er_check = False
            
            if er_check:
                
                prop_addr_num = row['street_address'].lower().strip().split(' ')[0]
    
                rc_id = row['property_reis_rc_id']
                
                er_ids = row['foundation_ids_list'].split(',')
                
                for prop in er_ids:
                    if prop[1:] in log_ids and prop not in found_ids and prop[0] == self.sector_map[self.sector]['prefix']:
                        found_ids.append(prop)
                        prop_choice = prop
                        count += 1
                if count == 1:
                    used_ids.append(prop_choice)
                elif count > 1:
                    if self.use_mult:
                        match = False
                        for x in found_ids:
                            log_addr = log[log['realid'] == x[1:]].drop_duplicates('realid').reset_index().loc[0]['address']
                            log_addr_num = log_addr.strip().split(' ')[0].lower()
                            if log_addr_num == prop_addr_num and x not in used_ids:
                                prop_choice = x
                                used_ids.append(prop_choice)
                                match = True
                                break
                        if not match:
                            for x in found_ids:
                                if x == rc_id and x not in used_ids:
                                    prop_choice = x
                                    used_ids.append(prop_choice)
                                    match = True
                                    break
                        if not match:
                            for x in found_ids:
                                if x not in used_ids:
                                    prop_choice = x
                                    used_ids.append(prop_choice)
                                    match = True
                                    break
                            if not match:
                                prop_choice = found_ids[0]
                    else:
                        prop_choice = ''
                    logic_flag = 'Multiple Same Sector REIS IDs mapped to Catylist ID'
                    found_ids = ','.join(found_ids)
                if count == 0 and row['property_reis_rc_id'] != '' and self.use_rc_id:
                    if rc_id[1:] in log_ids and rc_id[0] == self.sector_map[self.sector]['prefix']:
                        prop_choice = rc_id
                        used_ids.append(prop_choice)
                        count += 1
                
            if ',' in row['property_geo_msa_list']:
                options_msa = undup.loc[index]['property_geo_msa_list'].split(',')
                if row['property_geo_subid_list'] != '':
                    options_sub = undup.loc[index]['property_geo_subid_list'].split(',')
                    options_sub = [float(x) for x in options_sub]
                else:
                    options_sub = []
                
                if prop_choice != '':
                    log_msa = log.drop_duplicates('realid').set_index('realid').loc[prop_choice[1:]]['metcode']
                    log_sub = log.drop_duplicates('realid').set_index('realid').loc[prop_choice[1:]]['subid']
                    if log_msa in options_msa:
                        msa_choice = log_msa   
                    else:
                        msa_choice = options_msa[0]
                    if log_sub in options_sub or len(options_sub) == 0:
                        sub_choice = log_sub
                    else:
                        sub_choice = options_sub[0]
                    
                elif prop_choice == '':
                    msa_choice = options_msa[0]
                    if len(options_sub) > 0:
                        sub_choice = options_sub[0]     
            else:
                msa_choice = undup.loc[index]['property_geo_msa_list']
                
            
            if ',' in str(row['property_geo_subid_list']):
                options_sub = undup.loc[index]['property_geo_subid_list'].split(',')
                options_sub = [float(x) for x in options_sub]

                if prop_choice != '':
                    log_sub = log.drop_duplicates('realid').set_index('realid').loc[prop_choice[1:]]['subid']
                    if log_sub in options_sub:
                        sub_choice = log_sub
                    elif len(options_sub) > 0:
                        sub_choice = options_sub[0]

                elif prop_choice == '' and len(options_sub) > 0:
                    sub_choice = options_sub[0]
            elif row['property_geo_subid_list'] != '':
                sub_choice = float(undup.loc[index]['property_geo_subid_list'])
            elif prop_choice != '':
                sub_choice = log.drop_duplicates('realid').set_index('realid').loc[prop_choice[1:]]['subid']
                    
                
            if row['property_geo_msa_list'] == '' and prop_choice != '':
                msa_choice = log.drop_duplicates('realid').set_index('realid').loc[prop_choice[1:]]['metcode']
                sub_choice = log.drop_duplicates('realid').set_index('realid').loc[prop_choice[1:]]['subid']
            
            if len(found_ids) == 0:
                found_ids = ''
            decision = decision.append({'property_source_id': row['property_source_id'], 'foundation_property_id': prop_choice, 'metcode': msa_choice, 'gsub': sub_choice, 'logic_flag': logic_flag, 'mult_ids': found_ids}, ignore_index=True)

        return decision
    
    def choose_catylist_size(self, test_data, log):
        
        test_data['n_size'] = np.nan
        test_data['a_size'] = np.nan
        
        if self.sector == "off":
            bp_field = 'business_park_office_size_sf'
            row_num = 'rownum_buspark_office_size_desc'
        elif self.sector == "ind":
            bp_field = 'business_park_industrial_size_sf'
            row_num = 'rownum_buspark_industrial_size_desc'
        elif self.sector == "ret":
            bp_field = 'business_park_retail_size_sf'
            row_num = 'rownum_buspark_retail_size_desc'
            
        log_temp = log.copy()
        log_temp = log.rename(columns={'size': 'tot_size', 'ind_size': 'tot_size'})
        test_data['id_noalpha'] = test_data['foundation_property_id'].str[1:]
        if self.sector == "off" or self.sector == "ind":
            join_cols = ['log_tot_size', 'log_bldgs', 'log_type2']
        elif self.sector == "ret":
            join_cols = ['log_tot_size', 'log_n_size', 'log_a_size', 'log_type2']
        test_data = test_data.join(log_temp[log_temp['tot_size'] != -1].drop_duplicates('realid').rename(columns={'realid': 'id_noalpha', 'tot_size': 'log_tot_size', 'n_size': 'log_n_size', 'a_size': 'log_a_size', 'bldgs': 'log_bldgs', 'type2': 'log_type2'}).set_index('id_noalpha')[join_cols], on='id_noalpha')
        
        test_data['size_diff_tot'] = abs((test_data['tot_size'] - test_data['log_tot_size'])) / test_data['log_tot_size']
        test_data['size_diff_tot'] = np.where((test_data['tot_size'].isnull() == True), np.inf, test_data['size_diff_tot'])
        
        test_data['orig_tot_size'] = test_data['tot_size']
        test_data['size_method'] = ''
        
        # Replace total building size with the sector specific size, if it is closer to the log total size than Catylist total building size is
        # Ideally, would use this field regardless of difference to log total size, but there is some bad data in the sector specific Catylist sizes so need this check
        if self.sector == "off":
            type_size = 'building_office_size_sf'
        elif self.sector == "ind":
            type_size = 'building_industrial_size_sf'
        elif self.sector == "ret":
            type_size = 'building_retail_size_sf'
        test_data['size_diff_sector'] = abs((test_data[type_size] - test_data['log_tot_size'])) / test_data['log_tot_size']
        if self.sector == "ind":
            test_data['size_diff_sector'] = np.where((test_data['log_type2'] == 'F') & (test_data['building_office_size_sf'].isnull() == False), np.nan, test_data['size_diff_sector'])
        test_data['tot_size'] = np.where((test_data['size_diff_sector'] < test_data['size_diff_tot']) & (test_data[type_size] > 0) & (test_data[type_size].isnull() == False), test_data[type_size], test_data['tot_size'])
        test_data['size_method'] = np.where((test_data['size_diff_sector'] < test_data['size_diff_tot']) & (test_data[type_size] > 0) & (test_data[type_size].isnull() == False), 'Sector Size SQFT', test_data['size_method'])
        test_data['size_diff_tot'] = abs((test_data['tot_size'] - test_data['log_tot_size'])) / test_data['log_tot_size']
        test_data['size_diff_tot'] = np.where((test_data['tot_size'].isnull() == True), np.inf, test_data['size_diff_tot'])
       
        # Replace total building size with the rentable sqft, if sector specific size was not used
        test_data['size_diff_rentable'] = abs((test_data['buildings_size_rentablesf'] - test_data['log_tot_size'])) / test_data['log_tot_size']
        test_data['tot_size'] = np.where((test_data['size_method'] != 'Sector Size SQFT') & ((test_data['buildings_size_rentablesf'] / test_data['tot_size'] > 0.25) | (test_data['tot_size'].isnull() == True)) & (test_data['size_diff_rentable'] < test_data['size_diff_tot']) & (test_data['buildings_size_rentablesf'] > 0) & (test_data['buildings_size_rentablesf'].isnull() == False), test_data['buildings_size_rentablesf'], test_data['tot_size'])
        test_data['size_method'] = np.where((test_data['size_method'] != 'Sector Size SQFT') & ((test_data['buildings_size_rentablesf'] / test_data['tot_size'] > 0.25) | (test_data['tot_size'].isnull() == True)) & (test_data['size_diff_rentable'] < test_data['size_diff_tot']) & (test_data['buildings_size_rentablesf'] > 0) & (test_data['buildings_size_rentablesf'].isnull() == False), 'Rentable SQFT', test_data['size_method'])
        test_data['size_diff_tot'] = abs((test_data['tot_size'] - test_data['log_tot_size'])) / test_data['log_tot_size']
        test_data['size_diff_tot'] = np.where((test_data['tot_size'].isnull() == True), np.inf, test_data['size_diff_tot'])
       
        # If a property did not have a gross building size but tot_size was able to be filled in by rentable sqft or sector sqft, determine the anchor status and size now, because view wont do it if no gross size
        if self.sector == "ret":
            test_data['test1'] = (test_data['tot_size'] >= 9300) & (test_data['occupancy_type'] == 'single_tenant') 
            test_data['test2'] = (test_data['tot_size'] >= 18600)
            test_data['test3'] = (test_data['rownum_buspark_retail_size_desc'] == 1) & (test_data['tot_size'] >= 9300)
            test_data['test4'] = (test_data['orig_tot_size'].isnull() == True) & (test_data['tot_size'].isnull() == False)
            test_data['retail_property_is_anchor_flag'] = np.where(((test_data['test1']) | (test_data['test2']) | (test_data['test3'])) & (test_data['test4']), 'Y', test_data['retail_property_is_anchor_flag'])
            test_data['retail_property_is_anchor_flag'] = np.where((test_data['test1'] == False) & (test_data['test2'] == False) & (test_data['test3'] == False) & (test_data['test4']), 'N', test_data['retail_property_is_anchor_flag'])
            test_data['a_size'] = np.where(((test_data['test1']) | (test_data['test2']) | (test_data['test3'])) & (test_data['test4']), test_data['tot_size'], test_data['a_size'])
            test_data['n_size'] = np.where((test_data['test1'] == False) & (test_data['test2'] == False) & (test_data['test3'] == False) & (test_data['test4']), test_data['tot_size'], test_data['n_size'])
            
        # To handle cases where REIS may be grouping multiple buildings under one ID, replace total building size with the business park total size
        test_data['size_diff_bp'] = abs((test_data[bp_field] - test_data['log_tot_size']) / test_data['log_tot_size'])
        if self.sector == "ret":
            test_data['use_park'] = (test_data['size_diff_bp'] < test_data['size_diff_tot']) & (test_data[bp_field].isnull() == False) & (test_data[bp_field] > 0)
        elif self.sector == "off" or self.sector == "ind":
            test_data['use_park'] = ((test_data['log_bldgs'] > 1) | (test_data['log_bldgs'].isnull() == True)) & (test_data['size_diff_bp'] < test_data['size_diff_tot']) & (test_data[bp_field].isnull() == False) & (test_data[bp_field] > 0)

        test_data['tot_size'] = np.where(test_data['use_park'], test_data[bp_field], test_data['tot_size'])
        if self.sector == "ret":
            test_data['n_size'] = np.where((test_data['use_park']), test_data['nonanchor_within_business_park_size_sf'], test_data['n_size'])
            test_data['a_size'] = np.where((test_data['use_park']), test_data['anchor_within_business_park_size_sf'], test_data['a_size'])
        test_data['size_method'] = np.where(test_data['use_park'], 'Business Park SQFT', test_data['size_method'])
         
        test_data['size_diff_use'] = np.where((test_data['use_park']), test_data['size_diff_bp'], test_data['size_diff_tot'])
        
        if self.sector == "ret":
            test_data['test1'] = (test_data['foundation_property_id'] != '')
            test_data['test2'] = (test_data['businesspark'] != '') & (test_data['businesspark'].isnull() == False)
            test_data['test3'] = test_data['business_park_retail_size_sf'].isnull() == False
            test_data['test4'] = test_data['size_diff_use'] <= 0.05
            test_data['test5'] = (((test_data['log_n_size'].isnull() == False) | (test_data['log_a_size'].isnull() == False)) & ((test_data['log_n_size'] > 0) & (test_data['log_a_size'] > 0))) | ((test_data['tot_size'].isnull() == True) & (test_data['log_tot_size'].isnull() == False))
            
            test_data['use_reis_breakout'] = np.where((test_data['test1']) & (test_data['test2']) & (test_data['test3']) & (test_data['test4']) & (test_data['test5']), True, False)
            test_data['use_reis_breakout'] = np.where((test_data['foundation_property_id'] != '') & ((test_data['businesspark'] == '') | (test_data['businesspark'].isnull() == True) | (test_data['business_park_retail_size_sf'] == 0) | (test_data['business_park_retail_size_sf'].isnull() == True)) & ((test_data['size_diff_use'] <= 0.05) | (test_data['tot_size'].isnull() == True)), True, test_data['use_reis_breakout'])
            test_data['tot_size'] = np.where((test_data['use_reis_breakout']) & (test_data['log_tot_size'].isnull() == False), test_data['log_tot_size'], test_data['tot_size'])
            test_data['n_size'] = np.where((test_data['use_reis_breakout']) & ((test_data['log_n_size'].isnull() == False) | (test_data['log_a_size'].isnull() == False)) & ((test_data['log_n_size']) > 0 | (test_data['log_a_size'] > 0)), test_data['log_n_size'], test_data['n_size'])
            test_data['a_size'] = np.where((test_data['use_reis_breakout']) & ((test_data['log_n_size'].isnull() == False) | (test_data['log_a_size'].isnull() == False)) & ((test_data['log_n_size']) > 0 | (test_data['log_a_size'] > 0)), test_data['log_a_size'], test_data['a_size'])
            test_data['n_size'] = np.where((test_data['use_reis_breakout']) & (test_data['n_size'].isnull() == True) & (test_data['a_size'] == test_data['tot_size']), 0, test_data['n_size'])
            test_data['a_size'] = np.where((test_data['use_reis_breakout']) & (test_data['a_size'].isnull() == True) & (test_data['n_size'] == test_data['tot_size']), 0, test_data['a_size'])
            test_data['tot_size'] = np.where((test_data['use_reis_breakout']) & (test_data['log_tot_size'].isnull() == True), test_data['a_size'] + test_data['n_size'], test_data['tot_size'])
            test_data['retail_property_is_anchor_flag'] = np.where((test_data['use_reis_breakout']) & (test_data['retail_property_is_anchor_flag'] == '') & (test_data['tot_size'] >= 9300), 'Y', test_data['retail_property_is_anchor_flag'])
            test_data['retail_property_is_anchor_flag'] = np.where((test_data['use_reis_breakout']) & (test_data['retail_property_is_anchor_flag'] == '') & (test_data['tot_size'] < 9300), 'N', test_data['retail_property_is_anchor_flag'])
            test_data['size_method'] = np.where((test_data['use_reis_breakout']), 'REIS Size SQFT', test_data['size_method'])
            
        temp = test_data.copy()
        temp = temp[(temp['foundation_property_id'] != '') & (temp['foundation_property_id'].isnull() == False)]
        temp = temp[temp['foundation_property_id'].str[0] == self.sector_map[self.sector]['prefix']]
        temp = temp[(temp['use_park'])]
        temp = temp[(temp['businesspark'] != '') & (temp['businesspark'].isnull() == False)]
        temp = temp.join(test_data[test_data['a_size'].isnull() == False].drop_duplicates('park_identity').set_index('park_identity').rename(columns={'a_size': 'a_size_filled', 'tot_size': 'tot_size_filled'})[['a_size_filled', 'tot_size_filled']], on='park_identity')
        temp = temp.join(test_data[test_data['n_size'].isnull() == False].drop_duplicates('park_identity').set_index('park_identity').rename(columns={'n_size': 'n_size_filled'})[['n_size_filled']], on='park_identity')
        temp[bp_field] = np.where((temp['tot_size_filled'].isnull() == False), temp['tot_size_filled'], temp[bp_field])
        temp['anchor_within_business_park_size_sf'] = np.where((temp['a_size_filled'].isnull() == False), temp['a_size_filled'], temp['anchor_within_business_park_size_sf'])
        temp['nonanchor_within_business_park_size_sf'] = np.where((temp['n_size_filled'].isnull() == False), temp['n_size_filled'], temp['nonanchor_within_business_park_size_sf'])
        temp = temp.drop_duplicates('park_identity')
        test_data = test_data.join(temp.set_index('park_identity').rename(columns={'foundation_property_id': 'foundation_property_id_from_park', 'metcode': 'metcode_from_park', 'gsub': 'subid_from_park', bp_field: 'tot_size_from_park', 'anchor_within_business_park_size_sf': 'anchor_size_from_park', 'nonanchor_within_business_park_size_sf': 'nonanchor_size_from_park', 'type1': 'type1_from_park'})[['foundation_property_id_from_park', 'metcode_from_park', 'subid_from_park', 'tot_size_from_park', 'anchor_size_from_park', 'nonanchor_size_from_park', 'type1_from_park']], on='park_identity')
        test_data['use_park'] = np.where((test_data['foundation_property_id'] == '') & (test_data['foundation_property_id_from_park'].isnull() == False) & (test_data['space_category'].isin(self.space_map[self.sector] + [''])), True, test_data['use_park'])
        test_data['size_method'] = np.where((test_data['foundation_property_id'] == '') & (test_data['foundation_property_id'] == '') & (test_data['foundation_property_id_from_park'].isnull() == False) & (test_data['space_category'].isin(self.space_map[self.sector] + [''])), 'Linked To Catylist ID Via BP', test_data['size_method'])
        test_data['metcode'] = np.where((test_data['foundation_property_id'] == '') & (test_data['metcode_from_park'].isnull() == False) & (test_data['space_category'].isin(self.space_map[self.sector] + [''])), test_data['metcode_from_park'], test_data['metcode'])
        test_data['gsub'] = np.where((test_data['foundation_property_id'] == '') & (test_data['subid_from_park'].isnull() == False) & (test_data['space_category'].isin(self.space_map[self.sector] + [''])), test_data['subid_from_park'], test_data['gsub'])
        test_data['tot_size'] = np.where((test_data['foundation_property_id'] == '') & (test_data['tot_size_from_park'].isnull() == False) & (test_data['space_category'].isin(self.space_map[self.sector] + [''])), test_data['tot_size_from_park'], test_data['tot_size'])
    
        # Because prop size may have changed from the initial value, recalc anchor status as long as the size method was not bp size (in that case, we do want to retain the individual original anchor statuses, so we can calculate anchor and non anchor rents at the park for the different properties that compose it as in traditional REIS)
        if self.sector == "ret":
            test_data['test1'] = (test_data['tot_size'] >= 9300) & (test_data['occupancy_type'] == 'single_tenant') 
            test_data['test2'] = (test_data['tot_size'] >= 18600)
            test_data['test3'] = (test_data['rownum_buspark_retail_size_desc'] == 1) & (test_data['tot_size'] >= 9300)
            test_data['retail_property_is_anchor_flag'] = np.where(((test_data['test1']) | (test_data['test2']) | (test_data['test3'])) & (test_data['size_method'] != 'Business Park SQFT') & (test_data['use_reis_breakout'] == False), 'Y', test_data['retail_property_is_anchor_flag'])
            test_data['retail_property_is_anchor_flag'] = np.where((test_data['test1'] == False) & (test_data['test2'] == False) & (test_data['test3'] == False) & (test_data['size_method'] != 'Business Park SQFT') & (test_data['use_reis_breakout'] == False), 'N', test_data['retail_property_is_anchor_flag'])
            test_data['retail_property_is_anchor_flag'] = np.where((test_data['use_reis_breakout']) & ((test_data['log_n_size'] == 0) | (test_data['log_n_size'].isnull() == True)) & (test_data['log_a_size'] > 0), 'Y', test_data['retail_property_is_anchor_flag'])
            test_data['retail_property_is_anchor_flag'] = np.where((test_data['use_reis_breakout']) & ((test_data['log_a_size'] == 0) | (test_data['log_a_size'].isnull() == True)) & (test_data['log_n_size'] > 0), 'N', test_data['retail_property_is_anchor_flag'])
    
        if self.sector == "ret":
            test_data['type1'] = np.where((test_data['foundation_property_id'] == '') & (test_data['type1_from_park'].isnull() == False) & (test_data['space_category'].isin(self.space_map[self.sector] + [''])) & (test_data['type1'] == ''), test_data['type1_from_park'], test_data['type1'])
            test_data['n_size'] = np.where((test_data['foundation_property_id'] == '') & (test_data['nonanchor_size_from_park'].isnull() == False) & (test_data['space_category'].isin(self.space_map[self.sector] + [''])), test_data['nonanchor_size_from_park'], test_data['n_size'])
            test_data['a_size'] = np.where((test_data['foundation_property_id'] == '') & (test_data['anchor_size_from_park'].isnull() == False) & (test_data['space_category'].isin(self.space_map[self.sector] + [''])), test_data['anchor_size_from_park'], test_data['a_size'])
        test_data['foundation_property_id'] = np.where((test_data['foundation_property_id'] == '') & (test_data['foundation_property_id_from_park'].isnull() == False) & (test_data['space_category'].isin(self.space_map[self.sector] + [''])), test_data['foundation_property_id_from_park'], test_data['foundation_property_id'])
        
        if self.sector == "ret":
            test_data['n_size'] = np.where((test_data['n_size'].isnull() == True) & (test_data['retail_property_is_anchor_flag'] == 'N') & ((test_data['use_reis_breakout'] == False) | (test_data['tot_size'] == test_data['log_tot_size'])), test_data['tot_size'], test_data['n_size'])
            test_data['a_size'] = np.where((test_data['a_size'].isnull() == True) & (test_data['retail_property_is_anchor_flag'] == 'Y') & ((test_data['use_reis_breakout'] == False) | (test_data['tot_size'] == test_data['log_tot_size'])), test_data['tot_size'], test_data['a_size'])
            test_data['n_size'] = np.where((test_data['a_size'].isnull() == False) & (test_data['n_size'].isnull() == True), 0, test_data['n_size'])
            test_data['a_size'] = np.where((test_data['n_size'].isnull() == False) & (test_data['a_size'].isnull() == True), 0, test_data['a_size'])
            test_data['n_size'] = np.where((test_data['n_size'].isnull() == True) & (test_data['use_reis_breakout'] == False), 0, test_data['n_size'])
            test_data['a_size'] = np.where((test_data['a_size'].isnull() == True) & (test_data['use_reis_breakout'] == False), 0, test_data['a_size'])
        
        if self.sector == "off" or self.sector == "ind":
            test_data['use_reis_breakout'] = False
        test_data['size_method'] = np.where(((test_data['tot_size'] == test_data['orig_tot_size']) | ((test_data['orig_tot_size'].isnull() == True) & (test_data['tot_size'].isnull() == True))) & ((test_data['use_reis_breakout'] == False)| (test_data['size_method'] == '')), 'Building Size SQFT', test_data['size_method'])
        
        return test_data
    
    def drop_mult_caty_single_reis_links(self, test_data, id_check):
        
        cat_map = {'off': ['office', 'government'],
           'ind': ['industrial', 'life_science'],
           'ret': ['retail']}
        
        test_data['count'] = test_data.groupby('id_use')['property_source_id'].transform('nunique')
        test_data['not_complete'] = np.where((test_data['count'] > 1) & (~test_data['buildings_building_status'].isin(['existing', ''])), 1, 0)
        temp = test_data.copy()
        temp = temp[(temp['not_complete'] == 1)]
        if len(temp) > 0:
            temp['reason'] = 'REIS ID linked to multiple Catylist properties, and Catylist property does not have existing status'
            self.drop_log = self.drop_log.append(temp[['property_source_id', 'id_use', 'reason']].drop_duplicates('property_source_id'), ignore_index=True)
        test_data = test_data[test_data['not_complete'] == 0]
        test_data['count'] = test_data.groupby('id_use')['property_source_id'].transform('nunique')
        test_data['foundation_ids_list_temp'] = test_data['foundation_ids_list']
        test_data['foundation_ids_list_temp'] = np.where((test_data['foundation_ids_list_temp'] == ''), np.nan, test_data['foundation_ids_list_temp'])
        test_data['count_er'] = test_data.groupby('id_use')['foundation_ids_list_temp'].transform('count')
        temp = test_data.copy()
        temp = temp[temp['size_method'] == 'Linked To Catylist ID Via BP']
        temp['has_bp_link'] = 1
        test_data = test_data.join(temp.drop_duplicates('id_use').set_index('id_use')[['has_bp_link']], on='id_use')
        test_data['rc_no_er'] = np.where((test_data['count'] > 1) & (test_data['has_bp_link'].isnull() == True) & (test_data['count_er'] > 0) & (test_data['foundation_ids_list'] == ''), 1, 0)
        temp = test_data.copy()
        temp = temp[temp['rc_no_er'] == 1]
        if len(temp) > 0:
            temp['reason'] = 'REIS ID linked to multiple Catylist properties, and ER did not match this RC ID'
            self.drop_log = self.drop_log.append(temp[['property_source_id', 'id_use', 'reason']].drop_duplicates('property_source_id'), ignore_index=True)
        test_data = test_data[test_data['rc_no_er'] == 0]
        test_data['count'] = test_data.groupby('id_use')['property_source_id'].transform('nunique')
        test_data['cumcount'] = test_data.groupby('property_source_id')['property_source_id'].transform('cumcount')
        test_data['size_aggreg'] = test_data[test_data['cumcount'] == 0].groupby('id_use')['tot_size'].transform('sum')
        test_data['size_aggreg'] = test_data.groupby('id_use')['size_aggreg'].bfill()
        test_data['size_aggreg'] = test_data.groupby('id_use')['size_aggreg'].ffill()
        test_data['test1'] = test_data['count'] > 1
        test_data['test2'] = test_data['use_park'] == False
        test_data['test3'] = test_data['size_aggreg'] > test_data['log_tot_size'] + 1000
        temp = test_data.copy()
        temp['testing'] = np.where((temp['test1']) & (temp['test2']) & (temp['test3']), True, False)
        temp['extra_sector'] = np.where((temp['testing']) & (~temp['category'].isin(cat_map[self.sector])), True, False)
        if len(temp[temp['extra_sector']]) > 0:
            temp = temp[temp['extra_sector']]
            temp['reason'] = 'REIS ID linked to multiple Catylist properties, and subcategory not in line with publishable REIS types'
            self.drop_log = self.drop_log.append(temp[['property_source_id', 'id_use', 'reason']].drop_duplicates('property_source_id'), ignore_index=True)
            test_data = test_data.join(temp.drop_duplicates('property_source_id').set_index('property_source_id').rename(columns={'reason': 'drop_these'})[['drop_these']], on='property_source_id')
            test_data = test_data[test_data['drop_these'].isnull() == True]
            test_data = test_data.drop(['drop_these'],axis=1)
            test_data['count'] = test_data.groupby('id_use')['property_source_id'].transform('nunique')
            test_data['cumcount'] = test_data.groupby('property_source_id')['property_source_id'].transform('cumcount')
            test_data['size_aggreg'] = test_data[test_data['cumcount'] == 0].groupby('id_use')['tot_size'].transform('sum')
            test_data['size_aggreg'] = test_data.groupby('id_use')['size_aggreg'].bfill()
            test_data['size_aggreg'] = test_data.groupby('id_use')['size_aggreg'].ffill()
            if self.sector == "ret":
                test_data['n_size_aggreg'] = test_data[test_data['cumcount'] == 0].groupby('id_use')['n_size'].transform('sum')
                test_data['n_size_aggreg'] = test_data.groupby('id_use')['n_size_aggreg'].bfill()
                test_data['n_size_aggreg'] = test_data.groupby('id_use')['n_size_aggreg'].ffill()
                test_data['a_size_aggreg'] = test_data[test_data['cumcount'] == 0].groupby('id_use')['a_size'].transform('sum')
                test_data['a_size_aggreg'] = test_data.groupby('id_use')['a_size_aggreg'].bfill()
                test_data['a_size_aggreg'] = test_data.groupby('id_use')['a_size_aggreg'].ffill()
            test_data['test1'] = test_data['count'] > 1
            test_data['test2'] = test_data['use_park'] == False
            test_data['test3'] = (test_data['size_aggreg'] > test_data['log_tot_size'] + 5000) & (abs((test_data['size_aggreg'] - test_data['log_tot_size']) / test_data['log_tot_size']) > 0.1)
            temp = test_data.copy()
            temp['testing'] = np.where((temp['test1']) & (temp['test2']) & (temp['test3']), True, False)
        temp = temp[(temp['testing'])]
        if len(temp) > 0:
            temp['flag'] = 'Multiple Catylist properties linked to one REIS ID'
            temp['status'] = 'Econ Link'
            id_check = id_check.append(temp.drop_duplicates('property_source_id')[['id_use', 'property_source_id', 'flag', 'status']])
            temp['status'] = 'dropped'
            self.logic_log = self.logic_log.append(temp.drop_duplicates('property_source_id')[['id_use', 'property_source_id', 'flag', 'status']])
            test_data = test_data.join(temp.drop_duplicates('property_source_id').set_index('property_source_id')[['flag']], on='property_source_id')
            test_data = test_data[(test_data['flag'].isnull() == True)]
            test_data = test_data.drop(['flag'], axis=1)
        test_data['tot_size'] = np.where((test_data['test1']) & (test_data['test2']) & (test_data['test3'] == False), test_data['size_aggreg'], test_data['tot_size'])
        if self.sector == "ret":
            test_data['n_size'] = np.where((test_data['test1']) & (test_data['test2']) & (test_data['test3'] == False), test_data['n_size_aggreg'], test_data['n_size'])
            test_data['a_size'] = np.where((test_data['test1']) & (test_data['test2']) & (test_data['test3'] == False), test_data['a_size_aggreg'], test_data['a_size'])
        test_data['size_method'] = np.where((test_data['test1']) & (test_data['test2']) & (test_data['test3'] == False), 'Mult Link SQFT', test_data['size_method'])
            
        return test_data, id_check
        
    def apply_reis_id(self, test_data, decision, log):
        
        test_data = test_data.join(decision.set_index('property_source_id'), on='property_source_id')
        test_data['foundation_property_id'] = np.where((test_data['foundation_property_id'].isnull() == True), '', test_data['foundation_property_id'])
        test_data['metcode'] = np.where((test_data['metcode'].isnull() == True), '', test_data['metcode'])
        temp = test_data.copy()
        temp = temp[temp['logic_flag'] != '']
        if self.use_mult:
            temp['status'] = 'flagged'
        else:
            temp['status'] = 'dropped'
        self.logic_log = self.logic_log.append(temp.rename(columns={'mult_ids': 'ids', 'logic_flag': 'flag'})[['ids', 'flag', 'property_source_id', 'status']], ignore_index=True)

        test_data = self.choose_catylist_size(test_data, log)
            
        test_data['id_use'] = np.nan
        test_data['id_use'] = np.where((test_data['foundation_property_id'] != '') & (test_data['foundation_property_id'].isnull() == False), test_data['foundation_property_id'], test_data['id_use'])
            
        test_data['id_use'] = np.where((test_data['id_use'].isnull() == True) & (test_data['property_source_id'] != '') & (test_data['property_source_id'].isnull() == False), test_data['property_source_id'], test_data['id_use'])
        
        if len(test_data[(test_data['id_use'] == '') | (test_data['id_use'].isnull() == True)]) > 0:
            self.stop = True
        
        id_check = pd.DataFrame()
        temp = test_data.copy()
        temp = temp[(test_data['id_use'].isnull() == True) | (test_data['id_use'] == '')]
        if len(temp) > 0:
            temp['flag'] = 'missing id'
            id_check = id_check.drop_duplicates('id_use').append(temp[['id_use', 'property_source_id', 'foundation_property_id', 'foundation_ids_list', 'flag']])
        temp = test_data.copy()
        temp = temp[(temp['foundation_ids_list'] == '') & (temp['property_reis_rc_id'] == '')]
        if len(temp) > 0 and self.use_rc_id:
            if self.legacy_only:
                temp1 = temp.copy()
                temp1 = temp1[(temp1['mult_rc_tag']) & (temp1['foundation_property_id'] == '')]
                if len(temp1) > 0:
                    temp1['flag'] = 'rc id linked to multiple Catylist ids'
                    temp1['status'] = 'Econ Link'
                    id_check = id_check.append(temp1.drop_duplicates('property_source_id')[['id_use', 'property_source_id', 'flag', 'status']])
            
            temp = temp[((temp['mult_rc_tag'] == False) | (not self.legacy_only)) & (temp['foundation_ids_list'] == '') & (temp['property_reis_rc_id'] == '') & (temp['foundation_property_id'] == '')]
            if len(temp) > 0:
                temp['flag'] = 'no REIS Catylist ER link'
                temp['status'] = 'No Econ Link'
                id_check = id_check.append(temp.drop_duplicates('property_source_id')[['id_use', 'property_source_id', 'flag', 'status']])
        temp = test_data.copy()
        temp = temp[((temp['foundation_property_id'].isnull() == True) | (temp['foundation_property_id'] == '')) & ((temp['foundation_ids_list'] != '') | (temp['property_reis_rc_id'] != '')) & ((temp['foundation_ids_list'].str.contains(self.sector_map[self.sector]['prefix']) == True) | (temp['property_reis_rc_id'].str.contains(self.sector_map[self.sector]['prefix']) == True))]
        if len(temp) > 0:
            temp['flag'] = 'property linked to REIS id that is not live'
            temp['status'] = 'No Econ Link'
            temp['ids_to_add'] = temp[['foundation_ids_list', 'property_reis_rc_id']].apply(lambda x: ', '.join(x[x.notnull()]), axis=1)
            temp['ids_to_add'] = temp['ids_to_add'].str.strip()
            temp['ids_to_add'] = np.where((temp['ids_to_add'].str[0] == ','), temp['ids_to_add'].str[1:].str.strip(), temp['ids_to_add'])
            temp['ids_to_add'] = np.where((temp['ids_to_add'].str[-1] == ','), temp['ids_to_add'].str[:-1].str.strip(), temp['ids_to_add'])
            id_check = id_check.append(temp.drop_duplicates('property_source_id')[['id_use', 'property_source_id', 'ids_to_add', 'flag', 'status']])
        temp = test_data.copy()
        temp = temp[((temp['foundation_property_id'].isnull() == True) | (temp['foundation_property_id'] == '')) & ((temp['foundation_ids_list'] != '') | (temp['property_reis_rc_id'] != '')) & (temp['foundation_ids_list'].str.contains(self.sector_map[self.sector]['prefix']) == False) & (temp['property_reis_rc_id'].str.contains(self.sector_map[self.sector]['prefix']) == False)]
        if len(temp) > 0:
            temp['flag'] = 'property not linked to REIS id in this sector'
            temp['status'] = 'No Econ Link'
            temp['ids_to_add'] = temp[['foundation_ids_list', 'property_reis_rc_id']].apply(lambda x: ', '.join(x[x.notnull()]), axis=1)
            temp['ids_to_add'] = temp['ids_to_add'].str.strip()
            temp['ids_to_add'] = np.where((temp['ids_to_add'].str[0] == ','), temp['ids_to_add'].str[1:].str.strip(), temp['ids_to_add'])
            temp['ids_to_add'] = np.where((temp['ids_to_add'].str[-1] == ','), temp['ids_to_add'].str[:-1].str.strip(), temp['ids_to_add'])
            id_check = id_check.append(temp.drop_duplicates('id_use')[['id_use', 'property_source_id', 'ids_to_add', 'flag', 'status']])
        
        # Need to drop cases where multiple Catylist properties are linked to the same REIS ID to avoid the scenario where the total avail can exceed property size when rolled up to id_use
        test_data, id_check = self.drop_mult_caty_single_reis_links(test_data, id_check)
        
        id_check = id_check.drop_duplicates('property_source_id')
        
        logging.info("{:,} rows are missing an ID".format(len(test_data[(test_data['id_use'].isnull() == True) | (test_data['id_use'] == '')])))
        
        test_data['leg'] = np.where((test_data['foundation_property_id'].isnull() == True) | (test_data['foundation_property_id'] == ''), False, True)

        test_data['reis_sector'] = ''
        for key, value in self.sector_map.items():
            test_data['reis_sector'] = np.where((test_data['reis_sector'] == '') & (test_data['id_use'].isnull() == False) & (test_data['leg']) & (test_data['id_use'].str[0] == value['prefix']), key, test_data['reis_sector'])
        for key, value in self.sector_map.items():
            test_data['reis_sector'] = np.where((test_data['leg'] == False) & (test_data['catylist_sector'].isin(value['sector'])), key, test_data['reis_sector'])
        
        test_data['id_use'] = np.where((test_data['id_use'].isnull() == True), '', test_data['id_use'])
        test_data['id_use'] = test_data['id_use'].astype(str)
        
        temp = test_data.copy()
        temp = temp[(temp['buildings_building_status'] != 'existing') & (temp['buildings_building_status'] != '') & (temp['leg'])]
        if len(temp) > 0:
            temp['flag'] = 'REIS building complete, Catylist building UC'
            temp['status'] = 'dropped'
            self.logic_log = self.logic_log.append(temp.drop_duplicates('property_source_id')[['property_source_id', 'flag', 'id_use', 'status']])            

        return test_data, self.stop, id_check
               
    def format_realid(self, test_data, log):
        
        if self.legacy_only:
            test_data = test_data[(test_data['leg'] == True) | ((test_data['first_year'] >= self.curryr - 1) & (self.include_cons))] 
               
        # If the sector is off or retail, the last digit of the realid is actually the phase
        if self.sector == "off" or self.sector == "ind":
            test_data['phase'] = 0
            phase = log.copy()
            phase['phase_log'] = phase['phase']
            test_data['id_join'] = test_data['id_use'].str[1:]
            test_data = test_data.join(phase.rename(columns={'realid': 'id_join'}).drop_duplicates('id_join').set_index('id_join')[['phase_log']], on='id_join')
            test_data['phase'] = np.where((test_data['leg']) & (test_data['phase_log'].isnull() == False), test_data['phase_log'], test_data['phase'])
            test_data['phase'] = np.where((test_data['phase'].isnull() == True), 0, test_data['phase'])
            test_data = test_data.drop(['phase_log', 'id_join'], axis=1)
            
        # Remove the alpha char at the beginning of realid
        test_data['id_use'] = np.where((test_data['id_use'] != '') & (test_data['leg']), test_data['id_use'].str[1:], test_data['id_use'])

        # Add an alpha char to all non legacy properties, to avoid the potential for id overlap
        test_data['id_use'] = np.where((test_data['leg'] == False), test_data['id_use'] + 'a', test_data['id_use'])
        
        if self.sector == "off" or self.sector == "ind":
            test_data['phase'] = test_data['phase'].astype(int)  
               
        return test_data
               
    def gen_valid_vals(self, log):
        
        valid_vals = {}
        
        log['realid'] = log['realid'].astype(str)
        
        if isinstance(log.reset_index().loc[0]['zip'], str):
            log['zip'] = np.where((log['zip'].str.isdigit() == False), '', log['zip'])
            
        if 'phase' in log.columns:
            log['phase'] = log['phase'].astype(float)
        if 'foodct' in log.columns:
            log['foodct'] = log['foodct'].astype(str)

        for key, value in self.type_dict.items():
            if value['type'] == 'float' and isinstance(log.reset_index().loc[0][key], str):
                log[key] = np.where(log[key] == '', np.nan, log[key])
                log[key] = log[key].astype(float)

        for key, value in self.type_dict.items():
            if self.type_dict[key]['null_check'] == 'no':
                valid_vals[key] = [x for x in log[key].unique() if x != -1]
                
        self.valid_vals = valid_vals
               
        return log
    
    def gen_type(self, test_data, log):
        
        if self.sector == "off":
            test_data['type2'] = np.where((test_data['occupancy_type'] == 'single_tenant'), 'T', '')
            test_data['type2'] = np.where((test_data['occupancy_type'] == 'multi_tenant'), 'O', test_data['type2'])

            
        if self.sector == "off" or self.sector == "ind":
        
            log_aligned = log.copy()
            log_aligned['type2_log'] = log_aligned['type2']
            test_data = test_data.join(log_aligned.drop_duplicates('realid').rename(columns={'realid': 'id_use'}).set_index('id_use')[['type2_log']], on='id_use')
            if self.sector == "off":
                test_data['type2'] = np.where(((test_data['type2']  == '') | (test_data['type2'].isnull() == True)) & (test_data['type2_log'].isnull() == False), test_data['type2_log'], test_data['type2'])
            elif self.sector == "ind":
                test_data['type2'] = ''
                # Note: Looks like Catylist sector is Flx for any cases where subcategory is warehouse flex or warehouse office, regardless of what percentage of the space is office. So moving away from using that as core determinant and will rely on the log
                test_data['type2'] = np.where((test_data['type2_log'].isnull() == False), test_data['type2_log'], test_data['type2'])
                test_data['type2'] = np.where((test_data['type2'] == '') & (test_data['catylist_sector'] == 'Flx'), 'F', test_data['type2'])
            test_data = test_data.drop(['type2_log'], axis=1)
        
        elif self.sector == "ret":
            log_aligned = log.copy()
            log_aligned['type1_log'] = log_aligned['type1']
            test_data = test_data.join(log_aligned.drop_duplicates('realid').rename(columns={'realid': 'id_use'}).set_index('id_use')[['type1_log']], on='id_use')
            test_data['type1'] = np.where((test_data['type1_log'].isnull() == False), test_data['type1_log'], test_data['type1'])
            test_data = test_data.drop(['type1_log'], axis=1)
        
        if self.sector == "off":
            type2_default = 'O'
        elif self.sector == "ind": 
            type2_default = 'W'
        if self.sector != 'ret':
            test_data['type2'] = np.where(((test_data['type2'] == '') | (test_data['type2'].isnull() == True)) & (test_data['leg'] == 'no'), type2_default, test_data['type2'])

        elif self.sector == "ret":
            test_data['type2'] = test_data['type1']

        if self.sector == "ind":
            test_data['space_size_available'] = np.where((test['space_size_available'] > 0) & (test_data['type2'] != 'F') & (test_data['space_industrial_size_sf'] > 100) & (test_data['space_industrial_size_sf'] <= test_data['space_size_available']), test_data['space_industrial_size_sf'], test_data['space_size_available'])
            test_data['space_size_available'] = np.where((test['space_size_available'] > 0) & (test_data['type2'] != 'F') & (test_data['space_office_size_sf'] > 100) & (test_data['space_industrial_size_sf'].isnull() == True) & (test_data['space_size_available'] - test_data['space_office_size_sf'] >= 500), test_data['space_size_available'] - test_data['space_office_size_sf'], test_data['space_size_available'])
            
    
        return test_data
    
    def select_comp(self, test_data):
        
        temp = test_data.copy()
        temp = temp[(temp['buildings_building_status'] != 'existing') & ((temp['buildings_building_status'] != '') | (temp['leg'] == False))]
        if len(temp) > 0:
            temp['reason'] = 'building not complete'
            self.drop_log = self.drop_log.append(temp[['property_source_id', 'id_use', 'reason', 'space_category']].drop_duplicates('property_source_id'), ignore_index=True)
        
        test_data = test_data[(test_data['buildings_building_status'] == 'existing') | ((test_data['buildings_building_status'] == '') & (test_data['leg']))]
        
        temp = test_data.copy()
        temp = temp[temp['category'] == 'land']
        if len(temp) > 0:
            temp['reason'] = 'C prop is land'
            self.drop_log = self.drop_log.append(temp[['property_source_id', 'id_use', 'reason', 'space_category']].drop_duplicates('property_source_id'), ignore_index=True)
        test_data = test_data[test_data['category'] != 'land']
        
        # According to Allen Benson, listing level owner occupied tags should only be invalidated if the property level value is marked as OO
        test_data['count'] = test_data.groupby('property_source_id')['listed_space_id'].transform('count')
        test_data['count_owned'] = test_data.groupby('property_source_id')['availability_owneroccupied'].transform('sum')
        test_data['count_owned_null'] = test_data['availability_owneroccupied'].isnull()
        test_data['count_owned_null'] = test_data.groupby('property_source_id')['count_owned_null'].transform('sum').astype(int)
        temp = test_data.copy()
        temp = temp[((temp['occupancy_owneroccupied'] == 1) & ((temp['count'] == temp['count_owned']) | (temp['count'] == temp['count_owned_null'])))]
        if len(temp) > 0:
            temp['reason'] = 'C prop owner occupied'
            self.drop_log = self.drop_log.append(temp[['property_source_id', 'id_use', 'reason', 'space_category']].drop_duplicates('property_source_id'), ignore_index=True)
            test_data = test_data.join(temp.drop_duplicates('property_source_id').set_index('property_source_id').rename(columns={'reason': 'drop_this'})[['drop_this']], on='property_source_id')
            test_data = test_data[(test_data['drop_this'].isnull() == True)]
            test_data = test_data.drop(['drop_this'],axis=1)
        
        if not self.legacy_only or self.include_cons:
            
            if self.sector == "off":
                size_by_use = 'building_office_size_sf'
            elif self.sector == "ind":
                size_by_use = "building_industrial_size_sf"
            elif self.sector == "ret":
                size_by_use = "building_retail_size_sf"

            test_data['include'] = False
            test_data['include'] = np.where((test_data['subcategory'] == 'mixed_use') & (test_data[size_by_use] > 0), True, test_data['include'])
            test_data['include'] = np.where((~test_data['category'].isin(self.sector_map[self.sector]['category'])) | (~test_data['subcategory'].isin(self.sector_map[self.sector]['subcategory'] + [''])), False, test_data['include'])
            test_data['include'] = np.where((test_data['subcategory'] == 'warehouse_office') & (self.sector == 'ind') & (test_data['building_industrial_use_size_sf'].isnull() == True) & (test_data['building_office_use_size_sf'].isnull() == True), False, test_data['include'])
            test_data['include'] = np.where((test_data['buildings_condominiumized_flag'] == 'Y'), False, test_data['include'])
            test_data['include'] = np.where(((test_data['retail_center_type'] != '') | (test_data['subcategory'] == '')) & (self.sector == 'ret'), False, test_data['include'])
            test_data['include'] = np.where((self.sector in ['off', 'ind']) & (test_data['tot_size'] < 10000), False, test_data['include'])
            test_data['include'] = np.where((test_data['property_geo_msa_code'] == '') | (test_data['property_geo_subid'].isnull() == True), False, test_data['include'])
            test_data['include'] = np.where((test_data['legacy']), True, test_data['include'])
            
            temp = test_data.copy()
            temp = temp[temp['include'] == False]
            if len(temp) > 0:
                temp['reason'] = 'Net new Catylist property not eligible for inclusion'
                self.drop_log = self.drop_log.append(temp[['property_source_id', 'id_use', 'reason']].drop_duplicates('property_source_id'), ignore_index=True)
            test_data = test_data[(test_data['include'])]
            if self.sector == 'ind':
                test_data['off_perc'] = np.where((test_data['building_office_use_size_sf'].isnull() == False), test_data['building_office_use_size_sf'] / test_data['size'], (test_data['size'] - test_data['building_industrial_use_size_sf']) / test_data['size'])
                test_data['subcategory'] = np.where((test_data['leg'] == False) & (test_data['off_perc'] >= 0.25), 'warehouse_flex', test_data['subcategory'])
                test_data['subcategory'] = np.where((test_data['leg'] == False) & (test_data['off_perc'] < 0.25), 'warehouse_distribution', test_data['subcategory'])
                test_data['type2'] = np.where((test_data['leg'] == False) & (test_data['subcategory'] == 'warehouse_distribution'), 'W', test_data['type2'])
                test_data['type2'] = np.where((test_data['leg'] == False) & (test_data['subcategory'] == 'warehouse_flex'), 'F', test_data['type2'])
            
        # Drop properties that have no spaces that are for publishable reis types for the sector
        # Also infer that space is leased in cases where a property is assigned a reis id, has no publishable space types in the listings data, but is not fully avail
        cols = test_data.columns
        
        temp1 = test_data.copy()
        temp1 = temp1[((~temp1['space_category'].isin(self.space_map[self.sector])) | ((self.sector == 'ind') & (temp1['space_category'] == 'office') & (temp1['type2'] != "F"))) & (temp1['space_category'].isnull() == False) & (temp1['space_category'] != '')]
        temp1 = temp1[(temp1['lease_sublease'] == 0) | (temp1['lease_sublease'].isnull() == True)]
        temp1['prop_dir_avail'] = temp.groupby('property_source_id')['space_size_available'].transform('sum', min_count=1)
        temp1 = temp1.drop_duplicates('property_source_id')
        test_data = test_data.join(temp1.set_index('property_source_id')[['prop_dir_avail']], on='property_source_id')
        temp1 = test_data.copy()
        temp1 = temp1[((~temp1['space_category'].isin(self.space_map[self.sector])) | ((self.sector == 'ind') & (temp1['space_category'] == 'office') & (temp1['type2'] != "F"))) & (temp1['space_category'].isnull() == False) & (temp1['space_category'] != '')]
        temp1 = temp1[(temp1['lease_sublease'] == 1)]
        temp1['prop_sub_avail'] = temp1.groupby('property_source_id')['space_size_available'].transform('sum', min_count=1)
        temp1 = temp1.drop_duplicates('property_source_id')
        test_data = test_data.join(temp1.set_index('property_source_id')[['prop_sub_avail']], on='property_source_id')
        test_data['prop_dir_avail'] = test_data['prop_dir_avail'].fillna(0)
        test_data['prop_sub_avail'] = test_data['prop_sub_avail'].fillna(0)
        test_data['avail_test'] = np.where((test_data['prop_dir_avail'] + test_data['prop_sub_avail'] >= test_data['tot_size']), 1, 0)
        test_data['avail_test'] = np.where((~test_data['category'].isin(self.sector_map[self.sector]['category'])) & (test_data['subcategory'] != 'mixed_use'), 1, test_data['avail_test'])
        
        test_data['count'] = test_data[((test_data['space_category'].isin(self.space_map[self.sector])) | (test_data['space_category'].isnull() == True) | (test_data['space_category'] == '')) & ((test_data['space_category'] != 'office') | (self.sector != 'ind') | (test_data['type2'] == "F"))].groupby('property_source_id')['property_source_id'].transform('count')
        test_data['count'] = test_data.groupby('property_source_id')['count'].bfill()
        test_data['count'] = test_data.groupby('property_source_id')['count'].ffill()
        test_data['count'] = test_data['count'].fillna(0)
        
        test_data['count_links'] = test_data.groupby('id_use')['property_source_id'].transform('nunique')
        
        test_data['size_by_use_test'] = False
        test_data['size_by_use_test'] = np.where((self.sector == 'off') & (test_data['subcategory'] == 'mixed_use') & ((test_data['building_office_size_sf'] > 0) | (test_data['category'].isin(self.sector_map[self.sector]['category']))), True, test_data['size_by_use_test'])
        test_data['size_by_use_test'] = np.where((self.sector == 'ind') & (test_data['subcategory'] == 'mixed_use') & ((test_data['building_industrial_size_sf'] > 0) | (test_data['category'].isin(self.sector_map[self.sector]['category']))), True, test_data['size_by_use_test'])
        test_data['size_by_use_test'] = np.where((self.sector == 'ret') & (test_data['subcategory'] == 'mixed_use') & ((test_data['building_retail_size_sf'] > 0) | (test_data['category'].isin(self.sector_map[self.sector]['category']))), True, test_data['size_by_use_test'])
        
        temp = test_data.copy()
        temp = temp[((~temp['space_category'].isin(self.space_map[self.sector])) | ((self.sector == 'ind') & (temp['space_category'] == 'office') & (temp['type2'] != "F"))) & (temp['space_category'] != '') & (temp['count'] == 0) & ((temp['avail_test'] == 1) | (temp['leg'] == False) | (temp['count_links'] > 1) | ((temp['size_by_use_test'] == False) & (temp['subcategory'] == 'mixed_use')))]
        if len(temp) > 0:
            temp['reason'] = 'property has no spaces that fit publishable reis types for this sector'
            self.drop_log = self.drop_log.append(temp[['property_source_id', 'reason', 'space_category', 'id_use']].drop_duplicates('property_source_id'), ignore_index=True)
        
        temp2 = test_data.copy()
        temp2['inferred_lease'] = np.where((temp2['count'] == 0) & (temp2['avail_test'] == 0) & (temp2['leg']) & (temp2['count_links'] == 1) & ((test_data['size_by_use_test']) | (test_data['subcategory'] != 'mixed_use')), True, False)
        temp2 = temp2[temp2['inferred_lease']]
        temp2 = temp2.drop_duplicates('id_use')
        temp2['space_size_available'] = 0
        temp2['availability_status'] = 'leased'
        temp2['space_category'] = self.space_map[self.sector][0]
        nan_cols = ['occupancy_cam_amount_amount', 'occupancy_cam_amount_currency', 'occupancy_cam_period', 
                    'occupancy_cam_size', 'occupancy_expenses_amount_amount', 'occupancy_expenses_amount_currency',
                    'occupancy_expenses_period', 'occupancy_expenses_size', 'rent_basis', 'property_real_estate_tax_amt', 
                    'property_real_estate_tax_currency', 'property_real_estate_tax_year', 'lease_sublease', 'lease_terms', 
                    'lease_transaction_leasetermmonths', 'lease_transaction_freerentmonths', 'space_size_maximumcontiguous', 
                    'commission_amount_percentage', 'commission_amount_total_amount', 'commission_amount_total_currency', 
                    'commission_amount_type', 'lease_transaction_tenantimprovementallowancepsf_amount', 'lease_transaction_tenantimprovementallowancepsf_currency', 
                    'lease_asking_rent_max_amt', 'lease_asking_rent_min_amt', 'lease_asking_rent_price_period', 'lease_asking_rent_price_size', 'lease_transaction_rent_price_max_amt', 'lease_transaction_rent_price_min_amt']
        temp2[nan_cols] = np.nan
        test_data['inferred_lease'] = False
        test_data = test_data.append(temp2, ignore_index=True)
        test_data = test_data.reset_index(drop=True)
        
        test_data[test_data['inferred_lease']].to_csv('{}/OutputFiles/{}/logic_logs/inferred_leased_{}m{}.csv'.format(self.home, self.sector, self.curryr, self.currmon), index=False)
        
        test_data = test_data[((test_data['space_category'].isin(self.space_map[self.sector])) | (test_data['space_category'].isnull() == True) | (test_data['space_category'] == '')) & ((test_data['space_category'] != 'office') | (self.sector != 'ind') | (test_data['type2'] == "F"))]

        logging.info('{:,} unique {} spaces in incrementals'.format(len(test_data.drop_duplicates('listed_space_id')), self.sector))
        logging.info('{:,} unique {} properties in incrementals'.format(len(test_data.drop_duplicates('property_source_id')), self.sector))
        logging.info('{:,} unique {} properties have an initial rent observation'.format(len(test_data[(((test_data['lease_asking_rent_max_amt'].isnull() == False) | (test_data['lease_asking_rent_min_amt'].isnull() == False)) & (~test_data['availability_status'].isin(['leased', 'withdrawn']))) | (((test_data['lease_transaction_rent_price_max_amt'].isnull() == False) | (test_data['lease_transaction_rent_price_min_amt'].isnull() == False)) & (test_data['availability_status'].isin(['leased', 'withdrawn'])))].drop_duplicates('property_source_id')), self.sector))
        logging.info("\n")
        
        test_data['listed_space_id'] = np.where((test_data['inferred_lease']), '', test_data['listed_space_id'])
        test_data = test_data[cols]
            
        return test_data
        
    def check_double_sublet(self, test_data):
        
        test_data['count'] = test_data.groupby('property_source_id')['property_source_id'].transform('count')
        test_data['count_sublease'] = test_data[test_data['lease_sublease'] == 1].groupby('property_source_id')['property_source_id'].transform('count')
        test_data['count_sublease'] = test_data.groupby('property_source_id')['count_sublease'].bfill()
        test_data['count_sublease'] = test_data.groupby('property_source_id')['count_sublease'].ffill()
        test_data['total_avail'] = test_data.groupby('property_source_id')['space_size_available'].transform('sum', min_count=1)
        temp = test_data.copy()
        temp = temp[(temp['count_sublease'] > 0) & (temp['count_sublease'] < temp['count']) & (temp['total_avail'] > temp['tot_size'])]
        if len(temp) > 0:
            temp = temp.drop_duplicates('property_source_id')
            temp['flag'] = 'Direct and sublease space available at property, but total avail exceeds size, indicating that the same space is listed for both sublease and direct lease'
            temp['column'] = 'sublet'
            temp['status'] = 'flagged'
            self.logic_log = self.logic_log.append(temp[['flag', 'column', 'property_source_id', 'status']], ignore_index=True)

        test_data['space_size_available'] = np.where((test_data['count_sublease'] > 0) & (test_data['count_sublease'] < test_data['count']) & (test_data['total_avail'] > test_data['tot_size']), np.nan, test_data['space_size_available'])

        test_data['space_ident'] = np.where((test_data['space_size_available_leased'].isnull() == False), test_data['listed_space_title'].astype(str) + ' Floor ' + test_data['space_floor'].astype(str) + ' Suite ' + test_data['space_suite'].astype(str) + ' ' + test_data['space_size_available_leased'].astype(str), test_data['listed_space_title'].astype(str) + ' Floor ' + test_data['space_floor'].astype(str) + ' Suite ' + test_data['space_suite'].astype(str) + ' ' + test_data['space_size_available'].astype(str))
        test_data['space_ident'] = test_data['space_ident'].str.strip()
        test_data['count'] = test_data.groupby(['property_source_id', 'space_ident'])['property_source_id'].transform('count')
        temp = test_data.copy()
        temp = temp[(temp['count'] > 1) & (temp['space_size_available'].isnull() == False) & ((temp['space_floor'] != '') | (temp['space_suite'] != ''))]
        if len(temp) > 0:
            temp = temp.drop_duplicates(['property_source_id', 'space_ident'])
            temp['flag'] = 'Property has space listed more than once, may indicate duplicate listing'
            temp['status'] = 'flagged'
            temp['c_value'] = temp['space_ident']
            self.logic_log = self.logic_log.append(temp[['flag', 'property_source_id', 'status', 'c_value']], ignore_index=True)          

        return test_data
    
    def calc_ranges(self, log, test_data):
        
        logging.info('Calculating range boundaries from log files...')
        
        df = log.copy()
        df['survdate'] = pd.to_datetime(df['survdate'])
        for col in df.columns:
            if col == 'survdate':
                continue
            df[col] = np.where((df[col] == -1) | (df[col] == '-1'), np.nan, df[col])
        
        if self.sector == "ind":
            df['type2_temp'] = np.where(df['type2'] != "F", "DW", df['type2'])
            test_data['type2_temp'] = np.where(test_data['type2'] != "F", "DW", test_data['type2'])
        
        if self.sector == "off":
            test_data['identity'] = test_data['metcode']
            df['identity'] = df['metcode']
            test_data['us'] = 'us'
            df['us'] = 'us'
            backup = 'us'
        elif self.sector == "ind":
            test_data['identity'] = test_data['metcode'] + '/' + test_data['type2_temp']
            df['identity'] = df['metcode'] + '/' + df['type2_temp']
            backup = 'type2_temp'
        elif self.sector == "ret":
            test_data['identity'] = test_data['metcode'] + '/' + test_data['type1']
            df['identity'] = df['metcode'] + '/' + df['type1']
            backup = 'type1'
        
        for key, value in self.type_dict.items():

            if value['range_lev'] == '':
                continue
            
            if value['structural']:
                cutoff = 120
            else:
                cutoff = 24
                
            multiplier = 15
             
            df_use = df.copy()
            # will always use 2022m1 as start of benchmark, as that marks the end of foundation surveys, and using curryr and currmon every period will cause the pool to not reflect a true 2 year window since no more surveys are being entered in Foundation
            # As a result though, this check becomes less and less relevant as time goes on
            if key != 'phase':
                df_use['diff_mon'] = (2022 - df_use['survdate'].dt.year) * 12 + (1 - df_use['survdate'].dt.month)
                df_use = df_use[df_use['diff_mon'] <= cutoff]
            
            df_use['count_obs_' + key] = df_use.groupby('identity')[key].transform('count')
            df_use['count_obs_' + key + '_us'] = df_use.groupby(backup)[key].transform('count')
            test_data = test_data.join(df_use.drop_duplicates('identity').set_index('identity')[['count_obs_' + key]], on='identity')
            test_data = test_data.join(df_use.drop_duplicates(backup).set_index(backup)[['count_obs_' + key + '_us']], on=backup)
            test_data['count_obs_' + key] = test_data['count_obs_' + key].fillna(0)
            test_data['count_obs_' + key + '_us'] = test_data['count_obs_' + key + '_us'].fillna(0)
            
            if value['range_lev'] == 'minmax':
                
                for ident, suffix in zip(['identity', backup], ['', '_us']):
                    df_use[key + '_l_range' + suffix] = df_use.groupby(ident)[key].transform('min')
                    df_use[key + '_h_range' + suffix] = df_use.groupby(ident)[key].transform('max')
                    df_use = df_use.drop_duplicates(ident)
                    test_data = test_data.join(df_use.set_index(ident)[[key + '_l_range' + suffix, key + '_h_range' + suffix]], on=ident)

                test_data[key + '_l_range'] = np.where((test_data['count_obs_' + key] < cutoff * multiplier) & (key != 'phase'), test_data[key + '_l_range_us'], test_data[key + '_l_range'])
                test_data[key + '_h_range'] = np.where((test_data['count_obs_' + key] < cutoff * multiplier) & (key != 'phase'), test_data[key + '_h_range_us'], test_data[key + '_h_range'])
                
            elif value['range_lev'] == 'min':
                
                for ident, suffix in zip(['identity', backup], ['', '_us']):
                    temp = pd.DataFrame(df_use.groupby(ident)[key].quantile(0.01))
                    temp.columns = [key + '_l_range' + suffix]
                    test_data = test_data.join(temp, on=ident)
                
                test_data[key + '_h_range'] = date.today().year
                    
                test_data[key + '_l_range'] = np.where((test_data['count_obs_' + key] < cutoff * multiplier), test_data[key + '_l_range_us'], test_data[key + '_l_range'])
                test_data[key + '_l_range'] = np.where((test_data[key + '_l_range'] > 1950), 1950, test_data[key + '_l_range'])
            
            elif value['range_lev'] == '1p99p' or value['range_lev'] == 'zerook':

                if value['range_lev'] == 'zerook':
                    df_use = df_use[df_use[key] > 0]
                
                for ident, suffix in zip(['identity', backup], ['', '_us']):
                    temp = pd.DataFrame(df_use.groupby(ident)[key].quantile(0.01))
                    temp.columns = [key + '_l_range' + suffix]
                    test_data = test_data.join(temp, on=ident)
                    temp = pd.DataFrame(df_use.groupby(ident)[key].quantile(0.99))
                    temp.columns = [key + '_h_range' + suffix]
                    test_data = test_data.join(temp, on=ident)

                if self.sector == "ret" and key in ['n_avrent', 'a_avrent']:
                    test_data[key + '_l_range'] = np.where((test_data['count_obs_' + key] >= cutoff * multiplier) | ((test_data['count_obs_' + key] >= cutoff * (multiplier - 5)) & (test_data[key + '_l_range'] < test_data[key + '_l_range_us'])), test_data[key + '_l_range'], test_data[key + '_l_range_us'])
                    test_data[key + '_h_range'] = np.where((test_data['count_obs_' + key] >= cutoff * multiplier) | ((test_data['count_obs_' + key] >= cutoff * (multiplier - 5)) & (test_data[key + '_h_range'] < test_data[key + '_h_range_us'])), test_data[key + '_h_range'], test_data[key + '_h_range_us'])
                else:
                    test_data[key + '_l_range'] = np.where((test_data['count_obs_' + key] < cutoff * multiplier), test_data[key + '_l_range_us'], test_data[key + '_l_range'])
                    test_data[key + '_h_range'] = np.where((test_data['count_obs_' + key] < cutoff * multiplier), test_data[key + '_h_range_us'], test_data[key + '_h_range'])
            
            elif value['range_lev'] == 'max':
                for ident, suffix in zip(['identity', backup], ['', '_us']):
                    temp = pd.DataFrame(df_use.groupby(ident)[key].quantile(0.99))
                    temp.columns = [key + '_h_range' + suffix]
                    test_data = test_data.join(temp, on=ident)
                
                test_data[key + '_l_range'] = -1
                    
                test_data[key + '_h_range'] = np.where((test_data['count_obs_' + key] < cutoff * multiplier), test_data[key + '_h_range_us'], test_data[key + '_h_range'])
                
            if key in ['avrent', 'ind_avrent', 'a_avrent', 'n_avrent']:
                test_data[key + '_l_range'] = np.where((test_data['count_obs_' + key + '_us'] < (cutoff * multiplier) / 3), 0.02, test_data[key + '_l_range'])
                test_data[key + '_h_range'] = np.where((test_data['count_obs_' + key + '_us'] < (cutoff * multiplier) / 3), 200, test_data[key + '_h_range'])
            else:
                test_data[key + '_l_range'] = np.where((test_data['count_obs_' + key + '_us'] < (cutoff * multiplier) / 3), -np.inf, test_data[key + '_l_range'])
                test_data[key + '_h_range'] = np.where((test_data['count_obs_' + key + '_us'] < (cutoff * multiplier) / 3), np.inf, test_data[key + '_h_range'])
            test_data = test_data.drop(['count_obs_' + key + '_us'], axis=1)
                
        logging.info('\n')
        
        if self.sector == "ind":
            test_data = test_data.drop(['type2_temp'],axis=1)
        
        ranges = test_data.copy()
        ranges = ranges.drop_duplicates('identity')
        range_cols = [x for x in ranges.columns if x[-7:] == 'l_range' or x[-7:] == 'h_range' or 'count_obs' in x]
        ranges = ranges[['identity'] + range_cols]
        ranges.to_csv('{}/OutputFiles/{}/logic_logs/range_vals.csv'.format(self.home, self.sector), index=False)
        
        return test_data
    
    def gen_mr_perf(self, test_data, log):
        
        if self.sector == "off":
            rent_log = 'avrent'
        elif self.sector == "ind":
            rent_log = 'ind_avrent'
        elif self.sector == "ret":
            rent_log = 'n_avrent'
        
        for col in [rent_log, 'op_exp', 're_tax']:
        
            mr = log.copy()
            mr = mr[(mr[col].isnull() == False) & (mr[col] != '')]
            mr['survdate'] = pd.to_datetime(mr['survdate'])
            mr.sort_values(by=['survdate'], ascending=[False], inplace=True)
            mr = mr.drop_duplicates('realid')
            mr = mr.rename(columns={col: col + "_mr"})
        
            test_data = test_data.join(mr.rename(columns={'realid': 'id_use'}).set_index('id_use')[[col + "_mr"]], on='id_use')
    
        return test_data
    
    def normalize(self, test_data, period):
        
        test_data['avg_rent'] = np.nan
        test_data['avg_rent'] = np.where((test_data['lease_asking_rent_min_amt'].isnull() == False) & (test_data['lease_asking_rent_max_amt'].isnull() == False), (test_data['lease_asking_rent_max_amt'] + test_data['lease_asking_rent_min_amt']) / 2, test_data['avg_rent']) 
        test_data['avg_rent'] = np.where((test_data['lease_asking_rent_min_amt'].isnull() == False) & (test_data['lease_asking_rent_max_amt'].isnull() == True), test_data['lease_asking_rent_min_amt'], test_data['avg_rent']) 
        test_data['avg_rent'] = np.where((test_data['lease_asking_rent_min_amt'].isnull() == True) & (test_data['lease_asking_rent_max_amt'].isnull() == False), test_data['lease_asking_rent_max_amt'], test_data['avg_rent']) 
        
        test_data['t_avg_rent'] = np.nan
        test_data['t_avg_rent'] = np.where((test_data['lease_transaction_rent_price_min_amt'].isnull() == False) & (test_data['lease_transaction_rent_price_max_amt'].isnull() == False), (test_data['lease_transaction_rent_price_max_amt'] + test_data['lease_transaction_rent_price_min_amt']) / 2, test_data['t_avg_rent']) 
        test_data['t_avg_rent'] = np.where((test_data['lease_transaction_rent_price_min_amt'].isnull() == False) & (test_data['lease_transaction_rent_price_max_amt'].isnull() == True), test_data['lease_transaction_rent_price_min_amt'], test_data['t_avg_rent']) 
        test_data['t_avg_rent'] = np.where((test_data['lease_transaction_rent_price_min_amt'].isnull() == True) & (test_data['lease_transaction_rent_price_max_amt'].isnull() == False), test_data['lease_transaction_rent_price_max_amt'], test_data['t_avg_rent']) 
        
        if period:
            test_data['space_size_available_leased'] = test_data['space_size_available']

        test_data['avrent_norm'] = np.where((test_data['lease_asking_rent_price_size'] == 'total') & (test_data['space_size_available_leased'] > 0) & (test_data['space_size_available_leased'].isnull() == False), test_data['avg_rent'] / test_data['space_size_available_leased'], test_data['avg_rent'])
        test_data['avrent_norm'] = np.where((test_data['lease_asking_rent_price_period'] == 'monthly'), test_data['avrent_norm'] * 12, test_data['avrent_norm'])
        test_data['avrent_norm'] = np.where((test_data['space_size_available_leased'] == 0) & (test_data['lease_asking_rent_price_size'] == 'total'), np.nan, test_data['avrent_norm'])    

        if period:
            test_data['lease_or_property_expenses_amount'] = test_data['occupancy_expenses_amount_amount']
        test_data['use_prop_level'] = np.where((test_data['occupancy_expenses_amount_amount'] == test_data['lease_or_property_expenses_amount']) | ((test_data['lease_or_property_expenses_amount'] <= 0.2) & (test_data['occupancy_expenses_amount_amount'].isnull() == False) & (test_data['state'] != 'CA')), True, False)
        test_data['opex_norm'] = np.where((test_data['use_prop_level']) & (test_data['occupancy_expenses_size'] == 'total') & (test_data['space_size_available_leased'] > 0) & (test_data['space_size_available_leased'].isnull() == False), test_data['lease_or_property_expenses_amount'] / test_data['space_size_available_leased'], test_data['occupancy_expenses_amount_amount'])
        test_data['opex_norm'] = np.where((test_data['use_prop_level']) & (test_data['occupancy_expenses_period'] == 'monthly'), test_data['opex_norm'] * 12, test_data['opex_norm'])
        test_data['opex_norm'] = np.where((test_data['use_prop_level']) & (test_data['space_size_available_leased'] == 0) & (test_data['occupancy_expenses_size'] == 'total'), np.nan, test_data['opex_norm'])    
        test_data['lease_or_property_expenses_amount'] = np.where((test_data['state'] == 'CA') & (test_data['use_prop_level'] == False) & ((test_data['lease_or_property_expenses_amount'] < 1) | ((test_data['lease_or_property_expenses_amount'] * 12) / test_data['avrent_norm'] < 0.5)) & ((test_data['lease_asking_rent_price_period'] == 'monthly') | (test_data['occupancy_expenses_period'] == 'monthly') | ((test_data['lease_or_property_expenses_amount'] < 0.35) & ((test_data['lease_or_property_expenses_amount'] * 12) / test_data['avrent_norm'] < 0.5)) | ((test_data['lease_or_property_expenses_amount'] < 0.2) & (test_data['occupancy_expenses_period'] != 'annual') & (test_data['avrent_norm'].isnull() == True))), test_data['lease_or_property_expenses_amount'] * 12, test_data['lease_or_property_expenses_amount'])
        test_data['opex_norm'] = np.where((test_data['use_prop_level'] == False), test_data['lease_or_property_expenses_amount'], test_data['opex_norm'])

        test_data['retax_norm'] = np.where((test_data['tot_size'] > 0) & (test_data['property_real_estate_tax_year'] == self.curryr) & (test_data['tot_size'] > 0) & (test_data['tot_size'].isnull() == False), test_data['property_real_estate_tax_amt'] / test_data['tot_size'], np.nan)
        

        test_data['crd_norm'] = np.where((test_data['lease_transaction_rent_price_size'] == 'total') & (test_data['space_size_available_leased'] > 0) & (test_data['space_size_available_leased'].isnull() == False), test_data['t_avg_rent'] / test_data['space_size_available_leased'], test_data['t_avg_rent'])
        test_data['crd_norm'] = np.where((test_data['lease_transaction_rent_price_period'] == 'monthly'), test_data['crd_norm'] * 12, test_data['crd_norm'])
        test_data['crd_norm'] = np.where((test_data['space_size_available_leased'] == 0) & (test_data['lease_transaction_rent_price_size'] == 'total'), np.nan, test_data['crd_norm'])    
        
        
        if self.sector == "ret":
            test_data['cam_norm'] = np.where((test_data['occupancy_cam_size'] == 'total') & (test_data['space_size_available_leased'] > 0) & (test_data['space_size_available_leased'].isnull() == False), test_data['occupancy_cam_amount_amount'] / test_data['space_size_available_leased'], test_data['occupancy_cam_amount_amount'])
            test_data['cam_norm'] = np.where((test_data['occupancy_cam_period'] == 'monthly'), test_data['cam_norm'] * 12, test_data['cam_norm'])
            test_data['cam_norm'] = np.where((test_data['space_size_available_leased'] == 0) & (test_data['occupancy_cam_size'] == 'total'), np.nan, test_data['cam_norm'])    
        
        return test_data
    
    
    def eval_period_over_period(self, test_data):
        
        period_report = pd.DataFrame()
        
        if self.currmon == 1:
            p_curryr = self.curryr - 1
            p_currmon = 12
        else:
            p_curryr = self.curryr
            p_currmon = self.currmon - 1
            
        p_data = pd.read_csv('{}/InputFiles/listings_{}m{}.csv'.format(self.home, p_curryr, p_currmon), na_values= "", keep_default_na = False)
        p_data['property_source_id'] = np.where(p_data['property_source_id'].str.contains('oid') == True, p_data['property_source_id'].str.replace('"', '').str.replace('$', '').str.replace('{oid:', '').str.replace('}', ''), p_data['property_source_id'])
        p_data['listed_space_id'] = np.where(p_data['listed_space_id'].str.contains('oid') == True, p_data['listed_space_id'].str.replace('"', '').str.replace('$', '').str.replace('{oid:', '').str.replace('}', ''), p_data['listed_space_id'])
        p_data['in_p'] = 1
        p_data['category'] = p_data['category'].str.lower()
        p_data['subcategory'] = p_data['subcategory'].str.lower()
        p_data['space_category'] = p_data['space_category'].str.lower()
        p_data['availability_status'] = p_data['availability_status'].str.lower()
        p_data['occupancy_type'] = p_data['occupancy_type'].str.lower()
        
        p_data = p_data.rename(columns={'business_park_size_sf': 'business_park_retail_size_sf'})
        
        data = test_data.copy()
        data['in_c'] = 1
        
        p_data = p_data.join(data.drop_duplicates('listed_space_id').set_index('listed_space_id')[['in_c']], on='listed_space_id')
        temp = p_data.copy()
        temp = temp[temp['in_c'].isnull() == True]
        if len(temp) > 0:
            temp = temp.drop_duplicates('listed_space_id')
            temp['flag'] = "No longer in listings in currmon"
            period_report = period_report.append(temp[['property_source_id', 'listed_space_id', 'flag', 'availability_status']])
        
        data = data.join(p_data.drop_duplicates('listed_space_id').set_index('listed_space_id')[['in_p']], on='listed_space_id')
        temp = data.copy()
        temp = temp[temp['in_p'].isnull() == True]
        if len(temp) > 0:
            temp = temp.drop_duplicates('listed_space_id')
            temp['flag'] = "New listing in currmon"
            period_report = period_report.append(temp[['property_source_id', 'listed_space_id', 'flag', 'availability_status']])
        
        p_data = self.normalize(p_data, True)
        
        test_cols = ['space_size_available', 'avrent_norm', 'opex_norm', 'retax_norm', 'crd_norm', 'cam_norm', 
                     'commission_amount_percentage', 'lease_transaction_freerentmonths', 'lease_transaction_tenantimprovementallowancepsf_amount',
                     'lease_transaction_leasetermmonths', 'rent_basis', 'type1', 'tot_size', 'first_year', 'buildings_construction_expected_completion_month',
                     'category', 'subcategory', 'space_category', 'occupancy_owneroccupied', 'occupancy_type', 'foundation_ids_list', 
                     'lease_sublease', 'business_park_retail_size_sf', 'retail_property_is_anchor_flag', 'anchor_within_business_park_size_sf', 
                     'nonanchor_within_business_park_size_sf']
        if self.currmon == 1:
            test_cols.remove('retax_norm')
        if self.sector != 'ret':
            test_cols.remove('business_park_retail_size_sf')
            test_cols.remove('retail_property_is_anchor_flag')
            test_cols.remove('anchor_within_business_park_size_sf')
            test_cols.remove('nonanchor_within_business_park_size_sf')
            test_cols.remove('type1')
            test_cols.remove('cam_norm')
        if 'business_park_retail_size_sf' not in p_data.columns:
            test_cols.remove('business_park_retail_size_sf')
            
        for col in test_cols:
            p_data.rename(columns={col: 'p_' + col}, inplace=True)
        data = data.join(p_data.set_index('listed_space_id')[['p_' + x for x in test_cols]], on='listed_space_id')
        
        temp = data.copy()
        temp = temp[temp['in_p'].isnull() == False]
        if len(temp) > 0:
            temp = temp.drop_duplicates('listed_space_id')

            for col in test_cols:
                temp[col] = np.where((temp[col] == ''), np.nan, temp[col])
                temp['p_' + col] = np.where((temp['p_' + col] == ''), np.nan, temp['p_' + col])
                temp['diff_' + col] = np.where((temp[col] != temp['p_' + col]) & ((temp[col].isnull() == False) | (temp['p_' + col].isnull() == False)), 1, 0)
                temp1 = temp.copy()
                temp1 = temp1[temp1['diff_' + col] == 1]
                if len(temp1) > 0:
                    temp1['flag'] = "New value for {}".format(col)
                    temp1['c_val'] = temp1[col]
                    temp1['p_val'] = temp1['p_' + col]
                    period_report = period_report.append(temp1[['property_source_id', 'listed_space_id', 'flag', 'c_val', 'p_val']])


        p_data = p_data.rename(columns={'availability_status': 'p_availability_status'})
        data = data.join(p_data.set_index('listed_space_id')[['p_availability_status']], on='listed_space_id')
        temp = data.copy()
        temp = temp[temp['p_availability_status'].isnull() == False]
        temp = temp[temp['availability_status'] != temp['p_availability_status']]
        if len(temp) > 0:
            temp['flag'] = 'Availability Status Change'
            temp['c_val'] = temp['availability_status']
            temp['p_val'] = temp['p_availability_status']
            period_report = period_report.append(temp[['property_source_id', 'listed_space_id', 'flag', 'c_val', 'p_val']])
        
        p_data = p_data.join(data.set_index('listed_space_id').rename(columns={'availability_status': 'c_availability_status'})[['c_availability_status']], on='listed_space_id')
        temp = p_data.copy()
        temp = temp[temp['c_availability_status'].isnull() == False]
        temp = temp[(temp['p_availability_status'] == 'leased') & (temp['c_availability_status'] == 'leased')]
        if len(temp) > 0:
            temp['flag'] = 'Removal of Leased Status'
            temp['c_val'] = temp['c_availability_status']
            temp['p_val'] = temp['p_availability_status']
            period_report = period_report.append(temp[['property_source_id', 'listed_space_id', 'flag', 'c_val', 'p_val']])

        p_snap = pd.read_csv('{}/OutputFiles/{}/snapshots/{}m{}_snapshot_{}.csv'.format(self.home, self.sector, p_curryr, p_currmon, self.sector), na_values= "", keep_default_na = False)
        p_snap['realid'] = p_snap['realid'].astype(str)
        p_snap['property_source_id'] = p_snap['property_source_id'].astype(str)
        data = data.join(p_snap.rename(columns={'realid': 'id_use', 'property_source_id': 'p_property_source_id'}).drop_duplicates('id_use').set_index('id_use')[['p_property_source_id']], on='id_use')
        temp = data.copy()
        temp = temp[(temp['property_source_id'] != temp['p_property_source_id']) & (temp['p_property_source_id'].isnull() == False)]
        if len(temp):
            temp = temp.drop_duplicates('property_source_id')
            logging.info("There are {:,} properties that are now linked to new property source ids".format(len(temp)))
            logging.info("\n")
            temp['flag'] = 'Linked to new Catylist property'
            temp['c_val'] = temp['property_source_id']
            temp['p_val'] = temp['p_property_source_id']
            period_report = period_report.append(temp[['property_source_id', 'flag', 'c_val', 'p_val']])

        period_report.to_csv('{}/OutputFiles/{}/logic_logs/period_over_period_changes_{}m{}.csv'.format(self.home, self.sector, self.curryr, self.currmon), index=False)
    
    def calc_prop_level(self, test_data):
        
        transaction_cols = ['lease_transaction_tenantimprovementallowancepsf_amount', 'lease_transaction_freerentmonths', 'lease_transaction_leasetermmonths', 'crd_norm']

        for col in transaction_cols:
            test_data[col] = np.where((~test_data['availability_status'].isin(['leased', 'withdrawn'])), np.nan, test_data[col])
        
        test_data['avrent_norm'] = np.where((test_data['crd_norm'].isnull() == False), np.nan, test_data['avrent_norm'])
           
        if self.sector == "ret":
            prefixes = ['a_', 'n_']
        else:
            prefixes = ['']
        
        # Before 2022m5, had been re-computing anchor status at all types of props at the space level. Cant do that, as might end up with a certain cut's avail/rent at props without size for that cut
        # But can still do this for props that have a mix of anchor and non anchor size due to taking the reis breakout
        if self.sector == "ret":
            test_data['has_mix'] = np.where((test_data['n_size'] > 0) & (test_data['a_size'] > 0), True, False)
            test_data['retail_anchor_space_level'] = ''
            test_data['retail_anchor_space_level'] = np.where((test_data['has_mix'] == False), test_data['retail_property_is_anchor_flag'], test_data['retail_anchor_space_level'])
            test_data['retail_anchor_space_level'] = np.where((test_data['has_mix']) & (test_data['space_size_available'] < 9300) & (test_data['space_size_available'] > 0), 'N', test_data['retail_anchor_space_level'])
            test_data['retail_anchor_space_level'] = np.where((test_data['has_mix']) & (test_data['space_size_available'] >= 9300), 'Y', test_data['retail_anchor_space_level'])
            test_data['retail_anchor_space_level'] = np.where((test_data['has_mix']) & (test_data['space_size_available_leased'] < 9300) & (test_data['space_size_available_leased'].isnull() == False), 'N', test_data['retail_anchor_space_level'])
            test_data['retail_anchor_space_level'] = np.where((test_data['has_mix']) & (test_data['space_size_available_leased'] >= 9300) & (test_data['space_size_available_leased'].isnull() == False), 'Y', test_data['retail_anchor_space_level'])


        for prefix in prefixes:

            temp = test_data.copy()
            
            if self.sector == "ind":
                temp = temp[(temp['type2'] == "F") | (temp['space_category'] != 'office')]
            
            if self.sector == "ret" and prefix == 'a_':
                temp = temp[temp['retail_anchor_space_level'] == 'Y']
            elif self.sector == "ret" and prefix == 'n_':
                temp = temp[temp['retail_anchor_space_level'] == 'N']
            
            temp1 = temp.copy()
            temp1 = temp1[(temp1['space_size_available'] <= 1) & (temp1['space_size_available'] > 0)]
            if len(temp1) > 0:
                if self.sector == "off":
                    avail = 'avail'
                elif self.sector == "ind":
                    avail = 'ind_avail'
                elif self.sector == "ret" and prefix == "n_":
                    avail = 'n_avail'
                elif self.sector == "ret" and prefix == "a_":
                    avail = 'a_avail'
                logging.info("{} has values outside the historical surveyed range boundaries".format(avail))
                temp1['flag'] = 'Range Outlier - Space Level'
                temp1['value'] = temp1['space_size_available']
                temp1['column'] = avail
                temp1['status'] = 'dropped'
                self.logic_log = self.logic_log.append(temp1.rename(columns={'id_use': 'realid'})[['realid', 'listed_space_id', 'flag', 'value', 'column', 'property_source_id', 'status']], ignore_index=True)
            
            temp = temp[(temp['lease_sublease'] == 0) | (temp['lease_sublease'].isnull() == True)]
            temp['space_size_available'] = np.where((temp['space_size_available'] <= 1) & (temp['space_size_available'] > 0), np.nan, temp['space_size_available'])
            temp[prefix + 'prop_dir_avail'] = temp.groupby('id_use')['space_size_available'].transform('sum', min_count=1)
            temp = temp.drop_duplicates('id_use')
            test_data = test_data.join(temp.set_index('id_use')[[prefix + 'prop_dir_avail']], on='id_use')
            
            if self.sector == "off" or self.sector == "ind":
                temp = test_data.copy()
                temp = temp[(temp['lease_sublease'] == 1)]
                temp['space_size_available'] = np.where((temp['space_size_available'] <= 1) & (temp['space_size_available'] > 0), np.nan, temp['space_size_available'])
                temp['prop_sub_avail'] = temp.groupby('id_use')['space_size_available'].transform('sum', min_count=1)
                temp = temp.drop_duplicates('id_use')
                test_data = test_data.join(temp.set_index('id_use')[['prop_sub_avail']], on='id_use')
                
                test_data[prefix + 'prop_sub_avail'] = np.where((test_data[prefix + 'prop_sub_avail'].isnull() == True) & (test_data[prefix + 'prop_dir_avail'].isnull() == False), 0, test_data[prefix + 'prop_sub_avail'])
                test_data[prefix + 'prop_dir_avail'] = np.where((test_data[prefix + 'prop_dir_avail'].isnull() == True) & (test_data[prefix + 'prop_sub_avail'].isnull() == False), 0, test_data[prefix + 'prop_dir_avail'])
            
            if self.sector == "ret":
                test_data[prefix + 'prop_dir_avail'] = np.where((test_data[prefix + 'prop_dir_avail'].isnull() == True), 0, test_data[prefix + 'prop_dir_avail'])
            
            # Drop owner occupied listings here - want to include them in the avail rollup, but not for the other performance fields
            test_data = test_data[(test_data['availability_owneroccupied'] == 0) | (test_data['availability_owneroccupied'].isnull() == True) | (test_data['occupancy_owneroccupied'] == 0) | (test_data['occupancy_owneroccupied'].isnull() == True)]
            
            temp = test_data.copy()
            
            # Because there might be scenarios where the space level anchor designation method does not jive with the property level method used to determine size, select spaces that might not be anchor on space level but that dont have non anchor size and vice versa
            if self.sector == "ret" and prefix == 'a_':
                temp = temp[((temp['retail_anchor_space_level'] == 'Y') & ((temp['a_size'] > 0) | (temp['a_size'].isnull() == True))) | ((temp['n_size'] == 0) & ((temp['a_size'] > 0) | (temp['a_size'].isnull() == True)))]
            elif self.sector == "ret" and prefix == 'n_':
                temp = temp[((temp['retail_anchor_space_level'] == 'N') & ((temp['n_size'] > 0) | (temp['n_size'].isnull() == True))) | ((temp['a_size'] == 0) & ((temp['n_size'] > 0) | (temp['n_size'].isnull() == True)))]

            # Drop sublease listings when calculating rent, but theoretically all other performance data points are fine to include in the prop rollup
            temp1 = temp.copy()
            temp1 = temp1[(temp1['lease_sublease'] == 0) | (temp1['lease_sublease'].isnull() == True)]

            if self.sector == "off":
                rent_log = 'avrent'
            elif self.sector == "ind":
                rent_log = 'ind_avrent'
            elif self.sector == "ret":
                rent_log = prefix + 'avrent'

            if prefix != 'a_':
                if self.sector == "off":
                    temp1['mr_diff'] = (temp1['avrent_norm'] - temp1['avrent_mr']) / temp1['avrent_mr']
                elif self.sector == "ind":
                    temp1['mr_diff'] = (temp1['avrent_norm'] - temp1['ind_avrent_mr']) / temp1['ind_avrent_mr']
                elif self.sector == "ret":
                    temp1['mr_diff'] = (temp1['avrent_norm'] - temp1['n_avrent_mr']) / temp1['n_avrent_mr']
            else:
                temp1['mr_diff'] = np.inf
                
            temp2 = temp1.copy()
            temp2 = temp2[((temp2['avrent_norm'] < temp2[rent_log + '_l_range']) | (temp2['avrent_norm'] > temp2[rent_log + '_h_range'])) & ((abs(temp2['mr_diff']) > 0.02) | (temp2['mr_diff'].isnull() == True))]
            if len(temp2) > 0:
                logging.info("{} has values outside the historical surveyed range boundaries".format(rent_log))
                temp2['flag'] = 'Range Outlier - Space Level'
                temp2['value'] = temp2['avrent_norm']
                temp2['column'] = rent_log
                temp2['status'] = 'dropped'
                self.logic_log = self.logic_log.append(temp2.rename(columns={'id_use': 'realid'})[['realid', 'listed_space_id', 'flag', 'value', 'column', 'property_source_id', 'status']], ignore_index=True)
            temp1['avrent_norm'] = np.where(((temp1['avrent_norm'] < temp1[rent_log + '_l_range']) | (temp1['avrent_norm'] > temp1[rent_log + '_h_range'])) & ((abs(temp1['mr_diff']) > 0.02) | (temp1['mr_diff'].isnull() == True)), np.nan, temp1['avrent_norm'])
            temp1[prefix + 'prop_avrent'] = temp1.groupby('id_use')['avrent_norm'].transform('mean')
            temp1 = temp1.drop_duplicates('id_use')
            test_data = test_data.join(temp1.set_index('id_use')[[prefix + 'prop_avrent']], on='id_use')
            test_data[prefix + 'prop_avrent'] = round(test_data[prefix + 'prop_avrent'], 2)
            
            temp1 = temp.copy()
            temp1 = temp1[(temp1['commission_amount_percentage'] < temp1['comm1' + '_l_range']) | (temp1['commission_amount_percentage'] > temp1['comm1' + '_h_range'])]   
            if len(temp1) > 0:
                logging.info("comm1 has values outside the historical surveyed range boundaries")
                temp1['flag'] = 'Range Outlier - Space Level'
                temp1['value'] = temp1['commission_amount_percentage']
                temp1['column'] = 'comm1'
                temp1['status'] = 'dropped'
                self.logic_log = self.logic_log.append(temp1.rename(columns={'id_use': 'realid'})[['realid', 'listed_space_id', 'flag', 'value', 'column', 'property_source_id', 'status']], ignore_index=True)
            
            temp['commission_amount_percentage'] = np.where((temp['commission_amount_percentage'] < temp['comm1' + '_l_range']) | (temp['commission_amount_percentage'] > temp['comm1' + '_h_range']), np.nan, temp['commission_amount_percentage'])
            temp[prefix + 'prop_comm'] = temp.groupby('id_use')['commission_amount_percentage'].transform('mean')
            temp = temp.drop_duplicates('id_use')
            test_data = test_data.join(temp.set_index('id_use')[[prefix + 'prop_comm']], on='id_use')
                
            if self.sector == "off" or self.sector == "ind" or (self.sector == "ret" and prefix == 'n_'):
                free = 'free_re'
            elif self.sector == "ret" and prefix == 'a_':
                free = 'a_freere'
            
            temp1 = temp.copy()
            temp1 = temp1[(temp1['lease_transaction_freerentmonths'] < temp1[free + '_l_range']) | (temp1['lease_transaction_freerentmonths'] > temp1[free + '_h_range'])]
            if len(temp1) > 0:    
                logging.info("freerent has values outside the historical surveyed range boundaries")
                temp1['flag'] = 'Range Outlier - Space Level'
                temp1['value'] = temp1['lease_transaction_freerentmonths']
                temp1['column'] = free
                temp1['status'] = 'dropped'
                self.logic_log = self.logic_log.append(temp1.rename(columns={'id_use': 'realid'})[['realid', 'listed_space_id', 'flag', 'value', 'column', 'property_source_id', 'status']], ignore_index=True)
            
            temp['lease_transaction_freerentmonths'] = np.where((temp['lease_transaction_freerentmonths'] < temp[free + '_l_range']) | (temp['lease_transaction_freerentmonths'] > temp[free + '_h_range']), np.nan, temp['lease_transaction_freerentmonths'])
            temp[prefix + 'prop_free'] = temp.groupby('id_use')['lease_transaction_freerentmonths'].transform('mean')
            temp = temp.drop_duplicates('id_use')
            test_data = test_data.join(temp.set_index('id_use')[[prefix + 'prop_free']], on='id_use')
            test_data[prefix + 'prop_free'] = round(test_data[prefix + 'prop_free'], 2)
                
            if self.sector == "off" or self.sector == "ind" or (self.sector == "ret" and prefix == 'n_'):
                ti = 'ti2'
            elif self.sector == "ret" and prefix == 'a_':
                ti = 'a_ti'
            
            temp1 = temp.copy()
            temp1 = temp1[(temp1['lease_transaction_tenantimprovementallowancepsf_amount'] < temp1[ti + '_l_range']) | (temp1['lease_transaction_tenantimprovementallowancepsf_amount'] > temp1[ti + '_h_range'])]
            if len(temp1) > 0:    
                logging.info("ti has values outside the historical surveyed range boundaries")
                temp1['flag'] = 'Range Outlier - Space Level'
                temp1['value'] = temp1['lease_transaction_tenantimprovementallowancepsf_amount']
                temp1['column'] = ti
                temp1['status'] = 'dropped'
                self.logic_log = self.logic_log.append(temp1.rename(columns={'id_use': 'realid'})[['realid', 'listed_space_id', 'flag', 'value', 'column', 'property_source_id', 'status']], ignore_index=True)
            
            temp['lease_transaction_tenantimprovementallowancepsf_amount'] = np.where((temp['lease_transaction_tenantimprovementallowancepsf_amount'] < temp[ti + '_l_range']) | (temp['lease_transaction_tenantimprovementallowancepsf_amount'] > temp[ti + '_h_range']), np.nan, temp['lease_transaction_tenantimprovementallowancepsf_amount'])
            temp[prefix + 'prop_ti'] = temp.groupby('id_use')['lease_transaction_tenantimprovementallowancepsf_amount'].transform('mean')
            temp = temp.drop_duplicates('id_use')
            test_data = test_data.join(temp.set_index('id_use')[[prefix + 'prop_ti']], on='id_use') 
            test_data[prefix + 'prop_ti'] = round(test_data[prefix + 'prop_ti'], 2)
            
            if self.sector == "off" or self.sector == "ind":
                lse = 'lse_term'
            elif self.sector == "ret" and prefix == 'n_':
                lse = 'non_term'
            elif self.sector == "ret" and prefix == 'a_':
                lse = 'anc_term'
            
            temp1 = temp.copy()
            temp1 = temp1[(temp1['lease_transaction_leasetermmonths'] / 12 < temp1[lse + '_l_range']) | (temp1['lease_transaction_leasetermmonths'] / 12 > temp1[lse + '_h_range'])]
            if len(temp1) > 0:    
                logging.info("lease term has values outside the historical surveyed range boundaries")
                temp1['flag'] = 'Range Outlier - Space Level'
                temp1['value'] = temp1['lease_transaction_leasetermmonths']
                temp1['column'] = lse
                temp1['status'] = 'dropped'
                self.logic_log = self.logic_log.append(temp1.rename(columns={'id_use': 'realid'})[['realid', 'listed_space_id', 'flag', 'value', 'column', 'property_source_id', 'status']], ignore_index=True)
            
            temp['lease_transaction_leasetermmonths'] = np.where((temp['lease_transaction_leasetermmonths'] / 12 < temp[lse + '_l_range']) | (temp['lease_transaction_leasetermmonths'] / 12 > temp[lse + '_h_range']), np.nan, temp['lease_transaction_leasetermmonths'])
            temp[prefix + 'prop_term'] = temp.groupby('id_use')['lease_transaction_leasetermmonths'].transform('mean')
            temp = temp.drop_duplicates('id_use')
            test_data = test_data.join(temp.set_index('id_use')[[prefix + 'prop_term']], on='id_use') 
            test_data[prefix + 'prop_term'] = round(test_data[prefix + 'prop_term'] / 12, 2)
            
            if self.sector == "off" or self.sector == "ind":
                crd = 'c_rent'
            elif self.sector == "ret" and prefix == 'n_':
                crd = 'nc_rent'
            elif self.sector == "ret" and prefix == 'a_':
                crd = 'ac_rent'
            
            temp1 = temp.copy()
            temp1 = temp1[(temp1['crd_norm'] < temp1[crd + '_l_range']) | (temp1['crd_norm'] > temp1[crd + '_h_range'])]
            if len(temp1) > 0:    
                logging.info("contract rent has values outside the historical surveyed range boundaries")
                temp1['flag'] = 'Range Outlier - Space Level'
                temp1['value'] = temp1['crd_norm']
                temp1['column'] = crd
                temp1['status'] = 'dropped'
                self.logic_log = self.logic_log.append(temp1.rename(columns={'id_use': 'realid'})[['realid', 'listed_space_id', 'flag', 'value', 'column', 'property_source_id', 'status']], ignore_index=True)

            temp['crd_norm'] = np.where((temp['crd_norm'] < temp[crd + '_l_range']) | (temp['crd_norm'] > temp[crd + '_h_range']), np.nan, temp['crd_norm'])
            temp[prefix + 'prop_crd'] = temp.groupby('id_use')['crd_norm'].transform('mean')
            temp = temp.drop_duplicates('id_use')
            test_data = test_data.join(temp.set_index('id_use')[[prefix + 'prop_crd']], on='id_use')
            
        if self.sector == "ret":
            thresh = 1000
            test_data['n_avail_unused'] = np.where((test_data['n_size'] > test_data['n_prop_dir_avail']), test_data['n_size'] - test_data['n_prop_dir_avail'], 0)
            test_data['a_avail_unused'] = np.where((test_data['a_size'] > test_data['a_prop_dir_avail']), test_data['a_size'] - test_data['a_prop_dir_avail'], 0)
            test_data['n_avail_over'] = np.where((test_data['n_size'] + thresh < test_data['n_prop_dir_avail']), test_data['n_prop_dir_avail'] - test_data['n_size'], 0)
            test_data['a_avail_over'] = np.where((test_data['a_size'] + thresh < test_data['a_prop_dir_avail']), test_data['a_prop_dir_avail'] - test_data['a_size'], 0)

            test_data['n_prop_dir_avail'] = np.where((test_data['n_size'] + thresh >= test_data['n_prop_dir_avail']) & (test_data['n_size'] < test_data['n_prop_dir_avail']), test_data['n_size'], test_data['n_prop_dir_avail'])
            test_data['a_prop_dir_avail'] = np.where((test_data['a_size'] + thresh >= test_data['a_prop_dir_avail']) & (test_data['a_size'] < test_data['a_prop_dir_avail']), test_data['a_size'], test_data['a_prop_dir_avail'])

            test_data['a_prop_dir_avail'] = np.where((test_data['has_mix']) & (test_data['n_avail_over'] > 0), test_data['a_prop_dir_avail'] + test_data[['n_avail_over', 'a_avail_unused']].min(1), test_data['a_prop_dir_avail'])
            test_data['n_prop_dir_avail'] = np.where((test_data['has_mix']) & (test_data['n_avail_over'] > 0), test_data['n_prop_dir_avail'] - test_data[['n_avail_over', 'a_avail_unused']].min(1), test_data['n_prop_dir_avail'])
            test_data['n_prop_dir_avail'] = np.where((test_data['has_mix']) & (test_data['a_avail_over'] > 0), test_data['n_prop_dir_avail'] + test_data[['a_avail_over', 'n_avail_unused']].min(1), test_data['n_prop_dir_avail'])
            test_data['a_prop_dir_avail'] = np.where((test_data['has_mix']) & (test_data['a_avail_over'] > 0), test_data['a_prop_dir_avail'] - test_data[['a_avail_over', 'n_avail_unused']].min(1), test_data['a_prop_dir_avail'])
        
        temp = test_data.copy()
        if self.sector == "ret":
            # Opex, Tax, and Cam are collected at the property level in RDMA, so filter by property level anchor status
            temp = temp[temp['retail_property_is_anchor_flag'] == 'N']

        if prefix != 'a_':
            temp['mr_diff'] = (temp['opex_norm'] - temp['op_exp_mr']) / temp['op_exp_mr']
        else:
            temp['mr_diff'] = np.inf
        
        temp1 = temp.copy()
        temp1 = temp1[((temp1['opex_norm'] < temp1['op_exp' + '_l_range']) | (temp1['opex_norm'] > temp1['op_exp' + '_h_range'])) & ((abs(temp1['mr_diff']) > 0.02) | (temp1['mr_diff'].isnull() == True))]
        if len(temp1) > 0:  
            logging.info("opex has values outside the historical surveyed range boundaries")
            temp1['flag'] = 'Range Outlier - Space Level'
            temp1['value'] = temp1['opex_norm']
            temp1['column'] = 'op_exp'
            temp1['status'] = 'dropped'
            self.logic_log = self.logic_log.append(temp1.rename(columns={'id_use': 'realid'})[['realid', 'listed_space_id', 'flag', 'value', 'column', 'property_source_id', 'status']], ignore_index=True)

        temp['opex_norm'] = np.where(((temp['opex_norm'] < temp['op_exp' + '_l_range']) | (temp['opex_norm'] > temp['op_exp' + '_h_range'])) & ((abs(temp['mr_diff']) > 0.02) | (temp['mr_diff'].isnull() == True)), np.nan, temp['opex_norm'])
        temp['prop_opex'] = temp.groupby('id_use')['opex_norm'].transform('mean')
        temp = temp.drop_duplicates('id_use')
        test_data = test_data.join(temp.set_index('id_use')[['prop_opex']], on='id_use')
        test_data['prop_opex'] = round(test_data['prop_opex'], 2)
        
        if prefix != 'a_':
            temp['mr_diff'] = (temp['retax_norm'] - temp['re_tax_mr']) / temp['re_tax_mr']
        else:
            temp['mr_diff'] = np.inf
        
        temp1 = temp.copy()
        temp1 = temp1[((temp1['retax_norm'] < temp1['re_tax' + '_l_range']) | (temp1['retax_norm'] > temp1['re_tax' + '_h_range'])) & ((abs(temp1['mr_diff']) > 0.02) | (temp1['mr_diff'].isnull() == True))]
        if len(temp1) > 0:
            logging.info("retax has values outside the historical surveyed range boundaries")
            temp1['flag'] = 'Range Outlier - Space Level'
            temp1['value'] = temp1['retax_norm']
            temp1['column'] = 're_tax'
            temp1['status'] = 'dropped'
            self.logic_log = self.logic_log.append(temp1.rename(columns={'id_use': 'realid'})[['realid', 'listed_space_id', 'flag', 'value', 'column', 'property_source_id', 'status']], ignore_index=True)
        
        temp['retax_norm'] = np.where(((temp['retax_norm'] < temp['re_tax' + '_l_range']) | (temp['retax_norm'] > temp['re_tax' + '_h_range'])) & ((abs(temp['mr_diff']) > 0.02) | (temp['mr_diff'].isnull() == True)), np.nan, temp['retax_norm'])
        temp['prop_retax'] = temp.groupby('id_use')['retax_norm'].transform('mean')
        temp = temp.drop_duplicates('id_use')
        test_data = test_data.join(temp.set_index('id_use')[['prop_retax']], on='id_use')
        test_data['prop_retax'] = round(test_data['prop_retax'], 2)


        if self.sector == "ret" and prefix == "n_":
            temp1 = temp.copy()
            temp1 = temp1[(temp1['cam_norm'] < temp1['cam' + '_l_range']) | (temp1['cam_norm'] > temp1['cam' + '_h_range'])]
            if len(temp1) > 0:
                logging.info("cam has values outside the historical surveyed range boundaries")
                temp1['flag'] = 'Range Outlier - Space Level'
                temp1['value'] = temp1['cam_norm']
                temp1['column'] = 'cam'
                temp1['status'] = 'dropped'
                self.logic_log = self.logic_log.append(temp1.rename(columns={'id_use': 'realid'})[['realid', 'listed_space_id', 'flag', 'value', 'column', 'property_source_id', 'status']], ignore_index=True)
            
            temp['cam_norm'] = np.where((temp['cam_norm'] < temp['cam' + '_l_range']) | (temp['cam_norm'] > temp['cam' + '_h_range']), np.nan, temp['cam_norm'])
            temp['prop_cam'] = temp.groupby('id_use')['cam_norm'].transform('mean')
            temp = temp.drop_duplicates('id_use')
            test_data = test_data.join(temp.set_index('id_use')[['prop_cam']], on='id_use')
            test_data['prop_cam'] = round(test_data['prop_cam'], 2)
        
        for basis in ['N', 'G']:
            temp = test_data.copy()
            temp = temp[(temp['lease_sublease'] == 0) | (temp['lease_sublease'].isnull() == True)]
            temp['count_obs'] = temp.groupby('id_use')['rent_basis'].transform('count')
            temp['count_obs'] = temp['count_obs'].fillna(0)
            if self.sector == "ret":
                temp = temp[(temp['a_prop_avrent'].isnull() == False) | (temp['n_prop_avrent'].isnull() == False) | (temp['count_obs'] == 0)]
            else:
                temp = temp[(temp['prop_avrent'].isnull() == False) | (temp['count_obs'] == 0)]
            temp = temp[temp['rent_basis'] == basis]
            temp['count_term_' + basis] = temp.groupby('id_use')['rent_basis'].transform('count')
            temp = temp.drop_duplicates('id_use')
            test_data = test_data.join(temp.set_index('id_use')[['count_term_' + basis]], on='id_use')
            test_data['count_term_' + basis] = test_data['count_term_' + basis].fillna(0)
        test_data['max_term'] = np.where((test_data['count_term_N'] > test_data['count_term_G']), 'N', '')
        test_data['max_term'] = np.where((test_data['count_term_G'] > test_data['count_term_N']), 'G', test_data['max_term'])
        if self.sector == "off":
            test_data['max_term'] = np.where((test_data['count_term_N'] == test_data['count_term_G']) & (test_data['count_term_N'] > 0), 'G', test_data['max_term'])
        elif self.sector == "ind" or self.sector == "ret":
            test_data['max_term'] = np.where((test_data['count_term_N'] == test_data['count_term_G']) & (test_data['count_term_G'] > 0), 'N', test_data['max_term'])
        test_data['rent_basis'] = test_data['max_term']
        
        if self.sector == "off":
            temp = test_data.copy()
            temp = temp[temp['type2'] == 'T']
            temp = temp[((temp['prop_dir_avail'] < temp['tot_size']) & (temp['prop_dir_avail'] > 0)) | ((temp['prop_sub_avail'] < temp['tot_size']) & (temp['prop_sub_avail'] > 0))]
            if len(temp) > 0:
                temp['flag'] = 'Prop is Single Tenant but is not fully leased or fully vacant'
                temp['status'] = 'flagged'
                self.logic_log = self.logic_log.append(temp.drop_duplicates('property_source_id').rename(columns={'id_use': 'realid'})[['realid', 'flag', 'property_source_id', 'status']], ignore_index=True)
            test_data['type2'] = np.where((test_data['type2'] == 'T') & ((test_data['prop_dir_avail'] < test_data['tot_size']) & (test_data['prop_dir_avail'] > 0)) | ((test_data['prop_sub_avail'] < test_data['tot_size']) & (test_data['prop_sub_avail'] > 0)), 'O', test_data['type2'])
        
        
        test_data.sort_values(by=['id_use'], ascending=[False], inplace=True)
        mult_prop_link = test_data.copy()
        mult_prop_link['count'] = mult_prop_link.groupby('id_use')['property_source_id'].transform('nunique')
        mult_prop_link['cumcount'] = mult_prop_link.groupby('id_use')['id_use'].transform('cumcount')
        mult_prop_link = mult_prop_link[(mult_prop_link['count'] > 1) & (mult_prop_link['cumcount'] > 0)]
        mult_prop_link = mult_prop_link.drop_duplicates('property_source_id')
        mult_prop_link = mult_prop_link[['property_source_id', 'id_use']]
        mult_prop_link.to_csv('{}/OutputFiles/{}/logic_logs/mult_prop_link_{}m{}.csv'.format(self.home, self.sector, self.curryr, self.currmon), index=False)
        test_data = test_data.drop_duplicates('id_use')
    
        logging.info('\n')
        
        return test_data, mult_prop_link
    
    def outdated_check(self, test_data, log):
        
        log_test = log.copy()
        
        log_test = log_test.rename(columns={'ind_avail': 'avail'})
        
        if self.sector == "off" or self.sector == "ind":
            log_test['avail'] = np.where((log_test['sublet'].isnull() == False), 0, log_test['avail'])
            log_test['sublet'] = np.where((log_test['avail'].isnull() == False), 0, log_test['sublet'])
            log_test['tot_avail'] = log_test['avail'] + log_test['sublet']
            test_data['tot_avail'] = test_data['prop_dir_avail'] + test_data['prop_sub_avail']
        elif self.sector == "ret":
            log_test['a_avail'] = np.where((log_test['n_avail'].isnull() == False), 0, log_test['a_avail'])
            log_test['n_avail'] = np.where((log_test['a_avail'].isnull() == False), 0, log_test['n_avail'])
            log_test['tot_avail'] = log_test['n_avail'] + log_test['a_avail']
            test_data['tot_avail'] = test_data['n_prop_dir_avail'] + test_data['a_prop_dir_avail']
        
        log_test['survdate_d'] = pd.to_datetime(log_test['survdate'])
        log_test = log_test[log_test['tot_avail'].isnull() == False]
        log_test.sort_values(by=['survdate_d'], ascending=[False], inplace=True)
        log_test = log_test.drop_duplicates('realid')

        test_data = test_data.join(log_test.set_index('realid').rename(columns={'tot_avail': 'latest_avail', 'survdate_d': 'latest_surv'})[['latest_surv', 'latest_avail']], on='id_use')
        test_data['orig_survdate_d'] = pd.to_datetime(test_data['orig_surv_date'])
        test_data['outdate_check'] = np.where((test_data['orig_survdate_d'] < test_data['latest_surv']) & (((test_data['latest_avail'] - test_data['tot_avail']) / test_data['tot_avail'] < -0.1) | ((test_data['latest_avail'] == 0) & (test_data['tot_avail'] > 0))) & (test_data['latest_avail'].isnull() == False), 1, 0)
        
        if len(test_data[test_data['outdate_check'] == 1]) > 0:
            temp = test_data.copy()
            temp = temp[(temp['outdate_check'] == 1)]
            temp['flag'] = 'Catylist Survey old, Foundation Survey indicates less avail'
            temp['status'] = 'flagged'
            temp['c_value'] = temp['tot_avail']
            temp['r_value'] = temp['latest_avail']
            self.logic_log = self.logic_log.append(temp[['property_source_id', 'listed_space_id', 'id_use', 'flag', 'status', 'c_value', 'r_value']], ignore_index=True)

               
    def rename_cols(self, test_data, log):

        test_data['street_address'] = test_data['street_address'].fillna('')
        test_data['address'] = test_data['street_address'].str.lower()

        if self.sector == 'off':
            test_data['g_n'] = 'G'
            test_data['p_gsize'] = test_data['tot_size']
            test_data['p_nsize'] = test_data['tot_size']
            test_data['s_gsize'] = test_data['tot_size']
            test_data['s_nsize'] = test_data['tot_size']
            test_data['surstat'] = np.where((test_data['buildings_building_status'] == '') | (test_data['buildings_building_status'].isnull() == True), 'C', test_data['buildings_building_status'].str[0].str.upper())
            test_data['surstat'] = np.where(test_data['surstat'] == 'E', 'C', test_data['surstat'])
            test_data['expstop'] = ''
            test_data['passthru'] = ''
            test_data['escal'] = ''
            test_data['avrent_f'] = np.nan
            test_data['gross_re'] = np.nan
            test_data['exp_flag'] = np.nan
            test_data['lossfact'] = np.nan
            test_data['code_out'] = ''
            test_data['expren'] = np.nan
            test_data['parking'] = np.nan
            test_data['contig'] = np.nan
            test_data['conv_yr'] = np.nan
            test_data['lowrent'] = np.nan
            test_data['hirent'] = np.nan
        elif self.sector == 'ind':
            test_data['ceil_avg'] = np.where((test_data['buildings_physical_characteristics_clear_height_min_ft'].isnull() == False) & (test_data['buildings_physical_characteristics_clear_height_max_ft'].isnull() == False), (test_data['buildings_physical_characteristics_clear_height_min_ft'] + test_data['buildings_physical_characteristics_clear_height_max_ft']) / 2, np.nan)
            test_data['ceil_avg'] = np.where((test_data['buildings_physical_characteristics_clear_height_min_ft'].isnull() == True) & (test_data['buildings_physical_characteristics_clear_height_max_ft'].isnull() == False), test_data['buildings_physical_characteristics_clear_height_max_ft'], test_data['ceil_avg'])
            test_data['ceil_avg'] = np.where((test_data['buildings_physical_characteristics_clear_height_min_ft'].isnull() == False) & (test_data['buildings_physical_characteristics_clear_height_max_ft'].isnull() == True), test_data['buildings_physical_characteristics_clear_height_min_ft'], test_data['ceil_avg'])
            test_data['selected'] = 'A'
            test_data['surstat'] = np.where((test_data['buildings_building_status'] == '') | (test_data['buildings_building_status'].isnull() == True), 'C', test_data['buildings_building_status'].str[0].str.upper())
            test_data['surstat'] = np.where(test_data['surstat'] == 'E', 'C', test_data['surstat'])
            test_data['off_lowrent'] = np.nan
            test_data['off_hirent'] = np.nan
            test_data['off_avrent'] = np.nan
            test_data['code_out'] = ''
            test_data['expren'] = np.nan
            test_data['parking'] = np.nan
            test_data['contig'] = np.nan
            test_data['conv_yr'] = np.nan
            test_data['ind_lowrent'] = np.nan
            test_data['ind_hirent'] = np.nan
        elif self.sector == "ret":
            test_data['exp_year'] = np.nan
            test_data['sales'] = -1
            test_data['foodct'] = '-1'
            test_data['a_lorent'] = np.nan
            test_data['a_hirent'] = np.nan
            test_data['n_lorent'] = np.nan
            test_data['n_hirent'] = np.nan
        
        test_data['status'] = np.where((test_data['buildings_building_status'] == '') | (test_data['buildings_building_status'].isnull() == True), 'C', test_data['buildings_building_status'].str[0].str.upper())
        test_data['status'] = np.where(test_data['status'] == 'E', 'C', test_data['status']) 
        test_data['lease_own'] = np.where((test_data['occupancy_owneroccupied'] == 0), 'L', '')
        test_data['lease_own'] = np.where(test_data['occupancy_owneroccupied'] == 1, 'O', test_data['lease_own'])
        test_data['lease_own'] = np.where(test_data['occupancy_owneroccupied'].isnull() == True, '', test_data['lease_own'])
        test_data['source'] = 'catylist listings'
        test_data['ti1'] = np.nan
        test_data['ti_renew'] = np.nan
        test_data['comm2'] = np.nan
        
        if self.sector == "ind":
            #test_data['off_size'] = np.where((test_data['building_office_size_sf'].isnull() == False), test_data['building_office_size_sf'], np.nan)
            test_data['off_size'] = np.nan
            
        for key, value in self.rename_dict.items():
            test_data.rename(columns={value: key}, inplace=True)

        for key, value in self.type_dict.items():

            if value['type'] == 'float' and isinstance(test_data.reset_index().loc[0][key], str):
                test_data[key] = np.where(test_data[key] == '', np.nan, test_data[key])
                test_data[key] = test_data[key].astype(float)
            elif value['type'] == 'str' and isinstance(test_data.reset_index().loc[0][key], float):
                test_data[key] = np.where(test_data[key].isnull() == True, '', test_data[key])
                test_data[key] = test_data[key].astype(str)

        if self.sector == "off" or self.sector == "ind":
            extra_cols = ['leg', 'property_source_id']
        elif self.sector == "ret":
            extra_cols = ['leg', 'status', 'lease_own', 'property_source_id']
        
        range_cols = [x for x in test_data.columns if x[-7:] == 'l_range' or x[-7:] == 'h_range']
        
        test_data = test_data[self.orig_cols + extra_cols + range_cols + ['identity']]
        test_data['survdate_d'] = pd.to_datetime(test_data['survdate'])                     
               
        return test_data, log
    
    def vartypes_check(self, test_data):
        
        issues = []
        
        for key, value in self.type_dict.items():
            if value['type'] == 'str' and key != 'realid':
                test_data[key] = test_data[key].astype(str)
                test_data['testing'] = np.where((test_data[key].str.isdigit()), True, False)
                if len(test_data[test_data['testing'] == True]) > 0:
                    issues.append(key)
                    logging.info("{} contains non string types".format(key))
                    self.stop = True
                test_data = test_data.drop(['testing'],axis=1)
                
            elif value['type'] == 'float':
                try:
                    test_data[key] = test_data[key].astype(float)
                except:
                    logging.info("{} contains non float types".format(key))
                    issues.append(key)
                    self.stop = True
        logging.info("\n")
        
        
        if isinstance(test_data.reset_index().loc[0]['fipscode'], str):
            logging.info('Fixing fipscode')
            logging.info('\n')
            test_data['fipscode'] = np.where((test_data['fipscode'].str.isdigit()), test_data['fipscode'], np.nan)
            issues.remove('fipscode')
        if 'propname' in issues:
            logging.info('Fixing propname')
            logging.info('\n')
            test_data['propname'] = test_data['propname'].astype(str)
            issues.remove('propname')
            
        if len(issues) == 0:
            self.stop = False
        
        return test_data, self.stop
    
    def sign_check(self, test_data):
        
        for key, value in self.type_dict.items():
            
            if key in ['p_gsize', 'p_nsize', 's_gsize', 's_nsize']:
                continue
            
            if value['sign'] == 'pos' and value['range_lev'] != '':
                temp = test_data.copy()
                temp = temp[((temp[key] <= 0) & (value['range_lev'] not in ['zerook', 'max']) & (key not in ['avail', 'ind_avail', 'a_avail', 'n_avail'])) | (temp[key] < 0)]
                if len(temp) > 0:
                    temp['flag'] = 'Illogical Sign'
                    temp['value'] = temp[key]
                    temp['column'] = key
                    temp['status'] = 'flagged'
                    self.logic_log = self.logic_log.append(temp[['realid', 'flag', 'value', 'column', 'property_source_id', 'status']], ignore_index=True)
            elif value['sign'] == 'neg' and value['range_lev'] != '':
                if (len(test_data[test_data[key] >= 0]) > 0 and value['range_lev'] != 'zerook') or len(test_data[test_data[key] > 0]) > 0:
                    temp = test_data.copy()
                    temp = temp[((temp[key] >= 0) & (value['range_lev'] not in ['zerook', 'max'])) | (temp[key] > 0)]
                    if len(temp) > 0:
                        temp['flag'] = 'Illogical Sign'
                        temp['value'] = temp[key]
                        temp['column'] = key
                        temp['status'] = 'flagged'
                        self.logic_log = self.logic_log.append(temp[['realid', 'flag', 'value', 'column', 'property_source_id', 'status']], ignore_index=True)
                    
        return test_data
    
    def range_check(self, test_data):
        
        found_issue = []
        
        for key, value in self.type_dict.items():
            if value['range_lev'] == '':
                continue
            else:
                df_test = test_data.copy()
                if value['range_lev'] == 'zerook':
                    df_test = df_test[df_test[key] > 0]
                
                for ident in df_test[(df_test['metcode'].isnull() == False) & (df_test['metcode'] != '')]['identity'].unique():
                    
                    temp = df_test.copy()
                    temp = temp[temp['identity'] == ident]
                    temp = temp[((temp[key] < temp[temp['identity'] == ident].reset_index().loc[0][key + '_l_range']) & (temp[key + '_l_range'].isnull() == False)) | ((temp[key] > temp[temp['identity'] == ident].reset_index().loc[0][key + '_h_range']) & (temp[key + '_h_range'].isnull() == False)) & (temp[key].isnull() == False)]
                    if len(temp) > 0:
                        if key not in found_issue:
                            logging.info("{} has values outside the historical surveyed range boundaries".format(key))
                            found_issue.append(key)
                        temp['flag'] = 'Range Outlier - Property Level'
                        temp['value'] = temp[key]
                        temp['column'] = key
                        temp['status'] = 'flagged'
                        self.logic_log = self.logic_log.append(temp[['realid', 'flag', 'value', 'column', 'property_source_id', 'status']], ignore_index=True)

        if self.sector == "off":
            avail = 'avail'
            size = 'size'
            lowrent = 'lowrent'
            hirent = 'hirent'
            rent = 'avrent'
            survyr = 'surv_yr'
            survqtr = 'surv_qtr'
        elif self.sector == "ind":
            avail = 'ind_avail'
            size = 'ind_size'
            lowrent = 'ind_lowrent'
            hirent = 'ind_hirent'
            rent = 'ind_avrent'
            survyr = 'realyr'
            survqtr = 'qtr'
        elif self.sector == "ret":
            survyr = 'surv_yr'
            survqtr = 'surv_qtr'
        
        
        if self.sector == "off" or self.sector == "ind":
            test_data['avail_check'] = np.where((test_data[avail] > test_data[size]), 1, 0)
            
            test_data['sublet_check'] = np.where((test_data['sublet'] > test_data[size]), 1, 0)
        
            test_data['sublet_temp'] = test_data['sublet']
            test_data['sublet_temp'] = test_data['sublet_temp'].fillna(0)
            test_data['avail_temp'] = test_data[avail]
            test_data['avail_temp'] = test_data['avail_temp'].fillna(0)
            test_data['all_avail_check'] = np.where((test_data['sublet_temp'] + test_data['avail_temp'] > test_data[size]) & (test_data['avail_check'] == 0), 1, 0)

            test_data[avail] = np.where((test_data[avail] > test_data[size]), test_data[size], test_data[avail])
            test_data['sublet'] = np.where((test_data['sublet'] > test_data[size]), test_data[size], test_data['sublet'])
            
            test_data['avrent_check'] = np.where((test_data[lowrent].isnull() == False) & (test_data[hirent].isnull() == False) & (round((test_data[lowrent] + test_data[hirent]) / 2,2) != round(test_data[rent], 2)), 1, 0)
            test_data['avrent_check'] = np.where((test_data[lowrent].isnull() == False) & (test_data[hirent].isnull() == True) & (test_data[lowrent] != test_data[rent]), 1, test_data['avrent_check'])
            test_data['avrent_check'] = np.where((test_data[lowrent].isnull() == True) & (test_data[hirent].isnull() == False) & (test_data[hirent] != test_data[rent]), 1, test_data['avrent_check'])

            test_data['crent_check'] = np.where((test_data['c_rent'].isnull() == False) & (test_data['c_rent'] > test_data[rent]), 1, 0)
        
            test_data['opex_check'] = np.where((test_data['op_exp'].isnull() == False) & (((test_data['op_exp'] >= test_data[rent]) & (test_data[rent].isnull() == False)) | (test_data['op_exp'] < test_data['re_tax']) | ((test_data['op_exp'] / test_data[rent] < 0.03) & (test_data[rent].isnull() == False))), 1, 0)
            test_data['retax_check'] = np.where((test_data['re_tax'].isnull() == False) & (test_data['re_tax'] >= test_data[rent]) & (test_data[rent].isnull() == False), 1, 0)
        
        elif self.sector == "ret":
            for prefix in ['a', 'n']:
                test_data[prefix + '_avail_check'] = np.where((test_data[prefix + '_avail'] > test_data[prefix + '_size']), 1, 0)
                
                test_data[prefix + '_avail'] = np.where((test_data[prefix + '_avail'] > test_data[prefix + '_size']), test_data[prefix + '_size'], test_data[prefix + '_avail'])
                
                test_data[prefix + '_avrent_check'] = np.where((test_data[prefix + '_lorent'].isnull() == False) & (test_data[prefix + '_hirent'].isnull() == False) & (round((test_data[prefix + '_lorent'] + test_data[prefix + '_hirent']) / 2,2) != round(test_data[prefix + '_avrent'], 2)), 1, 0)
                test_data[prefix + '_avrent_check'] = np.where((test_data[prefix + '_lorent'].isnull() == False) & (test_data[prefix + '_hirent'].isnull() == True) & (test_data[prefix + '_lorent'] != test_data[prefix + '_avrent']), 1, test_data[prefix + '_avrent_check'])
                test_data[prefix + '_avrent_check'] = np.where((test_data[prefix + '_lorent'].isnull() == True) & (test_data[prefix + '_hirent'].isnull() == False) & (test_data[prefix + '_hirent'] != test_data[prefix + '_avrent']), 1, test_data[prefix + '_avrent_check'])

                test_data[prefix + '_crent_check'] = np.where((test_data[prefix + 'c_rent'].isnull() == False) & (test_data[prefix + 'c_rent'] > test_data[prefix + '_avrent']), 1, 0)
        
            test_data['opex_check'] = np.where((test_data['op_exp'].isnull() == False) & ((test_data['op_exp'] >= test_data['n_avrent']) | (test_data['op_exp'] < test_data['re_tax'])), 1, 0)
            test_data['retax_check'] = np.where((test_data['re_tax'].isnull() == False) & (test_data['re_tax'] >= test_data['n_avrent']), 1, 0)
        
            test_data['arent_check'] = np.where((test_data['a_avrent'].isnull() == False) & (test_data['n_avrent'].isnull() == False) & (test_data['n_avrent'] < test_data['a_avrent']), 1, 0)
        
        test_data['survyr'] = test_data['survdate_d'].dt.year
        test_data['survmon'] = test_data['survdate_d'].dt.month
        test_data['survday'] = test_data['survdate_d'].dt.day
        test_data['reis_yr'] = np.where((test_data['survmon'] == 12) & (test_data['survday'] > 15), test_data['survyr'] + 1, test_data['survyr'])
        test_data['reis_mon'] = np.where((test_data['survmon'] == 12) & (test_data['survday'] > 15), 1, test_data['survmon'])
        test_data['reis_mon'] = np.where((test_data['survday'] > 15) & (test_data['survmon'] < 12), test_data['survmon'] + 1, test_data['reis_mon'])
        test_data['reis_qtr'] = np.ceil(test_data['reis_mon'] / 3)
        test_data['survyr_check'] = np.where((test_data[survyr] != test_data['reis_yr']), 1, 0)
        test_data['survqtr_check'] = np.where((test_data[survqtr] != test_data['reis_qtr']), 1, 0)

        
        test_data['met_state_check'] = 0
        for met in test_data['metcode'].unique():
            if met == "nan" or met == '':
                continue
            try:
                test_data['met_state_check'] = np.where((test_data['metcode'] == met) & (~test_data['state'].isin(self.met_state_dict[met])), 1, test_data['met_state_check'])
            except:
                logging.info("\n")
                logging.info("{} is listed as a metro, and does not exist for {}".format(met, self.sector))
                logging.info("\n")
                temp = test_data.copy()
                temp = temp[temp['metcode'] == met]
                temp['flag'] = 'metro doesnt exist'
                temp['c_value'] = temp['metcode']
                temp['status'] = 'flagged'
                self.logic_log.append(temp.drop_duplicates('property_source_id')[['realid', 'flag', 'c_value', 'property_source_id', 'status']])
            
        for x in [x for x in test_data.columns if x[-5:] == 'check']:   

            if len(test_data[test_data[x] == 1]) > 0:
                logging.info("There are illogical values for {}".format(x[:-6]))
                temp = test_data.copy()
                temp = temp[temp[x] == 1]
                if len(temp):
                    temp['flag'] = 'Illogical Value'
                    temp['column'] = x
                    temp['status'] = 'flagged'
                    self.logic_log = self.logic_log.append(temp[['realid', 'flag', 'column', 'property_source_id', 'status']], ignore_index=True)
        
        logging.info("\n")
        
        return test_data
    
    def structural_check(self, test_data, log):

        log_test = log.copy()
        
        for col in log_test.columns:
            log_test[col] = np.where((log_test[col] == -1) | (log_test[col] == '-1'), np.nan, log_test[col])
        log_test['rnt_term'] = np.where((log_test['rnt_term'] == 'Gross'), 'G', log_test['rnt_term']) 
        log_test['rnt_term'] = np.where((log_test['rnt_term'] == 'NNN'), 'N', log_test['rnt_term']) 
        

        log_test['survdate']= pd.to_datetime(log_test['survdate'])

        if self.sector == "ret":
            log_test.sort_values(by=['realid', 'survdate', 'tot_size'], ascending=[True, False, False], inplace=True)
        else:
            log_test.sort_values(by=['realid', 'survdate'], ascending=[True, False], inplace=True)

        log_test = log_test.drop_duplicates('realid')

        structural_cols = {
                           'type2': 0, 
                           'metcode': -1, 
                           'subid': -1, 
                           'state': -1, 
                           'year': 0, 
                           'month': 0, 
                           'renov': 0, 
                           'x': 0, 
                           'y': 0, 
                           'fipscode': 0
                        }
        if self.sector == "off":
            structural_cols['size'] = 0.15
            structural_cols['flrs'] = 0
            structural_cols['bldgs'] = 0
            structural_cols['status'] = 0

        elif self.sector == "ind":
            structural_cols['ind_size'] = 0.15
            structural_cols['flrs'] = 0
            structural_cols['bldgs'] = 0
            structural_cols['status'] = 0
            structural_cols['lease_own'] = -1
  
        elif self.sector == "ret":
            structural_cols['type1'] = 0
            structural_cols['n_size'] = 0.15
            structural_cols['a_size'] = 0.15
        
        for key, value in structural_cols.items():
            log_test.rename(columns={key: key + '_log'}, inplace=True)

        test_data = test_data.join(log_test.set_index('realid')[[x + '_log' for x in list(structural_cols.keys())]], on='realid')
        if self.sector == "ret":
            log_test = log_test.rename(columns={'tot_size': 'tot_size_log'})
            test_data = test_data.join(log_test.set_index('realid')[['tot_size_log']], on='realid')

        test_data['state'] = test_data['state'].str.upper()
        test_data['state_log'] = test_data['state_log'].str.upper()
        test_data['metcode'] = test_data['metcode'].str.upper()
        test_data['metcode_log'] = test_data['metcode_log'].str.upper()

        total_records = len(test_data[test_data['leg']])
        logging.info("Total number of records in structural test: {:,}".format(total_records))
        logging.info("\n")

        for key, value in structural_cols.items():            
            if key not in ['x', 'y'] and value in [0, -1] and len(test_data[(test_data[key] != test_data[key + '_log']) & (test_data['leg']) & (test_data[key] != '') & (test_data[key].isnull() == False) & (test_data[key + '_log'] != '') & (test_data[key + '_log'].isnull() == False)]) > 0:
                temp1 = test_data.copy()
                temp1 = temp1[(temp1[key] != temp1[key + '_log']) & (temp1['leg']) & (temp1[key] != '') & (temp1[key].isnull() == False) & (temp1[key + '_log'] != '') & (temp1[key + '_log'].isnull() == False)]
                logging.info("{:,} properties accounting for {:.1%} of the total pool have a difference for {}".format(len(temp1),len(temp1) / total_records, key))
                logging.info("\n")
                temp1['flag'] = 'Structural Differance to REIS link'
                temp1['c_value'] = temp1[key]
                temp1['r_value'] = temp1[key + '_log']
                temp1['column'] = key
                if value == -1:
                    if key not in ['subid', 'metcode'] or not self.use_reis_sub:
                        temp1['status'] = 'dropped'
                    else:
                        temp1['status'] = 'flagged'
                elif value == 0:
                    temp1['status'] = 'flagged'
                self.logic_log = self.logic_log.append(temp1[['realid', 'flag', 'c_value', 'r_value', 'column', 'property_source_id', 'status']], ignore_index=True)
            elif key not in ['x', 'y'] and value not in [0, -1]:
                if key in ['size', 'ind_size', 'n_size', 'a_size']:
                    test_data['test1'] = abs(test_data[key] - test_data[key + '_log']) > 10000
                else:
                    test_data['test1'] = True
                if len(test_data[(abs((test_data[key] - test_data[key + '_log']) / test_data[key + '_log']) > value) & (test_data[key] != test_data[key + '_log']) & (test_data['test1']) & (test_data['leg']) & (test_data[key] != '') & (test_data[key].isnull() == False) & (test_data[key + '_log'] != '') & (test_data[key + '_log'].isnull() == False)]) > 0: 
                    temp2 = test_data.copy()
                    temp2 = temp2[(abs((temp2[key] - temp2[key + '_log']) / temp2[key + '_log']) > value) & (temp2[key] != temp2[key + '_log']) & (temp2['test1']) & (temp2['leg']) & (temp2[key] != '') & (temp2[key].isnull() == False) & (temp2[key + '_log'] != '') & (temp2[key + '_log'].isnull() == False)]
                    logging.info("{:,} properties accounting for {:.1%} of the total pool have a difference for {}".format(len(temp2), len(temp2) / total_records, key))
                    logging.info("\n")
                    temp2['flag'] = 'Structural Differance to REIS link'
                    temp2['c_value'] = temp2[key]
                    temp2['r_value'] = temp2[key + '_log']
                    temp2['column'] = key
                    temp2['status'] = 'dropped'
                    self.logic_log = self.logic_log.append(temp2[['realid', 'flag', 'c_value', 'r_value', 'column', 'property_source_id', 'status']], ignore_index=True)
            elif key in ['x', 'y']:
                if len(test_data[(abs(test_data[key] - test_data[key + '_log']) > 0.003) & (test_data['leg']) & (test_data[key] != '') & (test_data[key].isnull() == False) & (test_data[key + '_log'] != '') & (test_data[key + '_log'].isnull() == False)]) > 0:
                    temp3 = test_data.copy()
                    temp3 = temp3[(abs(temp3[key] - temp3[key + '_log']) > 0.003) & (temp3['leg']) & (temp3[key] != '') & (temp3[key].isnull() == False) & (temp3[key + '_log'] != '') & (temp3[key + '_log'].isnull() == False)]
                    logging.info("{:,} properties accounting for {:.1%} of the total pool have a difference for {}".format(len(temp3), len(temp3) / total_records, key))
                    logging.info("\n")
                    temp3['flag'] = 'Structural Differance to REIS link'
                    temp3['c_value'] = temp3[key]
                    temp3['r_value'] = temp3[key + '_log']
                    temp3['column'] = key
                    temp3['status'] = 'flagged'
                    self.logic_log = self.logic_log.append(temp3[['realid', 'flag', 'c_value', 'r_value', 'column', 'property_source_id', 'status']], ignore_index=True)

            if self.sector != "ind":
                test_data['lease_own'] = ''
                test_data['lease_own_log'] = ''
            
            if value == -1:
                
                test_data['test1'] = (test_data[key] == test_data[key + '_log'])
                test_data['test2'] = (test_data[key] == '') | (test_data[key + '_log'] == '') | (test_data[key].isnull() == True) | (test_data[key + '_log'].isnull() == True)
                test_data['test3'] = (key == 'lease_own') & ((test_data['lease_own'] == '') | (test_data['lease_own_log'] == ''))
                if key not in ['metcode', 'subid'] or not self.use_reis_sub:
                    temp = test_data.copy()
                    temp = temp[(temp['test1'] == False) & (temp['test2'] == False) & (temp['test3'] == False) & (temp['leg'])]
                    if len(temp) > 0:
                        temp['reason'] = 'core structural difference for {}'.format(key)
                        self.drop_log = self.drop_log.append(temp[['property_source_id', 'realid', 'reason']].drop_duplicates('property_source_id'), ignore_index=True)

                    test_data = test_data[(test_data['test1']) | (test_data['test2']) | (test_data['test3']) | (test_data['leg'] == False)]
                elif key in ['metcode', 'subid'] and self.use_reis_sub:
                    test_data[key] = np.where((test_data['test1'] == False) & (test_data['test2'] == False) & (test_data['test3'] == False) & (test_data['leg']), test_data[key + '_log'], test_data[key])
            
            elif value != 0:
                
                test_data['test1'] = (abs((test_data[key] - test_data[key + '_log'])) / test_data[key + '_log'] <= value) | (test_data[key] == test_data[key + '_log'])
                test_data['test2'] = (abs(test_data[key] - test_data[key + '_log']) <= 10000) & (key in ['size', 'ind_size', 'n_size', 'a_size'])
                test_data['test3'] = (test_data[key] == '') | (test_data[key + '_log'] == '') | (test_data[key + '_log'].isnull() == True) | (test_data[key].isnull() == True)
                temp = test_data.copy()
                temp = temp[(temp['test1'] == False) & (temp['test2'] == False) & (temp['test3'] == False) & (temp['leg'])]
                if len(temp) > 0:
                    temp['reason'] = 'core structural difference for {}'.format(key)
                    self.drop_log = self.drop_log.append(temp[['property_source_id', 'realid', 'reason']].drop_duplicates('property_source_id'), ignore_index=True)
                
                test_data = test_data[(test_data['test1']) | (test_data['test2']) | (test_data['test3']) | (test_data['leg'] == False)]     
            
            if self.sector != 'ind':
                test_data = test_data.drop(['lease_own', 'lease_own_log'],axis=1)
                
        if self.sector == "ret":
            temp = test_data.copy()
            temp = temp[(temp['tot_size'] != temp['a_size'] + temp['n_size']) & (temp['tot_size'] != temp['tot_size_log'])]
            if len(temp) > 0:
                temp['reason'] = 'Anchor size and Non Anchor size do not add up to Total Size'
                self.drop_log = self.drop_log.append(temp[['property_source_id', 'realid', 'reason']].drop_duplicates('property_source_id'), ignore_index=True)
            temp = test_data.copy()
            temp = temp[(temp['tot_size'] != temp['a_size'] + temp['n_size']) & (temp['tot_size'] == temp['tot_size_log']) & (temp['a_size_log'] != 0) & (temp['n_size_log'] != 0)]
            if len(temp) > 0:
                temp['flag'] = 'Anchor size and Non Anchor size do not add up to Total Size in Foundation'
                temp['status'] = 'flagged'
                self.logic_log = self.logic_log.append(temp[['realid', 'flag', 'property_source_id', 'status']], ignore_index=True)
                
            test_data = test_data[(test_data['tot_size'] == test_data['a_size'] + test_data['n_size']) | (test_data['tot_size'] == test_data['tot_size_log'])]
            
        if self.sector == "off":
            sizes = ['size']
            avails = ['avail']
            sizes_log = ['size_log']
        elif self.sector == "ind":
            sizes = ['ind_size']
            avails = ['ind_avail']
            sizes_log = ['ind_size_log']
        elif self.sector == "ret":
            sizes = ['n_size', 'a_size']
            avails = ['n_avail', 'a_avail']
            sizes_log = ['n_size_log', 'a_size_log']
        
        for size, avail, size_log in zip(sizes, avails, sizes_log):
            temp = test_data.copy()
            temp = temp[(temp[size].isnull() == True) & (temp[avail] > temp[size_log] + 2000) & (temp[avail].isnull() == False)]
            if len(temp) > 0:
                temp['reason'] = 'Catylist property has no size, and Foundation Size is smaller than Catylist availability'
                self.drop_log = self.drop_log.append(temp[['property_source_id', 'realid', 'reason']].drop_duplicates('property_source_id'), ignore_index=True)
            test_data = test_data[(test_data[size].isnull() == False) | (test_data[avail] <= test_data[size_log] + 2000) | (test_data[avail].isnull() == True)]
            
        return test_data
                    
    def performance_check(self, test_data, log):

        log_test = log.copy()
        
        for col in log_test.columns:
            log_test[col] = np.where((log_test[col] == -1) | (log_test[col] == '-1'), np.nan, log_test[col])
        
        log_test['survdate'] = pd.to_datetime(log_test['survdate'])
        
        perf_cols = {}
        perf_cols['op_exp'] = 0.05
        perf_cols['re_tax'] = 0.05
        if self.sector == 'off':
            perf_cols['avail'] = 0.5
            perf_cols['avrent'] = 0.05
            perf_cols['sublet'] = 0.5
            perf_cols['lse_term'] = 0.5
        elif self.sector == "ind":
            perf_cols['ind_avail'] = 0.5
            perf_cols['ind_avrent'] = 0.05
            perf_cols['sublet'] = 0.5
            perf_cols['lse_term'] = 0.5
        elif self.sector == "ret":
            perf_cols['n_avail'] = 0.5
            perf_cols['n_avrent'] = 0.05
            perf_cols['a_avail'] = 0.5
            perf_cols['a_avrent'] = 0.05
            perf_cols['anc_term'] = 0.5
            perf_cols['non_term'] = 0.5
        
        for col in list(perf_cols.keys()):
            if isinstance(test_data.reset_index().loc[0][col], str):
                log_test[col] = np.where(log_test[col] == '', np.nan, log_test[col])
                log_test[col] = log_test[col].astype(float)

        for col in list(perf_cols.keys()):
            log_test.rename(columns={col: col + '_log'}, inplace=True)
        log_test = log_test.rename(columns={'survdate': 'survdate_log'})

        for key, value in list(perf_cols.items()):
            temp = log_test.copy()
            temp.sort_values(by=['realid', 'survdate_log'], ascending=[True, False], inplace=True)
            temp = temp[temp[key + '_log'].isnull() == False]
            temp = temp.drop_duplicates('realid')
            
            if self.sector == "off":
                size = 'size'
            elif self.sector == 'ind':
                size = 'ind_size'
            elif self.sector == "ret" and key == 'n_avail':
                size = 'n_size'
            elif self.sector == "ret" and key == 'a_avail':
                size = 'a_size'

            test_data = test_data.join(temp.set_index('realid')[[key + '_log', 'survdate_log']], on='realid')

            test_data['diff_mon_' + key] = (date.today().year - test_data['survdate_log'].dt.year) * 12 + (date.today().month - test_data['survdate_log'].dt.month)
            test_data['diff_mon_' + key] = np.where(test_data['diff_mon_' + key] == 0, 1, test_data['diff_mon_' + key])
            if key in ['avail', 'sublet', 'ind_avail', 'n_avail', 'a_avail']:
                test_data['pct_diff_' + key] = abs(test_data[key] - test_data[key + '_log']) / test_data[size]
            else:
                test_data['pct_diff_' + key] = (test_data[key] - test_data[key + '_log']) / test_data[key + '_log']
            if key in ['avrent', 'ind_avrent', 'n_avrent', 'a_avrent', 'op_exp', 're_tax']:
                test_data['pct_diff_mon_' + key] = test_data['pct_diff_' + key] / test_data['diff_mon_' + key]
            test_data = test_data.drop(['survdate_log'], axis=1)

        total_records = len(test_data[test_data['leg']])
        logging.info("Total number of records in performance test: {:,}".format(total_records))
        logging.info("\n")
        
        for key, value in perf_cols.items():
            if key in ['avail', 'sublet', 'ind_avail', 'n_avail', 'a_avail']:
                if len(test_data[(abs(test_data['pct_diff_' + key]) > value) & (test_data['diff_mon_' + key] < 12) & (test_data['leg'])]) > 0:
                    logging.info("{:,} properties accounting for {:.1%} of the total pool have significant difference for {}".format(len(test_data[(abs(test_data['pct_diff_' + key]) > value) & (test_data['diff_mon_' + key] < 12)]), len(test_data[(abs(test_data['pct_diff_' + key]) > value) & (test_data['diff_mon_' + key] < 12)]) / total_records, key))
                    logging.info("\n")
                if self.sector == "off":
                    temp = test_data.copy()
                    temp = temp[(abs(temp['pct_diff_' + key]) > value) & (temp['diff_mon_' + key] < 12) & (temp['type2'] == 'O') & (test_data['leg'])]
                    temp['flag'] = 'Performance Differance to Last REIS Observation'
                    temp['c_value'] = temp[key]
                    temp['r_value'] = temp[key + '_log']
                    temp['diff_val'] = temp['pct_diff_' + key]
                    temp['column'] = key
                    temp['status'] = 'flagged'
                    self.logic_log = self.logic_log.append(temp[['realid', 'flag', 'c_value', 'r_value', 'diff_val', 'column', 'property_source_id', 'status']], ignore_index=True)
                    # After consultation with DQ, and guidance from Allen, will turn off the drop of props with different avail from Catylist and just go with Catylist figures
                    #test_data[key] = np.where((abs(test_data['pct_diff_' + key]) > value) & (test_data['diff_mon_' + key] < 12) & (test_data['type2'] == 'O') & (test_data['leg']), np.nan, test_data[key])
                else:
                    temp = test_data.copy()
                    temp = temp[(abs(temp['pct_diff_' + key]) > value) & (temp['diff_mon_' + key] < 12)]
                    temp['flag'] = 'Performance Differance to Last REIS Observation'
                    temp['c_value'] = temp[key]
                    temp['r_value'] = temp[key + '_log']
                    temp['diff_val'] = temp['pct_diff_' + key]
                    temp['column'] = key
                    temp['status'] = 'flagged'
                    self.logic_log = self.logic_log.append(temp[['realid', 'flag', 'c_value', 'r_value', 'diff_val', 'column', 'property_source_id', 'status']], ignore_index=True)
                    # After consultation with DQ, and guidance from Allen, will turn off the drop of props with different avail from Catylist and just go with Catylist figures
                    #test_data[key] = np.where((abs(test_data['pct_diff_' + key]) > value) & (test_data['diff_mon_' + key] < 12) & (test_data['leg']), np.nan, test_data[key])
                
            if key in ['lse_term', 'anc_term', 'non_term']: 
                if len(test_data[(abs(test_data['pct_diff_' + key]) > value) & (test_data['leg'])]) > 0:
                    logging.info("{:,} properties accounting for {:.1%} of the total pool have significant difference for {}".format(len(test_data[(abs(test_data['pct_diff_' + key]) > value) & (test_data['leg'])]),len(test_data[(abs(test_data['pct_diff_' + key]) > value) & (test_data['leg'])]) / total_records, key))
                    logging.info("\n")
                    temp = test_data.copy()
                    temp = temp[(abs(temp['pct_diff_' + key]) > value) & (test_data['leg'])]
                    temp['flag'] = 'Performance Differance to Last REIS Observation'
                    temp['c_value'] = temp[key]
                    temp['r_value'] = temp[key + '_log']
                    temp['diff_val'] = temp['pct_diff_' + key]
                    temp['column'] = key
                    temp['status'] = 'flagged'
                    self.logic_log = self.logic_log.append(temp[['realid', 'flag', 'c_value', 'r_value', 'diff_val', 'column', 'property_source_id', 'status']], ignore_index=True)
                    
            elif key in ['avrent', 'op_exp', 're_tax', 'ind_avrent', 'a_avrent', 'n_avrent']:
                if len(test_data[(abs(test_data['pct_diff_mon_' + key]) > value)& (test_data['leg'])]) > 0:
                    logging.info("{:,} properties accounting for {:.1%} of the total pool have significant difference for {}".format(len(test_data[(abs(test_data['pct_diff_mon_' + key]) > value) & (test_data['leg'])]),len(test_data[(abs(test_data['pct_diff_mon_' + key]) > value) & (test_data['leg'])]) / total_records, key))
                    logging.info("\n")
                    temp = test_data.copy()
                    temp = temp[abs(temp['pct_diff_mon_' + key]) > value]
                    temp['flag'] = 'Performance Differance to Last REIS Observation'
                    temp['c_value'] = temp[key]
                    temp['r_value'] = temp[key + '_log']
                    temp['diff_val'] = temp['pct_diff_mon_' + key]
                    temp['column'] = key
                    temp['status'] = 'dropped'
                    self.logic_log = self.logic_log.append(temp[['realid', 'flag', 'c_value', 'r_value', 'diff_val', 'column', 'property_source_id', 'status']], ignore_index=True)
                    
                test_data[key] = np.where((abs(test_data['pct_diff_mon_' + key]) > value) & (test_data['leg']), np.nan, test_data[key])

        temp = log_test.copy()
        temp['rnt_term_log'] = temp['rnt_term']
        temp.sort_values(by=['realid', 'survdate_log'], ascending=[True, False], inplace=True)
        temp = temp[temp['rnt_term_log'].isnull() == False]
        temp = temp.drop_duplicates('realid')

        test_data = test_data.join(temp.set_index('realid')[['rnt_term_log', 'survdate_log']], on='realid')

        if len(test_data[(test_data['rnt_term'] != test_data['rnt_term_log']) & (test_data['rnt_term'] != '') & (test_data['rnt_term_log'] != '') & (test_data['rnt_term'].isnull() == False) & (test_data['rnt_term'].isnull() == False) & (test_data['leg'])]) > 0:
            logging.info("{:,} properties accounting for {:.1%} have a significant difference for {}".format(len(test_data[(test_data['rnt_term'] != test_data['rnt_term_log']) & (test_data['rnt_term'] != '') & (test_data['rnt_term_log'].isnull() == False) & (test_data['leg'])]), len(test_data[(test_data['rnt_term'] != test_data['rnt_term_log']) & (test_data['rnt_term'] != '') & (test_data['rnt_term_log'] != '') & (test_data['rnt_term'].isnull() == False) & (test_data['rnt_term_log'].isnull() == False) & (test_data['leg'])]) / total_records, 'rnt_term'))
            logging.info("\n")
            temp = test_data.copy()
            temp = temp[(temp['rnt_term'] != temp['rnt_term_log']) & (test_data['rnt_term'] != '') & (test_data['rnt_term_log'] != '') & (temp['rnt_term'].isnull() == False) & (temp['rnt_term_log'].isnull() == False)]
            temp['flag'] = 'Performance Differance to Last REIS Observation'
            temp['c_value'] = temp['rnt_term']
            temp['r_value'] = temp['rnt_term_log']
            temp['column'] = 'rnt_term'
            temp['status'] = 'flagged'
            self.logic_log = self.logic_log.append(temp[['realid', 'flag', 'c_value', 'r_value', 'column', 'property_source_id', 'status']], ignore_index=True)


        test_data = test_data[self.orig_cols + ['leg', 'property_source_id']]
        
        if self.sector == "off":
            test_data['rnt_term'] = np.where((test_data['avrent'].isnull() == True), '', test_data['rnt_term'])
        elif self.sector == "ind":
            test_data['rnt_term'] = np.where((test_data['ind_avrent'].isnull() == True), '', test_data['rnt_term'])
        elif self.sector == "ret":
            test_data['rnt_term'] = np.where((test_data['n_avrent'].isnull() == True) & (test_data['a_avrent'].isnull() == True), '', test_data['rnt_term'])
        
        return test_data
    
    def val_check(self, test_data):
        
        for key, value in self.type_dict.items():

            if value['null_check'] == "no":
                test1 = test_data.copy()
                test1 = test1[(test1[key].isnull() == False) & (test1[key] != '')]
                vals = test1[key].unique()
                not_valid = [x for x in vals if x not in self.valid_vals[key]]

                if len(not_valid) > 0:
                    logging.info("{} has the following invalid values: {}".format(key, not_valid))
            
            elif value['null_check'] == 'only' or value['null_check'] == 'no':
                test2 = test_data.copy()
                test2 = test2[(test2[key].isnull() == True) | (test2[key] == '')]
                if len(test2) > 0:
                    logging.info("{} has null values".format(key))
        logging.info("\n")
    
    def null_check(self, test_data):
        
        if self.sector == "off":
            null_ok = ['lowrent', 'hirent', 'source', 'expren', 'parking', 'contig', 'avrent_f', 'gross_re', 'exp_flag', 'ti1', 'expstop', 'lossfact', 'passthru', 'escal', 'conv_yr', 'code_out']
        elif self.sector == "ind":
            null_ok = ['source', 'expren', 'contig', 'parking', 'ti1', 'conv_yr', 'code_out', 'ind_lowrent', 'ind_hirent', 'off_lowrent', 'off_hirent', 'off_avrent', 'selected']
        elif self.sector == "ret":
            null_ok = ['source', 'ti1', 'exp_year', 'n_lorent', 'n_hirent', 'a_lorent', 'a_hirent', 'cam', 'sales', 'foodct']

        for key, value in self.type_dict.items():
            if value['type'] == 'str' and key not in null_ok:
                if len(test_data[test_data[key] != '']) == 0:
                    logging.info("{} has null values for all rows".format(key))

            elif key not in null_ok:
                if len(test_data[test_data[key].isnull() == False]) == 0:
                    logging.info("{} has null values for all rows".format(key))

        logging.info('\n')
        
    def align_structurals(self, inc, log_aligned):
        
        if self.sector == "off" or self.sector == "ret":
            log_aligned.sort_values(by=['surv_yr', 'surv_qtr'], ascending=[False, False], inplace=True)
        elif self.sector == "ind":
            log_aligned.sort_values(by=['realyr', 'qtr'], ascending=[False, False], inplace=True)
        
        for key, value in self.type_dict.items():
            
            if key in ['surstat', 'status']:
                inc[key + '_test'] = inc[key]
                log_aligned = log_aligned.join(inc.drop_duplicates('realid').set_index('realid')[[key + '_test']], on='realid')
                log_aligned[key] = np.where(((log_aligned[key].isnull() == True) | (log_aligned[key] == '')) & (log_aligned[key + '_test'].isnull() == False) & (log_aligned[key + '_test'] != ''), log_aligned[key + '_test'], log_aligned[key])
                # There are some oddball cases where the property is complete in foundation and has surveys, but someone forgot to update property status to complete. Will set these to complete here so structural alignment check wont trigger
                log_aligned[key] = np.where((log_aligned[key] != 'C') & (log_aligned[key + '_test'] == 'C'), 'C', log_aligned[key])
                log_aligned = log_aligned.drop([key + '_test'], axis=1)
                inc = inc.drop([key + '_test'], axis=1)
                
            if value['structural'] and value['promote'] == 'R':
                
                log_aligned[key + '_log'] = log_aligned[key]
                inc = inc.join(log_aligned.drop_duplicates('realid').set_index('realid')[[key + '_log']], on='realid')
                inc[key] = np.where((inc[key] != inc[key + '_log']) & (inc[key + '_log'].isnull() == False), inc[key + '_log'], inc[key])
                if key not in ['propname', 'address', 'city', 'zip', 'county', 'state', 'fipscode']:
                    inc[key] = np.where((inc[key + '_log'].isnull() == True), np.nan, inc[key])
                log_aligned = log_aligned.drop([key + '_log'], axis=1)
                inc = inc.drop([key + '_log'], axis=1)

            elif value['structural'] and value['promote'] == 'C':

                inc[key + '_test'] = inc[key]
                log_aligned = log_aligned.join(inc.drop_duplicates('realid').set_index('realid')[[key + '_test']], on='realid')
                if key == 'renov':
                    log_aligned[key] = np.where((log_aligned[key] != log_aligned[key + '_test']) & (log_aligned[key + '_test'].isnull() == False) & (log_aligned[key + '_test'] != '') & ((log_aligned[key + '_test'] > log_aligned['year']) | (log_aligned['year'].isnull() == True)), log_aligned[key + '_test'], log_aligned[key])
                else:
                    log_aligned[key] = np.where((log_aligned[key] != log_aligned[key + '_test']) & (log_aligned[key + '_test'].isnull() == False) & (log_aligned[key + '_test'] != ''), log_aligned[key + '_test'], log_aligned[key])
                log_aligned = log_aligned.drop([key + '_test'], axis=1)
                inc = inc.drop([key + '_test'], axis=1)
                
                if key in ['propname', 'address', 'city', 'zip', 'county', 'state', 'fipscode', 'renov', 'off_size', 'docks', 'dockhigh_doors', 'drivein_doors', 'rail_doors', 'ceil_low', 'ceil_high', 'ceil_avg', 'a_size', 'n_size', 'tot_size', 'size', 'ind_size', 'p_gsize', 'p_nsize', 's_gsize', 's_nsize']:
                    log_aligned[key + '_log'] = log_aligned[key]
                    inc = inc.join(log_aligned.drop_duplicates('realid').set_index('realid')[[key + '_log']], on='realid')
                    inc[key] = np.where(((inc[key]  == '') | (inc[key].isnull() == True)) & (inc[key + '_log'].isnull() == False) & (inc[key + '_log'] != ''), inc[key + '_log'], inc[key])
                    log_aligned = log_aligned.drop([key + '_log'], axis=1)
                    inc = inc.drop([key + '_log'], axis=1)

                if key == 'renov':
                    log_aligned['year_log'] = log_aligned['year']
                    inc = inc.join(log_aligned.drop_duplicates('realid').set_index('realid')[['year_log']], on='realid')
                    inc['renov'] = np.where((inc['renov'].isnull() == False) & (inc['year_log'].isnull() == False) & (inc['year_log'] >= inc['renov']), np.nan, inc['renov'])
                    inc = inc.drop(['year_log'], axis=1)
                    log_aligned.drop(['year_log'],axis=1)

                    log_aligned['renov_log'] = log_aligned['renov']
                    inc = inc.join(log_aligned.drop_duplicates('realid').set_index('realid')[['renov_log']], on='realid')
                    inc['renov'] = np.where((inc['renov'].isnull() == True) & (inc['renov_log'].isnull() == False), inc['renov_log'], inc['renov'])
                    inc = inc.drop(['renov_log'], axis=1)
                    log_aligned.drop(['renov_log'], axis=1)
                    
        return inc, log_aligned
    
    def append_nc_completions(self, combo, d_prop):
        
        if len(d_prop) == 0 and self.home[0:2] == 's3' and self.live_load:
            logging.info('Loading D_Property...')
            logging.info('\n')
            cursor.execute("""select dp.property_source_id, 
                            dp.property_catylist_nc_id,
                            dp.property_reis_rc_id, 
                            dp.property_er_id, 
                            dp.property_er_to_foundation_ids_list,
                            dp.category, 
                            dp.subcategory, 
                            dp.property_geo_msa_code, 
                            dp.property_geo_subid, 
                            dp.property_name, 
                            dp.location_street_address,
                            dp.location_address_locality,
                            dp.location_address_region,
                            dp.location_address_postal_code,
                            dp.location_county,
                            dp.location_geopoint_latitude,
                            dp.location_geopoint_longitude,
                            dp.housing_type,
                            dp.retail_center_type,
                            dp.buildings_condominiumized_flag,
                            dp.buildings_physical_characteristics_number_of_floors,
                            dp.buildings_number_of_buildings,
                            dp.buildings_physical_characteristics_clear_height_max_ft,
                            dp.buildings_physical_characteristics_clear_height_min_ft,
                            dp.buildings_physical_characteristics_doors_crossdockdoors_count,
                            dp.buildings_physical_characteristics_doors_dockhighdoors_count,
                            dp.buildings_physical_characteristics_doors_gradeleveldoors_count,
                            dp.buildings_physical_characteristics_doors_raildoors_count,
                            dp.buildings_size_gross_sf,
                            dp.buildings_size_rentable_sf,
                            dp.occupancy_number_of_units,
                            dp.occupancy_is_owner_occupied_flag,
                            dp.occupancy_type,
                            dp.parcels_fips,
                            dp.buildings_construction_year_built,
                            dp.buildings_construction_expected_completion_date,
                            dp.buildings_construction_year_renovated,
                            sbu.building_office_use_size_sf,
                            sbu.building_industrial_use_size_sf,
                            sbu.building_retail_use_size_sf
                            from consumption.v_d_property dp
                            left join consumption.v_property_building_use_size sbu on dp.property_source_id = sbu.property_source_id
                            where extract(YEAR FROM date(dp.buildings_construction_expected_completion_date)) >= {} and dp.building_status = 'EXISTING'""".format(self.curryr - 1))
            d_prop: pd.DataFrame = cursor.fetch_dataframe()
            d_prop.replace([None], np.nan, inplace=True)
            conn.close()
            d_prop = d_prop.drop_duplicates('property_source_id')
            d_prop.to_csv('{}/InputFiles/d_prop.csv'.format(self.home), index=False)
            
        elif len(d_prop) == 0:
            d_prop = pd.read_csv('{}/InputFiles/d_prop.csv'.format(self.home), na_values= "", keep_default_na = False)
          
        nc_add = d_prop.copy()
        
        nc_add['const_year'] = pd.to_datetime(nc_add['buildings_construction_expected_completion_date']).dt.year
        nc_add['month'] = pd.to_datetime(nc_add['buildings_construction_expected_completion_date']).dt.month
        
        nc_add = nc_add[(nc_add['const_year'] < self.curryr) | ((nc_add['const_year'] == self.curryr) & (nc_add['month'] <= self.currmon))]
            
        nc_add = nc_add[(nc_add['const_year'] == nc_add['buildings_construction_year_built']) | (nc_add['buildings_construction_year_built'].isnull() ==True) | (nc_add['const_year'] == nc_add['buildings_construction_year_renovated'])]
        
        nc_add['property_reis_rc_id'] = np.where(nc_add['property_reis_rc_id'] == 'None', '', nc_add['property_reis_rc_id'])
        nc_add['property_reis_rc_id'] = nc_add['property_reis_rc_id'].str.replace('-', '')

        to_lower = ['occupancy_type', 'category', 'subcategory','housing_type', 'retail_center_type']
        
        for col in to_lower:
            nc_add[col] = nc_add[col].str.lower()
        
        null_to_string = ['occupancy_type', 'category', 'subcategory', 'property_source_id', 'property_reis_rc_id',
                          'property_catylist_nc_id', 'property_er_to_foundation_ids_list', 'property_geo_msa_code', 
                          'property_name', 'location_street_address', 'location_address_locality', 'location_address_region',
                          'location_address_region', 'location_county', 'housing_type', 'retail_center_type']
        
        for col in null_to_string:
            nc_add[col] = np.where((nc_add[col].isnull() == True), '', nc_add[col])
        
        
        nc_add['property_source_id'] = nc_add['property_source_id'].astype(str)
        nc_add['property_reis_rc_id'] = nc_add['property_reis_rc_id'].astype(str)
        nc_add['property_reis_rc_id'] = np.where((nc_add['property_reis_rc_id'].str[0].isin(['I', 'O'])), nc_add['property_reis_rc_id'].str[:-1], nc_add['property_reis_rc_id'])
        
        nc_add['buildings_size_rentable_sf'] = np.where((nc_add['buildings_size_rentable_sf'] == ''), np.nan,nc_add['buildings_size_rentable_sf'])
        nc_add['buildings_size_rentable_sf'] = nc_add['buildings_size_rentable_sf'].astype(float)
        
        temp = combo.copy()
        temp['in_log'] = 1
        temp['property_source_id'] = temp['property_source_id'].astype(str)
        nc_add = nc_add.join(temp.drop_duplicates('property_source_id').set_index('property_source_id')[['in_log']], on='property_source_id')
        nc_add = nc_add[nc_add['in_log'].isnull() == True]
        
        if self.sector == "off":
            size_by_use = 'building_office_use_size_sf'
        elif self.sector == "ind":
            size_by_use = "building_industrial_use_size_sf"
        elif self.sector == "ret":
            size_by_use = "building_retail_use_size_sf"
            
        nc_add['mixed_use_check'] = np.where((nc_add['subcategory'] == 'mixed_use') & (nc_add[size_by_use] > 0), True, False)
        
        nc_add = nc_add[((nc_add['category'].isin(self.sector_map[self.sector]['category'])) & (nc_add['subcategory'].isin(self.sector_map[self.sector]['subcategory'] + ['']))) | (nc_add['mixed_use_check'])]
        
        if self.sector == "ind":
            nc_add['drop'] = np.where((nc_add['subcategory'] == 'warehouse_office'), True, False)
            nc_add['drop'] = np.where((nc_add['subcategory'] == 'warehouse_office') & ((nc_add['building_office_use_size_sf'].isnull() == False) | (nc_add['building_industrial_use_size_sf'].isnull() == False)), False, nc_add['drop'])
            nc_add['drop'] = np.where((nc_add['subcategory'] == '') & (nc_add['building_office_use_size_sf'].isnull() == True) & (nc_add['building_industrial_use_size_sf'].isnull() == True), True, nc_add['drop'])
            nc_add = nc_add[nc_add['drop'] == False]
        if self.sector == "ret":
            nc_add = nc_add[nc_add['retail_center_type'] == '']
            nc_add = nc_add[nc_add['subcategory'] != '']
        
        nc_add['buildings_condominiumized_flag'] = np.where((nc_add['buildings_condominiumized_flag'] == 'Y'), 1, 0)
        nc_add = nc_add[(nc_add['buildings_condominiumized_flag'] == 0)]
            
        nc_add['size'] = nc_add['buildings_size_gross_sf']
        nc_add['size'] = np.where((nc_add['buildings_size_rentable_sf'] > 0), nc_add['buildings_size_rentable_sf'], nc_add['size'])
        nc_add['size'] = np.where((nc_add[size_by_use] > 0) & (nc_add[size_by_use] <= nc_add['size']), nc_add[size_by_use], nc_add['size'])
        
        if self.sector == "off" or self.sector == "ind":
            nc_add = nc_add[(nc_add['size'].isnull() == False) & (nc_add['size'] >= 10000)]
        elif self.sector == "ret":
            nc_add = nc_add[(nc_add['size'].isnull() == False)]
        
        if self.sector == "ind":
            nc_add['off_perc'] = np.where((nc_add['building_office_use_size_sf'].isnull() == False), nc_add['building_office_use_size_sf'] / nc_add['size'], (nc_add['size'] - nc_add['building_industrial_use_size_sf']) / nc_add['size'])
            nc_add['subcategory'] = np.where(((nc_add['subcategory'] == 'warehouse_office') | (nc_add['subcategory'] == '')) & (nc_add['off_perc'] >= 0.25), 'warehouse_flex', nc_add['subcategory'])
            nc_add['subcategory'] = np.where(((nc_add['subcategory'] == 'warehouse_office') | (nc_add['subcategory'] == '')) & (nc_add['off_perc'] < 0.25), 'warehouse_distribution', nc_add['subcategory'])
        
            nc_add['size'] = np.where((nc_add['subcategory'] == 'warehouse_distribution') & (nc_add['building_office_use_size_sf'] > 0) & (nc_add['size'] - nc_add['building_office_use_size_sf'] >= 10000), nc_add['size'] - nc_add['building_office_use_size_sf'], nc_add['size'])
        
        nc_add['occupancy_is_owner_occupied_flag'] = np.where((nc_add['occupancy_is_owner_occupied_flag'] == 'Y'), 1, 0)
        nc_add = nc_add[(nc_add['occupancy_is_owner_occupied_flag'] == 0)]
        
        nc_add = nc_add[(nc_add['property_geo_msa_code'] != '') & (nc_add['property_geo_subid'].isnull() == False)]
        
        temp['realid'] = temp['realid'].astype(str)
        log_ids = list(temp.drop_duplicates('realid')['realid'])
        
        drop_list = []
        for index, row in nc_add.iterrows():
            dropped = False
            if row['property_er_to_foundation_ids_list'] != '':
                er_ids = row['property_er_to_foundation_ids_list'].split(',')
                
                for er_id in er_ids:
                    if er_id[0] == self.sector_map[self.sector]['prefix']:
                        if er_id[1:] in log_ids:
                            drop_list.append(row['property_source_id'])
                            dropped = True
                            break
            if not dropped and row['property_reis_rc_id'] != '':
                rc_id = row['property_reis_rc_id']
                if rc_id[0] == self.sector_map[self.sector]['prefix']:
                    if rc_id[1:] in log_ids:
                        drop_list.append(row['property_source_id'])
                            
        nc_add = nc_add[~nc_add['property_source_id'].isin(drop_list)]
                    
        nc_add = nc_add[nc_add['month'].isnull() == False]

        rename_cols = {'property_geo_msa_code': 'metcode',
                       'property_geo_subid': 'subid',
                       'property_name': 'propname',
                       'location_street_address': 'address',
                       'location_address_locality': 'city',
                       'location_address_postal_code':'zip',
                       'location_county': 'county',
                       'location_address_region': 'state',
                       'const_year': 'year',
                       'buildings_physical_characteristics_number_of_floors': 'flrs',
                       'buildings_number_of_buildings': 'bldgs',
                       'location_geopoint_longitude': 'x',
                       'location_geopoint_latitude': 'y',
                       'parcel_fips': 'fipscode',
                       'buildings_physical_characteristics_doors_crossdockdoors_count': 'docks',
                       'buildings_physical_characteristics_doors_dockhighdoors_count': 'dockhigh_doors',
                       'buildings_physical_characteristics_doors_gradeleveldoors_count': 'drivein_doors',
                       'buildings_physical_characteristics_doors_raildoors_count': 'rail_doors',
                       'buildings_physical_characteristics_clear_height_min_ft': 'ceil_low',
                       'buildings_physical_characteristics_clear_height_max_ft': 'ceil_high'
                      }

        nan_cols = ['g_n', 'parking', 'avail', 'ind_avail', 'n_avail', 'a_avail', 'contig', 'sublet', 'lowrent', 'hirent',
                   'avrent', 'ind_avrent', 'ind_lowrent', 'ind_hirent', 'off_lowrent', 'off_hirent', 'off_avrent', 'n_avrent', 
                    'a_avrent', 'avrent_f', 'c_rent', 'gross_re', 'exp_flag', 'lse_term', 'n_lorent', 'n_hirent', 'a_lorent', 
                    'a_hirent', 'cam', 'ti1', 'ti2', 'ti_renew', 'free_re', 'comm1', 'comm2', 'op_exp', 'expstop', 'lossfact', 
                    'passthru', 'escal', 'conv_yr', 'code_out', 're_tax', 'expren', 'off_size' 'renov', 'ac_rent', 'nc_rent',
                    'a_ti', 'a_freere', 'a_comm', 'sales', 'foodct']

        if len(nc_add) > 0:
            for col in nan_cols:
                if col in self.type_dict.keys():
                    nc_add[col] = np.nan

            nc_add['realid'] = np.where((nc_add['property_source_id'].str.isdigit()), 'a' + nc_add['property_source_id'].astype(str), nc_add['property_source_id'])
            nc_add['phase'] = 0
            if self.sector == "off":
                nc_add['type2'] = np.where((nc_add['occupancy_type'].isin(['single_tenant', ''])), 'T', 'O')
                nc_add['surv_yr'] = self.curryr
                nc_add['surv_qtr'] = np.ceil(self.currmon / 3)
                nc_add['status'] = 'C'
                nc_add['surstat'] = 'C'
                nc_add['p_gsize'] = nc_add['size']
                nc_add['p_nsize'] = nc_add['size']
                nc_add['s_gsize'] = nc_add['size']
                nc_add['s_nsize'] = nc_add['size']
                nc_add['rnt_term'] = ''
            elif self.sector == "ind":
                nc_add['type2'] = np.where((nc_add['subcategory'] == 'warehouse_distribution'), 'W', 'F')
                nc_add['realyr'] = self.curryr
                nc_add['qtr'] = np.ceil(self.currmon / 3)
                nc_add['status'] = 'C'
                nc_add['surstat'] = 'C'
                nc_add = nc_add.rename(columns={'size': 'ind_size'})
                nc_add['lease_own'] = 'L'
                nc_add['rnt_term'] = ''
                nc_add['ceil_avg'] = np.where((nc_add['buildings_physical_characteristics_clear_height_min_ft'].isnull() == False) & (nc_add['buildings_physical_characteristics_clear_height_max_ft'].isnull() == False), (nc_add['buildings_physical_characteristics_clear_height_min_ft'] + nc_add['buildings_physical_characteristics_clear_height_max_ft']) / 2, np.nan)
                nc_add['ceil_avg'] = np.where((nc_add['buildings_physical_characteristics_clear_height_min_ft'].isnull() == False) & (nc_add['buildings_physical_characteristics_clear_height_max_ft'].isnull() == True), nc_add['buildings_physical_characteristics_clear_height_min_ft'], nc_add['ceil_avg'])
                nc_add['ceil_avg'] = np.where((nc_add['buildings_physical_characteristics_clear_height_min_ft'].isnull() == True) & (nc_add['buildings_physical_characteristics_clear_height_max_ft'].isnull() == False), nc_add['buildings_physical_characteristics_clear_height_max_ft'], nc_add['ceil_avg'])
                nc_add['selected'] = ''
            elif self.sector == "ret":
                nc_add['type1'] = np.where((nc_add['subcategory'].isin(['neighborhood_grocery_anchor', 'neighborhood_center', 'retail'])), 'N', 'C')
                nc_add['type2'] = nc_add['type1']
                nc_add['surv_yr'] = self.curryr
                nc_add['surv_qtr'] = np.ceil(self.currmon / 3)
                nc_add['tot_size'] = nc_add['size']
                nc_add['n_size'] = np.where((nc_add['tot_size'] < 9300), nc_add['tot_size'], 0)
                nc_add['a_size'] = np.where((nc_add['tot_size'] >= 9300), nc_add['tot_size'], 0)
                nc_add['rnt_term'] = ''
                nc_add['anc_term'] = ''
                nc_add['non_term'] = ''

            nc_add['survdate'] = ''.format(self.currmon, '15', self.curryr)
            nc_add['source'] = 'nc no surv'

            nc_add['leg'] = 'no'
            
            for col, rename in rename_cols.items():
                nc_add.rename(columns={col: rename}, inplace=True)
        
            for col in nc_add:
                if col not in combo.columns:
                    nc_add = nc_add.drop([col],axis=1)
            
            combo = combo.append(nc_add, ignore_index=True)
        
        nc_add[[x for x in combo.columns if x in nc_add.columns]].to_csv('{}/OutputFiles/{}/logic_logs/nc_additions_{}m{}.csv'.format(self.home, self.sector, self.curryr, self.currmon), index=False)
        
        logging.info('{:,} newly completed properties without surveys were added to the log'.format(len(nc_add)))
        logging.info('\n')
        
        return d_prop, combo, nc_add

    
    def append_incrementals(self, test_data, log, load, d_prop):

        logging.info('Appending historical REIS logs...')
        logging.info('\n')
        
        log['leg'] = 'yes'   
        test_data = test_data[self.orig_cols + ['leg', 'property_source_id']]
        
        
        temp_aggreg = pd.DataFrame()
        log_aligned = log.copy()
        
        root = "{}/OutputFiles/{}/snapshots".format(self.home, self.sector)
        if self.home[0:2] == 's3':
            fs = s3fs.S3FileSystem()
            dir_list = fs.ls(root)
            dir_list = [x for x in dir_list if x[-1] != '/']
            dir_list = [f for f in dir_list if len(f.split('/')[-1].split('.csv')[0]) == 19 or len(f.split('/')[-1].split('.csv')[0]) == 20]
        else:
            dir_list = [f for f in listdir(root) if isfile(join(root, f))]
            dir_list = [f for f in dir_list if len(f.split('/')[-1].split('.csv')[0]) == 19 or len(f.split('/')[-1].split('.csv')[0]) == 20]

        if self.currmon < 10:
            end_val = 6
        else:
            end_val = 7
        dir_list = [x for x in dir_list if x.split('/')[-1][0:end_val] != str(self.curryr) + 'm' + str(self.currmon)]

        for file in dir_list:
            if self.home[0:2] == 's3': 
                file_read = '{}{}'.format(self.prefix, file)
            else:
                file_read = root + '/' + file
            logging.info("Appending historical incrementals for {}".format(file.split('/')[-1].split('_')[0]))
            temp = pd.read_csv(file_read, encoding = 'utf-8',  na_values= "", keep_default_na = False,
                                   dtype=self.dtypes)
            temp['leg'] = 'yes'
            temp_aggreg = temp_aggreg.append(temp, ignore_index=False)

        for key, value in self.type_dict.items():
            if value['structural']:
                temp_aggreg['count'] = temp_aggreg.groupby('realid')[key].transform('nunique')
                temp_aggreg['survdate_dt'] = pd.to_datetime(temp_aggreg['survdate'])
                temp_aggreg.sort_values(by=['survdate_dt'], ascending=[False], inplace=True)
                temp_aggreg['cumcount'] = temp_aggreg.groupby('realid')['realid'].transform('cumcount')
                temp_aggreg[key] = np.where((temp_aggreg['count'] > 1) & (temp_aggreg['cumcount'] > 0), np.nan, temp_aggreg[key])
                temp_aggreg[key] = temp_aggreg.groupby('realid')[key].bfill()
                temp_aggreg[key] = temp_aggreg.groupby('realid')[key].ffill()

        if len(dir_list) > 0:
            temp_aggreg, log_aligned = self.align_structurals(temp_aggreg, log_aligned)
            log_aligned = log_aligned.append(temp_aggreg, ignore_index=True)

        if len(dir_list) == 0:
            logging.info("No historical incremental files to load")
        logging.info("\n")


        test_data, log_aligned = self.align_structurals(test_data, log_aligned)

        if len(test_data) > 0:
            self.val_check(test_data)
            self.null_check(test_data)

        if not self.stop:
            
            if self.sector == "off":
                avail = 'avail'
                rent = 'avrent'
            elif self.sector == "ind":
                avail = 'ind_avail'
                rent = 'ind_avrent'
            elif self.sector == "ret":
                avail = 'n_avail'
                rent = 'n_avrent'
            
            logging.info("Final number of {} properties added to pool from incrementals: {:,}".format(self.sector, len(test_data)))
            logging.info("Total number of Vacancy observations added to pool from incrementals: {:,}".format(len(test_data[(test_data[avail].isnull() == False) & (test_data[avail] != '')])))
            logging.info("Total number of Rent observations added to pool from incrementals: {:,}".format(len(test_data[(test_data[rent].isnull() == False) & (test_data[rent] != '')])))
            if self.sector == "ret":
                logging.info("Total number of Anchor Vacancy observations added to pool from incrementals: {:,}".format(len(test_data[(test_data['a_avail'].isnull() == False) & (test_data['a_avail'] != '')])))
                logging.info("Total number of Anchor Rent observations added to pool from incrementals: {:,}".format(len(test_data[(test_data['a_avrent'].isnull() == False) & (test_data['a_avrent'] != '')])))
            logging.info("\n")
            
            combo = test_data.copy()
            combo['leg'] = 'no'
            combo = combo.append(log_aligned, ignore_index=True)
            combo = combo.reset_index(drop=True)
            
            test = combo.copy()
            test1 = test_data.copy()
            test1['in_test'] = 1
            test = test.join(test1.drop_duplicates('realid').set_index('realid')[['in_test']], on='realid')
            test = test[test['in_test'].isnull() == False]
            for key, value in self.type_dict.items():
                if key == "renov":
                    test['count_renov'] = test[(test['leg'] == 'yes') & (test['renov'] != -1)].drop_duplicates(['realid', 'renov']).groupby('realid')['realid'].transform('count')
                    test['count_renov'] = test.groupby('realid')['count_renov'].bfill()
                    test['count_renov'] = test.groupby('realid')['count_renov'].ffill()
                    test['count_renov'] = test['count_renov'].fillna(0)
                if value['structural']:
                    temp = test.copy()
                    temp[key] = np.where(temp[key].isnull() == True, 'temp', temp[key])
                    temp['count'] = temp.groupby(['realid'])[key].transform('nunique')
                    if key == 'renov':
                        temp = temp[(temp['count_renov'] != temp['count'])]
                    if len(temp[temp['count'] != 1]) > 0:
                        logging.info('Lack of structural consistency within an ID for {}'.format(key))
                        logging.info("\n")
                        self.stop = True
                        display(temp[temp['count'] != 1][['realid', key, 'count']].sort_values(by='realid').drop_duplicates(['realid', key]).head(10))
                        
            d_prop, combo, nc_add = self.append_nc_completions(combo, d_prop)
            
            if not self.stop:
                test_data = test_data[self.orig_cols + ['property_source_id', 'leg']]
                if len(nc_add) > 0:
                    nc_add['leg'] = False
                    test_data = test_data.append(nc_add[[x for x in test_data.columns if x in nc_add.columns]], ignore_index=False)
                test_data.to_csv('{}/OutputFiles/{}/snapshots/{}m{}_snapshot_{}.csv'.format(self.home, self.sector, self.curryr, self.currmon, self.sector), index=False)
                
                combo.sort_values(by=['metcode', 'realid'], ascending=[True, True], inplace=True)
            
        else:
            combo = pd.DataFrame()
        
        return combo, test_data, self.stop, d_prop
    
    
    def select_cols(self, combo):

        combo = combo[self.orig_cols]
        
        combo['survdate'] = pd.to_datetime(combo['survdate']).dt.strftime('%m/%d/%Y')
        
        if self.sector == "ret":
            for key, value in self.consistency_dict.items():
                combo.rename(columns={value: key}, inplace=True)
        
        return combo
    
    def calc_hist_coverage(self, log):
        
        filt = log.copy()
        
        filt = filt.rename(columns={'realyr': 'surv_yr', 'qtr': 'surv_qtr', 'ind_avail': 'avail', 'n_avail': 'avail', 'ind_avrent': 'avrent', 'n_avrent': 'avrent', 'msa': 'metcode'})

        filt['surv_yr'] = np.where((filt['surv_yr'] == '') | (filt['surv_yr'].isnull() == True), self.curryr, filt['surv_yr'])
        filt['surv_qtr'] = np.where((filt['surv_qtr'] == '') | (filt['surv_qtr'].isnull() == True), np.ceil(self.currmon / 3), filt['surv_qtr'])
        filt['survdate'] = pd.to_datetime(filt['survdate'])
        filt['survyr'] = filt['survdate'].dt.year
        filt['survmon'] = filt['survdate'].dt.month
        filt['survday'] = filt['survdate'].dt.day
        filt['reis_yr'] = np.where((filt['survmon'] == 12) & (filt['survday'] > 15), filt['survyr'] + 1, filt['survyr'])
        filt['reis_mon'] = np.where((filt['survmon'] == 12) & (filt['survday'] > 15), 1, filt['survmon'])
        filt['reis_mon'] = np.where((filt['survday'] > 15) & (filt['survmon'] < 12), filt['survmon'] + 1, filt['reis_mon'])
        filt['reis_qtr'] = np.ceil(filt['reis_mon'] / 3)

        filt = filt[(filt['reis_yr'] >= 2019) & (filt['reis_yr'] < 2022)]
        
        filt['avail'] = np.where(filt['avail'] == '', np.nan, filt['avail'])
        filt['avrent'] = np.where(filt['avrent'] == '', np.nan, filt['avrent'])
        
        vac = filt.copy()
        vac.sort_values(by=['avail'], ascending=[False], inplace=True)
        vac = vac.drop_duplicates(['realid', 'reis_yr', 'reis_mon'])
        vac['vac_survs'] = vac.groupby(['reis_yr', 'reis_mon'])['avail'].transform('count')
        vac.sort_values(by=['vac_survs'], ascending=[False], inplace=True)
        vac = vac.drop_duplicates(['reis_yr', 'reis_mon'])
        vac = vac[['reis_yr', 'reis_mon', 'vac_survs']]

        rent = filt.copy()
        rent.sort_values(by=['avrent'], ascending=[False], inplace=True)
        rent = rent.drop_duplicates(['realid', 'reis_yr', 'reis_mon'])
        rent['rent_survs'] = rent.groupby(['reis_yr', 'reis_mon'])['avrent'].transform('count')
        rent.sort_values(by=['rent_survs'], ascending=[False], inplace=True)
        rent = rent.drop_duplicates(['reis_yr', 'reis_mon'])
        rent = rent[['reis_yr', 'reis_mon', 'rent_survs']]
        
        aggreg = pd.DataFrame()
        for index, row in vac.iterrows():
            aggreg = aggreg.append({'year': row['reis_yr'], 'month': row['reis_mon'], 'count_survs': row['vac_survs'], 'var': 'vac'}, ignore_index=True)
        for index, row in rent.iterrows():
            aggreg = aggreg.append({'year': row['reis_yr'], 'month': row['reis_mon'], 'count_survs': row['rent_survs'], 'var': 'rent'}, ignore_index=True)
            
        aggreg.sort_values(by=['var', 'year', 'month'], ascending=[True, True, True], inplace=True)
        aggreg = aggreg[['year', 'month', 'var', 'count_survs']]
        if self.home[0:2] == 's3':
            aggreg.to_csv('{}/OutputFiles/{}/coverage_logs/nat_coverage.csv'.format(self.home, self.sector), index=False)
        
        vac = filt.copy()
        vac.sort_values(by=['avail'], ascending=[False], inplace=True)
        vac = vac.drop_duplicates(['realid', 'reis_yr', 'reis_mon'])
        vac = vac[(vac['reis_yr'] == self.curryr - 1) & (vac['reis_mon'] == self.currmon)]
        vac['vac_survs'] = vac.groupby(['metcode'])['avail'].transform('count')
        vac.sort_values(by=['vac_survs'], ascending=[False], inplace=True)
        vac = vac.drop_duplicates(['metcode'])
        vac = vac[['metcode', 'vac_survs']]
        
        rent = filt.copy()
        rent.sort_values(by=['avrent'], ascending=[False], inplace=True)
        rent = rent.drop_duplicates(['realid', 'reis_yr', 'reis_mon'])
        rent = rent[(rent['reis_yr'] == self.curryr - 1) & (rent['reis_mon'] == self.currmon)]
        rent['rent_survs'] = rent.groupby(['metcode'])['avrent'].transform('count')
        rent.sort_values(by=['rent_survs'], ascending=[False], inplace=True)
        rent = rent.drop_duplicates(['metcode'])
        rent = rent[['metcode', 'rent_survs']]
        
        aggreg_met = pd.DataFrame()
        for index, row in vac.iterrows():
            aggreg_met = aggreg_met.append({'metcode': row['metcode'], 'count_survs_hist': row['vac_survs'], 'var': 'vac'}, ignore_index=True)
        for index, row in rent.iterrows():
            aggreg_met = aggreg_met.append({'metcode': row['metcode'], 'count_survs_hist': row['rent_survs'], 'var': 'rent'}, ignore_index=True)
            
        return aggreg_met
    
    def gen_coverage_graphs(self, test_data_in, r_surv_graph, log, id_check, test_data, pre_drop, mult_prop_link):
    
        core_sq = pd.read_csv('{}/InputFiles/core_four_sqprops.csv'.format(self.home), na_values= "", keep_default_na = False, dtype={'id': int})

        core_sq = core_sq[core_sq['sector'] == self.sector.title()]

        all_mets = core_sq.copy()
        all_mets = all_mets.drop_duplicates('metcode')
        all_mets = all_mets[['metcode']]
        
        total_mets = len(all_mets)

        core_sq['county'] = core_sq['county'].str.lower().str.replace('.', '')
        core_sq['ident'] = core_sq['county'] + '/' + core_sq['state'] + '/' + core_sq['sector']

        all_data = test_data_in.copy()

        all_data['county'] = all_data['county'].str.lower().str.replace('.', '')
        all_data['category'] = all_data['category'].str.lower()

        live = all_data.copy()

        if self.sector == 'off':
            category = 'office'
        elif self.sector == "ind":
            category = 'industrial'
        elif self.sector == "ret":
            category = 'retail'
        all_data = all_data[(all_data['category'] == category)]
        all_data['ident'] = all_data['county'] + '/' + all_data['state'] + '/' + self.sector.title()

        all_data = all_data.join(core_sq.drop_duplicates('ident').set_index('ident').rename(columns={'metcode': 'met_reis'})[['met_reis']], on='ident')
        logging.info('{:,} do not have a reis met joined for graphing, {:.1%} of the total properties in the raw file'.format(len(all_data[(all_data['met_reis'].isnull() == True)].drop_duplicates('property_source_id')), len(all_data[(all_data['met_reis'].isnull() == True)].drop_duplicates('property_source_id')) / len(all_data.drop_duplicates('property_source_id'))))
        all_data['met_reis'] = np.where((all_data['met_reis'].isnull() == True) & (all_data['property_geo_msa_list'] != '') & (all_data['property_geo_msa_list'].str.split(',').str[0].isin(all_mets['metcode'].unique())), all_data['property_geo_msa_list'].str.split(',').str[0], all_data['met_reis'])
        logging.info('{:,} do not have a reis met filled at all for graphing, {:.1%} of the total properties in the raw file'.format(len(all_data[(all_data['met_reis'].isnull() == True)].drop_duplicates('property_source_id')), len(all_data[(all_data['met_reis'].isnull() == True)].drop_duplicates('property_source_id')) / len(all_data.drop_duplicates('property_source_id'))))
        base = all_data.copy()
        all_data = all_data[(all_data['met_reis'].isnull() == False) & (all_data['met_reis'] != '')]

        temp = all_data.copy()
        temp = temp[temp['met_reis'].isnull() == False]
        temp['has_rent'] = np.where((temp['lease_asking_rent_max_amt'].isnull() == False) | (temp['lease_asking_rent_min_amt'].isnull() == False) | (temp['lease_transaction_rent_price_max_amt'].isnull() == False) | (temp['lease_transaction_rent_price_min_amt'].isnull() == False), 1, 0)
        temp.sort_values(by=['has_rent'], ascending=[False], inplace=True)
        temp = temp.drop_duplicates('property_source_id')
        temp['count_v_survs_raw'] = temp.groupby('met_reis')['property_source_id'].transform('count')
        temp['count_r_survs_raw'] = temp.groupby('met_reis')['has_rent'].transform('sum')
        temp = temp.drop_duplicates('met_reis')
        all_mets = all_mets.join(temp.set_index('met_reis')[['count_v_survs_raw', 'count_r_survs_raw']], on='metcode')
        all_mets['count_v_survs_raw'] = all_mets['count_v_survs_raw'].fillna(0)
        all_mets['count_r_survs_raw'] = all_mets['count_r_survs_raw'].fillna(0)

        live = live.join(id_check.drop_duplicates('property_source_id').set_index('property_source_id')[['status']], on='property_source_id')
        live = live[(live['status'].isnull() == True) | (live['status'] == 'Econ Link')]
        live_base = live.copy()
        live['ident'] = live['county'] + '/' + live['state'] + '/' + self.sector.title()
        live = live.join(core_sq.drop_duplicates('ident').set_index('ident').rename(columns={'metcode': 'met_reis'})[['met_reis']], on='ident')
        live['met_reis'] = np.where((live['met_reis'].isnull() == True) & (live['property_geo_msa_list'] != '') & (live['property_geo_msa_list'].str.split(',').str[0].isin(all_mets['metcode'].unique())), live['property_geo_msa_list'].str.split(',').str[0], live['met_reis'])
        live = live[(live['met_reis'].isnull() == False) & (live['met_reis'] != '')]
        live['has_rent'] = np.where((live['lease_asking_rent_max_amt'].isnull() == False) | (live['lease_asking_rent_min_amt'].isnull() == False) | (live['lease_transaction_rent_price_max_amt'].isnull() == False) | (live['lease_transaction_rent_price_min_amt'].isnull() == False), 1, 0)
        live.sort_values(by=['has_rent'], ascending=[False], inplace=True)
        live = live.drop_duplicates('property_source_id')
        live['count_v_survs_live'] = live.groupby('met_reis')['property_source_id'].transform('count')
        live['count_r_survs_live'] = live.groupby('met_reis')['has_rent'].transform('sum')
        live = live.drop_duplicates('met_reis')
        all_mets = all_mets.join(live.set_index('met_reis')[['count_v_survs_live', 'count_r_survs_live']], on='metcode')
        all_mets['count_v_survs_live'] = all_mets['count_v_survs_live'].fillna(0)
        all_mets['count_r_survs_live'] = all_mets['count_r_survs_live'].fillna(0)
        
        v_surv = test_data.copy()
        v_surv['count_v_survs_filt'] = v_surv.groupby('metcode')['realid'].transform('count')
        v_surv = v_surv.drop_duplicates('metcode')
        all_mets = all_mets.join(v_surv.set_index('metcode')[['count_v_survs_filt']], on='metcode')
        all_mets['count_v_survs_filt'] = all_mets['count_v_survs_filt'].fillna(0)

        r_surv_graph['count_r_survs_filt'] = r_surv_graph.groupby('metcode')['has_r_surv'].transform('count')
        r_surv_graph = r_surv_graph.drop_duplicates('metcode')
        all_mets = all_mets.join(r_surv_graph.set_index('metcode')[['count_r_survs_filt']], on='metcode')
        all_mets['count_r_survs_filt'] = all_mets['count_r_survs_filt'].fillna(0)

        hist_aggreg = self.calc_hist_coverage(log)
        all_mets = all_mets.join(hist_aggreg[hist_aggreg['var'] == 'vac'].set_index('metcode').rename(columns={'count_survs_hist': 'count_v_survs_hist'})[['count_v_survs_hist']], on='metcode')
        all_mets = all_mets.join(hist_aggreg[hist_aggreg['var'] == 'rent'].set_index('metcode').rename(columns={'count_survs_hist': 'count_r_survs_hist'})[['count_r_survs_hist']], on='metcode')
        all_mets['count_v_survs_hist'] = all_mets['count_v_survs_hist'].fillna(0)
        all_mets['count_r_survs_hist'] = all_mets['count_r_survs_hist'].fillna(0) 
        
        for overall_var in ['Vacancy', 'Rent']:
            aggreg = pd.DataFrame()
            if overall_var == 'Vacancy':
                cols = ['count_v_survs_raw', 'count_v_survs_live', 'count_v_survs_filt', 'count_v_survs_hist']
            elif overall_var == 'Rent':
                cols = ['count_r_survs_raw', 'count_r_survs_live', 'count_r_survs_filt', 'count_r_survs_hist']
            categories = ['Raw Data', 'Live Link Data', 'Filtered Data', 'Historical Data', ]
            colors = ['blue', 'orange', 'red', 'green']
            for var, category in zip(cols, categories):
                temp = all_mets.copy()
                temp['group'] = ''
                temp['group'] = np.where((temp[var] > 100), '100+', temp['group'])
                temp['group'] = np.where((temp[var] > 50) & (temp[var] <= 100), '51-100', temp['group'])
                temp['group'] = np.where((temp[var] > 20) & (temp[var] <= 50), '21-50', temp['group'])
                temp['group'] = np.where((temp[var] > 5) & (temp[var] <= 20), '6-20', temp['group'])
                temp['group'] = np.where((temp[var] > 0) & (temp[var] <= 5), '1-5', temp['group'])
                temp['group'] = np.where((temp[var] == 0), '0', temp['group'])
                temp['count_group'] = temp.groupby('group')['group'].transform('count')
                temp = temp.drop_duplicates('group')
                temp['category'] = category
                aggreg = aggreg.append(temp[['group', 'category','count_group']], ignore_index=True)

            aggreg_pivot = pd.pivot_table(aggreg,values="count_group",index="group",columns="category",aggfunc=np.sum)
            aggreg_pivot = aggreg_pivot[categories]
            aggreg_pivot = aggreg_pivot.reset_index()
            aggreg_pivot['order'] = np.where((aggreg_pivot['group'] == '1-5'), 2, 1)
            aggreg_pivot['order'] = np.where((aggreg_pivot['group'] == '6-20'), 3, aggreg_pivot['order'])
            aggreg_pivot['order'] = np.where((aggreg_pivot['group'] == '21-50'), 4, aggreg_pivot['order'])
            aggreg_pivot['order'] = np.where((aggreg_pivot['group'] == '51-100'), 5, aggreg_pivot['order'])
            aggreg_pivot['order'] = np.where((aggreg_pivot['group'] == '100+'), 6, aggreg_pivot['order'])
            aggreg_pivot.sort_values(by=['order'], ascending=[True], inplace=True)
            aggreg_pivot = aggreg_pivot.drop(['order'], axis=1)
            aggreg_pivot = aggreg_pivot.set_index('group')
            aggreg_pivot.plot(kind='bar', color=colors, figsize=(10,10))
            plt.xlabel('Count Survey Buckets')
            plt.ylabel('Total Metros')
            plt.xticks(ticks = range(0, len(aggreg_pivot)), labels=['0', '1-5', '6-20', '21-50', '51-100', '100+'])
            plt.title('Distribution of {} Survey Counts For {} {} Metros'.format(overall_var, total_mets, self.sector.title()))
            plt.legend(bbox_to_anchor=(1.38, 1), loc='upper right', borderaxespad=0)
            
            if self.home[0:2] == 's3':
                img_data = io.BytesIO()
                plt.savefig(img_data, format='pdf', bbox_inches='tight')
                img_data.seek(0)

                s3 = s3fs.S3FileSystem(anon=False)
                with s3.open('s3://{}/OutputFiles/{}/graphs/{}_{}_met_surv_dist_{}m{}.pdf'.format(self.home, self.sector, self.sector, overall_var.lower(), self.curryr, self.currmon), 'wb') as f:
                    f.write(img_data.getbuffer())
            else:
                plt.savefig('{}/OutputFiles/{}/graphs/{}_{}_met_surv_dist_{}m{}.png'.format(self.home, self.sector, self.sector, overall_var.lower(), self.curryr, self.currmon))
            
            plt.show()
            
        base = base.drop_duplicates('property_source_id')
        base['foundation_ids_list'] = np.where((base['foundation_ids_list'].isnull() == True), '', base['foundation_ids_list'])
        base['property_reis_rc_id'] = np.where((base['property_reis_rc_id'].isnull() == True), '', base['property_reis_rc_id'])

        live_base['in_live'] = 1
        base = base.join(live_base.drop_duplicates('property_source_id').set_index('property_source_id')[['in_live']], on='property_source_id')
        base = base.join(pre_drop.drop_duplicates('property_source_id').set_index('property_source_id').rename(columns={'id_use': 'linked_r_id'})[['linked_r_id']], on='property_source_id')
        core_sq['in_core'] = 1
        core_sq['id'] = core_sq['id'].astype(str)
        if self.sector == "off" or self.sector == "ind":
            core_sq['linked_r_id'] = core_sq['id'].str[:-1]
        else:
            core_sq['linked_r_id'] = core_sq['id']
        base['linked_r_id'] = base['linked_r_id'].astype(str)
        base = base.join(core_sq.set_index('linked_r_id')[['in_core']], on='linked_r_id')
        base = base.join(self.drop_log.drop_duplicates('c_id').set_index('c_id')[['reason']], on='property_source_id')
        in_snap = test_data.copy()
        in_snap['in_snap'] = 1
        base = base.join(in_snap.drop_duplicates('property_source_id').set_index('property_source_id')[['in_snap']], on='property_source_id')

        in_mult = mult_prop_link.copy()
        in_mult['in_mult'] = 1
        base = base.join(in_mult.drop_duplicates('id_use').set_index('id_use')[['in_mult']], on='linked_r_id')

        base['cat'] = ''
        base['cat'] = np.where((base['cat'] == '') & (base['foundation_ids_list'] == '') & (base['property_reis_rc_id'] == '') & (base['in_snap'].isnull() == True) & (base['in_mult'].isnull() == True), 'No ER Link', base['cat'])
        base['cat'] = np.where((base['cat'] == '') & (base['foundation_ids_list'].str.contains(self.sector_map[self.sector]['prefix']) == False) & (base['property_reis_rc_id'].str.contains(self.sector_map[self.sector]['prefix']) == False) & (base['in_snap'].isnull() == True) & (base['in_mult'].isnull() == True), 'No Sector Link', base['cat'])
        base['cat'] = np.where((base['cat'] == '') & (base['in_live'].isnull() == True) & (base['in_snap'].isnull() == True) & (base['in_mult'].isnull() == True), 'No Live Link', base['cat'])
        #base['cat'] = np.where((base['cat'] == '') & ((base['met_reis'].isnull() == True) | (base['met_reis'] == '')) & (base['in_snap'].isnull() == True) & (base['in_mult'].isnull() == True), 'Not In REIS Metro', base['cat'])
        base['cat'] = np.where((base['cat'] == '') & ((base['in_core'].isnull() == True) | (((base['met_reis'].isnull() == True) | (base['met_reis'] == '')) & (base['in_snap'].isnull() == True))) & (base['reason'].isnull() == True), 'No Square Link', base['cat'])
        base['cat'] = np.where((base['cat'] == '') & (base['reason'].isnull() == False), 'Dropped - Structural Diff', base['cat'])
        base['cat'] = np.where((base['cat'] == '') & ((base['in_snap'] == 1) | (base['in_mult'] == 1)), 'In Square Pool', base['cat'])
        
        cat_colors = {'No ER Link': 'red', 'No Sector Link': 'blue', 'No Live Link': 'orange', 
                      'No Square Link': 'purple', 'Dropped - Structural Diff': 'brown', 'In Square Pool': 'green'}
                
        base['count_cat'] = base.groupby('cat')['property_source_id'].transform('count')
        base = base.drop_duplicates('cat')
        base.sort_values(by=['cat'], ascending=[False], inplace=True)
        
        sizes = []
        colors = []
        groups = base['cat'].unique()
        for x in range(0, len(groups)):
            sizes.append(base[base['cat'] == groups[x]].reset_index().loc[0]['count_cat'])
            colors.append(cat_colors[groups[x]])

        fig_pie = plt.figure(figsize=(30,30))
        patches, texts, pcts = plt.pie(sizes, labels=groups, colors=colors, autopct='%1.1f%%', labeldistance=1.1, startangle=180, rotatelabels=True, textprops={'fontsize': 24})
        plt.setp(pcts, color='white', fontweight='bold')
        plt.axis('equal')
        if self.home[0:2] == 's3':
            img_data = io.BytesIO()
            plt.savefig(img_data, format='pdf', bbox_inches='tight')
            img_data.seek(0)

            s3 = s3fs.S3FileSystem(anon=False)
            with s3.open('s3://{}/OutputFiles/{}/graphs/{}_coverage_pie_{}m{}.pdf'.format(self.home, self.sector, self.sector, self.curryr, self.currmon), 'wb') as f:
                f.write(img_data.getbuffer())
        else:
            plt.savefig('{}/OutputFiles/{}/graphs/{}_coverage_pie_{}m{}.png'.format(self.home, self.sector, self.sector, self.curryr, self.currmon))

        plt.show()

    def outsheet_files(self, log, test_data_in, test_data, id_check, sector, sectors, l_col_order, d_col_order, size_method, pre_drop, mult_prop_link):
        
        temp_add = test_data_in.copy()
        temp_add = temp_add.drop_duplicates('property_source_id')
        temp_add = temp_add.set_index('property_source_id')
        temp_add['metcode'] = temp_add['property_geo_msa_list'].str.split(',').fillna('').str[0]
        temp_add['property_geo_subid_list'] = np.where((temp_add['property_geo_subid_list'].isnull() == True), '', temp_add['property_geo_subid_list'])
        temp_add['property_geo_subid_list'] = temp_add['property_geo_subid_list'].astype(str)
        temp_add['subid'] = temp_add['property_geo_subid_list'].str.split(',').fillna('').str[0]
        temp_add = temp_add[['metcode', 'subid', 'property_er_id', 'category', 'subcategory', 'street_address', 'city', 'state', 'zip']]
        for col in temp_add:
            temp_add.rename(columns={col: 'c_' + col}, inplace=True)
        self.logic_log = self.logic_log.join(temp_add, on='property_source_id')

        temp_add_log = log.copy()
        temp_add_log = temp_add_log.drop_duplicates('realid')
        temp_add_log = temp_add_log.set_index('realid')
        temp_add_log = temp_add_log[['metcode', 'subid', 'address', 'city', 'state', 'zip']]
        for col in temp_add_log:
            temp_add_log.rename(columns={col: 'r_' + col}, inplace=True)
        self.logic_log = self.logic_log.join(temp_add_log, on='realid')
        self.logic_log = self.logic_log.rename(columns={'realid': 'r_id'})
        self.logic_log['r_sector'] = self.sector
        
        has_r_surv = test_data.copy()
        if self.sector == "off":
            rent = 'avrent'
        elif self.sector == "ind":
            rent = 'ind_avrent'
        elif self.sector == "ret":
            rent = 'n_avrent'
        
        has_r_surv = has_r_surv[has_r_surv[rent].isnull() == False]
        has_r_surv['has_r_surv'] = 1
        self.logic_log = self.logic_log.join(has_r_surv.set_index('property_source_id')[['has_r_surv']], on='property_source_id')
        self.logic_log['rent_status'] = ''
        self.logic_log['rent_status'] = np.where(((self.logic_log['flag'] == 'Range Outlier - Space Level') | (self.logic_log['flag'] == 'Performance Differance to Last REIS Observation')) & (self.logic_log['status'] == 'dropped') & (self.logic_log['column'].isin(['avrent', 'ind_avrent', 'n_avrent'])) & (self.logic_log['has_r_surv'].isnull() == True), 'Lost Rent Survey', self.logic_log['rent_status'])
        self.logic_log = self.logic_log.drop(['has_r_surv'], axis=1)
        
        r_surv_graph = has_r_surv.copy()
        
        if self.sector == "ret":
            has_r_surv = test_data.copy()
            has_r_surv = has_r_surv[has_r_surv['a_avrent'].isnull() == False]
            has_r_surv['has_r_surv'] = 1
            self.logic_log = self.logic_log.join(has_r_surv.set_index('property_source_id')[['has_r_surv']], on='property_source_id')
            self.logic_log['rent_status'] = np.where(((self.logic_log['flag'] == 'Range Outlier - Space Level') | (self.logic_log['flag'] == 'Performance Differance to Last REIS Observation')) & (self.logic_log['status'] == 'dropped') & (self.logic_log['column'] == 'a_avrent') & (self.logic_log['has_r_surv'].isnull() == True), 'Lost Rent Survey', self.logic_log['rent_status'])
            self.logic_log = self.logic_log.drop(['has_r_surv'], axis=1)
            
        if sector == sectors[0]:
            c_cols = []
            r_cols = []
            extra_cols = []
            first_cols = ['property_source_id', 'listed_space_id', 'flag', 'status', 'rent_status']
            for col in self.logic_log.columns:
                if col in first_cols:
                    continue
                elif col[0:2] == 'c_':
                    c_cols.append(col)
                elif col[0:2] == 'r_':
                    r_cols.append(col)
                else:
                    extra_cols.append(col)
                l_col_order = first_cols + c_cols + r_cols + extra_cols
        else:
            for col in self.logic_log.columns:
                if col not in l_col_order:
                    l_col_order = l_col_order + [col]
        self.logic_log = self.logic_log[l_col_order]
        size_method['property_source_id'] = size_method['property_source_id'].astype(str)
        self.logic_log = self.logic_log.join(size_method.drop_duplicates('property_source_id').set_index('property_source_id')[['size_method']], on='property_source_id')
        self.logic_log.to_csv("{}/OutputFiles/{}/logic_logs/logic_log_{}m{}.csv".format(self.home, self.sector, self.curryr, self.currmon), index=False)

        self.drop_log['realid'] = np.where((self.drop_log['realid'].isnull() == True) | (self.drop_log['realid'] == ''), self.drop_log['id_use'], self.drop_log['realid'])
        self.drop_log['realid'] = np.where((self.drop_log['realid'].str[0] != self.sector_map[self.sector]['prefix']) & (self.drop_log['realid'] != '') & (self.drop_log['realid'].isnull() == False) & (self.drop_log['realid'].str.contains(',') == False), self.sector_map[self.sector]['prefix'] + self.drop_log['realid'], self.drop_log['realid'])
        self.drop_log = self.drop_log.rename(columns={'realid': 'r_id', 'property_source_id': 'c_id'})
        self.drop_log = self.drop_log.drop(['id_use'],axis=1)
        temp = id_check.copy()
        temp['ids_to_add'] = np.where((temp['ids_to_add'] == '') & (temp['ids_to_add'].isnull() == True), temp['id_use'], temp['ids_to_add'])
        temp = temp[['property_source_id', 'ids_to_add', 'flag']]
        temp = temp.rename(columns={'property_source_id': 'c_id', 'ids_to_add': 'r_id', 'flag': 'reason'})
        self.drop_log = self.drop_log.append(temp, ignore_index=True)
        if not self.use_mult:
            temp = self.logic_log.copy()
            temp = temp[(temp['flag'].str.contains('Multiple Same Sector') == True)]
            temp['property_source_id'] = temp['property_source_id'].astype(str).str.split('.').str[0]
            temp = temp.drop_duplicates('property_source_id')
            self.drop_log = self.drop_log.join(temp.rename(columns={'property_source_id': 'c_id'}).set_index('c_id')[['flag']], on='c_id')
            self.drop_log['reason'] = np.where((self.drop_log['flag'].isnull() == False), self.drop_log['flag'], self.drop_log['reason'])
            self.drop_log = self.drop_log.drop(['flag'], axis=1)
        
        self.drop_log = self.drop_log.join(temp_add, on='c_id')
        self.drop_log['realid'] = self.drop_log['r_id'].str[1:]
        self.drop_log = self.drop_log.join(temp_add_log, on='realid')
        self.drop_log = self.drop_log.drop(['realid'], axis=1)
        self.drop_log['r_sector'] = self.sector
        
        self.drop_log = self.drop_log.rename(columns={'space_category': 'c_space_category'})

        if sector == sectors[0]:
            c_cols = []
            r_cols = []
            extra_cols = []
            first_cols = ['c_id', 'reason']
            for col in self.drop_log.columns:
                if col in first_cols:
                    continue
                elif col[0:2] == 'c_':
                    c_cols.append(col)
                elif col[0:2] == 'r_':
                    r_cols.append(col)
                else:
                    extra_cols.append(col)
                d_col_order = first_cols + c_cols + r_cols + extra_cols
        else:
            for col in self.drop_log.columns:
                if col not in d_col_order:
                    d_col_order = d_col_order + [col]
        self.drop_log = self.drop_log[d_col_order]
        self.drop_log.to_csv("{}/OutputFiles/{}/logic_logs/drop_log_{}m{}.csv".format(self.home, self.sector, self.curryr, self.currmon), index=False)
    
        self.gen_coverage_graphs(test_data_in, r_surv_graph, log, id_check, test_data, pre_drop, mult_prop_link)
    
        if sector == sectors[-1] and len(sectors) == 3:
            aggreg_drop = pd.DataFrame()
            aggreg_logic = pd.DataFrame()
            aggreg_snaps = pd.DataFrame()
            aggreg_mult_link = pd.DataFrame()
            
            for sector in sectors:
            
                root = "{}/OutputFiles/{}/logic_logs".format(self.home, sector)
            
                if self.home[0:2] == 's3': 
                    file_read = '{}{}'.format(self.prefix, root)
                else:
                    file_read = root
                path = '{}/drop_log_{}m{}.csv'.format(file_read, self.curryr, self.currmon)
                temp = pd.read_csv(path, sep=',', encoding = 'utf-8',  na_values= "", keep_default_na = False)
                aggreg_drop = aggreg_drop.append(temp, ignore_index=True)

                path = '{}/logic_log_{}m{}.csv'.format(file_read, self.curryr, self.currmon)
                temp = pd.read_csv(path, sep=',', encoding = 'utf-8',  na_values= "", keep_default_na = False)
                aggreg_logic = aggreg_logic.append(temp, ignore_index=True)
                
                path = '{}/mult_prop_link_{}m{}.csv'.format(file_read, self.curryr, self.currmon)
                temp = pd.read_csv(path, sep=',', encoding = 'utf-8',  na_values= "", keep_default_na = False)
                temp['r_sector'] = sector
                aggreg_mult_link = aggreg_mult_link.append(temp, ignore_index=True)
                
                root_snap = "{}/OutputFiles/{}/snapshots".format(self.home, sector)
                file_read = '{}{}'.format(self.prefix, root_snap)
                path = '{}/{}m{}_snapshot_{}.csv'.format(file_read, self.curryr, self.currmon, sector)
                temp = pd.read_csv(path, sep=',', encoding = 'utf-8',  na_values= "", keep_default_na = False)
                if sector == "off":
                    temp['has_vac'] = np.where((temp['avail'].isnull() == False), 1, 0)
                    temp['has_rent'] = np.where((temp['avrent'].isnull() == False), 1, 0)
                elif sector == "ind":
                    temp['has_vac'] = np.where((temp['ind_avail'].isnull() == False), 1, 0)
                    temp['has_rent'] = np.where((temp['ind_avrent'].isnull() == False), 1, 0)
                elif sector == "ret":
                    temp['has_vac'] = np.where((temp['n_avail'].isnull() == False) | (temp['a_avail'].isnull() == False), 1, 0)
                    temp['has_rent'] = np.where((temp['n_avrent'].isnull() == False) | (temp['a_avrent'].isnull() == False), 1, 0)
                temp = temp[['property_source_id', 'realid', 'has_vac', 'has_rent']]
                temp['r_sector'] = sector
                aggreg_snaps = aggreg_snaps.append(temp, ignore_index=True)

            aggreg_drop['reason'] = np.where((aggreg_drop['reason'] == 'property not linked to REIS id in this sector') | (aggreg_drop['reason'] == 'property linked to REIS id that is not live'), 'property linked to REIS id that is not squarable', aggreg_drop['reason'])
            aggreg_drop.sort_values(by=['c_id', 'reason'], ascending=[False, False], inplace=True)
            aggreg_drop['count'] = aggreg_drop.groupby(['c_id', 'reason'])['c_id'].transform('count')
            aggreg_drop['cumcount'] = aggreg_drop.groupby(['c_id', 'reason'])['c_id'].transform('cumcount')
            aggreg_drop = aggreg_drop[(~aggreg_drop['reason'].isin(['property linked to REIS id that is not squarable', 'no REIS Catylist ER link', 'Survey is after end of REIS monthly survey window'])) | ((aggreg_drop['count'] == 3) & (aggreg_drop['cumcount'] == 0))]
            aggreg_drop = aggreg_drop.drop(['count', 'cumcount'], axis=1)
            aggreg_drop['r_sector'] = np.where((aggreg_drop['reason'] == 'no REIS Catylist ER link') | (aggreg_drop['reason'] == 'property linked to REIS id that is not squarable'), '', aggreg_drop['r_sector'])
            
            aggreg_drop['has_core'] = np.where((aggreg_drop['reason'].str.contains('core structural') == True), 1, 0)
            aggreg_drop['sum_has_core'] = aggreg_drop.groupby('c_id')['has_core'].transform('sum')
            aggreg_drop['count'] = aggreg_drop.groupby('c_id')['c_id'].transform('count')
            aggreg_drop['drop_this'] = np.where((aggreg_drop['count'] > 1) & (aggreg_drop['has_core'] == 0) & (aggreg_drop['sum_has_core'] > 0), 1, 0)
            aggreg_drop = aggreg_drop[aggreg_drop['drop_this'] == 0]
            aggreg_drop['match_cat'] = 0
            aggreg_drop['match_cat'] = np.where((aggreg_drop['r_sector'] == 'off') & (aggreg_drop['c_category'] == 'OFFICE'), 1, aggreg_drop['match_cat'])
            aggreg_drop['match_cat'] = np.where((aggreg_drop['r_sector'] == 'ind') & (aggreg_drop['c_category'] == 'INDUSTRIAL'), 1, aggreg_drop['match_cat'])
            aggreg_drop['match_cat'] = np.where((aggreg_drop['r_sector'] == 'ret') & (aggreg_drop['c_category'] == 'RETAIL'), 1, aggreg_drop['match_cat'])
            aggreg_drop['count_match_cat'] = aggreg_drop.groupby('c_id')['match_cat'].transform('sum')
            aggreg_drop['cumcount'] = aggreg_drop.groupby(['c_id'])['c_id'].transform('cumcount')
            aggreg_drop['count'] = aggreg_drop.groupby('c_id')['c_id'].transform('count')
            aggreg_drop['drop_this'] = np.where((aggreg_drop['count'] > 1) & (((aggreg_drop['match_cat'] == 0) & (aggreg_drop['count_match_cat'] > 0)) | ((aggreg_drop['count_match_cat'] == 0) & (aggreg_drop['cumcount'] > 0))), 1, 0)
            aggreg_drop = aggreg_drop[aggreg_drop['drop_this'] == 0]
            aggreg_drop = aggreg_drop.drop(['match_cat', 'count_match_cat', 'cumcount', 'count', 'drop_this', 'has_core', 'sum_has_core'],axis=1)
            
            aggreg_drop = aggreg_drop.join(aggreg_snaps.drop_duplicates('property_source_id').set_index('property_source_id')[['has_vac']], on='c_id')
            aggreg_drop = aggreg_drop[aggreg_drop['has_vac'].isnull() == True]
            aggreg_drop = aggreg_drop.drop(['has_vac'],axis=1)
            
            aggreg_drop.to_csv('{}/OutputFiles/aggreg_logs/all_drops_{}m{}.csv'.format(self.home, self.curryr, self.currmon), index=False)
            aggreg_logic.to_csv('{}/OutputFiles/aggreg_logs/all_logic_{}m{}.csv'.format(self.home, self.curryr, self.currmon), index=False)
            aggreg_snaps.to_csv('{}/OutputFiles/aggreg_logs/all_snaps_{}m{}.csv'.format(self.home, self.curryr, self.currmon), index=False)
            aggreg_mult_link.to_csv('{}/OutputFiles/aggreg_logs/all_mult_link_{}m{}.csv'.format(self.home, self.curryr, self.currmon), index=False)
            
            aggreg_snaps['in'] = 1
            aggreg_mult_link['in_mult'] = 1
            
            test_drops = test_data_in.copy()
            test_drops = test_drops.join(aggreg_snaps.drop_duplicates('property_source_id').set_index('property_source_id')[['in']], on='property_source_id')
            test_drops = test_drops.join(aggreg_mult_link.drop_duplicates('property_source_id').set_index('property_source_id')[['in_mult']], on='property_source_id')
            test_drops = test_drops.join(aggreg_drop.drop_duplicates('c_id').set_index('c_id')[['reason']], on='property_source_id')
            test_drops = test_drops[(test_drops['in'].isnull() == True) & (test_drops['in_mult'].isnull() == True) & (test_drops['reason'].isnull() == True)]
            if len(test_drops) > 0:
                logging.info("There are property level drops unaccounted for in the drop log")
                logging.info("\n")
                print("There are property level drops unaccounted for in the drop log")
                test_drops.drop_duplicates('property_source_id').to_csv('{}/OutputFiles/aggreg_logs/missing_drops.csv'.format(self.home), index=False)
                song_alert(sound='bowl')
            
        return l_col_order, d_col_order
    
    def update_ph_log(self, test_data, ph_log):
        
        orig_cols = list(ph_log.columns)
        
        temp = test_data.copy()
        temp = temp[temp['leg']]
        temp['realid'] = temp['realid'].astype(int)
        
        ph_log['min_phase'] = ph_log.groupby('id')['phase'].transform('min')
        
        ph_log = ph_log.join(temp.rename(columns={'realid': 'id', 'tot_size': 'tot_size_c', 'n_size': 'n_size_c', 'a_size': 'a_size_c'}).set_index('id')[['tot_size_c', 'n_size_c', 'a_size_c']], on='id')
        
        ph_log = ph_log[(ph_log['phase'] == ph_log['min_phase']) | (ph_log['tot_size_c'].isnull() == True) | (ph_log['phase'].isnull() == True)]
        
        ph_log['totsize'] = np.where((ph_log['tot_size_c'].isnull() == False), ph_log['tot_size_c'], ph_log['totsize'])
        ph_log['na_size'] = np.where((ph_log['n_size_c'].isnull() == False), ph_log['n_size_c'], ph_log['na_size'])
        ph_log['a_size'] = np.where((ph_log['a_size_c'].isnull() == False), ph_log['a_size_c'], ph_log['a_size'])
                
        ph_log = ph_log[orig_cols]
        
        for met in ph_log['msa'].unique():
            logging.info("Saving ph log file for {}".format(met))
            if self.home[0:2] == 's3':
                path = '{}/OutputFiles/{}/adjusted_phlogs/PH{}.log'.format(self.home, self.sector, met.lower())
            else:
                path = '/home/central/square/data/{}/download/test2022/PH{}.log'.format(self.sector, met.lower())
            ph_log[ph_log['msa'] == met].to_csv(r'{}'.format(path), header=ph_log.columns, index=None, sep=',', mode='w')
        logging.info("\n")
    
    
    def split_met(self, combo, log):
        
        logging.info("\n")
        if self.sector == "off" or self.sector == "ind":
            col_use = 'metcode'
        elif self.sector == "ret":
            col_use = 'msa'
        
        if len(combo[(combo[col_use].isnull() == True) | (combo[col_use] == '')]) > 0:
            logging.info('Dropping {:,} IDs that do not have a metro'.format(len(combo[(combo[col_use].isnull() == True) | (combo[col_use] == '')])))
            logging.info('\n')
        combo = combo[(combo[col_use].isnull() == False) & (combo[col_use] != '')]
        
        for met in log['metcode'].unique():
            if met == 'nan':
                continue
            logging.info("Saving file for {}".format(met))
            if self.home[0:2] == 's3':
                path = 's3://ma-cre-development-sandbox/data-analytics/bb/log_preprocess/OutputFiles/{}/preprocessed_logs/{}.log'.format(self.sector, met.lower())
            else:
                path = '/home/central/square/data/{}/download/test2022/{}.log'.format(self.sector, met.lower())
            combo[combo[col_use] == met].to_csv(r'{}'.format(path), header=combo.columns, index=None, sep=',', mode='w')
        logging.info("\n")