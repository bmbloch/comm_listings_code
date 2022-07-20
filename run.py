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

from prep_logs import PrepareLogs

def get_home():
    if os.name == "nt": return "//odin/reisadmin/central/square/data/zzz-bb-test2/python/catylist_snapshots"
    else: return "/home/central/square/data/zzz-bb-test2/python/catylist_snapshots"


def get_dicts():

    consistency_dict = {'ret': {
                                'id': 'realid',
                                'surv_date': 'survdate',
                                'msa': 'metcode',
                                'gsub': 'subid',
                                'x_long': 'x',
                                'y_lat': 'y',
                                'first_year': 'year',
                                'renov_year': 'renov',
                                'name': 'propname',
                                'addr': 'address',
                                'st': 'state',
                                'expoper': 'op_exp',
                                'rent_basis': 'rnt_term',
                                }
                       }

    rename_dict_all = {'off': {
                                'realid': 'id_use',
                                'survdate': 'surv_date',
                                'subid': 'gsub',
                                'size': 'tot_size',
                                'propname': 'property_name',
                                'year': 'first_year',
                                'month': 'buildings_construction_expected_completion_month',
                                'renov': 'renov_year',
                                'flrs': 'buildings_physical_characteristics_numberoffloors',
                                'bldgs': 'buildings_number_of_buildings',
                                'avail': 'prop_dir_avail',
                                'sublet': 'prop_sub_avail',
                                'avrent': 'prop_avrent',
                                'lse_term': 'prop_term',
                                'ti2': 'lease_transaction_tenantimprovementallowancepsf_amount',
                                'free_re': 'lease_transaction_freerentmonths',
                                'comm1': 'commission_amount_percentage',
                                'c_rent': 'prop_crd',
                                'x': 'x_long',
                                'y': 'y_lat',
                                're_tax': 'prop_retax',
                                'fipscode': 'parcel_fips',
                                'rnt_term': 'rent_basis',
                                'op_exp': 'prop_opex'
                          },
                       'ind': { 
                                'realid': 'id_use',
                                'realyr': 'surv_yr',
                                'qtr': 'surv_qtr',
                                'survdate': 'surv_date',
                                'subid': 'gsub',
                                'ind_size': 'tot_size',
                                'propname': 'property_name',
                                'year': 'first_year',
                                'month': 'buildings_construction_expected_completion_month',
                                'renov': 'renov_year',
                                'flrs': 'buildings_physical_characteristics_numberoffloors',
                                'bldgs': 'buildings_number_of_buildings',
                                'ind_avail': 'prop_dir_avail',
                                'sublet': 'prop_sub_avail',
                                'ind_avrent': 'prop_avrent',
                                'ti2': 'lease_transaction_tenantimprovementallowancepsf_amount',
                                'free_re': 'lease_transaction_freerentmonths',
                                'lse_term': 'prop_term',
                                'comm1': 'commission_amount_percentage',
                                'c_rent': 'prop_crd',
                                'x': 'x_long',
                                'y': 'y_lat',
                                're_tax': 'prop_retax',
                                'fipscode': 'parcel_fips',
                                'docks': 'buildings_physical_characteristics_doors_crossdockdoors_count', 
                                'dockhigh_doors': 'buildings_physical_characteristics_doors_dockhighdoors_count',
                                'drivein_doors': 'buildings_physical_characteristics_doors_gradeleveldoors_count',
                                'rail_doors': 'buildings_physical_characteristics_doors_raildoors_count',
                                'ceil_low': 'buildings_physical_characteristics_clear_height_min_ft',
                                'ceil_high': 'buildings_physical_characteristics_clear_height_max_ft',
                                'rnt_term': 'rent_basis',
                                'op_exp': 'prop_opex'
                              },
                       'ret': {
                                'realid': 'id_use',
                                'survdate': 'surv_date',
                                'subid': 'gsub',
                                'propname': 'property_name',
                                'year': 'first_year',
                                'month': 'buildings_construction_expected_completion_month',
                                'renov': 'renov_year',
                                'n_avail': 'n_prop_dir_avail',
                                'a_avail': 'a_prop_dir_avail',
                                'n_avrent': 'n_prop_avrent',
                                'a_avrent': 'a_prop_avrent',
                                'nc_rent': 'n_prop_crd',
                                'ac_rent': 'a_prop_crd',
                                'free_re': 'n_prop_free',
                                'a_freere': 'a_prop_free',
                                'anc_term': 'a_prop_term',
                                'non_term': 'n_prop_term',
                                'cam': 'prop_cam',
                                'ti2': 'n_prop_ti',
                                'a_ti': 'a_prop_ti',
                                'comm1': 'n_prop_comm',
                                'a_comm': 'a_prop_comm',
                                'x': 'x_long',
                                'y': 'y_lat',
                                're_tax': 'prop_retax',
                                'fipscode': 'parcel_fips',
                                'rnt_term': 'rent_basis',
                                'op_exp': 'prop_opex'
                              }
                    }


    type_dict_all = {'off': {
                        'realid': {'type': 'str', 'sign': 'na', 'null_check': "only", 'range_lev': '', 'structural': False, 'promote': ''},
                        'phase': {'type': 'float', 'sign': 'na', 'null_check': "no", 'range_lev': 'minmax', 'structural': False, 'promote': ''},
                        'type2': {'type': 'str', 'sign': 'na', 'null_check': "no", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'surv_yr': {'type': 'float', 'sign': 'pos', 'null_check': "only", 'range_lev': '', 'structural': False, 'promote': ''},
                        'surv_qtr': {'type': 'float', 'sign': 'pos', 'null_check': "no", 'range_lev': '', 'structural': False, 'promote': ''},
                        'source': {'type': 'str', 'sign': 'na', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': ''},
                        'surstat': {'type': 'str', 'sign': 'na', 'null_check': "no", 'range_lev': '', 'structural': True, 'promote': ''},
                        'survdate': {'type': 'str', 'sign': 'na', 'null_check': "only", 'range_lev': '', 'structural': False, 'promote': ''},
                        'metcode': {'type': 'str', 'sign': 'na', 'null_check': "no", 'range_lev': '', 'structural': True, 'promote': 'R'},
                        'subid': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': True, 'promote': 'R'},
                        'propname': {'type': 'str', 'sign': 'na', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'address': {'type': 'str', 'sign': 'na', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'city': {'type': 'str', 'sign': 'na', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'zip': {'type': 'float', 'sign': 'pos', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'county': {'type': 'str', 'sign': 'na', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'state': {'type': 'str', 'sign': 'na', 'null_check': "no", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'year': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': 'min', 'structural': True, 'promote': 'R'},
                        'month': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': 'minmax', 'structural': True, 'promote': 'R'},
                        'renov': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': 'min', 'structural': True, 'promote': 'C'},
                        'expren': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': ''},
                        'size': {'type': 'float', 'sign': 'pos', 'null_check': "only", 'range_lev': '1p99p', 'structural': True, 'promote': 'C'},
                        'g_n': {'type': 'str', 'sign': 'na', 'null_check': "no", 'range_lev': '', 'structural': False, 'promote': ''},
                        'p_gsize': {'type': 'float', 'sign': 'pos', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'p_nsize': {'type': 'float', 'sign': 'pos', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        's_gsize': {'type': 'float', 'sign': 'pos', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        's_nsize': {'type': 'float', 'sign': 'pos', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'status': {'type': 'str', 'sign': 'na', 'null_check': "no", 'range_lev': '', 'structural': True, 'promote': ''},
                        'flrs': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': True, 'promote': 'R'},
                        'bldgs': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': True, 'promote': 'R'},
                        'parking': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': ''},
                        'avail': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': 'C'},
                        'contig': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': 'C'},
                        'sublet': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': 'C'},
                        'lowrent': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': 'C'},
                        'hirent': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': 'C'},
                        'avrent': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'avrent_f': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': 'C'},
                        'c_rent': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'gross_re': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': ''},
                        'exp_flag': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': ''},
                        'rnt_term': {'type': 'str', 'sign': 'na', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': 'C'},
                        'lse_term': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'ti1': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'ti2': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'ti_renew': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'free_re': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': 'max', 'structural': False, 'promote': 'C'},
                        'comm1': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': 'max', 'structural': False, 'promote': 'C'},
                        'comm2': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': 'max', 'structural': False, 'promote': 'C'},
                        'op_exp': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'expstop': {'type': 'str', 'sign': 'na', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': ''},
                        'lossfact': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': ''},
                        'passthru': {'type': 'str', 'sign': 'na', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': ''},
                        'escal': {'type': 'str', 'sign': 'na', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': ''},
                        'x': {'type': 'float', 'sign': 'neg', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'y': {'type': 'float', 'sign': 'pos', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'conv_yr': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': True, 'promote': 'R'},
                        'code_out': {'type': 'str', 'sign': 'na', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': ''},
                        're_tax': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'fipscode': {'type': 'float', 'sign': 'pos', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'}
                    },
                'ind': {
                        'realid': {'type': 'str', 'sign': 'na', 'null_check': "only", 'range_lev': '', 'structural': False, 'promote': ''},
                        'phase': {'type': 'float', 'sign': 'na', 'null_check': "no", 'range_lev': 'minmax', 'structural': False, 'promote': ''},
                        'type2': {'type': 'str', 'sign': 'na', 'null_check': "no", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'realyr': {'type': 'float', 'sign': 'pos', 'null_check': "no", 'range_lev': '', 'structural': False, 'promote': ''},
                        'qtr': {'type': 'float', 'sign': 'pos', 'null_check': "no", 'range_lev': '', 'structural': False, 'promote': ''},
                        'source': {'type': 'str', 'sign': 'na', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': ''},
                        'surstat': {'type': 'str', 'sign': 'na', 'null_check': "no", 'range_lev': '', 'structural': True, 'promote': ''},
                        'survdate': {'type': 'str', 'sign': 'na', 'null_check': "only", 'range_lev': '', 'structural': False, 'promote': ''},
                        'metcode': {'type': 'str', 'sign': 'na', 'null_check': "no", 'range_lev': '', 'structural': True, 'promote': 'R'},
                        'subid': {'type': 'float', 'sign': 'pos', 'null_check': "no", 'range_lev': '', 'structural': True, 'promote': 'R'},
                        'propname': {'type': 'str', 'sign': 'na', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'address': {'type': 'str', 'sign': 'na', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'city': {'type': 'str', 'sign': 'na', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'zip': {'type': 'float', 'sign': 'pos', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'county': {'type': 'str', 'sign': 'na', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'state': {'type': 'str', 'sign': 'na', 'null_check': "no", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'year': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': 'min', 'structural': True, 'promote': 'R'},
                        'month': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': 'minmax', 'structural': True, 'promote': 'R'},
                        'renov': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'expren': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': ''},
                        'ind_size': {'type': 'float', 'sign': 'pos', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'off_size': {'type': 'float', 'sign': 'pos', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'lease_own': {'type': 'str', 'sign': 'na', 'null_check': "ok", 'range_lev': '', 'structural': True, 'promote': 'R'},
                        'status': {'type': 'str', 'sign': 'na', 'null_check': "no", 'range_lev': '', 'structural': True, 'promote': ''},
                        'flrs': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': True, 'promote': 'R'},
                        'bldgs': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': True, 'promote': 'R'},
                        'parking': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': ''},
                        'ind_avail': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': 'C'},
                        'contig': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': 'C'},
                        'sublet': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': 'C'},
                        'ind_lowrent': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': 'C'},
                        'ind_hirent': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': 'C'},
                        'ind_avrent': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'off_lowrent': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': 'C'},
                        'off_hirent': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': 'C'},
                        'off_avrent': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': 'C'},
                        'c_rent': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'rnt_term': {'type': 'str', 'sign': 'na', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': 'C'},
                        'lse_term': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'ti1': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'ti2': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'ti_renew': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'free_re': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': 'max', 'structural': False, 'promote': 'C'},
                        'comm1': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': 'max', 'structural': False, 'promote': 'C'},
                        'comm2': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': 'max', 'structural': False, 'promote': 'C'},
                        'op_exp': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'x': {'type': 'float', 'sign': 'neg', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'y': {'type': 'float', 'sign': 'pos', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'conv_yr': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': True, 'promote': 'R'},
                        'code_out': {'type': 'str', 'sign': 'na', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': 'R'},
                        're_tax': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'fipscode': {'type': 'float', 'sign': 'pos', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'docks': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': 'zerook', 'structural': True, 'promote': 'C'},
                        'dockhigh_doors': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': 'zerook', 'structural': True, 'promote': 'C'},
                        'drivein_doors': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': 'zerook', 'structural': True, 'promote': 'C'},
                        'rail_doors': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': 'zerook', 'structural': True, 'promote': 'C'},
                        'ceil_low': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': True, 'promote': 'C'},
                        'ceil_high': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': True, 'promote': 'C'},
                        'ceil_avg': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': True, 'promote': 'C'},
                        'selected': {'type': 'str', 'sign': 'na', 'null_check': "no", 'range_lev': '', 'structural': True, 'promote': 'R'},
                    },
                'ret': {
                        'realid': {'type': 'str', 'sign': 'na', 'null_check': "only", 'range_lev': '', 'structural': False, 'promote': ''},
                        'type1': {'type': 'str', 'sign': 'na', 'null_check': "no", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'type2': {'type': 'str', 'sign': 'na', 'null_check': "no", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'surv_yr': {'type': 'float', 'sign': 'pos', 'null_check': "only", 'range_lev': '', 'structural': False, 'promote': ''},
                        'surv_qtr': {'type': 'float', 'sign': 'pos', 'null_check': "no", 'range_lev': '', 'structural': False, 'promote': ''},
                        'source': {'type': 'str', 'sign': 'na', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': ''},
                        'survdate': {'type': 'str', 'sign': 'na', 'null_check': "only", 'range_lev': '', 'structural': False, 'promote': ''},
                        'metcode': {'type': 'str', 'sign': 'na', 'null_check': "no", 'range_lev': '', 'structural': True, 'promote': 'R'},
                        'subid': {'type': 'float', 'sign': 'pos', 'null_check': "no", 'range_lev': '', 'structural': True, 'promote': 'R'},
                        'propname': {'type': 'str', 'sign': 'na', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'address': {'type': 'str', 'sign': 'na', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'city': {'type': 'str', 'sign': 'na', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'zip': {'type': 'float', 'sign': 'pos', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'county': {'type': 'str', 'sign': 'na', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'state': {'type': 'str', 'sign': 'na', 'null_check': "no", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'year': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': 'min', 'structural': True, 'promote': 'R'},
                        'month': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': 'minmax', 'structural': True, 'promote': 'R'},
                        'renov': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': 'min', 'structural': True, 'promote': 'C'},
                        'exp_year': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': ''},
                        'tot_size': {'type': 'float', 'sign': 'pos', 'null_check': "only", 'range_lev': '1p99p', 'structural': True, 'promote': 'C'},
                        'n_size': {'type': 'float', 'sign': 'pos', 'null_check': "only", 'range_lev': 'zerook', 'structural': True, 'promote': 'C'},
                        'a_size': {'type': 'float', 'sign': 'pos', 'null_check': "only", 'range_lev': 'zerook', 'structural': True, 'promote': 'C'},
                        'n_avail': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': 'C'},
                        'a_avail': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': 'C'},
                        'n_lorent': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': 'C'},
                        'n_hirent': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': 'C'},
                        'n_avrent': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'a_lorent': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': 'C'},
                        'a_hirent': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': 'C'},
                        'a_avrent': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'cam': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'ac_rent': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'nc_rent': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'rnt_term': {'type': 'str', 'sign': 'na', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': 'C'},
                        'anc_term': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'non_term': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'a_ti': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'ti1': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'ti2': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'ti_renew': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'free_re': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': 'max', 'structural': False, 'promote': 'C'},
                        'a_freere': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': 'max', 'structural': False, 'promote': 'C'},
                        'a_comm': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': 'max', 'structural': False, 'promote': 'C'},
                        'comm1': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': 'max', 'structural': False, 'promote': 'C'},
                        'comm2': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': 'max', 'structural': False, 'promote': 'C'},
                        'op_exp': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'sales': {'type': 'float', 'sign': 'na', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': ''},
                        'foodct': {'type': 'str', 'sign': 'na', 'null_check': "ok", 'range_lev': '', 'structural': False, 'promote': ''},
                        'x': {'type': 'float', 'sign': 'neg', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        'y': {'type': 'float', 'sign': 'pos', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'},
                        're_tax': {'type': 'float', 'sign': 'pos', 'null_check': "ok", 'range_lev': '1p99p', 'structural': False, 'promote': 'C'},
                        'fipscode': {'type': 'float', 'sign': 'pos', 'null_check': "only", 'range_lev': '', 'structural': True, 'promote': 'C'}
                       }
                 }
    
    sector_map = {'off': {'sector': ['Off'], 'prefix': 'O', 'category': ['office', 'government'], 'subcategory': ['general', 'office', 'corporate_facility', 'financial']},
                   'ind': {'sector': ['Dis', 'Flx'], 'prefix': 'I', 'category': ['industrial'], 'subcategory': ['warehouse_distribution', 'warehouse_office', 'warehouse_flex']},
                   'ret': {'sector': ['Ret'], 'prefix': 'R', 'category': ['retail'], 'subcategory': ['neighborhood_grocery_anchor', 'neighborhood_center', 'strip_center_anchored', 'strip_center', 'community_specialty', 'retail']}
                  }
    space_map = {'ind': ['industrial', 'flex_r_and_d', 'life_science', 'office'],
                 'off': ['office'],
                 'ret': ['retail']
                }

    return consistency_dict, type_dict_all, rename_dict_all, sector_map, space_map

sectors = ['off']
curryr = 2022
currmon = 7
legacy_only = True # set to True if only want to keep properties that were in the legacy foundation database or are within the NC rebench window of curryr - 3
include_cons = True # Set to True if want to include Catylist non legacy REIS props with a completion year within the curryr - 3 construction rebench window, even if legacy_only is True
use_rc_id = True # set to True if rc_id should be used to link properties in the event that there is no live link from ER
use_reis_sub = True # set to True if we should take the log met/sub values in the case of a discrepancy to what is given in the view
use_mult = True # set to True if we should choose the first option for a REIS id link  in the case of multiple live linkages, instead of dropping cases where this occurs
live_load = False

logger = logging.getLogger()


log_file_name = 'DL_Log_Listings.log'

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

if get_home()[0:2] == 's3':
    splits = int(mp.cpu_count())
else:
    splits = int(mp.cpu_count()*0.7)
os.environ['NUMEXPR_MAX_THREADS'] = str(splits)
os.environ['NUMEXPR_NUM_THREADS'] = str(splits)

def assign_met(test_data, log):
    
    undup = test_data.copy()
    undup = undup.drop_duplicates('property_source_id')
    
    undup['num_links'] = undup['foundation_ids_list'].str.count(',')
    undup['num_links'] = np.where((undup['num_links'] > 0), undup['num_links'] + 1, undup['num_links'])
    undup['num_links'] = np.where((undup['foundation_ids_list'] != '') & (undup['num_links'] == 0), 1, undup['num_links'])
    max_links = undup['num_links'].max()
    undup['join_id'] = ''
    undup['metcode_use'] = np.where((undup['property_geo_msa_list'] != ''), undup['property_geo_msa_list'].str.split(',').str[0], '')
    for i in range(0, max_links):
        undup['join_id'] = np.where((undup['join_id'] == '') & (undup['num_links'] >= i + 1), undup['foundation_ids_list'].str.split(',').str[i].str[1:], undup['join_id'])
        undup = undup.join(log.drop_duplicates('realid').set_index('realid').rename(columns={'metcode': 'log_metcode'})[['log_metcode']], on='join_id')
        undup['metcode_use'] = np.where((undup['log_metcode'].isnull() == False) & (undup['metcode_use'] != ''), undup['log_metcode'], undup['metcode_use'])
        undup = undup.drop(['log_metcode'], axis=1)
    undup['join_id'] = np.where((undup['join_id'] == '') & (undup['property_reis_rc_id'] != ''), undup['property_reis_rc_id'].str[1:], undup['join_id'])
    undup = undup.join(log.drop_duplicates('realid').set_index('realid').rename(columns={'metcode': 'log_metcode'})[['log_metcode']], on='join_id')
    undup['metcode_use'] = np.where((undup['log_metcode'].isnull() == False) & (undup['metcode_use'] != ''), undup['log_metcode'], undup['metcode_use'])

    undup.sort_values(by=['metcode_use'], ascending=[False], inplace=True)
    
    metcodes = undup['metcode_use'].unique()
    split_data = []
    for i in range(0, len(metcodes), int(np.ceil(len(metcodes) / splits))):
        chunk = metcodes[i:i + int(np.ceil(len(metcodes) / splits))]
        split_data.append(undup[undup['metcode_use'].isin(chunk)])
    
    return split_data


try:

    print("Process for {}m{} initiated, check log for progress".format(curryr, currmon))
    print("\n")
    
    consistency_dict, type_dict_all, rename_dict_all, sector_map, space_map = get_dicts()
    
    # Instantiate the PrepareLogs class
    prepLogs = PrepareLogs(sector_map, space_map, legacy_only, include_cons, use_rc_id, 
                           use_reis_sub, use_mult, curryr, currmon, live_load)

    for sector in sectors:
        # Load the incrementals data
        # SELECT * FROM consumption.v_catylisturt_listing_to_econ;
        if sector == sectors[0]:
            load = True
            test_data_in, stop = prepLogs.load_incrementals(sector, type_dict_all, rename_dict_all, consistency_dict, load)
        else:
            load = False
            test_data_in, stop = prepLogs.load_incrementals(sector, type_dict_all, rename_dict_all, consistency_dict, load, test_data_in)

        if not stop:
            test_data_in_filt = prepLogs.filter_incrementals(test_data_in)

            test_data = test_data_in_filt.copy()
            test_data = test_data.rename(columns={'reis_sector': 'catylist_sector'})

            # Read in the individual metro log files and append to one aggregated dataframe
            #log = prepLogs.read_logs()
            for x in ['listed_space_id', 'leg', 'property_source_id', 'total_size']:
                if x in log.columns:
                    log = log.drop([x], axis=1)

            # Ensure that the column schema has all the columns present in the historical log file
            log, stop = prepLogs.check_column_schema(log)

            if stop:
                if get_home()[0:2] == 's3':
                    song_alert(sound='bowl')
                break

            if not stop:

                # Need to load the retail ph logs, as those need to get updated for the new size if C size is chosen
                if sector == "ret":
                    ph_log = prepLogs.read_ph_logs()

                # Generate a dict that maps what metros exist in each state
                prepLogs.map_met_to_state(log)

                # Convert variables to lower case, and perform other type conversions as necessary
                test_data = prepLogs.handle_case(test_data)

                # Clean the text field that holds some of the commission data
                test_data = prepLogs.clean_comm(test_data)
                
                # Clean the text field that holds some of the lease terms data
                test_data = prepLogs.clean_lease_terms(test_data)
                
                # Test to see if there are duplicated addresses across unique Catylist IDs
                prepLogs.check_duplicate_catylist(test_data)

                # Ensure anchor determination is correct
                if sector == "ret":
                    prepLogs.check_anchor_status(test_data)
                
                # Identify the correct Foundation ID, MSA, and Subid to link to the Catylist property
                log_ids = list(log.drop_duplicates('realid')['realid'])

                split_data = assign_met(test_data, log)
                pool = mp.Pool(splits)
                result_async = [pool.apply_async(prepLogs.select_reis_id, args = (data, log, log_ids, count_split)) for data, count_split in zip(split_data, np.arange(1, len(split_data) + 1))]
                results = [r.get() for r in result_async]
                del split_data
                decision = pd.DataFrame()
                decision = decision.append(results, ignore_index=True)
                del results
                decision = decision.reset_index(drop=True)
                pool.close()

                # Apply the chosen Foundation ID, MSA, and Subid
                test_data, stop, id_check = prepLogs.apply_reis_id(test_data, decision, log)
                id_check.to_csv("{}/OutputFiles/{}/logic_logs/id_check_{}m{}.csv".format(get_home(), sector, curryr, currmon), index=False)           
                size_method = test_data.copy()
                size_method = size_method[['property_source_id', 'id_use', 'size_method', 'tot_size']]
                
                if not stop:

                    # Format realid and phase correctly
                    test_data = prepLogs.format_realid(test_data, log)

                    # determine what the valid values are for each column based on what is in the legacy logs
                    log = prepLogs.gen_valid_vals(log)

                    # Generate type2 so it can be used to aggregate the range values
                    test_data = prepLogs.gen_type(test_data, log)

                    # Drop properties/listings that do not meet REIS sector specific inclusion criteria
                    pre_drop = test_data[test_data['leg']][['property_source_id', 'id_use']].copy()
                    test_data = prepLogs.select_comp(test_data)

                    # See if there are cases where a property might have a duplicate avail but one for direct and one for sublet
                    test_data = prepLogs.check_double_sublet(test_data)

                    # Generate acceptability ranges based on values in the historical logs
                    test_data = prepLogs.calc_ranges(log, test_data)

                    # Generate the difference between key performance fields between incrementals and historical files
                    test_data = prepLogs.gen_mr_perf(test_data, log)

                    # Calculate normalized values for key performance variables that may be recorded for different sizes/periods so that everything is annual per sqft
                    test_data = prepLogs.normalize(test_data, False)
                    
                    # Generate a report on changes from last month's raw file
                    prepLogs.eval_period_over_period(test_data)

                    # Aggregate space level data to the property level
                    test_data, mult_prop_link = prepLogs.calc_prop_level(test_data)
                    
                    # Test to see if there are any incremental surveys that exist for a property before a historical log survey
                    prepLogs.outdated_check(test_data, log)
                    
                    # Rename view columns to match legacy log file column headers
                    test_data, log = prepLogs.rename_cols(test_data, log)
                    
                    # Check if the var types are correct
                    test_data, stop = prepLogs.vartypes_check(test_data)

                    if not stop:

                        # Check that the values for each column have the correct sign
                        test_data = prepLogs.sign_check(test_data)

                        # Check that the values for each column are within an acceptable range
                        test_data = prepLogs.range_check(test_data)

                        # Checking if legacy reis ids match on structural points.
                        # Note: There may have been changes on the structural side in RDMA post migration from Foundation, so just because it doesnt match doesnt mean its wrong...
                        logging.info("Conducting structural check...")
                        test_data = prepLogs.structural_check(test_data, log)
                        logging.info("\n")
                        
                        # Checking if legacy reis ids have large changes to performance fields
                        if len(test_data) > 0: 
                            logging.info("Conducting performance check...")
                            test_data = prepLogs.performance_check(test_data, log)
                            logging.info("\n")
                        else:
                            logging.info("All properties were removed from the test pool due to knockout criteria")
                            logging.info("\n")

                        # append incrementals to historical log file
                        if load:
                            d_prop = pd.DataFrame()
                        combo, test_data, stop, d_prop = prepLogs.append_incrementals(test_data, log, load, d_prop)

                        if not stop:

                            # Drop the extra fields used for debugging purposes
                            combo = prepLogs.select_cols(combo)

                            # Outsheet the logic log and drop log for review
                            if sector == sectors[0]:
                                l_col_order = []
                                d_col_order = []
                            l_col_order, d_col_order = prepLogs.outsheet_files(log, test_data_in, test_data, id_check, sector, sectors, l_col_order, d_col_order, size_method, pre_drop, mult_prop_link)
                            
                            # If sector is retail, replace the data in the ph log with the size information from the Catylist ID
                            if sector == "ret" and get_home()[0:2] != 's3' and len(test_data) > 0 and type_dict_all[sector]['tot_size']['promote'] == 'C':
                                prepLogs.update_ph_log(test_data, ph_log)

                            # Split the export into msa files, and outsheet
                            if get_home()[0:2] != 's3':
                                prepLogs.split_met(combo, log)

            if stop:
                logging.info("Stop triggered, check!!!")
                if get_home()[0:2] == 's3':
                    song_alert(sound='bowl')
                break
            else:
                logging.info("Log Preparation for {} complete".format(sector.upper()))
                logging.info("\n")
                if get_home()[0:2] == 's3':
                    song_alert(sound='done')

    if not stop:
        logging.info("All log preperation complete")
        if get_home()[0:2] == 's3':
            song_alert(sound='bowl')
        
except:
    error = True
    logging.info("\n")
    logging.exception("error triggered")
    print("Error Triggered, Check Log!")
    if get_home()[0:2] == 's3':
        song_alert(sound='bowl')