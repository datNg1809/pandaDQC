
# coding: utf-8

'''
 Here are structure and ideas of this script:
 
 1 - Create CustomPandasDataset class to define some necessary functions for our test. Note that all tests need to be nested under MetaPandasDataset.column_map_expectation
 
 2 - Load CSV file which needs to be tested
 
 3 - Load excel file (configurations). This file can be edited by Business Analysts. 
 
 4 - Send result
'''

import great_expectations as ge
from great_expectations.dataset import PandasDataset, MetaPandasDataset
import pandas as pd
import phonenumbers
import csv


class CustomPandasDataset(PandasDataset):
    '''
    Adding custom tests for specific purposes, other tests are heritated from ge.dataset.PandasDataset
    
    List of custom tests are below:
    
    1. expect_column_values_to_be_phone_number
        - Customize alread existing function by adding library "phonenumbers" for country options
        
        
    '''
    #Phone number test
    @MetaPandasDataset.column_map_expectation
    def expect_column_values_to_be_phone_number(self, column, country=None,
                                            mostly=None,
                                            result_format=None, include_config=False, catch_exceptions=False, meta=None):
        
        return column.map(lambda x: self._isPhoneNumber(x,country))
    def _isPhoneNumber(self, phone, country) :
        try :
            
            x = phonenumbers.parse(str(phone), country)
            success = phonenumbers.is_possible_number(x)
        except phonenumbers.NumberParseException as error:
            success = False
        return success
    
    #Value length test: convert integer to string
    @MetaPandasDataset.column_map_expectation
    def expect_column_value_lengths_to_be_between_all_types(self, column, min_value=None, max_value=None,
                                                  mostly=None,
                                                  result_format=None, include_config=False, catch_exceptions=None, meta=None):
        def length_is_between(val):
            if min_value != None and max_value != None:
                return len(str(val)) >= min_value and len(str(val)) <= max_value

            elif min_value == None and max_value != None:
                return len(str(val)) <= max_value

            elif min_value != None and max_value == None:
                return len(str(val)) >= min_value
            else:
                return False
        return column.map(length_is_between)
    
    @MetaPandasDataset.column_pair_map_expectation
    def expect_column_A_has_value_if_column_B_has_value(self,column_A,column_B,
                                                        result_format=None, include_config=False,
                                                        ignore_row_if="both_values_are_missing",
                                                        catch_exceptions=None, meta=None):
        
        return ((column_A != '') & (column_B != '')) | ((column_A =='') & (column_B==''))
        
        
    

def validation(file_path,market,country,website):
    #Load data for testing
    with open(file_path, 'r') as csvfile:
        dialect = csv.Sniffer().sniff(csvfile.read(1024))
    try:
        data=ge.read_csv(file_path,delimiter=dialect.delimiter,error_bad_lines = False,encoding = 'utf-8',dtype={'CP':str})
    except UnicodeDecodeError:
        data=ge.read_csv(file_path,delimiter=dialect.delimiter,error_bad_lines = False,encoding = 'latin-1',dtype={'CP':str})
    
    data=CustomPandasDataset(data)
    
    # Load configurations of AUTO from excel file "vo_config_file.xlsx" or "immo_config_file.xlsx"
    '''Assume that we have already excel file which is structured as follows:
        - excel file has many tabs
        - tab 1: configurations of VO generally
        - tab 2: configurations of VO CA - Autotrader
        - tab 3: configurations of VO CA - Cars
        - ...
    Then, when we need to edit configurations of 1 specific website, others will not be affected.
    '''
    general_config=pd.read_excel('{}_{}_CONFIG.xlsx'.format(market.upper(),country.upper()),sheetname= (market + '_' + country).upper()).fillna('')
    website_config=pd.read_excel('{}_{}_CONFIG.xlsx'.format(market.upper(),country.upper()),sheetname= website.upper()).fillna('')
    total_config=pd.concat([website_config,general_config])
    #Prepare format of expectations_config based on total_config dataframe
    import yaml
    my_expectations_config={'dataset_name':None}
    my_expectations_config['meta']={} 
    my_expectations_config['meta']['great_expectations.__version__']='0.4.4__develop'
    my_expectations_config['expectations']=[]
    for index, row in total_config.iterrows():
        expectation_type=row['name']
        kwargs={}
        if row['columns']!='':
            kwargs = {'column':row['columns'].upper()}
        for index,item in yaml.load(row['parameters']).items():
            kwargs[index] = item
        my_expectations_config['expectations'].append({'expectation_type': expectation_type,'kwargs': kwargs})
    
#    print(my_expectations_config)
    #Validate the database based on newly loaded expectations
    result = data.validate(expectations_config=my_expectations_config,catch_exceptions=False, result_format = 'SUMMARY')

    #To see which tests were false
    false_tests=[]
    for test in result['results']:
        if test['success']==False:
            false_tests.append({(test['expectation_config']['expectation_type']) : (test['expectation_config']['kwargs']),
                                 'result': test['result']
                                 })
    
    return (result,false_tests)