
# coding: utf-8



import os
from datetime import datetime

################################################################################
'''Identify data!!! 
In future, based on name of the file (eg.: VO_ZA_GUMTREE_2018_12.csv) we will know the identity of the data. For now, I manually set it:
'''
filename='IMMO_DE_IMMOWELT_2018_10.csv'
os.chdir(r'D:\19. ETL project\QC check')
#sys.path.insert(0,r'D:\19. ETL project\IMMO DE')

market, country, website, year, month = filename[:-4].split(sep='_')

#import pandas as pd
#data=pd.read_csv(filename, sep='\t',error_bad_lines = False,encoding='latin-1',dtype={'CP':str},nrows=100)

#Call the test for that data
from QualityCheck import validation
result, failed_tests = validation(filename,market,country,website)

################################################################################
#Send result to elasticsearch
from elasticsearch import Elasticsearch
  
#Connent to elasticsearch and send log
content = {
        'market': market,
        'country': country,
        'website': website,
        'validation': result['success'],
        'statistics': result['statistics'],
        'failed': failed_tests,
        'all_result': result['results'],
        'timestamp': datetime.now(),
    }  
es=Elasticsearch('http://149.202.79.37:9200')    
es.index(index=country+"_"+website,doc_type="validation", body=content)
################################################################################
#_search=es.search(index="za_cars")
#for i in es.indices.get("*"):
#    print (i)







