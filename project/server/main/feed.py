import datetime
import os
import pymongo
import requests
from urllib import parse
from urllib.parse import quote_plus
import json

from project.server.main.logger import get_logger
from project.server.main.utils_swift import upload_object
logger = get_logger(__name__)

def harvest_and_insert(collection_name, year):
    nb_rows = 250
    cursor='*'
    data = []
    while True:
        url = f'https://api.archives-ouvertes.fr/search/?q=*:*&wt=json&fl=*&fq=publicationDateY_i:[{year}%20TO%20{year}]&sort=docid asc&rows={nb_rows}&cursorMark={cursor}'
        r = requests.get(url)
        res = r.json()
        if cursor == '*':
            logger.debug(f"HAL {year} : {res['response']['numFound']} documents to retrieve")
        new_cursor = quote_plus(res['nextCursorMark'])
        logger.debug(f'{year}|{len(data)}')
        data += res['response']['docs']
        if new_cursor == cursor:
            break
        cursor = new_cursor
    current_file = f'hal_{year}.json'
    json.dump(data, open(current_file, 'w'))
    os.system(f'gzip {current_file}')
    #insert_data(collection_name, current_file)
    upload_object('hal', f'{current_file}.gz', f'{collection_name}/{current_file}.gz')
    os.system(f'rm -rf {current_file}.gz')

def insert_data(collection_name, output_file):
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['hal']
    
    ## mongo start
    start = datetime.datetime.now()
    mongoimport = f"mongoimport --numInsertionWorkers 2 --uri mongodb://mongo:27017/hal --file {output_file}" \
                  f" --collection {collection_name} --jsonArray"
    logger.debug(f'Mongoimport {output_file} start at {start}')
    logger.debug(f'{mongoimport}')
    os.system(mongoimport)
    logger.debug(f'Checking indexes on collection {collection_name}')
    mycol = mydb[collection_name]
    mycol.create_index('halId_s')
    mycol.create_index('docid')
    mycol.create_index('publicationDateY_i')
    end = datetime.datetime.now()
    delta = end - start
    logger.debug(f'Mongoimport done in {delta}')
    ## mongo done
