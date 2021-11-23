import datetime
import os
import pymongo
import requests
from urllib import parse
from urllib.parse import quote_plus
import json

from project.server.main.logger import get_logger
from project.server.main.utils_swift import upload_object
from project.server.main.parse import parse_hal, get_aurehal_from_OS
from project.server.main.aurehal import harvest_and_save_aurehal

logger = get_logger(__name__)

def save_data(data, collection_name, year, chunk_index, aurehal):

    # 1. save raw data to OS
    current_file = f'hal_{year}_{chunk_index}.json'
    json.dump(data, open(current_file, 'w'))
    os.system(f'gzip {current_file}')
    upload_object('hal', f'{current_file}.gz', f'{collection_name}/raw/{current_file}.gz')
    os.system(f'rm -rf {current_file}.gz')

    # 2.transform data and save in mongo
    current_file_parsed = f'hal_parsed_{year}_{chunk_index}.json'
    data_parsed = [parse_hal(e, aurehal, collection_name) for e in data]
    json.dump(data_parsed, open(current_file_parsed, 'w'))
    insert_data(collection_name, current_file_parsed)
    os.system(f'gzip {current_file_parsed}')
    upload_object('hal', f'{current_file_parsed}.gz', f'{collection_name}/parsed/{current_file_parsed}.gz')
    os.system(f'rm -rf {current_file_parsed}.gz')

def harvest_and_insert(collection_name):
    # 1. save aurehal structures
    aurehal = {}
    for ref in ['structure', 'author']:
        harvest_and_save_aurehal(collection_name, ref)
        aurehal[ref] = get_aurehal_from_OS(collection_name, ref)

    # 2. drop mongo 
    logger.debug(f'dropping {collection_name} collection before insertion')
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    myclient['hal'][collection_name].drop()

    # 3. save publications
    year_start = 2012
    year_end = datetime.date.today().year
    year_end = 2012
    for year in range(year_start, year_end + 1):
         harvest_and_insert_one_year(collection_name, year, aurehal)

def harvest_and_insert_one_year(collection_name, year, aurehal):

    # todo save by chunk
    nb_rows = 250
    cursor='*'
    data = []
    chunk_index = 0
    MAX_DATA_SIZE = 25000
    while True:
        url = f'https://api.archives-ouvertes.fr/search/?q=*:*&wt=json&fl=*&fq=publicationDateY_i:[{year}%20TO%20{year}]&sort=docid asc&rows={nb_rows}&cursorMark={cursor}'
        r = requests.get(url)
        res = r.json()
        if cursor == '*':
            logger.debug(f"HAL {year} : {res['response']['numFound']} documents to retrieve")
        new_cursor = quote_plus(res['nextCursorMark'])
        logger.debug(f'{year}|{len(data)}')
        data += res['response']['docs']
        
        if len(data) > MAX_DATA_SIZE:
            save_data(data, collection_name, year, chunk_index, aurehal)
            data = []
            chunk_index += 1

        if new_cursor == cursor:
            if data:
                save_data(data, collection_name, year, chunk_index, aurehal)
            break
        cursor = new_cursor


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
