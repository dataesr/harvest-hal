import datetime
import os
import pymongo
import requests
from urllib import parse
from urllib.parse import quote_plus
import json
from retry import retry

from project.server.main.logger import get_logger
from project.server.main.utils_swift import upload_object
from project.server.main.parse import parse_hal, get_aurehal_from_OS
from project.server.main.aurehal import harvest_and_save_aurehal

logger = get_logger(__name__)

def save_data(data, collection_name, year_start, year_end, chunk_index, aurehal):

    year_start_end = 'all_years'
    if year_start and year_end:
        year_start_end = f'{year_start}_{year_end}'
    # 1. save raw data to OS
    current_file = f'hal_{year_start_end}_{chunk_index}.json'
    json.dump(data, open(current_file, 'w'))
    os.system(f'gzip {current_file}')
    upload_object('hal', f'{current_file}.gz', f'{collection_name}/raw/{current_file}.gz')
    os.system(f'rm -rf {current_file}.gz')

    # 2.transform data and save in mongo
    current_file_parsed = f'hal_parsed_{year_start_end}_{chunk_index}.json'
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
    year_start = None
    year_end = None
    # year_start = 1900
    # year_end = datetime.date.today().year
    harvest_and_insert_one_year(collection_name, year_start, year_end, aurehal)

@retry(delay=60, tries=5)
def get_data_hal(url):
    r = requests.get(url)
    res = r.json()
    return res

@retry(delay=60, tries=5)
def get_one_page(nb_rows,cursor,year_start,year_end):
    year_start_end = 'all_years'
    if year_start and year_end:
        year_start_end = f'{year_start}_{year_end}'
    url = f'https://api.archives-ouvertes.fr/search/?q=*:*&wt=json&fl=*'
    if year_start and year_end:
        url += f'&fq=publicationDateY_i:[{year_start}%20TO%20{year_end}]'
    url += f'&sort=docid asc&rows={nb_rows}&cursorMark={cursor}'
    res = get_data_hal(url)
    if cursor == '*':
        logger.debug(f"HAL {year_start_end} : {res['response']['numFound']} documents to retrieve")
    new_cursor = quote_plus(res['nextCursorMark'])
    return res, new_cursor

def harvest_and_insert_one_year(collection_name, year_start, year_end, aurehal):
    year_start_end = 'all_years'
    if year_start and year_end:
        year_start_end = f'{year_start}_{year_end}'

    # todo save by chunk
    nb_rows = 1000
    cursor='*'
    data = []
    chunk_index = 0
    MAX_DATA_SIZE = 25000
    while True:
        res, new_cursor = get_one_page(nb_rows, cursor, year_start, year_end)
        logger.debug(f'{year_start_end}|{len(data)}')
        data += res['response']['docs']
        if len(data) > MAX_DATA_SIZE:
            save_data(data, collection_name, year_start, year_end, chunk_index, aurehal)
            data = []
            chunk_index += 1

        if new_cursor == cursor:
            if data:
                save_data(data, collection_name, year_start, year_end, chunk_index, aurehal)
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
