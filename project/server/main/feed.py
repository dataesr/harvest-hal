import datetime
from datetime import date
import json
import os
import pymongo
import requests
from retry import retry
from urllib.parse import quote_plus

from project.server.main.aurehal import harvest_and_save_aurehal
from project.server.main.logger import get_logger
from project.server.main.parse import get_aurehal_from_OS, parse_hal
from project.server.main.utils_swift import get_objects_by_prefix, upload_object

logger = get_logger(__name__)

def nb_days_month(y, m):
    y2 = y
    m2 = m + 1
    if m == 12:
        y2 = y+1
        m2 = 1
    return (date(y2, m2, 1) - date(y, m, 1)).days

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

    # 2.transform data and save in object storage
    current_file_parsed = f'hal_parsed_{year_start_end}_{chunk_index}.json'
    data_parsed = [parse_hal(e, aurehal, collection_name) for e in data]
    json.dump(data_parsed, open(current_file_parsed, 'w'))
    os.system(f'gzip {current_file_parsed}')
    upload_object('hal', f'{current_file_parsed}.gz', f'{collection_name}/parsed/{current_file_parsed}.gz')
    os.system(f'rm -rf {current_file_parsed}.gz')

    #3. oa_details
    oa_details_data = []
    for d in data_parsed:
        elt = { 'hal_id': d['hal_id'], 'oa_details': d['oa_details'] }
        oa_details_data.append(elt)
    current_file_oa_details = f'hal_oa_details_{year_start_end}_{chunk_index}.json'
    json.dump(oa_details_data, open(current_file_oa_details, 'w'))
    insert_data(collection_name, current_file_oa_details)
    os.system(f'rm -rf {current_file_oa_details}.gz')


def harvest_and_insert(collection_name, harvest_aurehal=True, min_year=1000):
    # 1. save aurehal structures
    aurehal = {}
    for ref in ['structure', 'author']:
        if harvest_aurehal:
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
    year_prefix = '-01-01T00:00:00Z'
    year_suffix = '-12-31T23:59:59Z'
    years_start_end = [('1000'+year_prefix, '1990'+year_suffix),
                   ('1991'+year_prefix, '2000'+year_suffix),
                   ('2001'+year_prefix, '2010'+year_suffix)
                  ]
    for y in range(2011, 2016):
        years_start_end.append((str(y)+year_prefix, str(y)+year_suffix))
    for y in range(2016, datetime.datetime.now().year+1):
        for m in range(1, 13):
            D = nb_days_month(y, m)
            years_start_end.append((f'{y}-{str(m).zfill(2)}-01T00:00:00Z', f'{y}-{str(m).zfill(2)}-{D}T23:59:59Z'))
    years_start_end.append((str(datetime.datetime.now().year+1)+year_prefix, str(2100) + year_suffix))
    years_start_end = [y for y in years_start_end if y[0] >= str(min_year)]

    #years_start_end = [(1000, 1990),(1991,2000),(2001,2010)]
    #for y in range(2011, datetime.datetime.now().year+1):
    #    years_start_end.append((y, y))
    #years_start_end.append((datetime.datetime.now().year+1,2100))
    #years_start_end = [y for y in years_start_end if y[0] >= min_year]
    logger.debug(f'years_start_end = {years_start_end}')
    for (year_start, year_end) in years_start_end:
        harvest_and_insert_one_year(collection_name, year_start, year_end, aurehal)

@retry(delay=300, tries=5, logger=logger)
def get_data_hal(url, nb_rows_total):
    #logger.debug(f'{nb_rows_total} and new url {url}')
    r = requests.get(url, timeout=100)
    #logger.debug(f'status_code : {r.status_code}')
    try:
        res = r.json()
    except:
        logger.debug(f'ERROR for url {url}')
        logger.debug(r.status_code)
        logger.debug(r.text)
        r = requests.get(url)
        res = r.json()
    return res

@retry(delay=300, tries=5, logger=logger)
def get_one_page(nb_rows,cursor,year_start,year_end, nb_rows_total):
    year_start_end = 'all_years'
    if year_start and year_end:
        year_start_end = f'{year_start}_{year_end}'
    url = f'https://api.archives-ouvertes.fr/search/?q=*:*&wt=json&fl=*'
    if year_start and year_end:
        url += f'&fq=producedDate_tdate:[{year_start}%20TO%20{year_end}]'
    url += f'&sort=docid asc&rows={nb_rows}&cursorMark={cursor}'
    res = get_data_hal(url, nb_rows_total)
    if cursor == '*':
        logger.debug(f"HAL {year_start_end} : {res['response']['numFound']} documents to retrieve")
    new_cursor = quote_plus(res['nextCursorMark'])
    return res, new_cursor

def harvest_and_insert_one_year(collection_name, year_start, year_end, aurehal):
    year_start_end = 'all_years'
    if year_start and year_end:
        year_start_end = f'{year_start}_{year_end}'

    # todo save by chunk
    nb_rows = 200
    nb_rows_total = 0
    cursor='*'
    data = []
    chunk_index = 0
    MAX_DATA_SIZE = 25000
    while True:
        res, new_cursor = get_one_page(nb_rows, cursor, year_start, year_end, nb_rows_total)
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
        nb_rows_total += nb_rows


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
    mycol.create_index('hal_id')
    end = datetime.datetime.now()
    delta = end - start
    logger.debug(f'Mongoimport done in {delta}')
    ## mongo done


def load_collection_from_object_storage(collection_name: str) -> None:
    # 1. Drop mongo collection
    logger.debug(f'dropping {collection_name} collection before insertion')
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    myclient['hal'][collection_name].drop()
    # 2. Collect all files from Object Storage
    publications = get_objects_by_prefix(container='hal', prefix=f'{collection_name}/parsed/hal_parsed')
    # 3. Extract oa_details from publications
    oa_details_data = []
    for publication in publications:
        result = {
            'hal_id': publication.get('hal_id'),
            'oa_details': publication.get('oa_details')
        }
        oa_details_data.append(result)
    # 4. Save it into mongo collection
    current_file_oa_details = f'hal_oa_details.json'
    json.dump(oa_details_data, open(current_file_oa_details, 'w'))
    insert_data(collection_name=collection_name, output_file=current_file_oa_details)
    return