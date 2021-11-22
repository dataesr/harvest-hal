import os
import json
from project.server.main.utils_swift import upload_object, download_object
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def get_aurehal_struct(collection_name):
    current_file = f'aurehal_structures_dict.json'
    download_object('hal', f'{collection_name}/{current_file}.gz', f'{current_file}.gz')
    os.system(f'gunzip {current_file}.gz')
    return json.load(open(current_file, 'r'))

def parse_hal(notice, aurehal_struct):
    res = {}
    res['sources'] = ['HAL']
    if isinstance(notice.get('doiId_s'), str):
        res['doi'] = notice.get('doiId_s').lower().strip()
    if isinstance(notice.get('halId_s'), str):
        res['external_ids'] = [{'id_type': 'hal_id', 'id_value': notice.get('halId_s')}]
    if isinstance(notice.get('title_s'), list):
        if len(notice.get('title_s')) > 0:
            res['title'] = notice.get('title_s')[0].strip()
    if isinstance(notice.get('abstract_s'), list):
        if len(notice.get('abstract_s')) > 0:
            res['abstract'] = [{'abstract': notice.get('abstract_s')[0].strip()}]
    if isinstance(notice.get('structId_i'), list):
        affiliations = []
        for s in notice.get('structId_i'):
            if s in aurehal_struct:
                affiliations.append(aurehal_struct[s])
        if affiliations:
            res['affiliations'] = affiliations
    return res
