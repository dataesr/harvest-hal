import os
import requests
from project.server.main.utils_swift import upload_object
from urllib.parse import quote_plus
import json

import pycountry
country_code_to_name = {}
for c in list(pycountry.countries):
    country_code = c.alpha_2.lower()
    country_code_to_name[country_code] = c.name

def get_aurehal_struct():
    nb_rows = 10000
    cursor='*'
    data = []
    while True:
        url = f'https://api.archives-ouvertes.fr/ref/structure/?q=*:*&wt=json&fl=*&sort=docid asc&rows={nb_rows}&cursorMark={cursor}'
        r = requests.get(url)
        res = r.json()
        new_cursor = quote_plus(res['nextCursorMark'])
        data += res['response']['docs']
        if new_cursor == cursor:
            break
        cursor = new_cursor
    return data

def parse_affiliation(elt):
    affiliation = {}
    affiliation_name = ''
    country = None
    if isinstance(elt.get('country_s'), str):
        country = country_code_to_name.get(elt.get('country_s'))
        affiliation['detected_countries'] = [elt['country_s']]

    for field in ['name_s', 'code_s', 'acronym_s', 'parentAcronym_s', 'parentName_s']:
        if isinstance(elt.get(field), str):
            affiliation_name += elt.get(field)+", "
        if isinstance(elt.get(field), list):
            affiliation_name += ', '.join(elt.get(field))+', '

    if isinstance(elt.get('address_s'), str):
        affiliation_name += elt.get('address_s')+', '
    if country:
        affiliation_name += country

    affiliation['name'] = affiliation_name
    affiliation['hal_docid'] = elt['docid']
    if country:
        affiliation['country'] = country

    if isinstance(elt.get('rnsr_s'), list):
        if len(elt.get('rnsr_s')) > 1:
            print(elt)
        affiliation['rnsr'] = elt.get('rnsr_s')[0]
    if isinstance(elt.get('ror_s'), list):
        if len(elt.get('ror_s')) > 1:
            print(elt)
        affiliation['ror'] = elt.get('ror_s')[0]
    return affiliation

def create_docid_struct_map(data):
    docid_struct_map = {}
    parsed_data = []
    for d in data:
        docids = [d['docid']]
        if isinstance(d.get('aliasDocid_i'), list):
            docids += d.get('aliasDocid_i')
        docids = list(set(docids))
        parsed_affiliation = parse_affiliation(d)
        parsed_data.append(parsed_affiliation)
        for docid in docids:
            docid_struct_map[docid] = parsed_affiliation
    return parsed_data, docid_struct_map

def harvest_and_save_struct(collection_name):
    data = get_aurehal_struct()
    parsed_data, docid_struct_map = create_docid_struct_map(data)
    current_file = f'aurehal_structures.json'
    json.dump(parsed_data, open(current_file, 'w'))
    os.system(f'gzip {current_file}')
    upload_object('hal', f'{current_file}.gz', f'{collection_name}/{current_file}.gz')
    os.system(f'rm -rf {current_file}.gz')
    
    current_file = f'aurehal_structures_dict.json'
    json.dump(docid_struct_map, open(current_file, 'w'))
    os.system(f'gzip {current_file}')
    upload_object('hal', f'{current_file}.gz', f'{collection_name}/{current_file}.gz')
    os.system(f'rm -rf {current_file}.gz')
