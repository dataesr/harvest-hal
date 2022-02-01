import os
import requests
import json
import pycountry
from retry import retry
from project.server.main.utils_swift import upload_object
from urllib.parse import quote_plus
from project.server.main.logger import get_logger


logger = get_logger(__name__)

country_code_to_name = {}
for c in list(pycountry.countries):
    country_code = c.alpha_2.lower()
    country_code_to_name[country_code] = c.name

@retry(delay=60, tries=5)
def get_aurehal(aurehal_type):
    nb_rows = 10000
    cursor='*'
    data = []
    while True:
        url = f'https://api.archives-ouvertes.fr/ref/{aurehal_type}/?q=*:*&wt=json&fl=*&sort=docid asc&rows={nb_rows}&cursorMark={cursor}'
        r = requests.get(url)
        res = r.json()
        new_cursor = quote_plus(res['nextCursorMark'])
        data += res['response']['docs']
        if new_cursor == cursor:
            break
        cursor = new_cursor
    return data

def parse_aurehal(elt, aurehal_type):
    if aurehal_type == 'structure':
        return parse_structure(elt)
    elif aurehal_type == 'author':
        return parse_author(elt)

def parse_author(elt):
    author = {}
    author['hal_docid'] = str(elt['docid'])
    
    if isinstance(elt.get('firstName_s'), str):
        author['first_name'] = elt.get('firstName_s').strip()
        
    if isinstance(elt.get('lastName_s'), str):
        author['last_name'] = elt.get('lastName_s').strip()
        
    if author.get('first_name') and author.get('last_name'):
        author['full_name'] = f"{author.get('first_name')} {author.get('last_name')}"
    elif isinstance(elt.get('fullName_s'), str):
        author['full_name'] = elt.get('fullName_s')
        
    if isinstance(elt.get('idHal_i'), int) and elt.get('idHal_i') > 0:
        author['id_hal_i'] = str(elt.get('idHal_i'))
    if isinstance(elt.get('idHal_s'), str):
        author['id_hal_s'] = elt.get('idHal_s')
        
    if isinstance(elt.get('emailDomain_s'), str):
        author['email_domain'] = elt.get('emailDomain_s').strip()
        
    if isinstance(elt.get('idref_id'), str):
        author['idref'] = elt.get('idref_id').strip()
        
    if isinstance(elt.get('orcid_id'), str):
        author['orcid'] = elt.get('orcid_id').strip()
    
    return author

def parse_structure(elt):
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
    affiliation['hal_docid'] = str(elt['docid'])
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

def create_docid_map(data, aurehal_type):
    docid_map = {}
    parsed_data = []
    for d in data:
        docids = [d['docid']]
        if isinstance(d.get('aliasDocid_i'), list):
            docids += d.get('aliasDocid_i')
        docids = list(set(docids))
        parsed_elt = parse_aurehal(d, aurehal_type)
        parsed_data.append(parsed_elt)
        for docid in docids:
            docid_map[str(docid)] = parsed_elt
    logger.debug(f'{aurehal_type} : {len(data)} elts and {len(docid_map)} docids in map')
    return parsed_data, docid_map

def harvest_and_save_aurehal(collection_name, aurehal_type):
    # raw data
    data = get_aurehal(aurehal_type)
    current_file = f'aurehal_raw_{aurehal_type}.json'
    json.dump(data, open(current_file, 'w'))
    os.system(f'gzip {current_file}')
    upload_object('hal', f'{current_file}.gz', f'{collection_name}/{current_file}.gz')
    os.system(f'rm -rf {current_file}.gz')
    
    #parsed data
    parsed_data, docid_map = create_docid_map(data, aurehal_type)
    
    current_file = f'aurehal_{aurehal_type}.json'
    json.dump(parsed_data, open(current_file, 'w'))
    os.system(f'gzip {current_file}')
    upload_object('hal', f'{current_file}.gz', f'{collection_name}/{current_file}.gz')
    os.system(f'rm -rf {current_file}.gz')
    
    # doc id mapping
    current_file = f'aurehal_{aurehal_type}_dict.json'
    json.dump(docid_map, open(current_file, 'w'))
    os.system(f'gzip {current_file}')
    upload_object('hal', f'{current_file}.gz', f'{collection_name}/{current_file}.gz')
    os.system(f'rm -rf {current_file}.gz')
