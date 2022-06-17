import pandas as pd
import json
from SPARQLWrapper import SPARQLWrapper, JSON

from project.server.main.utils_swift import upload_object, download_object
from project.server.main.logger import get_logger

logger = get_logger(__name__)

sparql = SPARQLWrapper("https://data.idref.fr/sparql")

QUERY_START = """
SELECT ?idref ?ext_id
WHERE {?idref owl:sameAs ?ext_id.
?idref a foaf:Person.
FILTER (STRSTARTS(STR(?ext_id),
"""

def to_jsonl(input_list, output_file, mode = 'a'):
    with open(output_file, mode) as outfile:
        for entry in input_list:
            entry = {f: entry[f] for f in entry if entry[f]==entry[f] }
            json.dump(entry, outfile)
            outfile.write('\n')

def clean_json(elt):
    keys = list(elt.keys()).copy()
    for f in keys:
        if isinstance(elt[f], dict):
            elt[f] = clean_json(elt[f])
        elif (not elt[f] == elt[f]) or (elt[f] is None):
            del elt[f]
    return elt

def add_data(data, uri_prefix, target_index, target_id):
    QUERY_END = f"'{uri_prefix}'))" + "}"
    query = QUERY_START+QUERY_END
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    result = sparql.query().convert()
    nb_add = 0
    for r in result['results']['bindings']:
        idref = 'idref'+r['idref']['value'].split('/')[3]
        ext_id = r['ext_id']['value'].split('/')[target_index].split('#')[0]
        if target_id == 'orcid' and ext_id[0:2] != '00':
            continue
        if idref not in data:
            data[idref] = {'id':idref, 'externalIds':[]}
        externalIds = data[idref]['externalIds']
        has_ext = False
        for e in externalIds:
            if e['type']==target_id:
                has_ext = True
                if e['id'] != ext_id:
                    logger.debug(f"replace !!! {idref} {target_id} {e['id']} ==> {ext_id}")
                    e['id'] = ext_id
        if has_ext is False:
            data[idref]['externalIds'].append({'type': target_id, 'id': ext_id})
            nb_add += 1
    logger.debug(f'adding {nb_add} links idref-{target_id}')


def update_vip():
    download_object('misc', 'vip.jsonl', f'vip.jsonl')
    input_idrefs = pd.read_json('vip.jsonl', lines=True).to_dict(orient='records')
    data = {}
    for e in input_idrefs:
        data[e['id']] = e
    add_data(data, 'https://data.archives-ouvertes.fr', 4, 'id_hal_s')
    add_data(data, 'https://orcid.org', 3, 'orcid')
    add_data(data, 'https://univ-droit.fr', 4, 'univ-droit')
    vip = [clean_json(v) for v in list(data.values())]
    to_jsonl(vip, 'vip.jsonl', 'w')
    upload_object('misc', 'vip.jsonl', 'vip.jsonl')
