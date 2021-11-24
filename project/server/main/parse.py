import os
import json
from dateutil import parser
from tokenizers import normalizers
from tokenizers.normalizers import NFD, StripAccents, Lowercase
from tokenizers import pre_tokenizers
from tokenizers.pre_tokenizers import Whitespace
from project.server.main.utils_swift import upload_object, download_object
from project.server.main.logger import get_logger

normalizer = normalizers.Sequence([NFD(), StripAccents(), Lowercase()])
pre_tokenizer = pre_tokenizers.Sequence([Whitespace()])

logger = get_logger(__name__)

def normalize(x):
    normalized = normalizer.normalize_str(x)
    return " ".join([e[0] for e in pre_tokenizer.pre_tokenize_str(normalized) if len(e[0]) > 1])

def get_millesime(x: str) -> str:
    try:
        if x[0:4] < '2021':
            return x[0:4]
        month = int(x[4:6])
        if 1 <= month <= 3:
            return x[0:4] + 'Q1'
        if 4 <= month <= 6:
            return x[0:4] + 'Q2'
        if 7 <= month <= 9:
            return x[0:4] + 'Q3'
        if 10 <= month <= 12:
            return x[0:4] + 'Q4'
        return 'unk'
    except:
        return x

def get_aurehal_from_OS(collection_name, aurehal_type):
    target_file = f'aurehal_{collection_name}_{aurehal_type}_dict.json'
    os.system(f'rm -rf {target_file}.gz')
    os.system(f'rm -rf {target_file}')
    download_object('hal', f'{collection_name}/aurehal_{aurehal_type}_dict.json.gz', f'{target_file}.gz')
    os.system(f'gunzip {target_file}.gz')
    return json.load(open(target_file, 'r'))

def parse_hal(notice, aurehal, snapshot_date):
    res = {}
    res['sources'] = ['HAL']
    if isinstance(notice.get('doiId_s'), str):
        res['doi'] = notice.get('doiId_s').lower().strip()
    if isinstance(notice.get('halId_s'), str):
        res['external_ids'] = [{'id_type': 'hal_id', 'id_value': notice.get('halId_s')}]
        res['hal_id'] = notice.get('halId_s')
    if isinstance(notice.get('title_s'), list):
        if len(notice.get('title_s')) > 0:
            res['title'] = notice.get('title_s')[0].strip()
    if isinstance(notice.get('abstract_s'), list):
        if len(notice.get('abstract_s')) > 0:
            res['abstract'] = [{'abstract': notice.get('abstract_s')[0].strip()}]
    if isinstance(notice.get('structId_i'), list):
        affiliations = []
        for s in notice.get('structId_i'):
            structId = str(s)
            if structId in aurehal['structure']:
                if aurehal['structure'][structId] not in affiliations:
                    affiliations.append(aurehal['structure'][structId])
            else:
                logger.debug(f'from structure;{structId}; not in aurehal data ? type: {type(structId)}')
        if affiliations:
            res['affiliations'] = affiliations

        countries = []
        for aff in affiliations:
            if isinstance(aff.get('detected_countries'), list):
                countries += aff.get('detected_countries')
        res['detected_countries'] = list(set(countries))

    ## AUTHORS
    authors_affiliations = {}
    if isinstance(notice.get('authIdHasStructure_fs'), list):
        for facet in notice.get('authIdHasStructure_fs'):
            authorId = str(facet.split('JoinSep')[0].split('FacetSep')[0]).replace('_', '')
            structId = str(facet.split('JoinSep')[1].split('FacetSep')[0]).replace('_', '')
            if authorId not in authors_affiliations:
                authors_affiliations[authorId] = []
            if structId in aurehal['structure']:
                authors_affiliations[authorId].append(aurehal['structure'][structId])
            else:
                logger.debug(f'from authors : struct ;{structId}; not in aurehal; type: {type(structId)};facet {facet}')

    authors = []
    if isinstance(notice.get('authId_i'), list):
        for authorId in notice.get('authId_i'):
            authorIdStr = str(authorId)
            if authorIdStr in aurehal['author']:
                author = aurehal['author'][authorIdStr]
                if authorIdStr in authors_affiliations:
                    author['affiliations'] = authors_affiliations[authorIdStr]
                authors.append(author)
            else:
                logger.debug(f'author ;{authorIdStr}; not in aureal ?; type: {type(authorIdStr)}')
    if authors:
        res['authors'] = authors

    # DATE
    publication_date = None
    for f in ['publicationDate_s', 'ePublicationDate_s', 'defenseDate_s', 'producedDate_s']:
        if isinstance(notice.get(f), str) and len(notice[f]) > 4:
            publication_date = parser.parse(notice[f]).isoformat()
    if publication_date is None:
        for f in ['publicationDate_s', 'ePublicationDate_s', 'defenseDate_s', 'producedDate_s']:
            if isinstance(notice.get(f), str):
                publication_date = parser.parse(notice[f]).isoformat()
    if publication_date:
        res['published_date'] = publication_date
        res['year'] = publication_date[0:4]

    # PUBLISHER
    if isinstance(notice.get('journalPublisher_s'), str):
        res['publisher'] = notice.get('journalPublisher_s').strip()
   
    # ISSN
    journal_issns = []
    for f in ['journalIssn_s', 'journalEissn_s']:
        if isinstance(notice.get(f), str):
            journal_issns.append(notice.get(f).strip())
    if journal_issns:
        res['journal_issns'] = ','.join(journal_issns)

    # KEYWORDS
    keywords = []
    if isinstance(notice.get('keyword_s'), list):
        for k in notice['keyword_s']:
            if isinstance(k, str):
                keywords.append({'keyword': k.strip()})
    if keywords:
        res['keywords'] = keywords

    # HAL CLASSIF
    hal_classification = []
    if isinstance(notice.get('en_domainAllCodeLabel_fs'), list):
        for c in notice.get('en_domainAllCodeLabel_fs'):
            hal_classification.append({'code': c.split('FacetSep')[0], 'label': c.split('FacetSep')[1]})
    if hal_classification:
        res['hal_classification'] = hal_classification
    
    ### GRANTS
    grants = []
    if isinstance(notice.get('funding_s'), list):
        for f in notice['funding_s']:
            if isinstance(f, str):
                grants.append({'grant':f})
                
    if isinstance(notice.get('anrProjectReference_s'), list):
        for f in notice['anrProjectReference_s']:
            if isinstance(f, str):
                grants.append({'grantid':f, 'agency': 'ANR', 'country': 'France'})
                
    if isinstance(notice.get('europeanProjectReference_s'), list):
        for f in notice['europeanProjectReference_s']:
            if isinstance(f, str):
                grants.append({'grantid':f, 'agency': 'Europe'})
    
    if grants:
        res['grants'] = grants
        res['has_grant'] = True
    else:
        res['has_grant'] = False
    
    ## OA #####
    oa_details = {}
    is_oa = False
    if notice.get('openAccess_bool') or notice.get('linkExtUrl_s'):
        is_oa = True
    observation_date = get_millesime(snapshot_date)
    oa_locations = []
    license = None
    if isinstance(notice.get('licence_s'), str):
        license = notice.get('licence_s')
    if isinstance(notice.get('fileMain_s'), str):
        oa_locations.append(
            {'url': notice.get('fileMain_s'), 
             'repository_institution': 'HAL',
             'license': license,
             'is_oa': is_oa,
            'snapshot_date': snapshot_date,
            'observation_date': observation_date,
             'host_type': 'repository'})
    elif isinstance(notice.get('linkExtUrl_s'), str):
        host_type = None
        if notice.get('linkExtId_s').lower().strip() in ['arxiv', 'pubmedcentral']:
            host_type = 'repository'
        elif 'doi' in notice.get('linkExtId_s').lower().strip():
            host_type = 'publisher'
        oa_locations.append(
            {'url': notice.get('linkExtUrl_s'), 
            'repository_institution': notice.get('linkExtId_s'),
            'license': host_type,
            'snapshot_date': snapshot_date,
            'observation_date': observation_date,
             'is_oa': is_oa,
            'host_type': host_type})
    if not oa_locations:
        oa_locations.append({'is_oa': False, 'snapshot_date': snapshot_date, 'observation_date': observation_date})
    oa_details[snapshot_date] = oa_locations
    res['oa_details'] = oa_details

    ## title - first author
    title_first_author = ""
    if res.get('title'):
        title_first_author += normalize(res.get('title')).strip()
    if isinstance(res.get('authors'), list) and len(res['authors']) > 0:
        if res['authors'][0].get('full_name'):
            title_first_author += ';'+normalize(res['authors'][0].get('full_name'))
    if title_first_author:
        res['title_first_author'] = title_first_author
    return res
