import os
import json
import re
from dateutil import parser
from tokenizers import normalizers
from tokenizers.normalizers import NFD, StripAccents, Lowercase, BertNormalizer, Sequence, Strip
from tokenizers import pre_tokenizers
from tokenizers.pre_tokenizers import Whitespace
from project.server.main.utils_swift import upload_object, download_object
from project.server.main.logger import get_logger

normalizer = Sequence([BertNormalizer(clean_text=True,
        handle_chinese_chars=True,
        strip_accents=True,
        lowercase=True), Strip()])
pre_tokenizer = pre_tokenizers.Sequence([Whitespace()])

logger = get_logger(__name__)

def normalize(x, min_length = 0):
    normalized = normalizer.normalize_str(x)
    normalized = normalized.replace('\n', ' ')
    normalized = re.sub(' +', ' ', normalized)
    return " ".join([e[0] for e in pre_tokenizer.pre_tokenize_str(normalized) if len(e[0]) > min_length])

def get_repository(a_repo: str) -> str:
    if a_repo.replace('www.', '')[0:3].lower() == 'hal':
        return 'HAL'
    for r in ['bioRxiv', 'medRxiv', 'arXiv', 'Research Square', 'Zenodo', 'Archimer', 'RePEc', 'CiteSeerX', 'univOAK']:
        if r.lower().replace(' ', '') in a_repo.lower():
            return r
    if 'lilloa' in a_repo.lower():
        return 'LillOA (Lille Open Archive)'
    if 'ucl.ac.uk' in a_repo.lower():
        return 'UCL Discovery'
    if 'lirias' in a_repo.lower() and 'kuleuven' in a_repo.lower():
        return 'LIRIAS (KU Leuven)'
    if 'pure.atira.dk' in a_repo.lower():
        return 'Pure (Denmark)'
    if 'digital.csic.es' in a_repo.lower():
        return 'DIGITAL.CSIC (Spain)'
    if 'escholarship.org/ark' in a_repo.lower():
        return 'California Digital Library - eScholarship'
    if 'jupiter.its.unimelb.edu.au' in a_repo.lower():
        return 'University of Melbourne - Minerva Access'
    if 'helda.helsinki' in a_repo.lower():
        return 'HELDA - Digital Repository of the University of Helsinki'
    if 'osti.gov' in a_repo.lower():
        return 'US Office of Scientific and Technical Information'
    for f in ['pubmedcentral', 'ncbi.nlm.nih.gov/pmc', 'europepmc']:
        if f in a_repo:
            return 'PubMed Central'
    return a_repo

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
    external_ids = []
    if isinstance(notice.get('halId_s'), str):
        external_ids.append({'id_type': 'hal_id', 'id_value': notice.get('halId_s')})
        res['hal_id'] = notice.get('halId_s')
    if isinstance(notice.get('nntId_s'), str):
        external_ids.append({'id_type': 'nnt_id', 'id_value': notice.get('nntId_s')})
        res['nnt_id'] = notice.get('nntId_s')
    if external_ids:
        res['external_ids'] = external_ids
    title = ''
    if isinstance(notice.get('title_s'), list):
        if len(notice.get('title_s')) > 0:
            title = notice.get('title_s')[0].strip()
    subtitle = ''
    if isinstance(notice.get('subTitle_s'), list):
        if len(notice.get('subTitle_s')) > 0:
            subtitle = notice.get('subTitle_s')[0].strip()
    if title and subtitle:
        title = f'{title} : {subtitle}'
    if title:
        res['title'] = title
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

    if isinstance(notice.get('docType_s'), str):
        doctype = notice.get('docType_s')
        if doctype == 'ART':
            res['genre'] = 'journal-article'
        elif doctype in ['COMM', 'DOUV']:
            res['genre'] = 'proceedings'
        elif doctype == 'OUV':
            res['genre'] = 'book'
        elif doctype == 'COUV':
            res['genre'] = 'book-chapter'
        elif doctype == 'THESE':
            res['genre'] = 'thesis'
        else:
            res['genre'] = doctype.lower()

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
    nb_auth_quality = 0
    if isinstance(notice.get('authQuality_s'), list):
        nb_auth_quality = len(notice.get('authQuality_s'))
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
        nb_author = len(notice.get('authId_i'))
        for ix, a in enumerate(authors):
            a['author_position'] = ix + 1
            if nb_author == nb_auth_quality:
                a['role'] = notice.get('authQuality_s')[ix]
        res['authors'] = authors

    # DATE
    publication_date = None
    for f in ['publicationDate_s', 'ePublicationDate_s', 'defenseDate_s', 'producedDate_s']:
        if isinstance(notice.get(f), str) and publication_date is None:
            try:
                publication_date = parser.parse(notice[f]).isoformat()
                res['published_date'] = publication_date
                res['year'] = publication_date[0:4]
                #logger.debug(f'publication_date keeping {f}')
            except:
                pass

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
    oa_host_type = None
    if notice.get('openAccess_bool') or notice.get('linkExtUrl_s'):
        is_oa = True
    observation_date = get_millesime(snapshot_date)
    oa_locations = []
    license = None
    if isinstance(notice.get('licence_s'), str):
        license = notice.get('licence_s')
    if isinstance(notice.get('fileMain_s'), str):
        is_oa = True
        oa_host_type = 'repository'
        oa_locations.append(
            {'url': notice.get('fileMain_s'), 
             'repository_institution': 'HAL',
             'license': license,
             'host_type': oa_host_type})
    elif isinstance(notice.get('linkExtUrl_s'), str):
        is_oa = True
        oa_host_type = None
        url = notice.get('linkExtId_s').strip()
        if get_repository(url) != url:
            host_type = 'repository'
            repository = get_repository(url)
            oa_locations.append(
                {'url': url, 
                'repository_normalized': repository,
                'license': host_type,
                'host_type': oa_host_type})
        elif 'doi' in notice.get('linkExtId_s').lower().strip():
            oa_host_type = 'publisher'
            oa_locations.append(
                {'url': url, 
                'license': host_type,
                'host_type': oa_host_type})

    res['oa_details'] = {}
    res['oa_details'][snapshot_date] = {'is_oa': is_oa, 'snapshot_date': snapshot_date, 'observation_date': observation_date}
    if is_oa:
        res['oa_details'][snapshot_date]['oa_host_type'] = oa_host_type
        if oa_host_type == 'repository':
            res['oa_details'][snapshot_date]['oa_colors'] = ['green']
            res['oa_details'][snapshot_date]['oa_colors_with_priority_to_publisher'] = ['green_only']
        else:
            res['oa_details'][snapshot_date]['oa_colors'] = ['other']
            res['oa_details'][snapshot_date]['oa_colors_with_priority_to_publisher'] = ['other']
        res['oa_details'][snapshot_date]['repositories'] = [k['repository_normalized'] for k in oa_locations if 'repository_normalized' in k]
        res['oa_details'][snapshot_date]['oa_locations'] = oa_locations


    ## title - first author
    title_first_author = ""
    if res.get('title'):
        title_first_author += normalize(res.get('title'), 1).strip()
    if isinstance(res.get('authors'), list) and len(res['authors']) > 0:
        if res['authors'][0].get('full_name'):
            title_first_author += ';'+normalize(res['authors'][0].get('full_name'), 1)
    if title_first_author:
        res['title_first_author'] = title_first_author
    return res
