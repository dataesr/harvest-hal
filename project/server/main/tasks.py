import time
import datetime
import os
import requests
from project.server.main.feed import harvest_and_insert, load_collection_from_object_storage

from project.server.main.logger import get_logger

logger = get_logger(__name__)

def create_task_harvest(arg):
    collection_name = arg.get('collection_name')
    harvest_aurehal = arg.get('harvest_aurehal', True)
    min_year = arg.get('min_year', 1000)
    if collection_name:
        harvest_and_insert(collection_name, harvest_aurehal, min_year)

#    url_hal_update = "https://api.archives-ouvertes.fr/search/?fq=doiId_s:*%20AND%20structCountry_s:fr%20AND%20modifiedDate_tdate:[{0}T00:00:00Z%20TO%20{1}T00:00:00Z]%20AND%20producedDate_tdate:[2013-01-01T00:00:00Z%20TO%20{1}T00:00:00Z]&fl=halId_s,doiId_s,openAccess_bool&rows={2}&start={3}"

def create_task_load_collection_from_object_storage(args):
    collection_name = args.get('collection_name')
    if collection_name:
        load_collection_from_object_storage(collection_name)
