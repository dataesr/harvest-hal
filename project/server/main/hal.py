def get_hal_notice(hal_id):
        _URL = "https://api.archives-ouvertes.fr/search/?q={}:{}"
    _URL += "&fl=europeanProjectAcronym_s,europeanProjectReference_s,"
    _URL += "europeanProjectTitle_s,anrProjectAcronym_s,"
    _URL += "anrProjectReference_s,anrProjectTitle_s,"
    _URL += "authFullNameId_fs,authIdHasPrimaryStructure_fs,"
    _URL += "abstract_s,doiId_s,docid,modifiedDate_tdate,"
    _URL += "structIdName_fs,structCountry_s,halId_s,"
    _URL += "submittedDate_tdate,producedDate_tdate,en_keyword_s,"
    _URL += "fr_keyword_s,docType_s,title_s,journalEissn_s,journalIssn_s,"
    _URL += "journalPublisher_s,journalTitle_s,bookTitle_s,conferenceTitle_s,"
    _URL += "issue_s,openAccess_bool,page_s,licence_s,language_s,funding_s,"
    _URL += "domain_s,linkExtUrl_s,fileMain_s&rows=10000&start=0"
