import os
import requests
import json
from datetime import datetime, timedelta
import logging
import concurrent.futures
from flatten_util import covid_flatten
import pandas as pd
from math import ceil

logging_level = os.environ.get("RULE_LOGGING_LEVEL") or "INFO"
try:
    logging.basicConfig(level=logging.getLevelName(logging_level))
except:
    logging.basicConfig(level=logging.INFO)


class Config:
    SSL_VERIFY = False
    API_ENDPOINT = os.environ.get('API_ENDPOINT') or 'https://sdq-dev.pfizer.com'
    MAX_THREAD_WORKERS = int(os.environ.get('MAX_THREAD_WORKERS') or 20)
    THREAD_BATCH_COUNT = int(os.environ.get('THREAD_BATCH_COUNT') or 250)
    DEV_MODE = False

class APIException(Exception):
    pass


class BaseSDQApi:
    API_HOST = Config.API_ENDPOINT

    def __init__(self, study_id, rule_id, version, batch_id=None, thread_id=None,
                 thread_batch_count=Config.THREAD_BATCH_COUNT, subjects=None):
        self.study_id = study_id
        self.rule_id = rule_id
        self.version = version
        self.batch_id = batch_id
        self._started_at = datetime.utcnow()
        self._end_at = None
        self._data_processed = {}
        self._discrepancy_count = {}
        self.last_run_dt = None
        self.rule_meta_data = self.get_rule_meta(study_id=study_id, rule_id=rule_id, version=version)
        self.batch_id = self.rule_meta_data['current_batch_id'] + 1
        self.thread_id = thread_id
        self.thread_batch_count = thread_batch_count
        self.subjects = subjects

    def execute(self):
        raise NotImplementedError('execute method not implemented')

    def run(self):
        if Config.DEV_MODE or self.thread_id:
            if Config.DEV_MODE:
                logging.info('=' * 10 + 'DEV MODE' + '=' * 10)
            logging.info('starting rule: %s %s %s thread %s' % (self.study_id, self.rule_id, self.version, self.thread_id))
            self.execute()
            self._save_tracking_info()

        else:
            try:
                domain_list = getattr(self, 'domain_list', [])
                last_run_dt = self.get_last_run_at()
                logging.info('domain_list: %s last_run_dt: %s' % (domain_list, last_run_dt))
                all_subjects = self.get_subjects(self.study_id, per_page=self.thread_batch_count, domain_list=domain_list,
                                                modif_dts=last_run_dt)
                all_subjects_count = len(all_subjects)
                logging.info('%s rule: %s  version: %s - subjects: %s domain_list: %s per_thread: %s' %
                            (self.study_id, self.rule_id, self.version, all_subjects_count, domain_list, self.thread_batch_count))

                with concurrent.futures.ThreadPoolExecutor(max_workers=Config.MAX_THREAD_WORKERS) as executor:

                    for thread_id, idx in enumerate(range(0, all_subjects_count, self.thread_batch_count), 1):
                        subject_list = all_subjects[idx: idx + self.thread_batch_count]
                        executor.submit(run_rule, self.__class__, self.study_id, self.rule_id, self.version, thread_id,
                                        self.thread_batch_count, subject_list)

                logging.info('updating last_run_dt')
                self._update_last_run_dt()
            except Exception as exept:
                print(exept)
                logging.info('updating last_run_dt as an exception occurs')
                self._update_last_run_dt()

    def _save_tracking_info(self):
        processed_subjects = len(self._get_processed_data_metrics()) if self._data_processed else 0
        logging.info('thread %s  processed: %s' % (self.thread_id, processed_subjects))
        logging.info('thread %s  discrepancy_counts:  %s' % (self.thread_id, self._get_discrepancy_count()))
        self._end_at = datetime.utcnow()
        disc_items = self._get_discrepancy_count()
        if disc_items:
            track_data = self._get_processed_data_metrics()
            payload = {
                'disc_list': [],
                'start_dt': self._started_at.strftime('%Y-%m-%d %H:%M:%S'),
                'end_dt': self._end_at.strftime('%Y-%m-%d %H:%M:%S'),
                'batch_id': self.batch_id,
                'study_id': self.study_id.lower(),
                'rule_id': self.rule_id,
                'version': int(self.version)
            }

            for subjid, count in disc_items.items():
                track_obj = {
                    'subjid': subjid,
                    'count': count,
                    'track': track_data[subjid]
                }
                payload['disc_list'].append(track_obj)

            url = f"{self.API_HOST}/api-service/1/{self.study_id}/query-metrics"
            response = requests.request("POST", url, data=json.dumps(payload), headers={'content-type': 'application/json'},
                                        verify=Config.SSL_VERIFY)
            if response.status_code not in (200, 201):
                logging.error(response.text)
                raise APIException('Error in save_tracking_info')
            response_json = response.json()
            return response_json['data']

    def _update_last_run_dt(self):
        url = f"{self.API_HOST}/api-service/1/rule/{self.study_id}/{self.rule_id}/{self.version}/update"
        payload = {}
        domain_list = getattr(self, 'domain_list', [])
        if domain_list:
            payload['domain_list'] = domain_list
        response = requests.request("POST", url, data=json.dumps(payload), headers={'content-type': 'application/json'},
                                    verify=Config.SSL_VERIFY)
        if response.status_code not in (200, 201):
            logging.error(response.text)
            raise APIException('Error in update_last_run_dt')
        response_json = response.json()
        return response_json['data']

    @classmethod
    def get_all_rules(cls, study_id=None, status=None):
        url = f"{cls.API_HOST}/api-service/1/rule"
        params = {
            'study_id': study_id,
            'status': status,
        }
        response = requests.request("GET", url, params=params, verify=Config.SSL_VERIFY)
        if response.status_code not in (200,):
            logging.error(response.text)
            raise APIException('Error in fetching rule list')
        response_json = response.json()
        return response_json['data']

    def get_rule_meta(self, study_id=None, rule_id=None, version=None):
        # returns rule metadata matching study_id, rule_id, version from rule_master & map_rules
        if study_id and rule_id and version:
            url = f"{self.API_HOST}/api-service/1/rule/{study_id}/{rule_id}/{version}"
        else:
            url = f"{self.API_HOST}/api-service/1/rule/{self.study_id}/{self.rule_id}/{self.version}"

        response = requests.request("GET", url, verify=Config.SSL_VERIFY)
        if response.status_code not in (200,):
            logging.error(response.text)
            raise APIException('Error in fetching rule meta')
        response_json = response.json()
        return response_json['data']

    def is_rule_active(self, study_id, rule_id, version):
        rule_meta = self.get_rule_meta(study_id=study_id, rule_id=rule_id, version=version)
        return rule_meta['status'] == 'ACTIVE'

    @classmethod
    def get_active_rules(cls, study_id=None):
        return cls.get_all_rules(study_id=study_id, status=['ACTIVE'])

    @classmethod
    def get_studies(cls):
        url = f"{cls.API_HOST}/api-service/1/study"
        response = requests.request("GET", url, verify=Config.SSL_VERIFY)
        if response.status_code not in (200, ):
            logging.error(response.text)
            raise APIException('Error in fetching study list')
        response_json = response.json()
        return response_json['data']

    def _get_subjects(self, study_id, page=1, per_page=10000, domain_list=None, modif_dts=None):
        url = f"{self.API_HOST}/api-service/1/{study_id}/subject/list"
        params = {
            'page': page,
            'per_page': per_page,
            'domain': domain_list,
            'modif_dts': modif_dts
        }
        response = requests.request("GET", url, params=params, verify=Config.SSL_VERIFY)
        if response.status_code not in (200, ):
            logging.error(response.text)
            raise APIException('Error in fetching subject list')
        response_json = response.json()
        return response_json['data']

    def _get_deleted_subjects(self, study_id, page=1, per_page=10000, domain_list=None, modif_dts=None):
        url = f"{self.API_HOST}/api-service/1/{study_id}/deleted-subject/list"
        params = {
            'page': page,
            'per_page': per_page,
            'domain': domain_list,
            'modif_dts': modif_dts
        }
        response = requests.request("GET", url, params=params, verify=Config.SSL_VERIFY)
        if response.status_code not in (200, ):
            logging.error(response.text)
            raise APIException('Error in fetching deleted subject list')
        response_json = response.json()
        return response_json['data']

    def get_subjects_count(self, study_id, per_page=10000, domain_list=None, modif_dts=None):
        url = f"{self.API_HOST}/api-service/1/{study_id}/subject/count"
        params = {
            'per_page': per_page,
            'domain': domain_list,
            'modif_dts': modif_dts
        }
        response = requests.request("GET", url, params=params, verify=Config.SSL_VERIFY)
        if response.status_code not in (200, ):
            logging.error(response.text)
            raise APIException('Error in fetching subject list')
        response_json = response.json()
        return response_json['data']['pages']

    def get_latest_subjects(self, study_id, per_page=10000, domain_list=None, modif_dts=None):
        all_subjects = []
        page = 1
        paginate = True
        while paginate:
            _data = self._get_subjects(study_id, page=page, per_page=per_page, domain_list=domain_list,
                                       modif_dts=modif_dts)
            _data_count = len(_data)
            if _data_count == 0:
                paginate = False
            else:
                page += 1
                all_subjects += _data
        return all_subjects

    def get_all_deleted_subjects(self, study_id, per_page=10000, domain_list=None, modif_dts=None):
        deleted_subjects = []
        page = 1
        paginate = True
        while paginate:
            _data = self._get_deleted_subjects(study_id, page=page, per_page=per_page, domain_list=domain_list,
                                               modif_dts=modif_dts)
            _data_count = len(_data)
            if _data_count == 0:
                paginate = False
            else:
                page += 1
                deleted_subjects += _data
        return deleted_subjects

    def get_subjects(self, study_id, per_page=10000, domain_list=None, modif_dts=None):
        if self.thread_id:
            return self.subjects
        else:
            all_subjects = []
            modif_dts = modif_dts or self.get_last_run_at()

            subjects = self.get_latest_subjects(study_id, domain_list=domain_list, modif_dts=modif_dts)
            logging.info('Got %s new/modified subjects' % len(subjects))
            all_subjects.extend(subjects)

            deleted_subjects = self.get_all_deleted_subjects(study_id, domain_list=domain_list, modif_dts=modif_dts)
            logging.info('Got %s deleted subjects' % len(deleted_subjects))
            all_subjects.extend(deleted_subjects)

            return list(set(all_subjects))

    def _get_form_index_list(self, study_id, page=1, per_page=10000, domain_list=None, modif_dts=None):
        url = f"{self.API_HOST}/api-service/1/{study_id}/form-index/list"
        params = {
            'page': page,
            'per_page': per_page,
            'domain': domain_list,
            'modif_dts': modif_dts
        }
        response = requests.request("GET", url, params=params, verify=Config.SSL_VERIFY)
        if response.status_code not in (200, ):
            logging.error(response.text)
            raise APIException('Error in fetching form_index list')
        response_json = response.json()
        return response_json['data']

    def _get_itemrepn_list(self, study_id, page=1, per_page=10000, domain_list=None, modif_dts=None):
        url = f"{self.API_HOST}/api-service/1/{study_id}/itemrepn/list"
        params = {
            'page': page,
            'per_page': per_page,
            'domain': domain_list,
            'modif_dts': modif_dts
        }
        response = requests.request("GET", url, params=params, verify=Config.SSL_VERIFY)
        if response.status_code not in (200, ):
            logging.error(response.text)
            raise APIException('Error in fetching itemrepn list')
        response_json = response.json()
        return response_json['data']

    def get_form_index_list(self, study_id, per_page=10000, domain_list=None, modif_dts=None):
        form_index_list = []
        page = 1
        paginate = True
        while paginate:
            _data = self._get_form_index_list(study_id, page=page, per_page=per_page, domain_list=domain_list,
                                              modif_dts=modif_dts)
            _data_count = len(_data)
            if _data_count == 0:
                paginate = False
            else:
                page += 1
                form_index_list += _data
        return form_index_list

    def get_itemrepn_list(self, study_id, per_page=10000, domain_list=None, modif_dts=None):
        itemrepn_list = []
        page = 1
        paginate = True
        while paginate:
            _data = self._get_itemrepn_list(study_id, page=page, per_page=per_page, domain_list=domain_list,
                                            modif_dts=modif_dts)
            _data_count = len(_data)
            if _data_count == 0:
                paginate = False
            else:
                page += 1
                itemrepn_list += _data
        return itemrepn_list

    def get_all_form_index_list(self, study_id, per_page=10000, domain_list=None, modif_dts=None):
        all_form_index = []
        modif_dts = modif_dts or self.get_last_run_at()
        form_index_list = self.get_form_index_list(study_id, domain_list=domain_list, modif_dts=modif_dts)
        logging.info('Got %s form_index' % len(form_index_list))
        itemrepn_list = self.get_itemrepn_list(study_id, domain_list=domain_list, modif_dts=modif_dts)
        logging.info('Got %s itemrepn' % len(itemrepn_list))
        all_form_index.extend(itemrepn_list)
        return list(set(all_form_index))

    def get_last_run_at(self):
        if self.rule_meta_data.get('last_run_dt'):
            self.last_run_dt = datetime.strptime(self.rule_meta_data['last_run_dt'],
                                                 '%a, %d %b %Y %H:%M:%S %Z') + timedelta(seconds=1)
        else:
            self.last_run_dt = datetime(1900, 1, 1)
        return self.last_run_dt

    def get_site_ids(self, study_id):
        url = f"{self.API_HOST}/api-service/1/{study_id}/site"
        response = requests.request("GET", url, verify=Config.SSL_VERIFY)
        if response.status_code not in (200, ):
            logging.error(response.text)
            raise APIException('Error in fetching site list')
        response_json = response.json()
        return response_json['data']

    def get_flatten_count(self, study, subject, per_page=10000, formname_list=None, formrefname_list=None,
                          domain_list=None):
        url = f"{self.API_HOST}/api-service/1/{study}/{subject}/flatten-data/count"
        params = {
            'per_page': per_page,
            'formname': formname_list,
            'formrefname': formrefname_list,
            'domain': domain_list
        }
        response = requests.request("GET", url, params=params, verify=Config.SSL_VERIFY)
        if response.status_code not in (200,):
            logging.error(response.text)
            raise APIException('Error in fetching flatten count')
        response_json = response.json()
        return response_json['data']['pages']

    def get_flatten_list(self, study, subject, page=1, per_page=10000, formname_list=None, formrefname_list=None,
                         domain_list=None):
        url = f"{self.API_HOST}/api-service/1/{study}/{subject}/flatten-data/list"
        params = {
            'page': page,
            'per_page': per_page,
            'formname': formname_list,
            'formrefname': formrefname_list,
            'domain': domain_list
        }
        response = requests.request("GET", url, params=params, verify=Config.SSL_VERIFY)
        if response.status_code not in (200,):
            logging.error(response.text)
            raise APIException('Error in fetching flatten list')
        response_json = response.json()
        return response_json['data']

    def get_unflatten_count(self, study, subject, per_page=10000, formname_list=None, formrefname_list=None,
                            domain_list=None):
        url = f"{self.API_HOST}/api-service/1/{study}/{subject}/unflatten-data/count"
        params = {
            'per_page': per_page,
            'formname': formname_list,
            'formrefname': formrefname_list,
            'domain': domain_list
        }
        response = requests.request("GET", url, params=params, verify=Config.SSL_VERIFY)
        if response.status_code not in (200,):
            logging.error(response.text)
            raise APIException('Error in fetching unflatten count')
        response_json = response.json()
        return response_json['data']['pages']

    def get_unflatten_list(self, study, subject, page=1, per_page=10000, formname_list=None, formrefname_list=None,
                           domain_list=None, max_ck_event_id=None):
        url = f"{self.API_HOST}/api-service/1/{study}/{subject}/unflatten-data/list"
        params = {
            'page': page,
            'per_page': per_page,
            'formname': formname_list,
            'formrefname': formrefname_list,
            'domain': domain_list,
            'prev_ck_event_id': max_ck_event_id
        }
        response = requests.request("GET", url, params=params, verify=Config.SSL_VERIFY)
        if response.status_code not in (200,):
            logging.error(response.text)
            raise APIException('Error in fetching unflatten list')
        response_json = response.json()
        return response_json['data']

    def _validate_query_payload(self, payload):
        try:
            _payload = json.dumps(payload)
            return True
        except Exception as e:
            logging.exception(e)
            return False

    def insert_query(self, study, subject, payload):
        is_valid = self._validate_query_payload(payload)
        if not is_valid:
            raise Exception('Unable to parse pandas data type in json.dumps()')

        url = f"{self.API_HOST}/api-service/1/{study}/query"
        _payload = {
            "stg_ck_event_id": payload['stg_ck_event_id'],
            "subcategory": payload['subcategory'],
            "query_text": payload['query_text'],
            "form_index": payload['form_index'],
            "question_present": payload['question_present'],
            "report": payload['report'],
            "modif_dts": payload['modif_dts'],
            "visit_nm": payload.get('visit_nm'),
            "confid_score": round(payload.get('confid_score', 1), 2)
        }
        if payload.get('formrefname'):
            _formrefname, _sectionrefname, _inform = self.get_edc_mapping(payload['formrefname'])
            _payload['formrefname'] = _formrefname
            _payload['sectionrefname'] = _sectionrefname
            _payload['question_present']['INFORM'] = [_inform]
        else:
            _payload['formrefname'] = self.rule_meta_data['formrefname']
            _payload['sectionrefname'] = self.rule_meta_data['sectionrefname']

        response = requests.request("POST", url, data=json.dumps(_payload), headers={'content-type': 'application/json'},
                                    verify=Config.SSL_VERIFY)
        if response.status_code not in (200, 201):
            logging.error(response.text)
            raise APIException('Error in insert_query')
        response_json = response.json()

        self._incr_discrepancy_count(subject)

        return response_json['data']

    def get_edc_mapping(self, formrefname):
        rm_formrefname = self.rule_meta_data['formrefname'].split(',')
        rm_sectionrefname = self.rule_meta_data['sectionrefname'].split(',')
        rm_inform = self.rule_meta_data['item_nm_edc'].split(',')
        try:
            mapping_index = rm_formrefname.index(formrefname)
        except ValueError:
            logging.error(f"""{formrefname} not found in {rm_formrefname} for study: {self.study_id} rule: {self.rule_id} version: {self.version} in rule_master""")
            raise
        _formrefname = rm_formrefname[mapping_index]
        _sectionrefname = rm_sectionrefname[mapping_index]
        _inform = rm_inform[mapping_index]
        return _formrefname, _sectionrefname, _inform

    def get_query_text_json(self, study, subcategory, params=None):
        query_text = self.rule_meta_data.get('query_text')
        if query_text:
            if params:
                query_text = query_text.format(*params)
            return query_text
        return 'query_text not present'

    def get_model_query_text_json(self, study, subcategory, params={}):
        query_text = self.rule_meta_data.get('query_text')
        if query_text:
            if params:
                query_text = query_text.format(**params)
            return query_text
        return 'query_text not present'

    def get_subcategory_json(self, study, subcategory):
        try:
            _json = {
                'SRDM': self.rule_meta_data['item_nm_preconf'].split(','),
                'INFORM': self.rule_meta_data['item_nm_edc'].split(',')
            }
        except KeyError as e:
            logging.error(f"""{e} not found for study: {self.study_id} rule: {self.rule_id} version: {self.version} in rule_master""")
            raise
        return _json

    def _set_processed_data(self, subject, flatten_data):
        for domain, records in flatten_data.items():
            if subject in self._data_processed:
                self._data_processed[subject].update({domain: len(records)})
            else:
                self._data_processed[subject] = {domain: len(records)}

    def _get_processed_data_metrics(self, subject=None):
        if subject:
            return self._data_processed.get(subject)
        return self._data_processed

    def _get_discrepancy_count(self, subject=None):
        if subject:
            return self._discrepancy_count.get(subject) or 0
        return self._discrepancy_count

    def _incr_discrepancy_count(self, subject):
        if subject in self._discrepancy_count:
            self._discrepancy_count[subject] += 1
        else:
            self._discrepancy_count[subject] = 1

    '''def get_flatten_data(self, study, subject, per_page=10000, formname_list=None, formrefname_list=None,
                         domain_list=None):
        unflatten_data = self.get_unflatten_data(study, subject, per_page=per_page, formname_list=formname_list,
                                                 formrefname_list=formrefname_list, domain_list=domain_list)
        rows_df = pd.DataFrame(unflatten_data)
        flatten_data = covid_flatten(rows_df)
        self._set_processed_data(subject, flatten_data)
        return flatten_data'''
    def get_flatten_data(self, study, subject, per_page=10000, formname_list=None, formrefname_list=None,
                     domain_list=None):
        unflatten_data = self.get_unflatten_data(study, subject, per_page=per_page, formname_list=formname_list,
                                                 formrefname_list=formrefname_list, domain_list=domain_list)
        rows_df = pd.DataFrame(unflatten_data)
        study_type_col_map = self.get_study_type_mapping()
        study_type_df = pd.DataFrame(study_type_col_map)
        
        map_study = pd.DataFrame(self.get_studies())
        study_type = map_study[map_study['study_id'] == study.upper()]['study_type'].values.tolist()[0]
        
        flatten_data = covid_flatten(rows_df, study_type_df, study_type)
        self._set_processed_data(subject, flatten_data)
        return flatten_data
    
    def get_study_type_mapping(self):
        url = f"{self.API_HOST}/api-service/1/study-type-mapping"
        response = requests.request("GET", url, verify=Config.SSL_VERIFY)
        if response.status_code not in (200,):
            logging.error(response.text)
            raise APIException('Error in fetching study_type_mapping')
        response_json = response.json()
        return response_json['data']
    

    def get_unflatten_data(self, study, subject, per_page=10000, formname_list=None, formrefname_list=None,
                           domain_list=None):
        unflatten_data = []
        count = self.get_unflatten_count(study, subject, per_page=per_page, formname_list=formname_list,
                                         formrefname_list=formrefname_list, domain_list=domain_list)
        max_ck_event_id = None
        for page_no in range(1, count + 1):
            _data = self.get_unflatten_list(study, subject, page=page_no, per_page=per_page,
                                            formname_list=formname_list, formrefname_list=formrefname_list,
                                            domain_list=domain_list, max_ck_event_id=max_ck_event_id)
            max_ck_event_id = _data[-1]['ck_event_id']
            unflatten_data.extend(_data)
        return unflatten_data

    def get_fda_approved_diagnostics(self, study):
        url = f"{self.API_HOST}/api-service/1/{study}/fda-approved-diagnostics"
        response = requests.request("GET", url, verify=Config.SSL_VERIFY)
        if response.status_code not in (200,):
            logging.error(response.text)
            raise APIException('Error in fetching fda_approved_diagnostics')
        response_json = response.json()
        return response_json['data']
       
    def check_if_duplicate(self, study, subjid, subcat, index, report):
        url = f"{self.API_HOST}/api-service/1/{study}/is-duplicate"
        payload = {
            'subjid': subjid,
            'subcat': subcat,
            'index': index,
            'report': report,
        }
        response = requests.request("POST", url, data=json.dumps(payload), headers={'content-type': 'application/json'},
                                    verify=Config.SSL_VERIFY)
        if response.status_code not in (200, 201, ):
            logging.error(response.text)
            raise APIException('Error in check_if_duplicate')
        response_json = response.json()
        return response_json['data']['is_duplicate']
       
    def check_labtest_duplicate(self, study, subjid, subcat, visit_nm, visit_ix, formrefname, form_ix):
        url = f"{self.API_HOST}/api-service/1/{study}/is-lbtest-duplicate"
        payload = {
            'subjid': subjid,
            'subcat': subcat,
            'visit_nm': visit_nm,
            'visit_ix': visit_ix,
            'formrefname': formrefname,
            'form_ix': form_ix
        }
        response = requests.request("POST", url, data=json.dumps(payload), headers={'content-type': 'application/json'},
                                    verify=Config.SSL_VERIFY)
        if response.status_code not in (200, 201, ):
            logging.error(response.text)
            raise APIException('Error in check_if_duplicate')
        response_json = response.json()
        return response_json['data']['is_duplicate']

    def get_drugs_dict(self, study):
        url = f"{self.API_HOST}/api-service/1/{study}/get-drugs"
        response = requests.request("GET", url, verify=Config.SSL_VERIFY)
        
        if response.status_code not in (200, 201, ):
            logging.error(response.text)
            raise APIException('Error in get_drugs_dict')
        response_json = response.json()
        return response_json['data']['drugs_dict']

def run_rule(klass, study_id, rule_id, version, thread_id, thread_batch_count, subject_list):
    rule = klass(study_id, rule_id, version, thread_id=thread_id, thread_batch_count=thread_batch_count,
                 subjects=subject_list)
    rule.run()
