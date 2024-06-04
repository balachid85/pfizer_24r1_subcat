import os
import requests
import yaml
import json
from datetime import datetime, timedelta
import logging
import concurrent.futures
from flatten_util import covid_flatten_v1
import pandas as pd
from math import ceil
import traceback
from requests.adapters import HTTPAdapter, Retry
# from sqlalchemy import text, create_engine
# from pfizer_study_get_vol import pfizer_adapters


logging_level = os.environ.get("RULE_LOGGING_LEVEL") or "INFO"
try:
    logging.basicConfig(level=logging.getLevelName(logging_level))
except:
    logging.basicConfig(level=logging.INFO)

try:
    curr_file_path = os.path.realpath(__file__)
    curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
    subcat_config_path = os.path.join(curr_path, 'config.yaml')
    stream = open(subcat_config_path, 'r')
    config = yaml.load(stream, Loader=yaml.FullLoader)

    api_endpoint = config['API_ENDPOINT']
except Exception as e:
    print(traceback.format_exc())



class Config:
    SSL_VERIFY = False
    API_ENDPOINT = os.environ.get('API_ENDPOINT') or 'http://0.0.0.0:5004'
    MAX_THREAD_WORKERS = int(os.environ.get('MAX_THREAD_WORKERS') or 10)
    THREAD_BATCH_COUNT = int(os.environ.get('THREAD_BATCH_COUNT') or 250)
    DEV_MODE = False
    


class APIException(Exception):
    pass

TOTAL=100
BACKOFF_FACTOR=1
CONNECT = 10
READ = 10
STATUS_FORCELIST=[500, 501, 502, 503, 504 ,505,404]

class BaseSDQApi:
    API_HOST = Config.API_ENDPOINT
    

    def __init__(self, study_id, account_id, job_id, rule_id, version, batch_id=None, thread_id=None,
                 thread_batch_count=Config.THREAD_BATCH_COUNT, subjects=None):
        self.study_id = study_id
        self.account_id = account_id
        print("ACCOUNT ID IN BASE --->", self.account_id)
        self.job_id = job_id
        self.rule_id = rule_id
        self.version = version
        self._started_at = datetime.utcnow()
        self._end_at = None
        self._data_processed = {}
        self._discrepancy_count = {}
        self.last_run_dt = None
        self.rule_meta_data = self.get_rule_meta(study_id=study_id, rule_id=rule_id, version=version)
        self.batch_ids = self.get_batch_ids(self.rule_meta_data['last_run_dt']).get('batch_ids') or []
        self.thread_id = thread_id
        self.thread_batch_count = thread_batch_count
        self.subjects = subjects
        self.smc_model_dict = {'term': 'meddra_v1', 'med': 'whod_v1'}
        self.request_session = requests.Session()
        self.retries = Retry(total=TOTAL, read=READ,connect=CONNECT,backoff_factor=BACKOFF_FACTOR, status_forcelist=STATUS_FORCELIST)
        self.request_session.mount('http://',HTTPAdapter(max_retries=self.retries))


    def get_batch_ids(self, last_run_dt=None):
        params = {
            'last_run_dt': last_run_dt,
            'ml_model_id': self.rule_id,
            'subcat': self.rule_meta_data['rule_name']
        }
        url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/{self.study_id}/get-batch-ids/"
        response = requests.request("GET", url, params=params, verify=Config.SSL_VERIFY)
        if response.status_code not in (200, 201):
            logging.error(response.text)
            raise APIException('Error in fetching rule list')
        response_json = response.json()
        print("BATCH - IDS: ",response_json['data'])
        return response_json['data']

    def update_batch_rule_execution(self):
        url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/{self.study_id}/update-batch-run-status/"
        params = {
            'account_id': self.account_id,
            'study_id': self.study_id,
            'batch_ids': self.batch_ids,
            'job_run_id': self.job_id,
            'ml_model_id': self.rule_id,
            'initial_run': 'INITIAL_RUN',
            're_run': 'RE_RUN',
            'created_at': str(datetime.utcnow()),
            'updated_at': str(datetime.utcnow())
        }
        response = requests.request("POST", url, data=json.dumps(params),
                                    headers={'content-type': 'application/json'},
                                    verify=Config.SSL_VERIFY)
        if response.status_code not in (200, 201):
            logging.error(response.text)
            raise APIException('Error in save_tracking_info')
        response_json = response.json()

    def execute(self):
        raise NotImplementedError('execute method not implemented')


    def run(self):
        if Config.DEV_MODE or self.thread_id:
            if Config.DEV_MODE:
                logging.info('=' * 10 + 'DEV MODE' + '=' * 10)
            logging.info(
                'starting rule: %s %s %s thread %s' % (self.study_id, self.rule_id, self.version, self.thread_id))
            self.execute()
            self._save_tracking_info()

        else:
            domain_list = getattr(self, 'domain_list', [])
            print('DOM=',domain_list)
            try:
                last_run_dt = self.get_last_run_at()
                print('lastrun_dt:',last_run_dt)
            except Exception as e:
                print('Lastrun Exc=',e)
            logging.info('domain_list: %s last_run_dt: %s' % (domain_list, last_run_dt))
            all_subjects = self.get_subjects(self.study_id, per_page=self.thread_batch_count, domain_list=domain_list,
                                             modif_dts=last_run_dt)
            all_subjects_count = len(all_subjects)
            #print('subjects are :',all_subjects)
            logging.info('%s rule: %s  version: %s - subjects: %s domain_list: %s per_thread: %s' %
                         (self.study_id, self.rule_id, self.version, all_subjects_count, domain_list,
                          self.thread_batch_count))
            print('Number of Subjects :', all_subjects_count)
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=Config.MAX_THREAD_WORKERS) as executor:

                    for thread_id, idx in enumerate(range(0, all_subjects_count, self.thread_batch_count), 1):
                        subject_list = all_subjects[idx: idx + self.thread_batch_count]
                        executor.submit(run_rule, self.__class__, self.study_id, self.account_id, self.job_id, self.rule_id, self.version, thread_id,
                                        self.thread_batch_count, subject_list)
            except Exception as exep:
                logging.info(exep)
            logging.info('updating last_run_dt')
            self._update_last_run_dt()
            self.update_batch_rule_execution()

    def get_field_labels(self, account_id, domain_name):
        fields = {}
        # fields = get_field_labels(self.account_id, domain_name)
        curr_file_path = os.path.realpath(__file__)
        try:
            curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
            _path = os.path.abspath(os.path.join(curr_path, 'model_libs'))
            curr_path = os.path.abspath(os.path.join(_path, self.account_id))
            subcat_config_path = os.path.join(curr_path, 'subcate_config.yml')
            a = yaml.safe_load(open(subcat_config_path, 'r'))
        except:
            curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
            subcat_config_path = os.path.join(curr_path, 'subcate_config.yml')
            a = yaml.safe_load(open(subcat_config_path, 'r'))
        fields = a['FIELD_NAME_DICT']
        
        return fields
    
    def get_field_dict_yaml(self, account_id, study_id, subcat_name, fieldname ):
        
        fields = {}
        # fields = get_field_labels(self.account_id, domain_name)
        curr_file_path = os.path.realpath(__file__)
        
        try:
            curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
            _path = os.path.abspath(os.path.join(curr_path, 'model_libs'))
            curr_path = os.path.abspath(os.path.join(_path, self.account_id))
            subcat_config_path = os.path.join(curr_path, 'subcate_config.yml')
            a = yaml.safe_load(open(subcat_config_path, 'r'))
        except:
            curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
            subcat_config_path = os.path.join(curr_path, 'subcate_config.yml')
            a = yaml.safe_load(open(subcat_config_path, 'r'))
        if(str(fieldname).lower() == 'display_fields'):
            fields = a['FIELDS_FOR_UI'][subcat_name]
        elif str(fieldname).lower() == 'fn_config':
            fields = a['FN_CONFIG'][subcat_name]
        return fields

    def get_field_dict(self, account_id, study_id, subcat_name, fieldname):
        field_data = None
        try:
            if str(fieldname).lower() == 'fn_config':
                url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/get-fn-config/{self.sub_cat}"
            elif str(fieldname).lower() == 'display_fields':
                url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/get-dynamic-panel-config/{self.sub_cat}" 
            
            response = requests.request("GET", url, verify=Config.SSL_VERIFY, timeout=5)
            
            if response.status_code not in (200, 201):
                logging.error(response.text)
                raise APIException('Error in fetching fn_config/dynamic panel')
            
            response_json = response.json()
            field_data = response_json['data']
            return yaml.safe_load(field_data)
        except Exception as get_field_exc:
            print('Exception inside get_field_dict is :', get_field_exc)
            try:
                field_data = self.get_field_dict_yaml(account_id, study_id, subcat_name, fieldname)
                return field_data
            except Exception as yaml_exc:
                print('Exception in fetching config yaml data', yaml_exc)
                logging.info(traceback.format_exc())
            print('Exception in api calling/api not found', get_field_exc)
            logging.info(traceback.format_exc())
        
        return field_data
    
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
            url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/{self.study_id}/metrics"
            response = requests.request("POST", url, data=json.dumps(payload),
                                        headers={'content-type': 'application/json'},
                                        verify=Config.SSL_VERIFY)
            if response.status_code not in (200, 201):
                logging.error(response.text)
                raise APIException('Error in save_tracking_info')
            response_json = response.json()
            return response_json['data']

    def _update_last_run_dt(self):
        print('update_last_run happened')
        url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/{self.study_id}/ml-subcats/{self.rule_id}/{self.version}/last-run-time/"
        payload = {}
        domain_list = getattr(self, 'domain_list', [])
        if domain_list:
            payload['domain_list'] = domain_list
        response = requests.request("PUT", url, data=json.dumps(payload), headers={'content-type': 'application/json'},
                                    verify=Config.SSL_VERIFY)
        if response.status_code not in (200, 201):
            logging.error(response.text)
            raise APIException('Error in update_last_run_dt')
        response_json = response.json()
        return response_json['data']

    @classmethod
    def get_all_rules(cls, study_id=None, status=None):
        url = f"{cls.API_HOST}/apiservice/v1/rule"
        params = {
            'study_id': study_id,
            'status': status,
            'account_id': cls.account_id
        }
        response = requests.request("GET", url, params=params, verify=Config.SSL_VERIFY)
        if response.status_code not in (200,):
            logging.error(response.text)
            raise APIException('Error in fetching rule list')
        response_json = response.json()
        return response_json['data']

    def get_rule_meta(self, account_id=None, study_id=None, rule_id=None, version=None):
        # returns rule metadata matching study_id, rule_id, version from rule_master & map_rules
        if study_id and rule_id and version:
            url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/{study_id}/subcat/{rule_id}/metadata"
        else:
            url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/{self.study_id}/subcat/{self.rule_id}/metadata"

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
        url = f"{cls.API_HOST}/apiservice/v1/study"
        response = requests.request("GET", url, verify=Config.SSL_VERIFY)
        if response.status_code not in (200,):
            logging.error(response.text)
            raise APIException('Error in fetching study list')
        response_json = response.json()
        return response_json['data']

    def _get_subjects(self, study_id, page=1, per_page=10000, domain_list=None, modif_dts=None):
        if not self.batch_ids:
            return []
        url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/{study_id}/subjects"
        params = {
            'page': page,
            'per_page': per_page,
            'domain': domain_list,
            'task': 'list',
            'rule_id': self.rule_id,
            'modif_dts': modif_dts,
            'batch_ids': self.batch_ids
        }
        response = requests.request("GET", url, params=params, verify=Config.SSL_VERIFY)
        if response.status_code not in (200,):
            logging.error(response.text)
            raise APIException('Error in fetching subject list')
        response_json = response.json()
        return response_json['data']

    def get_subjects_count(self, study_id, per_page=10000, domain_list=None, modif_dts=None):
        url = f"{self.API_HOST}/apiservice/v1/{study_id}/subject/count"
        params = {
            'per_page': per_page,
            'domain': domain_list,
            'modif_dts': modif_dts
        }
        response = requests.request("GET", url, params=params, verify=Config.SSL_VERIFY)
        if response.status_code not in (200,):
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


    def get_subjects(self, study_id, per_page=10000, domain_list=None, modif_dts=None):
        if self.thread_id:
            return self.subjects
        else:
            all_subjects = []
            modif_dts = modif_dts or self.get_last_run_at()

            subjects = self.get_latest_subjects(study_id, domain_list=domain_list, modif_dts=modif_dts)
            logging.info('Got %s new/modified subjects' % len(subjects))
            all_subjects.extend(subjects)            

            return list(set(all_subjects))

    def _get_form_index_list(self, study_id, page=1, per_page=10000, domain_list=None, modif_dts=None):
        url = f"{self.API_HOST}/apiservice/v1/{study_id}/form-index/list"
        params = {
            'page': page,
            'per_page': per_page,
            'domain': domain_list,
            'modif_dts': modif_dts
        }
        response = requests.request("GET", url, params=params, verify=Config.SSL_VERIFY)
        if response.status_code not in (200,):
            logging.error(response.text)
            raise APIException('Error in fetching form_index list')
        response_json = response.json()
        return response_json['data']

    def _get_itemrepn_list(self, study_id, page=1, per_page=10000, domain_list=None, modif_dts=None):
        url = f"{self.API_HOST}/apiservice/v1/{study_id}/itemrepn/list"
        params = {
            'page': page,
            'per_page': per_page,
            'domain': domain_list,
            'modif_dts': modif_dts
        }
        response = requests.request("GET", url, params=params, verify=Config.SSL_VERIFY)
        if response.status_code not in (200,):
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
        url = f"{self.API_HOST}/apiservice/v1/{study_id}/site"
        response = requests.request("GET", url, verify=Config.SSL_VERIFY)
        if response.status_code not in (200,):
            logging.error(response.text)
            raise APIException('Error in fetching site list')
        response_json = response.json()
        return response_json['data']

    def get_flatten_count(self, study, subject, per_page=10000, formname_list=None, formrefname_list=None,
                          domain_list=None):
        url = f"{self.API_HOST}/apiservice/v1/{study}/{subject}/flatten-data/count"
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
        url = f"{self.API_HOST}/apiservice/v1/{study}/{subject}/flatten-data/list"
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
        url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/{study}/subjects/{subject}"
        if type(subject) in [list, tuple]:
            url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/{study}/subjects/{subject[0]}/{subject[1]}"
        params = {
            'per_page': per_page,
            'task': 'count',
            'rule_id': self.rule_id,
            'formname': formname_list,
            'formrefname': formrefname_list,
            'domain': domain_list,
            'batch_id': max(self.batch_ids)
        }
        response = requests.request("GET", url, params=params, verify=Config.SSL_VERIFY)
        if response.status_code not in (200,):
            logging.error(response.text)
            raise APIException('Error in fetching unflatten count')
        response_json = response.json()
        return response_json['data']['pages']

    def get_unflatten_list(self, study, subject, page=1, per_page=10000, formname_list=None, formrefname_list=None,
                           domain_list=None, max_ck_event_id=None):
        print("GETTING UNFLATTEN LIST")
        url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/{study}/subjects/{subject}"
        if type(subject) in [list, tuple]:
            url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/{study}/subjects/{subject[0]}/{subject[1]}"
        params = {
            'page': page,
            'task': 'list',
            'rule_id': self.rule_id,
            'per_page': per_page,
            'formname': formname_list,
            'formrefname': formrefname_list,
            'domain': domain_list,
            'prev_ck_event_id': max_ck_event_id,
            'batch_id': max(self.batch_ids)
        }
        response = requests.request("GET", url, params=params, verify=Config.SSL_VERIFY)
        if response.status_code not in (200,):
            logging.error(response.text)
            raise APIException('Error in fetching unflatten list')
        response_json = response.json()
        # print("UNFLATTEN LIST ----->", response_json['data'])
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

        url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/{study}/result"
        _payload = {
            "stg_ck_event_id": payload['stg_ck_event_id'],
            "subcategory": payload['subcategory'],
            "query_text": payload['query_text'],
            "form_index": payload['form_index'],
            "question_present": payload['question_present'],
            "report": payload['report'],
            "modif_dts": payload['modif_dts'],
            "visit_nm": payload.get('visit_nm'),
            "confid_score": payload.get('confid_score', 0.97),
            "rule_id": self.rule_id,
            "job_id": self.job_id,
            "cdr_skey": payload.get('cdr_skey','0'),
            "query_target": self.rule_meta_data['item_nm_preconf'],
            "subcat_description": self.rule_meta_data['rule_desc']
        }

        print('_payload is :',_payload)

        if payload.get('formrefname'):
            _formrefname, _sectionrefname, _inform = self.get_edc_mapping(payload['formrefname'])
            _payload['formrefname'] = _formrefname
            _payload['sectionrefname'] = _sectionrefname
            _payload['question_present']['INFORM'] = [_inform]
        else:
            _payload['formrefname'] = self.rule_meta_data['formrefname']
            _payload['sectionrefname'] = self.rule_meta_data['sectionrefname']

        print("%"*20,_payload,)
        response = requests.request("POST", url, data=json.dumps(_payload),
                                    headers={'content-type': 'application/json'},
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
            logging.error(
                f"""{formrefname} not found in {rm_formrefname} for study: {self.study_id} rule: {self.rule_id} version: {self.version} in rule_master""")
            raise
        _formrefname = rm_formrefname[mapping_index]
        try:
            _sectionrefname = rm_sectionrefname[mapping_index]
            study_src = self.get_study_source()
            if study_src and str(study_src).upper() == 'RAVE':
                _sectionrefname = _formrefname # For rave Copy formrefname to sectionrefname
        except Exception as e:
            print('sectionrefnm exc:',e)
            _sectionrefname = _formrefname
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
        print('Query text is :', query_text)
        if query_text:
            if params:
                query_text = query_text.format(**params)
            return query_text
        return 'query_text not present'

    def get_subcategory_json(self, study, subcategory):
        try:
            _json = {
                'SRDM': self.rule_meta_data['item_nm_edc'].split(','),
                'INFORM': self.rule_meta_data['item_nm_edc'].split(',')
            }
        except KeyError as e:
            logging.error(
                f"""{e} not found for study: {self.study_id} rule: {self.rule_id} version: {self.version} in rule_master""")
            raise
        return _json

    def _set_processed_data(self, subject, flatten_data):
        for domain, records in flatten_data.items():
            if type(subject) in [list,tuple]:
                if subject[0] in self._data_processed:
                    self._data_processed[subject[0]].update({domain: len(records)})
                else:
                    self._data_processed[subject[0]] = {domain: len(records)}
            else:
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
        if type(subject) in [list, tuple]:
            if subject[0] in self._discrepancy_count:
                self._discrepancy_count[subject[0]] += 1
            else:
                self._discrepancy_count[subject[0]] = 1
        else:
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
        # study_type_col_map = self.get_study_type_mapping()
        # study_type_df = pd.DataFrame(study_type_col_map)

        # map_study = pd.DataFrame(self.get_studies())
        # print(map_study.columns)
        # study_type = map_study[map_study['study_id'] == study.upper()]['study_type'].values.tolist()[0]

        # flatten_data = covid_flatten_v1(rows_df, study_type_df, study_type)
        pfizer_adapter = pfizer_adapters()
        study_type = pfizer_adapter.vol_info(self.study_id,self.account_id)
        exclude_flatten_data = pfizer_adapters.exclude_form(study_type,rows_df)
        # print("study_type------------",study_type)
        # print("exclude_flatten_data e------------",exclude_flatten_data.shape )
        
        flatten_data = covid_flatten_v1(exclude_flatten_data)
        self._set_processed_data(subject, flatten_data)
        return flatten_data

    def get_study_type_mapping(self):
        url = f"{self.API_HOST}/apiservice/v1/study-type-mapping"
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
        url = f"{self.API_HOST}/apiservice/v1/{study}/fda-approved-diagnostics"
        response = requests.request("GET", url, verify=Config.SSL_VERIFY)
        if response.status_code not in (200,):
            logging.error(response.text)
            raise APIException('Error in fetching fda_approved_diagnostics')
        response_json = response.json()
        return response_json['data']

    def check_if_duplicate(self, study, subjid, subcat, index, report):
        url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/{study}/is-duplicate"
        payload = {
            'subjid': subjid,
            'subcat': subcat,
            'index': index,
            'report': report,
        }
        response = requests.request("POST", url, data=json.dumps(payload), headers={'content-type': 'application/json'},
                                    verify=Config.SSL_VERIFY)
        if response.status_code not in (200, 201,):
            logging.error(response.text)
            raise APIException('Error in check_if_duplicate')
        response_json = response.json()
        return response_json['data']['is_duplicate']

    def check_labtest_duplicate(self, study, subjid, subcat, visit_nm, visit_ix, formrefname, form_ix):
        url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/{study}/is-lbtest-duplicate"
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
        if response.status_code not in (200, 201,):
            logging.error(response.text)
            raise APIException('Error in check_if_duplicate')
        response_json = response.json()
        return response_json['data']['is_duplicate']

    def get_decode_llt(self, term, decod = None, llt = None, mapper_flag = True, model = 'term'):
        decod_dict = {}
        if (pd.isna(decod) or str(decod).strip().upper() in ['NONE','NULL','NAN','']) or ((not mapper_flag) and pd.isna(llt) and str(llt).strip().upper() in ['NONE','NULL','NAN','']):
            if(not pd.isna(term)) and (str(term).strip().upper() not in ['NONE','NULL','NAN','']):
                decod_dict = self.predict_medical_code(term, model = model, mapper = mapper_flag)
                #print('The dictionary is :', decod_dict)
                if (pd.isna(decod) or str(decod).strip().upper() in ['NONE','NULL','NAN','']):
                    decod = decod_dict['preferred_term']
                    #print('Generated AEDECOD is :',decod, 'for the term', term )
                if not mapper_flag and pd.isna(llt) and str(llt).strip().upper() in ['NONE','NULL','NAN','']:
                    if(str(model).lower() == 'term'):
                        llt = decod_dict['lower_level_term']
                    if(str(model).lower() == 'med'):
                        llt = decod_dict['synonym_term']
                    #print('Generated llt term is :', llt)
            else:
                print(f'TERM is {term}. Could not generate DECODE/LLT')
            return (decod, llt)   
        return (decod, llt)


    
    def get_drugs_dict(self, study):
        url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/{study}/get-drugs"
        #print('outside base URL :',url)
        response = requests.request("GET", url, verify=Config.SSL_VERIFY)
        #print('response code:',response.status_code)
        #print('response json:',str(response.json()))

        if response.status_code not in (200, 201,):
            logging.error(response.text)
            raise APIException('Error in get_drugs_dict')
        response_json = response.json()
        return response_json['data']['drugs_dict']
    
    def predict_medical_code(self,inp_text,mapper = True, model='term'):
        try:
            model = str(model).lower()
            select_term = {}
            #print('Model, input_text :', model, inp_text)            
            if model in self.smc_model_dict:
                model_nm = self.smc_model_dict[model]
            else:
                logging.warning(f'{model} not present in smc_model_dict')
                model_nm = ''

            if model == 'term' and mapper == True:
                select_term = {'preferred_term': None}
            elif model == 'term' and mapper == False:
                select_term = {'preferred_term': None, 'pt_code': None,'lower_level_term': None, 'llt_code': None}
            elif model == 'med' and mapper == True:
                select_term = {'preferred_term': None}
            elif model == 'med' and mapper == False:
                select_term = {'preferred_term': None, 'pt_content_id': None,'synonym_term': None, 'synonym_content_id': None} 
            
            if not pd.isna(inp_text) and str(inp_text).strip().upper() not in ['NAN', 'NONE', 'NULL', '']:
                inp_text = ''.join( i if (i.isalnum() or i == ' ') else ' ' for i in str(inp_text) )
                #print('Input text is (Inside not na): ', inp_text)
            else:
                select_term['original'] = str(inp_text)
                #print(f'Select term as input text is null : {inp_text} is {select_term}')
                return select_term
                
            #url = f"https://account2.sdq-qa1.airesearch.lsac-dev.com/apiservice/v1/predict_medical_code/{inp_text}/{model_nm}"
            url = f"{self.API_HOST}/apiservice/v1/predict_medical_code/{inp_text}/{model_nm}"
            response = requests.request("GET", url, verify=Config.SSL_VERIFY)
            if response.status_code not in (200, 201,):
                print("failed term :" ,inp_text,response.text)
                top_prediction = {}
                pass
                # raise APIException('Error in predict_medical_code')
            else:
                response_json = response.json()
                top_prediction = response_json['data']['prediction']['rank_1']
            
            for item in select_term:
                if item in  top_prediction.keys():
                    select_term[item] = top_prediction[item]

            select_term['original'] = inp_text            
            #print(f'The medical dictionary for {inp_text} is :',select_term)
            return select_term
        
        except Exception as e:
            print(traceback.format_exc())
            print('Exception in API',e)
            return {'preferred_term': None,'original': inp_text}
        
    def get_aelab_mapping_csv(self):
        # url = f"{self.API_HOST}/api-service/1/get-aelab-mapping"
        curr_file_path = os.path.realpath(__file__)
        curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
        filename = 'model_libs/ae_lab_mapping-v2.csv'
        subcat_config_path = os.path.join(curr_path, filename)
        try:
            df = pd.read_csv(subcat_config_path)
        except:
            print('aelab file not found retrying another path')
            df = pd.read_csv(subcat_config_path.split('/')[-1])
            print('worked another path')
        print('get_aelab_mapping')
        return df
    
    def get_aelab_mapping(self):
        aelb_df = None
        try:
            url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/get-aelb-mapping/"        
            
            response = requests.request("GET", url, verify=Config.SSL_VERIFY, timeout=5)
            
            if response.status_code not in (200, 201):
                logging.error(response.text)
                raise APIException('Error in fetching aelb mapping data')
            
            response_json = response.json()
            #print("Data ae lb mapping : ",response_json['data'])
            # return yaml.safe_load(response_json['data'])
            return pd.read_json(response_json['data'])
        
        except Exception as get_aelb_exc:            
            print('Checking csv availability for aelb mapping')
            try:
                aelb_df = self.get_aelab_mapping_csv()
                #print("Data : ",aelb_df)
                return aelb_df
            except Exception as csv_exc:
                print('Exception in fetching ae lb data from csv', csv_exc)
                logging.info(traceback.format_exc())   
            print('Exception in api', get_aelb_exc)    
            logging.info(traceback.format_exc()) 
        return aelb_df
    
    def get_dm_pred_data(self, study, infer_subcat, subject, col):
        url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/{study}/get-dm-pred-data/{infer_subcat}/{subject}/{col}/"
        response = requests.request("GET", url, verify=Config.SSL_VERIFY)
        if response.status_code not in (200,):
            logging.error(response.text)
            raise APIException('Error in fetching dm_pred data')
        response_json = response.json()
        return response_json['data']

    def get_stg_cdr_skey(self, study, stg_ck_event_id):
        url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/{study}/get-stg-cdr-skey/{stg_ck_event_id}/"
        if type(subject) in [list, tuple]:
            url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/{study}/get-dm-pred-data/{infer_subcat}/{subject[0]}/{subject[1]}/{col}/"
        response = requests.request("GET", url, verify=Config.SSL_VERIFY)
        if response.status_code not in (200,):
            logging.error(response.text)
            raise APIException('Error in fetching stg_pred ck_event_id data')
        response_json = response.json()
        return response_json['data']
    
    def clear_query(self, study, subject, infer_subcat, cleared_cdr_skey):
        url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/{study}/clear-query/{subject}/{infer_subcat}/{cleared_cdr_skey}/"
        if type(subject) in [list, tuple]:
            url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/{study}/clear-query/{subject[0]}/{subject[1]}/{infer_subcat}/{cleared_cdr_skey}/"
        response = requests.request("POST", url, verify=Config.SSL_VERIFY)
        if response.status_code not in (200,):
            logging.error(response.text)
            raise APIException('Error in clearing query data')
        response_json = response.json()
        return response_json['data']

    def close_query(self, subject, payload_list):
        print('subject = ',subject,payload_list)
        # if not payload_list:
        #     return
        dm_cdr_skey = self.get_dm_pred_data(self.study_id, self.rule_meta_data['rule_name'], subject,
                                       'cdr_skey')
        stg_cdr_skey = []
        if payload_list:
            for qp in payload_list:
                stg_cdr_skey.append(self.get_stg_cdr_skey(self.study_id, qp['stg_ck_event_id']))

        print('==dm,stg=',dm_cdr_skey,stg_cdr_skey)
        # if len(stg_cdr_skey) == 0:
        cleared_cdr_skey = list(set(dm_cdr_skey)-(set(stg_cdr_skey)))
        # else:
        #     cleared_cdr_skey = list(set(dm_cdr_skey).intersection(set(stg_cdr_skey)))
        # if cleared_cdr_skey:
        #     cleared_cdr_skey = [dm_cdr_skey[0]]
        logging.info(
            f'Account ID - {self.account_id} - Study ID - {self.study_id} Subject - {subject} - cdr_skey to be closed for subject - {cleared_cdr_skey}')
        if cleared_cdr_skey:
            self.clear_query(self.study_id, subject, self.rule_meta_data['rule_name'], json.dumps(cleared_cdr_skey))
    
    def has_auto_closure(self, study_id, rule_id, version):
        rule_meta = self.get_rule_meta(study_id=study_id, rule_id=rule_id, version=version)
        return rule_meta.get('auto_closure', False) in (True, 'true')
    
    def get_study_source(self):
        try:
            url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/{self.study_id}/get-study-source/"
            response = requests.request("GET", url, verify=Config.SSL_VERIFY)
            if response.status_code not in (200,):
                logging.error(response.text)
                raise APIException('Error in getting study source data')
            response_json = response.json()
            print('respjs-=',response_json)
            return response_json['data']
        except Exception as E:
            print(f"### Exception while getting study source for deeplink - {E}")
            return ""
    
    def get_map_subject_id(self, subj_guid):
        try:
            url = f"{self.API_HOST}/apiservice/v1/{self.account_id}/{self.study_id}/get-map-subject-id/{subj_guid}"
            response = requests.request("GET", url, verify=Config.SSL_VERIFY)
            if response.status_code not in (200,):
                logging.error(response.text)
                raise APIException('Error in getting subject_id')
            response_json = response.json()
            print('respjs-subji-=',response_json)
            return response_json['data']
        except Exception as E:
            print(f"### Exception while getting Subject ID from Guid for deeplink - {E}")
            return ""
    
def run_rule(klass, study_id, account_id, job_id, rule_id, version, thread_id, thread_batch_count, subject_list):
    rule = klass(study_id, account_id, job_id, rule_id, version, thread_id=thread_id, thread_batch_count=thread_batch_count,
                 subjects=subject_list)
    rule.run()

    
