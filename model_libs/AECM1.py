# uncompyle6 version 3.9.0
# Python bytecode version base 3.6 (3379)
# Decompiled from: Python 3.10.1 (v3.10.1:2cd268a3a9, Dec  6 2021, 14:28:59) [Clang 13.0.0 (clang-1300.0.29.3)]
# Embedded file name: /users/sdqdev/dhilip/subcats_saama/pfizer_data_upload_py/pfizer_data_upload/dags/scripts/SubcatModels/model_libs/AECM1.py
# Compiled at: 2022-10-12 13:38:39
# Size of source mod 2**32: 9893 bytes
import pandas as pd, sys, re, warnings
warnings.filterwarnings('ignore')
try:
    from scripts.SubcatModels.model_libs.base import BaseSDQApi
    import scripts.SubcatModels.model_libs.utils as utils
except:
    from base import BaseSDQApi
    import utils

import traceback, tqdm, numpy as np, logging, yaml, os
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class AECM1(BaseSDQApi):
    def __init__(self, study_id, account_id, job_id, rule_id, version, thread_id=None, thread_batch_count = Config.THREAD_BATCH_COUNT, subjects=None):        
        try:
            super().__init__(study_id, account_id, job_id, rule_id, version, thread_id=thread_id, thread_batch_count = thread_batch_count, subjects=subjects)
            self.sub_cat = self.__class__.__name__ #'AECM0'
            self.fn_config = self.get_field_dict(self.account_id, self.study_id, self.sub_cat, 'fn_config')
            self.domain_list = self.fn_config['domain_list']    
        except Exception as init_exc:
            print('Exception while getting config values in init :', init_exc)
            print(traceback.format_exc())

    
    def execute(self):
        study = self.study_id
        sub_cat = self.sub_cat
        fn_config = self.fn_config
        f_d = 'display_fields'
        
        try:  
            fields_dict = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_d))
            fields_labels_1 = self.get_field_labels(self.account_id,self.domain_list[0])
            fields_labels_2 = self.get_field_labels(self.account_id,self.domain_list[1])
            fields_labels = {**fields_labels_1, **fields_labels_2}
            
            match_terms = fn_config['match']
            match_cond = fn_config['match_condition']
            cols = fn_config['cols']
            vals = fn_config['vals']
            smc = fn_config['use_smc'] if 'use_smc' in fn_config.keys() else False
            predict_med_code = None
            subjects = self.get_subjects(study, domain_list=self.domain_list, per_page = 10000)
        except Exception as fn_exc:
            print('Exception while getting config values / subjects :', fn_exc)
            print(traceback.format_exc())             

        for subject in tqdm.tqdm(subjects):
            print('The Subject is :', subject)
            payload_list = []
            try:
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list=(self.domain_list))
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)
                ae_df = pd.DataFrame(flatten_data.get(self.domain_list[0],[]))
                cm_df = pd.DataFrame(flatten_data.get(self.domain_list[1],[]))
                # ae_df = pd.DataFrame(flatten_data['AE'])
                # cm_df = pd.DataFrame(flatten_data['CM'])
                for ind in range(ae_df.shape[0]):
                    try:
                        ae_record = ae_df.iloc[[ind]]
                        formindex = ae_record['form_index'].values[0]
                        o_aespid = ae_record['AESPID'].values[0]
                        ae_cm_records = utils.extractor(ae_record, ae_df, cm_df, cols['aespid_col'][0], cols['cmaeno_col'][0])
                        if (type(ae_cm_records) == tuple) & (type(ae_cm_records) != bool):
                            if len(ae_cm_records[0]) > 0:
                                for ae_formindex, cm_records in ae_cm_records[0].items():
                                    for cm_record in cm_records:
                                        new_ae_records, new_cm_record = utils.get_cm_hierarchy(ae_record, ae_df, cm_record, key=cols['aestdtc_dts_col'][0])
                                        for aeptcd in new_ae_records[cols['aeptcd_col'][0]].unique().tolist():
                                            new_ae_record = new_ae_records[new_ae_records[cols['aeptcd_col'][0]] == aeptcd]
                                            if len(new_ae_record) > 1:
                                                new_ae_record = new_ae_record.head(1)
                                            aespid = new_ae_record[cols['aespid_col'][0]].values[0]
                                            new_ae_record1 = new_ae_record
                                            if aespid != o_aespid:
                                                continue
                                            if type(new_ae_record) != int:
                                                dtr_null_flag = False
                                                if cols['aestdat_dtr_col'][0] in new_ae_record.columns:
                                                    if new_ae_record[cols['aestdat_dtr_col'][0]].astype('str').values[0] in utils.null_list:
                                                        dtr_null_flag = True
                                                if cols['aestdat_dtr_col'][0] in new_ae_record.columns and not dtr_null_flag:
                                                    new_ae_record[cols['aestdat_dtr_col'][0]] = new_ae_record[cols['aestdat_dtr_col'][0]].astype('str')
                                                    ae_stdt = new_ae_record[cols['aestdat_dtr_col'][0]].values[0]
                                                else:
                                                    new_ae_record[cols['aestdtc_dts_col'][0]] = new_ae_record[cols['aestdtc_dts_col'][0]].astype('str')
                                                    ae_stdt = new_ae_record[cols['aestdtc_dts_col'][0]].values[0]
                                                new_cm_record[cols['cmstdat_dtr_col'][0]] = new_cm_record[cols['cmstdat_dtr_col'][0]].astype('str')
                                                cm_stdt = new_cm_record[cols['cmstdat_dtr_col'][0]].values[0]
                                                unk_flag, year_not_unk, date_dict = utils.impute_unk({'aestdat':ae_stdt,  'cmstdat':cm_stdt})
                                                if not year_not_unk:
                                                    continue

                                                aestdat = date_dict['aestdat'].date()
                                                cmstdat = date_dict['cmstdat'].date()
                                                aecmgiv = new_ae_record[cols['aecmgiv_col'][0]].values[0]

                                                if cmstdat < aestdat and aecmgiv.upper() == 'YES':
                                                    subcate_report_dict = {}
                                                    report_dict = {}
                                                    new_cm_record[cols['cmaeno_col'][0]] = cm_record[cols['cmaeno_col'][0]]
                                                    new_ae_record1, new_cm_record = utils.unk_format_datetime(unk_flag, new_ae_record1, new_cm_record)
                                                    piv_rec = {'AE':new_ae_record1.head(1), 
                                                        'CM':new_cm_record.head(1)}
                                                    for dom, columns in fields_dict.items():
                                                        piv_df = piv_rec[dom]
                                                        present_col = [col for col in columns if col in piv_df.columns.tolist()]
                                                        rep_df = piv_df[present_col]
                                                        try:
                                                            rep_df['deeplink'] = utils.get_deeplink(study, piv_df, self.get_study_source(), subject_id=self.get_map_subject_id(piv_df['subj_guid'].values[0]))
                                                        except:
                                                            rep_df['deeplink'] = '#'
                                                        rep_df = rep_df.rename(columns=fields_labels)
                                                        rep_df = rep_df.loc[:,~rep_df.columns.duplicated()]                                    
                                                        report_dict[dom]= rep_df.to_json(orient= 'records')
                                                    subcate_report_dict[sub_cat] =  report_dict
                                                    ae_record1 = ae_record.iloc[0]
                                                    query_text_param = {}
                                                    try:
                                                        query_text_param = utils.get_qt_payload(fn_config, piv_rec)
                                                    except:
                                                        print('QT Error: ',traceback.format_exc())        
                                                    payload = {'subcategory':sub_cat,
                                                        'cdr_skey':str(ae_record1['cdr_skey']), 
                                                        'query_text':self.get_model_query_text_json(study, sub_cat, params=query_text_param), 
                                                        'form_index':str(ae_record1['form_index']), 
                                                        'question_present':self.get_subcategory_json(study, sub_cat), 
                                                        'modif_dts':str(ae_record1['modif_dts']), 
                                                        'stg_ck_event_id':int(ae_record1['ck_event_id']), 
                                                        'formrefname':str(ae_record1['formrefname']), 
                                                        'report':subcate_report_dict, 
                                                        'confid_score':np.random.uniform(0.7, 0.97)}
                                                    print('Subject and Payload are :',subject,payload)
                                                    self.insert_query(study, subject, payload)
                                                    if payload not in payload_list:
                                                        payload_list.append(payload)

                    except Exception as exp:
                        logging.exception(exp)
                        continue

            except Exception as e:
                logging.exception(e)


            if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                self.close_query(subject, payload_list)

if __name__ == '__main__':
    study_id = sys.argv[1]
    account_id = sys.argv[2]
    job_id = sys.argv[3]
    rule_id = sys.argv[4]
    version = sys.argv[5]
    rule = AECM1(study_id, account_id, job_id, rule_id, version)
    rule.run()
