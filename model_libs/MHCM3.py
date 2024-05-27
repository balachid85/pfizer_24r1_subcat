import os
import sys
import logging
import yaml
import warnings
warnings.filterwarnings('ignore')
import traceback
import numpy as np
import pandas as pd
import utils
from base import BaseSDQApi

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class MHCM3(BaseSDQApi):
    domain_list = ['MH','CM']

    def execute(self):
        study= self.study_id
        sub_cat = self.__class__.__name__ #'MHCM3'
        try:
            f_d = 'display_fields'
            f_c = 'fn_config'
            
            fields_dict = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_d))
            fn_config = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_c))
            fields_labels_1 = self.get_field_labels(self.account_id,self.domain_list[0])
            fields_labels_2 = self.get_field_labels(self.account_id,self.domain_list[1])
            fields_labels = {**fields_labels_1, **fields_labels_2} 
            
            match_term = fn_config['match'][0]
            match_dsl_flag = False
            if(match_term.upper() == 'MATCH_DSL'):
                match_dsl_flag = True
                match_config_dsl = fn_config[match_term]
            else:
                #match_config = fn_config[match_term]
                match_config = fn_config['match_term']

            cols = fn_config['cols']
            dt_cols = cols['dt_cols']
            vals = fn_config['vals']
            cm_ongo_val = vals['cm_ongo_val']
            smc = fn_config['use_smc'] if 'use_smc' in fn_config.keys() else False
            predict_med_code = None

            subjects = self.get_subjects(study, domain_list = self.domain_list, per_page = 10000)
        except Exception as fn_exc:
            print(f'Exception while fetching config/ retreiving subjects: {fn_exc}')
            print(traceback.format_exc())

        for subject in tqdm.tqdm(subjects):
            payload_list = []subjects:
            print('Subject is :', subject)
            payload_list = []
            try:
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list = self.domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)
                try:
                    mh_df = pd.DataFrame(flatten_data.get(self.domain_list[0],[]))
                    cm_df = pd.DataFrame(flatten_data.get(self.domain_list[1],[]))
                    if(len(cm_df) ==0 or len(mh_df)==0):
                        continue
                    mh_df = mh_df[mh_df[dt_cols[1]].notna()]
                    cm_df[cols['cm_ongo_col'][0]] = cm_df[cols['cm_ongo_col'][0]].apply(lambda x: str(x).upper())
                    cm_df = cm_df[cm_df[cols['cm_ongo_col'][0]].isin(cm_ongo_val['yes_val'])]
                    if(len(cm_df) ==0 or len(mh_df)==0):
                        continue
                except Exception as df_exc:
                    print('Exception while fetching df:', df_exc)
                    continue

                for ind in range(mh_df.shape[0]):
                    try:
                        mh_record = mh_df.iloc[[ind]]
                        mhendat = mh_record[dt_cols[1]].values[0]
                        cm_records = pd.DataFrame()
                        if(not match_dsl_flag):    
                            try:
                                if smc:
                                    predict_med_code = self.predict_medical_code
                                    sec_col = match_config['sec_match_col']
                                    cm_df = cm_df[cm_df[sec_col].notna()]
                                else:
                                    predict_med_code = None

                                match_flag, cm_records = utils.ae_cm_mapper(prim_rec = mh_record,
                                                            sec_df = cm_df,
                                                            subcat= sub_cat,
                                                            use_smc = smc,
                                                            func = predict_med_code,
                                                            **match_config) 
                            except Exception as Ex:
                                logging.info(f'Error in match_flag_1 {Ex}')
                                continue
                        else:
                            try:
                                if smc:
                                    predict_med_code = self.predict_medical_code
                                
                                match_flag, cm_records = utils.ae_cm_mapper(prim_rec=mh_record,
                                            sec_df = cm_df,
                                            subcat = sub_cat,
                                            match_type = 'dsl',
                                            use_smc = smc,
                                            func = predict_med_code,
                                            match_DSL = match_config_dsl
                                            )
                            except Exception as ex:
                                print(f' Exception in match_DSL : {ex}')
                                print(traceback.format_exc())
                                continue            
                            
                        if len(cm_records) == 0:
                            continue
                        else:
                            if not match_dsl_flag:
                                cm_records.drop_duplicates([cols['uniq_id_col'][0]], keep = 'last',inplace= True)
                                
                        for cm_ind in range(cm_records.shape[0]):
                            cm_record = cm_records.iloc[[cm_ind]]
                            cmongo = cm_record[cols['cm_ongo_col'][0]].values[0]
                            
                            if (not pd.isnull(cmongo) and str(cmongo).upper() in cm_ongo_val['yes_val']) and (not pd.isnull(mhendat) and str(mhendat).upper() not in ['',' ','NAN',None,'NONE','NULL']):
                                subcate_report_dict = {}
                                report_dict = {}
                                cm_record = cm_record.replace({np.nan : 'blank'})
                                mh_record = mh_record.replace({np.nan : 'blank'})

                                piv_rec = {'MH' : mh_record, 'CM' : cm_record}
                                # print('*-'*20,ae_record.columns.tolist());return
                                for dom, columns in fields_dict.items():
                                    piv_df = piv_rec[dom]
                                    present_col = [col for col in columns if col in piv_df.columns.tolist()]
                                    rep_df = piv_df[present_col]
                                    rep_df['deeplink'] = utils.get_deeplink(study, piv_df, self.get_study_source(), subject_id=self.get_map_subject_id(piv_df['subj_guid'].values[0]))
                                    rep_df = rep_df.rename(columns= fields_labels)
                                    rep_df = rep_df.loc[:,~rep_df.columns.duplicated()]
                                    report_dict[dom]= rep_df.to_json(orient= 'records')

                                subcate_report_dict[sub_cat] = report_dict
                                query_text_param={  cols['mh_term_col'][0] : mh_record[cols['mh_term_col'][0]].values[0], 
                                                    dt_cols[1] : mh_record[dt_cols[1]].values[0]
                                                    } 

                                payload = {
                                    "subcategory": sub_cat,
                                    "cdr_skey" : str(mh_record['cdr_skey'].values[0]),
                                    "query_text": self.get_model_query_text_json(study, sub_cat, params = query_text_param),
                                    "form_index": str(mh_record['form_index'].values[0]),
                                    "question_present": self.get_subcategory_json(study, sub_cat),
                                    "modif_dts": str(mh_record['modif_dts'].values[0]),  
                                    "stg_ck_event_id": int(mh_record['ck_event_id'].values[0]),
                                    "formrefname" : str(mh_record['formrefname'].values[0]),
                                    "report" : subcate_report_dict,
                                    "confid_score": np.random.uniform(0.7, 0.97)
                                }
                                print('Subject and Payload : ',subject, payload)
                                self.insert_query(study, subject, payload)
                                if payload not in payload_list:
                                    payload_list.append(payload)

                    except:
                        print(traceback.format_exc())
                        continue
            except:
                print(traceback.format_exc())
                continue
            if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                self.close_query(subject, payload_list)
                    
if __name__ == "__main__":
    #(self.study_id, self.account_id, self.job_id, rule['ml_model_id'], 0.1
    # python MHCM3.py 1 1 78 1 0.1 > mhcm3log.txt
    study_id = sys.argv[1]
    account_id = sys.argv[2]
    job_id = sys.argv[3]
    rule_id = sys.argv[4]
    version = sys.argv[5]
    rule = MHCM3(study_id, account_id, job_id, rule_id, version)
    rule.run()
