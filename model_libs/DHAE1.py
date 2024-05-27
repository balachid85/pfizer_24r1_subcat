import os
import sys
import logging
import yaml
import traceback
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
import utils
from base import BaseSDQApi
import tqdm

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class DHAE1(BaseSDQApi):
    domain_list = ['AE','DD']

    def execute(self):
        study = self.study_id
        domain_list = self.domain_list
        sub_cat = self.__class__.__name__ #'DHAE1'

        f_d = 'display_fields'
        f_c = 'fn_config'
        
        try:
            fields_dict = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_d))
            fn_config = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_c))
            fields_labels_1 = self.get_field_labels(self.account_id,domain_list[0])
            fields_labels_2 = self.get_field_labels(self.account_id,domain_list[1])
            fields_labels = {**fields_labels_1,**fields_labels_2}
                     
            match_terms = fn_config['match']    
            match_cond = fn_config['match_condition']
            cols = fn_config['cols'] 
            vals = fn_config['vals']

            subjects = self.get_subjects(study, domain_list=self.domain_list, per_page = 10000)  
        except Exception as fn_exc:
            print('Exception while getting config values/subjects:', fn_exc)
            print(traceback.format_exc())      

        ae_df = pd.DataFrame()
        dh_df = pd.DataFrame()

        for subject in tqdm.tqdm(subjects):
            payload_list = []
            print('Subject is:', subject)
            payload_list = []
            try:
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list = domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)
                try:
                    dh_df = pd.DataFrame(flatten_data.get(self.domain_list[1],[]))
                    ae_df = pd.DataFrame(flatten_data.get(self.domain_list[0],[]))    
                    ae_df = ae_df[ae_df[cols['ae_tox_col'][0]].notna()]
                    if len(ae_df) == 0 or len(dh_df) == 0:
                        continue
                    else:
                        ae_cols = ae_df.columns.tolist()
                        aerefid = ''
                        for i in cols['ae_ref_id_col']:
                            if i in ae_cols:
                                aerefid = i
                                break
                        try:
                            #ae_df = ae_df[ae_df[cols['ae_tox_col'][0]].astype(dtype = np.float64).astype(dtype=np.int64) == int(float(vals['ae_tox_val'][0]))]
                            ae_df = ae_df[ae_df[cols['ae_tox_col'][0]].str.upper().isin(vals['ae_tox_val'])]
                        except Exception as tox_exc:
                            print(traceback.format_exc())
                            print(f'Invalid tox col value {tox_exc}')
                            continue                        
                except Exception as df_exc:
                    print('Exception while fetching df:', df_exc)
                    continue

                if len(ae_df) == 0 or len(dh_df) == 0:
                    continue

                for ind in range(ae_df.shape[0]):
                    try:
                        ae_record = ae_df.iloc[[ind]]
                        new_dh_df = pd.DataFrame()
                        new_dh_df = dh_df.copy()
                        new_dh_df1 = pd.DataFrame()
                        dh_match_df = pd.DataFrame()

                        for i in match_terms:
                            match_config = fn_config[i]
                            match_flag, dh_match_df = utils.ae_cm_mapper(prim_rec=ae_record,
                                                            sec_df = new_dh_df,
                                                            subcat = sub_cat,
                                                            **match_config)
                            if len(match_cond)>0 and match_cond[0].upper() == 'AND':
                                new_dh_df1 = dh_match_df
                                if(len(dh_match_df) > 0):
                                    new_dh_df = dh_match_df
                                else:
                                    break
                            else:
                                new_dh_df1 = new_dh_df1.append(dh_match_df)  
                    except Exception as E:
                        logging.info(f'Exception in match_flag_1 is {E}')
                        pass
                        
                    if len(new_dh_df1) == 0 : # or match_flag_2  
                        print('No matching records in AE')
                        subcate_report_dict = {}
                        report_dict = {}                 
                        
                        #piv_rec = {'AE': ae_record, 'DD': dh_df}
                        piv_rec = {'AE': ae_record, 'DD': dh_df.head(1)}

                        for dom, columns in fields_dict.items():
                            piv_df = piv_rec[dom]
                            present_col = [col for col in columns if col in piv_df.columns.tolist()]
                            rep_df = piv_df[present_col]
                            rep_df['deeplink'] = utils.get_deeplink(study, piv_df, self.get_study_source(), subject_id=self.get_map_subject_id(piv_df['subj_guid'].values[0]))
                            rep_df = rep_df.rename(columns= fields_labels)
                            rep_df = rep_df.loc[:,~rep_df.columns.duplicated()]
                            report_dict[dom]= rep_df.to_json(orient= 'records') 
                        #print('-*'*20,'\n')

                        subcate_report_dict[sub_cat] = report_dict
                            
                        query_text_param={
                                cols['ae_tox_col'][0] : vals['ae_tox_val'][0],
                                cols['ae_term_col'][0] : ae_record[cols['ae_term_col'][0]].values[0],
                        }

                        payload = {
                                "subcategory": sub_cat,
                                "cdr_skey" : str(ae_record['cdr_skey'].values[0]),
                                "query_text": self.get_model_query_text_json(study, sub_cat, params =query_text_param),
                                "form_index": str(ae_record['form_index'].values[0]),
                                "question_present": self.get_subcategory_json(study, sub_cat),
                                "modif_dts": str(ae_record['modif_dts'].values[0]),  
                                "stg_ck_event_id": int(ae_record['ck_event_id'].values[0]),
                                "formrefname" : str(ae_record['formrefname'].values[0]),
                                "report" : subcate_report_dict,
                                "confid_score": np.random.uniform(0.7, 0.97)
                                }
                        print('Subject and Payload is :', subject, payload)                                    
                        self.insert_query(study, subject, payload)
                        if payload not in payload_list:
                            payload_list.append(payload)
            except:
                print(traceback.format_exc())
                continue
            if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                self.close_query(subject, payload_list) 
            
if __name__ == '__main__':
    #(self.study_id, self.account_id, self.job_id, rule['ml_model_id'], 0.1
    # python DHAE1.py 1 1 52 1 0.1 > dhae1log.txt
    study_id = sys.argv[1]
    account_id = sys.argv[2]    
    rule_id = sys.argv[3]
    job_id = sys.argv[4]
    version = sys.argv[5]
    rule = DHAE1(study_id, account_id, job_id, rule_id, version)
    
    rule.run()
