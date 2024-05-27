import sys
import logging
import warnings
warnings.filterwarnings('ignore')
import traceback
import numpy as np
import pandas as pd
import utils

from base import BaseSDQApi

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class MHAE0(BaseSDQApi):
    domain_list = ['MH','AE']

    def execute(self):
        study= self.study_id
        sub_cat = self.__class__.__name__ #'MHAE0'
        
        try:
            f_d = 'display_fields'
            f_c = 'fn_config'

            fields_dict = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_d))
            fn_config = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_c))

            fields_labels_1 = self.get_field_labels(self.account_id, self.domain_list[0])
            fields_labels_2 = self.get_field_labels(self.account_id, self.domain_list[1])
            fields_labels = {**fields_labels_1, **fields_labels_2 }

            cols = fn_config['cols']
            vals = fn_config['vals']
            symptoms_list = fn_config['check']['symptoms_list'] 
            match_config = fn_config['match_term']
            smc = fn_config['use_smc'] if 'use_smc' in fn_config.keys() else False
            predict_med_code = None

            mh_ongo_yes_val = vals['mh_ongo_val']['yes_val']
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
                    ae_df = pd.DataFrame(flatten_data.get(self.domain_list[1],[]))
                    
                    if(len(mh_df)==0):
                        continue

                    if(len(ae_df)>0):
                        ae_df[cols['ae_term_col'][0]] = ae_df[cols['ae_term_col'][0]].apply(lambda x: str(x).upper())
                        ae_df[cols['ae_decod_col'][0]] = ae_df[cols['ae_decod_col'][0]].apply(lambda x: str(x).upper())
                    mh_df[cols['mh_ongo_col'][0]] = mh_df[cols['mh_ongo_col'][0]].apply(lambda x: str(x).upper())
                    mh_df = mh_df[mh_df[cols['mh_ongo_col'][0]].isin(mh_ongo_yes_val)]
                    if(len(mh_df) == 0):
                        continue
                    mh_df[cols['mh_decod_col'][0]] = mh_df[cols['mh_decod_col'][0]].apply(lambda x: str(x).upper())
                    
                except Exception as df_exc:
                    print('Exception while fetching df:', df_exc)
                    continue

                for ind in range(mh_df.shape[0]):
                    try:
                        mh_record = mh_df.iloc[[ind]]
                        ae_records = pd.DataFrame()
                        if(len(ae_df) > 0):
                            ae_cols = ae_df.columns.tolist()
                            aerefid = ''

                            for i in cols['ae_ref_id_col']:
                                if i in ae_cols:
                                    aerefid = i
                                    break
                            try:
                                if smc:
                                    predict_med_code = self.predict_medical_code
                                    #sec_col = match_config['sec_match_col']
                                    #ae_df = ae_df[ae_df[sec_col].notna()]
                                else:
                                    predict_med_code = None
                                match_flag, ae_records = utils.ae_cm_mapper(prim_rec = mh_record,
                                                        sec_df = ae_df,
                                                        subcat = sub_cat,
                                                        use_smc = smc,
                                                        func = predict_med_code, 
                                                        **match_config)
                            except Exception as ae_match_exc:
                                print(f'Exception is : {ae_match_exc}')
                                continue

                        if(len(ae_records) == 0):
                            continue
                                               
                        search_pattern = '|'.join(symptoms_list)          
                        
                        ae_records1 = ae_records[~(ae_records[cols['ae_term_col'][0]].str.contains(search_pattern, na=False))]
                        
                        for ae_ind in range(ae_records1.shape[0]):
                            ae_record = ae_records1.iloc[[ae_ind]]
                            subcate_report_dict = {}
                            report_dict = {}                                
                            ae_record = ae_record.replace({np.nan : 'blank'})
                            mh_record = mh_record.replace({np.nan : 'blank'})

                            piv_rec = {'MH' : mh_record, 'AE' : ae_record}
                            
                            for dom, columns in fields_dict.items():
                                piv_df = piv_rec[dom]
                                present_col = [col for col in columns if col in piv_df.columns.tolist()]
                                rep_df = piv_df[present_col]
                                rep_df['deeplink'] = utils.get_deeplink(study, piv_df, self.get_study_source(), subject_id=self.get_map_subject_id(piv_df['subj_guid'].values[0]))
                                rep_df = rep_df.rename(columns= fields_labels)
                                rep_df = rep_df.loc[:,~rep_df.columns.duplicated()]
                                report_dict[dom]= rep_df.to_json(orient= 'records')

                            subcate_report_dict[sub_cat] = report_dict
                            query_text_param={
                                            cols['mh_decod_col'][0] : str(mh_record[cols['mh_decod_col'][0]].values[0]),
                                            #'MHDECOD' : mh_record[cols['mh_decod_col'][0]].values[0],
                                            cols['ae_decod_col'][0] : str(ae_record[cols['ae_decod_col'][0]].values[0])
                                            #'AEDECOD' : ae_record[cols['ae_decod_col'][0]].values[0]
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
                            print('Subject and Payload are :', subject, payload)
                            self.insert_query(study, subject, payload)
                            if payload not in payload_list:
                                payload_list.append(payload)
                            break
                    except:
                        print(traceback.format_exc())
                        continue
            except Exception as e:
                print(traceback.format_exc())
                logging.info('Exception is: ', e)
                continue
            if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                self.close_query(subject, payload_list)
                    
if __name__ == "__main__":
    #(self.study_id, self.account_id, self.job_id, rule['ml_model_id'], 0.1
    #python MHAE0.py 1 1 1 80 0.1 > mhae0log.txt
    study_id = sys.argv[1]
    account_id = sys.argv[2]
    job_id = sys.argv[3]
    rule_id = sys.argv[4]
    version = sys.argv[5]
    rule = MHAE0(study_id, account_id, job_id, rule_id, version)
    
    rule.run()
