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

class MHCM0(BaseSDQApi):
    domain_list = ['MH','CM']

    def execute(self):
        study= self.study_id
        sub_cat = self.__class__.__name__ #'MHCM0'
        
        try:
            f_d = 'display_fields'  
            f_c = 'fn_config'      
            fields_dict = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_d))
            fn_config = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_c))
            fields_labels_1 = self.get_field_labels(self.account_id,self.domain_list[0])
            fields_labels_2 = self.get_field_labels(self.account_id,self.domain_list[1])
            fields_labels = {**fields_labels_1,**fields_labels_2}

            cols = fn_config['cols']
            vals = fn_config['vals']
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
                except Exception as exc:
                    logging.info('Exception while fetching mh and cm data :', exc)
                    print(traceback.format_exc())
                
                if(len(mh_df)==0):
                    continue
                else:
                    mh_cols = mh_df.columns.tolist()
                
                if(len(cols['mh_contrt_col'])>0 and cols['mh_contrt_col'][0] in mh_cols):
                    mh_contrt_yes = vals['mh_contrt_val']['yes_val']
                    mh_df[cols['mh_contrt_col'][0]] = mh_df[cols['mh_contrt_col'][0]].apply(lambda x: str(x).upper())                  
                    mh_df = mh_df[mh_df[cols['mh_contrt_col'][0]].isin(mh_contrt_yes)]
                if(len(mh_df) > 0):
                    mh_df = mh_df[mh_df[cols['mh_term_col'][0]].notna()]
                
                if(len(mh_df)==0):
                    continue
                
                if(len(cm_df) == 0):
                    for ind in range(mh_df.shape[0]):
                        try:
                            mh_record = mh_df.iloc[[ind]]
                            subcate_report_dict = {}
                            report_dict = {}
                        
                            mh_record = mh_record.replace({np.nan : 'blank'})
                            cm_blank = {}
                            for cm_columns in fields_dict[self.domain_list[1]]:
                                cm_blank[cm_columns] = [' ']
                            cm_blank = pd.DataFrame.from_dict(cm_blank).head(1)

                            piv_rec = {'MH' : mh_record, 'CM' : cm_blank}
                            for dom, columns in fields_dict.items():
                                piv_df = piv_rec[dom]
                                present_col = [col for col in columns if col in piv_df.columns.tolist()]
                                rep_df = piv_df[present_col]
                                try:
                                    rep_df['deeplink'] = utils.get_deeplink(study,piv_df,dom)
                                except:
                                    rep_df['deeplink'] = '#'
                                rep_df = rep_df.rename(columns = fields_labels)
                                rep_df = rep_df.loc[:,~rep_df.columns.duplicated()]
                                report_dict[dom]= rep_df.to_json(orient= 'records')

                            subcate_report_dict[sub_cat] = report_dict
                            query_text_param={#cols['mh_term_col'][0] : mh_record[cols['mh_term_col'][0]].values[0]
                                              'MHTERM' : mh_record[cols['mh_term_col'][0]].values[0]
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
                            print('Subject and payload :', subject, payload)
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
    # python MHCM0.py 1 1 12 1 0.1 > mhcm0log.txt
    study_id = sys.argv[1]
    account_id = sys.argv[2]
    job_id = sys.argv[3]
    rule_id = sys.argv[4]
    version = sys.argv[5]
    rule = MHCM0(study_id, account_id, job_id, rule_id, version)
    rule.run()
