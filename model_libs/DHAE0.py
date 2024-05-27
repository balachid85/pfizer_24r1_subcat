import sys
import logging
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

class DHAE0(BaseSDQApi):
    domain_list = ['DD','AE']

    def execute(self):
        study = self.study_id
        domain_list = self.domain_list
        sub_cat = self.__class__.__name__ #'DHAE0'

        f_d = 'display_fields'
        f_c = 'fn_config'
        
        try:
            fields_dict = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_d))
            fn_config = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_c))
            fields_labels_1 = self.get_field_labels(self.account_id,domain_list[0])
            fields_labels_2 = self.get_field_labels(self.account_id,domain_list[1])
            fields_labels = {**fields_labels_1,**fields_labels_2}
            
       
            match_config1 = fn_config['match_id']  
            match_config2 = fn_config['match_term']  
            cols = fn_config['cols'] 

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
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list = self.domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)
                try:
                    dh_df = pd.DataFrame(flatten_data.get(self.domain_list[0],[]))
                    ae_df = pd.DataFrame(flatten_data.get(self.domain_list[1],[]))               
                    #print('Len of dh df is:', len(dh_df))
                    
                    if len(dh_df) == 0 or len(ae_df) == 0:
                        continue

                    #print('Len of ae df is:', len(ae_df))
                    aerefid = ''

                    if(len(ae_df) > 0):
                        ae_cols = ae_df.columns.tolist()
                        aerefid = ''
                        for i in cols['ae_ref_id_col']:
                            if i in ae_cols:
                                aerefid = i
                                break
                
                except Exception as df_exc:
                    print('Exception while fetching df:', df_exc)
                    pass

                if len(dh_df) == 0 or len(ae_df) == 0:
                        continue
                    
                for ind in range(dh_df.shape[0]):
                    dh_record = dh_df.iloc[[ind]]
                    new_ae_df = pd.DataFrame()
                    new_ae_df1 = pd.DataFrame()
                    try:                        
                        match_config1['sec_match_col'] = aerefid
                        match_flag_1, new_ae_df = utils.ae_cm_mapper(prim_rec = dh_record,
                                                        sec_df = ae_df,
                                                        subcat = sub_cat,
                                                        **match_config1)        
                    except Exception as E:
                        logging.info(f'Exception in match_flag_1 is {E}')
                        pass
                    
                    if(len(new_ae_df) > 0):
                        #print('Len of matching ae_df is :', len(new_ae_df))
                        try:
                            match_flag_2, new_ae_df1 = utils.ae_cm_mapper(prim_rec=dh_record,
                                                        sec_df = new_ae_df,
                                                        subcat = sub_cat,
                                                        **match_config2) 
                        except Exception as E2:
                            logging.info(f'Exception in match_flag_2 is {E2}')
                            pass

                        if len(new_ae_df1) == 0 :# or len(ae_df) == 0:   
                            print('No matching records in AE')                              
                            subcate_report_dict = {}
                            report_dict = {}  

                            ae_blank_record = {}
                            for ae_columns in fields_dict[self.domain_list[1]]:
                                ae_blank_record[ae_columns] = [' ']                                                                       
                            ae_blank_record = pd.DataFrame.from_dict(ae_blank_record).head(1)

                            piv_rec = {'DD': dh_record, 'AE' : ae_blank_record}
                            
                            for dom, columns in fields_dict.items():
                                piv_df = piv_rec[dom]
                                present_col = [col for col in columns if col in piv_df.columns.tolist()]
                                rep_df = piv_df[present_col]
                                try:
                                    rep_df['deeplink'] = utils.get_deeplink(study, piv_df, self.get_study_source(), subject_id=self.get_map_subject_id(piv_df['subj_guid'].values[0]))
                                except:
                                    rep_df['deeplink'] = '#'
                                rep_df = rep_df.rename(columns= fields_labels)
                                rep_df = rep_df.loc[:,~rep_df.columns.duplicated()]
                                report_dict[dom]= rep_df.to_json(orient= 'records') 
                            #print('-*'*20,'\n')

                            subcate_report_dict[sub_cat] = report_dict
                                
                            query_text_param={
                                    cols['dh_ae_id_col'][0] : dh_record[cols['dh_ae_id_col'][0]].values[0], 
                                }

                            payload = {
                                    "subcategory": sub_cat,
                                    "cdr_skey" : str(dh_record['cdr_skey'].values[0]),
                                    "query_text": self.get_model_query_text_json(study, sub_cat, params =query_text_param),
                                    "form_index": str(dh_record['form_index'].values[0]),
                                    "question_present": self.get_subcategory_json(study, sub_cat),
                                    "modif_dts": str(dh_record['modif_dts'].values[0]),  
                                    "stg_ck_event_id": int(dh_record['ck_event_id'].values[0]),
                                    "formrefname" : str(dh_record['formrefname'].values[0]),
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
    # python DHAE0.py 1 1 52 1 0.1 > dhae0log.txt
    study_id = sys.argv[1]
    account_id = sys.argv[2]    
    rule_id = sys.argv[3]
    job_id = sys.argv[4]
    version = sys.argv[5]
    rule = DHAE0(study_id, account_id, job_id, rule_id, version)
    
    rule.run()
