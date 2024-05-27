import os
import sys
import logging
import yaml
import warnings
warnings.filterwarnings('ignore')
import traceback
import numpy as np
import pandas as pd
try:
    from scripts.SubcatModels.rave.model_libs.base import BaseSDQApi
    import scripts.SubcatModels.rave.model_libs.utils as utils
except:
    import utils
    from base import BaseSDQApi

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class AEMH0(BaseSDQApi):
    domain_list = ['AE', 'MH']

    def execute(self):
        study= self.study_id
        sub_cat = self.__class__.__name__ #'AEMH0'

        try:
            fn_config = (self.get_field_dict(self.account_id, self.study_id, sub_cat, 'fn_config'))
            fields_labels_1 = self.get_field_labels(self.account_id,self.domain_list[0])
            fields_labels_2 = self.get_field_labels(self.account_id,self.domain_list[1])
            fields_labels = {**fields_labels_1,**fields_labels_2}
            fields_dict = (self.get_field_dict(self.account_id, self.study_id, sub_cat, 'display_fields'))

            cols = fn_config['cols']
            check = fn_config['check']
            symptoms_list = check['symptoms_list']
            match_config = fn_config['match_term']
            smc = fn_config['use_smc'] if 'use_smc' in fn_config.keys() else False
            predict_med_code = None

            subjects = self.get_subjects(study, domain_list = self.domain_list, per_page = 10000)
        except Exception as fn_exc:
            print('Exception while getting config values / subjects :', fn_exc)
            print(traceback.format_exc())      
            
        for subject in tqdm.tqdm(subjects):
            payload_list = []subjects:
            try:
                print('Subject is :', subject)
                payload_list = []
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list = self.domain_list)
                
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)
                try:
                    mh_df = pd.DataFrame(flatten_data.get(self.domain_list[1],[]))  
                    ae_df = pd.DataFrame(flatten_data.get(self.domain_list[0],[]))
                    if(len(ae_df) == 0):
                        continue
                    ae_df[cols['ae_term_col'][0]] = ae_df[cols['ae_term_col'][0]].apply(lambda x: str(x).upper())
                    ae_df[cols['ae_decod_col'][0]] = ae_df[cols['ae_decod_col'][0]].apply(lambda x: str(x).upper())
                    
                    if(len(mh_df)>0):
                        mh_df[cols['mh_decod_col'][0]] = mh_df[cols['mh_decod_col'][0]].apply(lambda x: str(x).upper()) 
                              
                except Exception as abc:
                    logging.info(f'Exception while flattening {abc}')
                    continue 

                for i in cols['ae_ref_id_col']:
                    if i in ae_df.columns.tolist():
                        aerefid = i
                        break

                search_pattern = '|'.join(symptoms_list)
                #print('AETERM IS :',ae_df[cols['ae_term_col'][0]])
                ae_df1 = ae_df[ae_df[cols['ae_term_col'][0]].str.upper().isin(symptoms_list)]
                ae_df2 = ae_df[ae_df[cols['ae_term_col'][0]].str.contains(search_pattern, na=False)]
                ae_df1 = ae_df1.append(ae_df2) 
                ae_df1.drop_duplicates([cols['uniq_id_col'][0]],keep='first', inplace=True)
                
                for ind in range(ae_df1.shape[0]):
                    try:
                        ae_record = ae_df1.iloc[[ind]]
                        mh_records = pd.DataFrame()                  
                        try:                            
                            if(len(mh_df) > 0):   
                                if smc:
                                    predict_med_code = self.predict_medical_code
                                    sec_col = match_config['sec_match_col']
                                    mh_df = mh_df[mh_df[sec_col].notna()]
                                else:
                                    predict_med_code = None     
                                    
                                if len(mh_df)>0:
                                    match_flag, mh_records = utils.ae_cm_mapper(prim_rec = ae_record,
                                                                sec_df = mh_df,
                                                                subcat = sub_cat,
                                                                use_smc = smc,
                                                                func = predict_med_code, 
                                                                **match_config)                                 
                        except Exception as Ex:
                            logging.info(f'Error in match_flag_1 {Ex}')
                            continue
                        
                        if len(mh_records) == 0:
                            subcate_report_dict = {}
                            report_dict = {}
                            mh_blank_record = {}
                            for mh_columns in fields_dict[self.domain_list[1]]:
                                mh_blank_record[mh_columns] = [' '] 
                            mh_blank_record = pd.DataFrame.from_dict(mh_blank_record).head(1)
                            
                            piv_rec = {'AE' : ae_record, 'MH':mh_blank_record}
                            subcate_report_dict = {}
                            report_dict = {'AE':dict(), 'MH':dict()}
                            #report_dict = {'AE':dict()}

                            ae_record = ae_record.replace({np.nan : 'blank'})

                            for dom, columnss in fields_dict.items():
                                piv_df = piv_rec[dom]
                                present_col = [col for col in columnss if col in piv_df.columns.tolist()]
                                rep_df = piv_df[present_col]
                                try:
                                    rep_df['deeplink'] = utils.get_deeplink(study, piv_df, self.get_study_source(), subject_id=self.get_map_subject_id(piv_df['subj_guid'].values[0]))
                                except:
                                    rep_df['deeplink'] = '#' 
                                rep_df = rep_df.rename(columns = fields_labels)
                                rep_df = rep_df.loc[:,~rep_df.columns.duplicated()]
                                report_dict[dom]= rep_df.to_json(orient= 'records')
                                print('-*'*20,'\n')

                            subcate_report_dict[sub_cat] = report_dict
                            query_text_param={
                                           cols['ae_term_col'][0] : str(ae_record[cols['ae_term_col'][0]].values[0])
                                           } 
                            payload = {
                                    "subcategory": sub_cat,
                                    "cdr_skey" : str(ae_record['cdr_skey'].values[0]),
                                    "query_text": self.get_model_query_text_json(study, sub_cat, params = query_text_param),
                                    "form_index": str(ae_record['form_index'].values[0]),
                                    "question_present": self.get_subcategory_json(study, sub_cat),
                                    "modif_dts": str(ae_record['modif_dts'].values[0]),  
                                    "stg_ck_event_id": int(ae_record['ck_event_id']),
                                    "formrefname" : str(ae_record['formrefname'].values[0]),
                                    "report" : subcate_report_dict,
                                    "confid_score": np.random.uniform(0.7, 0.97)
                                }
                            print('Subject and Payload :', subject, payload)
                            self.insert_query(study, subject, payload)
                            if payload not in payload_list:
                                payload_list.append(payload)

                    except:
                        print(traceback.format_exc())
                        continue
            except:
                #print(traceback.format_exc())
                continue
            if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                self.close_query(subject, payload_list)
                    
if __name__ == "__main__":
    study_id = sys.argv[1]
    rule_id = sys.argv[2]
    version = sys.argv[3]
    rule = AEMH0(study_id, rule_id, version)
    rule.run()