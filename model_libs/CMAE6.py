'''
Rule ID: CMAE6
Release Version: R3.2M5.1
Changes: 
30-06-22 - REMOVING EXTRA KEYS
            
'''

import pandas as pd
import sys
import re
import warnings
warnings.filterwarnings('ignore')
try:
    from scripts.SubcatModels.model_libs.base import BaseSDQApi
    import scripts.SubcatModels.model_libs.utils as utils
except:
    from base import BaseSDQApi
    import utils as utils
import traceback
import tqdm
import logging
import yaml
import numpy as np
import os

curr_file_path = os.path.realpath(__file__)
curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
subcat_config_path = os.path.join(curr_path, 'subcate_config.yml')
a = yaml.safe_load(open(subcat_config_path, 'r'))

class CMAE6(BaseSDQApi):
    domain_list = ['AE', 'CM']
    def execute(self):
        study = self.study_id
        
        domain_list = ['AE', 'CM']
        sub_cat = 'CMAE6'
        subjects =self.get_subjects(study, domain_list = domain_list, per_page = 10000)
        index_col = 'itemrepn' if sub_cat.startswith('DR') else 'form_index'
        
        for subject in tqdm.tqdm(subjects):
            payload_list = []
            try:
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list = domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)

                cm_df = pd.DataFrame(flatten_data['CM'])
                #cm_df = cm_df[cm_df[index_col].isin(index_list)]

                ae_flag = True
                if 'AE' in flatten_data:
                    ae_df = pd.DataFrame(flatten_data['AE'])
                    if len(ae_df)>0:
                        ae_flag = True
                        continue
                    else:
                        ae_flag = False       
                else:
                    ae_flag = False

                for ind in range(cm_df.shape[0]):
                    try:
                        cm_record = cm_df.iloc[[ind]]

                        cmaer = cm_record['CMAER'].values[0]
                        if cmaer.upper() == 'YES' and ae_flag == False:
                            subcate_report_dict = {}
                            report_dict = {}

                            cm_record = cm_record.fillna('blank')
                            
                            cm_record['CMSTDAT'] = cm_record['CMSTDAT'].apply(utils.format_datetime)
                            cm_record['CMENDAT'] = cm_record['CMENDAT'].apply(utils.format_datetime)
                            
                            piv_rec = {'CM' : cm_record}
                            
                            for dom, cols in a['FIELDS_FOR_UI'][sub_cat].items():
                                piv_df = piv_rec[dom]
                                present_col = [col for col in cols if col in piv_df.columns.tolist()]
                                rep_df = piv_df[present_col]
                                rep_df['deeplink'] = utils.get_deeplink(study, piv_df)
                                rep_df = rep_df.rename(columns= a['FIELD_NAME_DICT'])
                                report_dict[dom]= rep_df.to_json(orient= 'records')
                                
                            subcate_report_dict[sub_cat] =  report_dict
                            #COMMENTING OUT SINCE ITS NOT NEEDED FOR THE LATEST QT
#                             formidx = cm_record['form_ix'].values[0]
#                             cmtrt = cm_record['CMTRT'].values[0]
#                             cmstdt = cm_record['CMSTDAT'].values[0]
#                             cmendt = cm_record['CMENDAT'].values[0]
                            
#                             if cmendt == np.nan:
#                                 cmendt = 'blank'
                            cm_record1 = cm_record.iloc[0]
                            
#                             query_text_param= {
                                
#                                 'CM_FORMIDX' : formidx,
#                                 'CMTRT' : cmtrt,
#                                 'CMSTDAT' : cmstdt,
#                                 'CMENDAT' : cmendt,
#                                 'CMONGO' : cm_record['CMONGO'].values[0]
#                                         }
                            
                            payload = {
                                "subcategory": sub_cat,
                                'cdr_skey':str(cm_record1['cdr_skey']),
                                "query_text": self.get_model_query_text_json(study, sub_cat),
                                "form_index": str(cm_record1['form_index']),
                                "question_present": self.get_subcategory_json(study, sub_cat),
                                "modif_dts": str(cm_record1['modif_dts']),  
                                "stg_ck_event_id": int(cm_record1['ck_event_id']),
                                "formrefname" : str(cm_record1['formrefname']),
                                "report" : subcate_report_dict,
                                "confid_score": np.random.uniform(0.7, 0.97)
                            }
                            
                            self.insert_query(study, subject, payload)
                            if payload not in payload_list:
                                payload_list.append(payload)

                            #break
                                
                    except:
                        print(traceback.format_exc())
                        continue       

            except Exception as e:
                logging.exception(e)
                print(e)
                                                
            if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                self.close_query(subject, payload_list)

if __name__ == '__main__':
    study_id = sys.argv[1]
    account_id = sys.argv[2]
    job_id = sys.argv[3]
    rule_id = sys.argv[4]
    version = sys.argv[5]
    rule = CMAE6(study_id, account_id, job_id, rule_id, version)
    rule.run()
