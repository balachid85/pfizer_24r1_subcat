'''
Rule ID: CMAE1
Release Version: R3.3M5.2
Changes: 
12-10-22 -  Code change to handle raw date fields          
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
a = yaml.load(open(subcat_config_path, 'r'), Loader=yaml.FullLoader)
class CMAE1(BaseSDQApi):
    domain_list = ['AE', 'CM']
    def execute(self):
        study = self.study_id
        domain_list = ['AE', 'CM']
        sub_cat = 'CMAE1'
        subjects = self.get_subjects(study, domain_list = domain_list, per_page = 10000) #[10121001]#
        index_col = 'itemrepn' if sub_cat.startswith('DR') else 'form_index'
        
        for subject in tqdm.tqdm(subjects):
            payload_list = []
            try:
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list = self.domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)

                ae_df = pd.DataFrame(flatten_data['AE'])
                cm_df = pd.DataFrame(flatten_data['CM'])

                for ind in range(cm_df.shape[0]):
                    try:
                        cm_record = cm_df.iloc[[ind]]

                        ae_cm_records = utils.extractor(cm_record, cm_df, ae_df, 'CMAENO', 'AESPID')
                                    
                        if (type(ae_cm_records) == tuple) & (type(ae_cm_records) != bool): 
                            if len(ae_cm_records[0]) > 0:      
                                for cm_formindex, ae_records in ae_cm_records[0].items():
                                    new_ae_records = utils.get_ae_hierarchy(ae_records, key = 'AESTDAT')
                                    
                                    for aeptcd in new_ae_records['AEPTCD'].unique().tolist():
                                        new_ae_record = new_ae_records[new_ae_records['AEPTCD'] == aeptcd]
                                        if len(new_ae_record) > 1:
                                            new_ae_record = new_ae_record.head(1)                                    
                                    
                                        if type(new_ae_record) != int: 
                                            cmaer = cm_record['CMAER'].values[0]
                                            
                                            dtr_null_flag=False
                                            if 'AESTDAT_DTR' in new_ae_record.columns:
                                                if new_ae_record['AESTDAT_DTR'].astype('str').values[0] in utils.null_list: 
                                                    dtr_null_flag=True
                                            
                                            if 'AESTDAT_DTR' in new_ae_record.columns and not dtr_null_flag:
                                                new_ae_record['AESTDAT_DTR'] = new_ae_record['AESTDAT_DTR'].astype('str')
                                                ae_stdt = new_ae_record['AESTDAT_DTR'].values[0]
                                            else:
                                                new_ae_record['AESTDAT'] = new_ae_record['AESTDAT'].astype('str')
                                                ae_stdt = new_ae_record['AESTDAT'].values[0]

                                            cm_record['CMSTDAT_DTR'] = cm_record['CMSTDAT_DTR'].astype('str')
                                            cm_stdt = cm_record['CMSTDAT_DTR'].values[0]

                                            unk_flag, year_not_unk, date_dict = utils.impute_unk({'aestdat':ae_stdt,
                                                                         'cmstdat':cm_stdt})

                                            
                                            # print(ae_stdt,cm_stdt)
                                            
                                            if not year_not_unk:
                                                continue
                                                
                                            aestdat = date_dict['aestdat'].date()
                                            cmstdat = date_dict['cmstdat'].date()
                                            
                                            
                                            # print(aestdat,cmstdat)


                                            if (cmstdat < aestdat) & (cmaer.upper() == 'YES'): 
                                                
                                                new_ae_record, cm_record = utils.unk_format_datetime(unk_flag, new_ae_record, cm_record)
                                                    
                                                subcate_report_dict = {}
                                                report_dict = {}

                                                piv_rec = {'CM' : cm_record, 'AE' : new_ae_record}

                                                for dom, cols in a['FIELDS_FOR_UI'][sub_cat].items():
                                                    piv_df = piv_rec[dom]
                                                    present_col = [col for col in cols if col in piv_df.columns.tolist()]
                                                    rep_df = piv_df[present_col]
                                                    rep_df['deeplink'] = utils.get_deeplink(study, piv_df)
                                                    rep_df = rep_df.rename(columns= a['FIELD_NAME_DICT'])
                                                    report_dict[dom]= rep_df.to_json(orient= 'records')

                                                subcate_report_dict[sub_cat] =  report_dict

                                                aespid = new_ae_record['AESPID'].values[0]
                                                aeterm = new_ae_record['AETERM'].values[0]
                                                formidx = cm_record['form_ix'].values[0]
                                                cmtrt = cm_record['CMTRT'].values[0]
                                                ae_stdt =  new_ae_record['AESTDAT'].values[0]
                                                cm_stdt = cm_record['CMSTDAT'].values[0]

                                                cm_record1 = cm_record.iloc[0]
                                                query_text_param = {
                                                'CM_FORMIDX':formidx,
                                                    'CMTRT':cmtrt,
                                                    'AESPID':aespid,
                                                    'AETERM':aeterm,
                                                    'CMSTDAT':cm_stdt,
                                                    'AESTDAT':ae_stdt
                                                }
                                                payload = {
                                                    "subcategory": sub_cat,
                                                    'cdr_skey':str(cm_record1['cdr_skey']),
                                                    "query_text": self.get_model_query_text_json(study, sub_cat, params = query_text_param),
                                                    "form_index": str(cm_record1['form_index']),
                                                    "question_present": self.get_subcategory_json(study, sub_cat),
                                                    "modif_dts": str(cm_record1['modif_dts']),  
                                                    "stg_ck_event_id": int(cm_record1['ck_event_id']),
                                                    "formrefname" : str(cm_record1['formrefname']),
                                                    "report" : subcate_report_dict,
                                                    "confid_score": np.random.uniform(0.7, 0.97)
                                                }
                                                #print(payload)
                                                self.insert_query(study, subject, payload)
                                                if payload not in payload_list:
                                                    payload_list.append(payload)

                                                #break
                                
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
    rule = CMAE1(study_id, account_id, job_id, rule_id, version)
    rule.run()
