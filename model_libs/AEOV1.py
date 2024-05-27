'''
Rule ID: AEOV1
Release Version: R3.2M5.1,[R3.6M5.3]
Changes: 
30-06-22 - UPDATING THE FUZZYWUZZY LOGIC
[EDIT#2]-21-12-22 -  DATE FORMAT CORRECTION AND PREDS SHOULD FIRE FOR ALL THE PREDS
'''

import pandas as pd
import sys
import re
import warnings
warnings.filterwarnings('ignore')
try:
    from scripts.SubcatModels.model_libs.base import BaseSDQApi
    import scripts.SubcatModels.model_libs.utils as utils
    from  is_ovduplicate import DuplicateCheck
except:
    from  is_ovduplicate import DuplicateCheck
    from base import BaseSDQApi
    import utils as utils
import traceback
import tqdm
import logging
import numpy as np
import yaml
import os
import json

curr_file_path = os.path.realpath(__file__)
curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
subcat_config_path = os.path.join(curr_path, 'subcate_config.yml')
subcate_config = yaml.safe_load(open(subcat_config_path, 'r'))

class AEOV1(BaseSDQApi):
    domain_list = ['AE']
    def execute(self):
        study = self.study_id
        ov_study_id = f'account_{self.account_id}_study_{self.study_id}'
        duplicate=DuplicateCheck(ov_study_id)
        check_if_duplicate=duplicate.check
        domain_list = ['AE']
        sub_cat = 'AEOV1'
        subjects =self.get_subjects(study, domain_list = self.domain_list, per_page = 10000)# [88880001]#
        for subject in tqdm.tqdm(subjects):
            payload_list = []
            try:
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list = domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)
                ae_df = pd.DataFrame(flatten_data['AE'])
                
                for ind in range(ae_df.shape[0]):
                    try:
                        
                        ae_record = ae_df.iloc[[ind]]
                        total_valid_present = 0
                        total_count = 0
                        aeterm = ae_record['AETERM'].values[0]
                        aespid = ae_record['AESPID'].values[0]
                        aedecod = ae_record['AEDECOD'].values[0]
                        prim_subj = ae_record['subjid'].values[0]
                        aest_dt = ae_record['AESTDAT'].apply(utils.format_datetime).values.tolist()[0]
                        aeen_dt = ae_record['AEENDAT'].apply(utils.format_datetime).values.tolist()[0]
                        formidx = ae_record['form_ix'].values[0]
                        aeongo = ae_record['AEONGO'].values[0]
                        # [EDIT#2] adding the copy methode since dont want to mess the ae_df values
                        ae_records = ae_df.copy()
                        aetoxgr_present = False
                        ae_cols = ae_record.columns.tolist()
                        print(subject,"###############")
                        
                        if 'AETOXGR' in ae_cols:
                            aetoxgr_present = True
                            aetoxgr = ae_record['AETOXGR'].values[0]
                        #[EDIT#2] DATE FORMAT CHANGING 
                        ae_records['AESTDAT'] = ae_records['AESTDAT'].apply(utils.format_datetime)
                        ae_records['AEENDAT'] = ae_records['AEENDAT'].apply(utils.format_datetime)
                        

                        if type(aest_dt) != float and type(aeen_dt) != float: 
                            if aetoxgr_present:
                                new_ae_record = ae_records[(ae_records['AESTDAT'] == aest_dt) & (ae_records['AEENDAT'] == aeen_dt) & (ae_records['AETOXGR'] == aetoxgr) & (ae_records['AEONGO'] == aeongo)]
                            else:
                                new_ae_record = ae_records[(ae_records['AESTDAT'] == aest_dt) & (ae_records['AEENDAT'] == aeen_dt) & (ae_records['AEONGO'] == aeongo)]
                        
                        elif type(aest_dt) != float and type(aeen_dt) == float:
                            if aetoxgr_present:
                                new_ae_record = ae_records[(ae_records['AESTDAT'] == aest_dt) & (ae_records['AETOXGR'] == aetoxgr) & (ae_records['AEONGO'] == aeongo)]
                            else:
                                new_ae_record = ae_records[(ae_records['AESTDAT'] == aest_dt) & (ae_records['AEONGO'] == aeongo)]

                        print(new_ae_record['form_ix'],"&&&&&&&&&&&&&")
                        print(formidx,"formidxxx")        
                        
                        new_ae_record = new_ae_record[new_ae_record['form_ix'] != formidx]
                        print(subject,"*************")
                        print(len(new_ae_record))
                        if len(new_ae_record) == 0:
                            continue
                                
                        elif len(new_ae_record) >= 1:
                            for ind in new_ae_record.index.tolist():
                                curr_aeterm = new_ae_record.loc[ind, 'AETERM']
                                curr_aedecod = new_ae_record.loc[ind, 'AEDECOD']
                                #fuzzy_logic is only for aeterm not for aedecod
                                
                                aeterm_flag = utils.check_similar_term_fuzzy(aeterm, curr_aeterm)
                                aedecod_flag = utils.check_similar_medication_verabtim2(aedecod, curr_aedecod)
                                if aeterm_flag and aedecod_flag:
                                # if aedecod_flag:
                                    ind1 = ind
                                    total_valid_present += 1
                                    break
                            if total_valid_present > 0:                             
                                #print(True, ae_record['form_index'].values[0])
                                subcate_report_dict = {}
                                report_dict = {}
                                new_ae_record = new_ae_record.loc[[ind1]]
                                piv_rec = {'AE1': ae_record.head(1),'AE2' : new_ae_record.head(1)}

                                for dom, cols in subcate_config['FIELDS_FOR_UI'][sub_cat].items():

                                    if len(cols) > 0:

                                        for k_dom in piv_rec.keys():
                                            piv_df = piv_rec[k_dom]
                                            present_col = [col for col in cols if col in piv_df.columns.tolist()]
                                            rep_df = piv_df[present_col]
                                            rep_df['deeplink'] = utils.get_deeplink(study, piv_df)
                                            rep_df = rep_df.rename(columns= subcate_config['FIELD_NAME_DICT'])
                                            #commenting out since it dropping the end date as the stdat and endat is same
#                                             rep_df=rep_df.T.drop_duplicates(subset=1).T
                                            report_dict[k_dom]= rep_df.to_json(orient= 'records')

                                subcate_report_dict[sub_cat] =  report_dict

                                aespid1 = new_ae_record['AESPID'].values[0]
                                aeterm1 = new_ae_record['AETERM'].values[0]
                                if aeen_dt == np.nan:
                                    aeen_dt = 'blank'

                                ae_record1 = ae_record.iloc[0]
                                total_count += 1

                                if aetoxgr_present == False:
                                    aetoxgr = 'blank'
                                query_text_param = {
                                    'AESPID':aespid,
                                    'AETERM':aeterm,
                                    'AESPID1':aespid1,
                                    'AETERM_1':aeterm1,
                                    'AEDECOD':aedecod,
                                    'AESTDAT':aest_dt,
                                    'AEENDAT':aeen_dt,
                                    'AETOXGR':aetoxgr,
                                    'AEONGO':aeongo
                                }
                                print('*',10,'TRUE')
                                is_ov_duplicate = check_if_duplicate(subjid= int(ae_record1['subjid']), subcat='AEOV1', index=int(ae_record1['form_index']), report=subcate_report_dict)
                                print('*',10,is_ov_duplicate)
                                if is_ov_duplicate == False:
                                    payload = {
                                        "subcategory": sub_cat,
                                        'cdr_skey':str(ae_record1['cdr_skey']),
                                        "query_text": self.get_model_query_text_json(study, sub_cat, params = query_text_param),
                                        "form_index": str(ae_record1['form_index']),
                                        "question_present": self.get_subcategory_json(study, sub_cat),
                                        "modif_dts": str(ae_record1['modif_dts']),  
                                        "stg_ck_event_id": int(ae_record1['ck_event_id']),
                                        "formrefname" : str(ae_record1['formrefname']),
                                        "report" : subcate_report_dict,
                                        "confid_score": np.random.uniform(0.7, 0.97)
                                    }
                                    self.insert_query(study, subject, payload)
                                    # print(subject,payload,total_valid_present)
                                    if payload not in payload_list:
                                        payload_list.append(payload)
                    except Exception as e:
                        print(e)
                        continue

            except Exception as e:
                print(e)
                logging.exception(e)
                continue
            if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                self.close_query(subject, payload_list)
                                                
if __name__ == '__main__':
    study_id = sys.argv[1]
    account_id = sys.argv[2]
    job_id = sys.argv[3]
    rule_id = sys.argv[4]
    version = sys.argv[5]
    rule = AEOV1(study_id, account_id, job_id, rule_id, version)
    rule.run()