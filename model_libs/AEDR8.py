'''
Rule ID: AEDR8
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
import os
import yaml
import numpy as np

curr_file_path = os.path.realpath(__file__)
curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
subcat_config_path = os.path.join(curr_path, 'subcate_config.yml')
a = yaml.safe_load(open(subcat_config_path, 'r'))
class AEDR8(BaseSDQApi):
    domain_list = ['AE', 'EC']
    def execute(self):
        study = self.study_id
        
        domain_list = ['AE', 'EC']
        sub_cat = 'AEDR8'
        #index_list = self.get_all_form_index_list(study, domain_list=self.domain_list, per_page=10000)
        index_col = 'itemrepn' if sub_cat.startswith('DR') else 'form_index'
        subjects = self.get_subjects(study, domain_list = domain_list, per_page = 10000)# #[12121235]#[10193003] #
        for subject in tqdm.tqdm(subjects):
            payload_list = []
            try:
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list = domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)

                ae_df = pd.DataFrame(flatten_data['AE'])
                ec_df = pd.DataFrame(flatten_data['EC'])
                #ae_df = ae_df[ae_df[index_col].isin(index_list)]
                ec_df['ECSTDAT'] = ec_df['ECSTDAT'].apply(utils.get_date)
                ec_df['ECENDAT'] = ec_df['ECENDAT'].apply(utils.get_date) 
                
                for ind in range(ae_df.shape[0]):
                    try:
                        ae_record = ae_df.iloc[[ind]]

                        ecadj_flag = False
                        misdost_flag = False
                        
                        curr_ec_len = -1
                        flag = -1

                        ae_cols = ae_record.columns.tolist()
                        ec_cols = ec_df.columns.tolist()
                        
                        new_ec = pd.DataFrame()

                        ae_ec_records = utils.extractor(ae_record, ae_df, ec_df, 'AESPID', 'ECAENO')
                        if (type(ae_ec_records) == tuple) & (type(ae_ec_records) != bool): 
                            if len(ae_ec_records[0]) > 0:  
                                for ae_formindex, ec_records in ae_ec_records[0].items():
                                        for ec_record in ec_records:
                                            new_ec = new_ec.append(ec_record, ignore_index = True)
                            else:
                                continue
                        else:
                            continue

                        new_ec = new_ec.copy()
                        ecadj_flag, misdost_flag = utils.get_aereason_col(ae_record, ae_df, new_ec)
                        dose_column = utils.get_dose_column(new_ec)
#                         dose_column = new_ec.apply(utils.get_dose_column_v1, ec_cols = new_ec.columns.tolist(), axis=1)
                        
                        aest_dat = ae_record['AESTDAT'].apply(utils.get_date)
                        aeen_dat =  ae_record['AEENDAT'].apply(utils.get_date)
       
                        new_ec = new_ec.sort_values(['ECSTDAT', 'ECENDAT'])
                        
                        values = ['DRUG INTERRUPTED']
                        aetrt_dict = self.get_drugs_dict(study)
                        ec_dict = utils.get_ec_hierarchy(ae_record, new_ec, values, sub_cat, study, aetrt_dict)
                        temp_lst = ['Adverse Events:', 'ADVERSE EVENT(S)', 'ADVERSE EVENT'] 

                        if bool(ec_dict) == False:
                            continue
                        
                        #import pdb;pdb.set_trace()
                        for i, (drug, records) in enumerate(ec_dict.items()):
                            new_ec = records 
                            
                            new_ec = new_ec[(new_ec['ECSTDAT'] >= aest_dat.values[0]) & (new_ec['ECSTDAT'] < aeen_dat.values[0])]

#                             temp_lst = ['Adverse Events:', 'ADVERSE EVENT(S)', 'ADVERSE EVENT'] 
                            
                            if 'ECADJ' in new_ec.columns.tolist() and 'MISDOST' in new_ec.columns.tolist():
                                new_ec = new_ec[(new_ec['ECADJ'].isin(temp_lst)) | (new_ec['MISDOST'].isin(temp_lst))]
                            elif 'ECADJ' not in new_ec.columns.tolist():
                                new_ec = new_ec[new_ec['MISDOST'].isin(temp_lst)]
                            elif 'MISDOST' not in new_ec.columns.tolist():
                                new_ec = new_ec[new_ec['ECADJ'].isin(temp_lst)]
                                
#                             new_ec = new_ec[new_ec[dose_column].astype(float, errors='ignore') != 0]    
                            dose_values = new_ec[dose_column].values.tolist()
                            new_dose_values = []
                            for d in dose_values:
                                try:
                                    new_dose_values.append(float(d))
                                except:
                                    new_dose_values.append(d)
                            dose_values = new_dose_values
                            if 0 not in dose_values and len(dose_values) > 0:
                                flag = 1

                            if flag == 1:
                                subcate_report_dict = {}
                                report_dict = {}
                                new_ec['ECSTDAT'] = new_ec['ECSTDAT'].dt.strftime("%d-%b-%Y")
                                new_ec = new_ec.head(1)
                                ecendat = new_ec['ECENDAT'].values[0]
                                if isinstance(ecendat, float) == False:
                                    new_ec['ECENDAT'] = new_ec['ECENDAT'].dt.strftime("%d-%b-%Y")
                                ae_record = ae_record.replace({np.nan : 'blank'})
                                new_ec = new_ec.replace({np.nan : 'blank'})
                                piv_rec = {'AE' : ae_record, 'EC' : new_ec}
                                
                                for dom, cols in a['FIELDS_FOR_UI'][sub_cat].items():
                                    piv_df = piv_rec[dom]
                                    present_col = [col for col in cols if col in piv_df.columns.tolist()]
                                    rep_df = piv_df[present_col]
                                    rep_df['deeplink'] = utils.get_deeplink(study, piv_df)
                                    rep_df = rep_df.rename(columns= a['FIELD_NAME_DICT'])
                                    rep_df = rep_df.loc[:,~rep_df.columns.duplicated()]
                                    report_dict[dom]= rep_df.to_json(orient= 'records')
                                    
                                subcate_report_dict[sub_cat] =  report_dict
                                #COMMENTING OUT SINCE THE KEYS ARE REMOVED IN NEW QT
#                                 aespid = ae_record['AESPID'].values[0]
#                                 aeterm = ae_record['AETERM'].values[0]
#                                 aeacn_item = utils.get_drug_item(drug, sub_cat, study, aetrt_dict)
#                                 aeacn = ae_record[aeacn_item].values[0]
                                

                                ae_record1 = ae_record.iloc[0]
                                
#                                 keys = {
#                                     'AESPID' : aespid,
#                                     'AETERM' : aeterm,
#                                     'AEACN' : aeacn
#                                 }

                                payload = {
                                    "subcategory": sub_cat,
                                    'cdr_skey':str(ae_record1['cdr_skey']),
                                    "query_text": self.get_model_query_text_json(study, sub_cat),
                                    "form_index": ae_record1['form_index'],
                                    "question_present": self.get_subcategory_json(study, sub_cat),
                                    "modif_dts": str(ae_record1['modif_dts']),  
                                    "stg_ck_event_id": int(ae_record1['ck_event_id']),
                                    "formrefname" : str(ae_record1['formrefname']),
                                    "report" : subcate_report_dict
                                }
                                self.insert_query(study, subject, payload)
                                # print(subject,payload)
                                if payload not in payload_list:
                                    payload_list.append(payload)
                            
                    except:
                        print(traceback.format_exc())
                        continue       

            except Exception as e:
                print(traceback.format_exc())
                logging.exception(e)
                                                
            if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                self.close_query(subject, payload_list)

if __name__ == '__main__':
    study_id = sys.argv[1]
    account_id = sys.argv[2]
    job_id = sys.argv[3]
    rule_id = sys.argv[4]
    version = sys.argv[5]
    rule = AEDR8(study_id, account_id, job_id, rule_id, version)
    rule.run()
