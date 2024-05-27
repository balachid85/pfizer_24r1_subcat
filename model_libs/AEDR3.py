'''
Rule ID: AEDR3
Release Version: R2.10M4.6
Changes: 
06-04-22 - Firing preds for all the ECTRT ASSOCIATED
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
subcate_config = yaml.safe_load(open(subcat_config_path, 'r'))
class AEDR3(BaseSDQApi):
    domain_list = ['AE', 'EC']
    def execute(self):
        study = self.study_id
        
        sub_cat = 'AEDR3'
        #index_list = self.get_all_form_index_list(study, domain_list=self.domain_list, per_page=10000)
        index_col = 'itemrepn' if sub_cat.startswith('DR') else 'form_index'
        #subjects = ['10021022']
        subjects =self.get_subjects(study, domain_list = self.domain_list, per_page = 10000) #[10651002,11031001,10051005][30000006]#
        # [10081001]#
        for subject in tqdm.tqdm(subjects):
            payload_list = []
            try:
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list = self.domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)

                full_ae_df = pd.DataFrame(flatten_data['AE'])
                
                if 'EC' in flatten_data:
                    ec_df = pd.DataFrame(flatten_data['EC'])
                    if len(ec_df)>0:
                        ec_df = pd.DataFrame(flatten_data['EC'])
                        ec_df['ECSTDAT'] = ec_df['ECSTDAT'].apply(utils.get_date)
                     # making the ecendat as non-mandatory
#                     ec_df['ECENDAT'] = ec_df['ECENDAT'].apply(utils.get_date)
                        ec_flag = True
                    else:
                        ec_flag = False
                        
                else:
                    ec_flag = False
                    
                values = ['DOSE REDUCED', 'DRUG INTERRUPTED']
                aeacn_cols = [col for col in full_ae_df.columns.tolist() if col.startswith('AEACN')]
#                 aeacn_list=[]
                ae_df = pd.DataFrame()
                for aeacn in aeacn_cols:
                    piv_ae_df = full_ae_df[full_ae_df[aeacn].isin(values)]
                    ae_df = ae_df.append(piv_ae_df, ignore_index=True)
                    ae_df=ae_df.drop_duplicates()
                  
                for ind in range(ae_df.shape[0]):
                    try:

                        ae_record = ae_df.iloc[[ind]]
                        aetrt_dict = self.get_drugs_dict(study)
                        payload_flag = False
                        if ec_flag == True:

                            new_ec = ec_df.copy()
                              
                            new_ec = new_ec.sort_values(['ECSTDAT']) # making the ecendat as non-mandatory #, 'ECENDAT'])

                            if study.upper() in ['B7451015', 'C1061011']:
                                new_ec = new_ec[new_ec['ECTRT'] == 'BLINDED THERAPY']

#                             aetrt_dict = self.get_drugs_dict(study)
                            
                            ec_dict = utils.get_ec_hierarchy(ae_record, new_ec, values, sub_cat, study, aetrt_dict)
                            
                            if bool(ec_dict) == False:
                                continue
                            temp_lst = ['Adverse Events:', 'ADVERSE EVENT(S)', 'ADVERSE EVENT']
                            payload_drg_list=[]
                            
                            for i, (drug, records) in enumerate(ec_dict.items()):
                                print(subject)
                                new_ec = records
                                
                                new_ec_1 = pd.DataFrame()
                                ae_ec_records = utils.extractor(ae_record, ae_df, new_ec, 'AESPID', 'ECAENO')
                                if (type(ae_ec_records) == tuple) & (type(ae_ec_records) != bool):
                                    if len(ae_ec_records[0]) > 0:  
                                        for ae_formindex, ec_records in ae_ec_records[0].items():
                                                for ec_record in ec_records:
                                                    new_ec_1 = new_ec_1.append(ec_record, ignore_index = True)
                                    else:
                                        payload_flag = True
                                        payload_drg_list.append(drug)
                                        
                                        
                                else:
                                    continue
                                new_ec = new_ec_1.copy()
                                prev_ec_len = len(new_ec)
                                if prev_ec_len > 0:
                                    if 'ECADJ' in new_ec.columns.tolist() and 'MISDOST' in new_ec.columns.tolist():
                                        new_ec = new_ec[~((new_ec['ECADJ'].isin(temp_lst)) | (new_ec['MISDOST'].isin(temp_lst)))]
                                    elif 'ECADJ' not in new_ec.columns.tolist():
                                        new_ec = new_ec[~(new_ec['MISDOST'].isin(temp_lst))]
                                    elif 'MISDOST' not in new_ec.columns.tolist():
                                        new_ec = new_ec[~(new_ec['ECADJ'].isin(temp_lst))]  

                                    if prev_ec_len == len(new_ec):
                                        payload_flag = True
                                        payload_drg_list.append(drug)

                                else:
                                    payload_flag = True
                                    payload_drg_list.append(drug)
                        else:
                            payload_flag = True
                            payload_drg_list=list(aetrt_dict.values())

                        if payload_flag:
                            
                            for payload_drug in list(set(payload_drg_list)):
                                subcate_report_dict = {}
                                report_dict = {}
                                ae_record['AESTDAT'] = ae_record['AESTDAT'].apply(utils.format_datetime)
                                ae_record['AEENDAT'] = ae_record['AEENDAT'].apply(utils.format_datetime)
                                ae_record = ae_record.replace({np.nan : 'blank'})
                                piv_rec = {'AE' : ae_record}
                                for dom, cols in subcate_config['FIELDS_FOR_UI'][sub_cat].items():
                                    piv_df = piv_rec[dom]
                                    present_col = [col for col in cols if col in piv_df.columns.tolist()]
                                    rep_df = piv_df[present_col]
                                    rep_df['deeplink'] = utils.get_deeplink(study, piv_df)
                                    rep_df = rep_df.rename(columns= subcate_config['FIELD_NAME_DICT'])
                                    rep_df = rep_df.loc[:,~rep_df.columns.duplicated()]
                                    report_dict[dom]= rep_df.to_json(orient= 'records')

                                subcate_report_dict[sub_cat] =  report_dict
                                
                                aespid = ae_record['AESPID'].values[0]
                                aeterm = ae_record['AETERM'].values[0]
                                aeacn_item = utils.get_drug_item(payload_drug, sub_cat, study, aetrt_dict)
                                if ec_flag == True:
                                    aeacn_item = utils.get_drug_item(payload_drug, sub_cat, study, aetrt_dict)
                                else:
                                    aeacn_item = utils.get_drug_item(payload_drug, sub_cat, study, aetrt_dict)
                                    
#                                     print(aeacn_item,payload_drug)
                                    if ae_record[aeacn_item].values[0] not in values:
                                        continue   
                                    elif ae_record[aeacn_item].values[0]  in values:
                                        aeacn_item = aeacn_item
#                                         break

                                aeacn = ae_record[aeacn_item].values[0]
                                ae_record1 = ae_record.iloc[0]
                                query_text_param = {
                                    "AESPID":aespid,
                                    "AETERM":aeterm,
                                    "AEACN":aeacn,
                                    "ECTRT":payload_drug
                                }
                                print(ae_record1['subjid'],ae_record1['cdr_skey'],"**************")
                                payload = {
                                    "subcategory": sub_cat,
                                    'cdr_skey':str(ae_record1['cdr_skey']),
                                    "query_text": self.get_model_query_text_json(study, sub_cat, params= query_text_param),
                                    "form_index": str(ae_record1['form_index']),
                                    "question_present": self.get_subcategory_json(study, sub_cat),
                                    "modif_dts": str(ae_record1['modif_dts']),  
                                    "stg_ck_event_id": int(ae_record1['ck_event_id']),
                                    "formrefname" : str(ae_record1['formrefname']),
                                    "report" : subcate_report_dict,
                                    "confid_score": np.random.uniform(0.7, 0.97)
                                }
                                self.insert_query(study, subject, payload)
                                if payload not in payload_list:
                                    payload_list.append(payload)


#                                 print(subject,payload)
#                                 print('***************'+payload_drug+'*****************')

                              


                    except Exception as e:
                        print(traceback.format_exc())
                        #print(e)
                        continue

            except Exception as e:
                print(e)
                logging.exception(e)

            if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                self.close_query(subject, payload_list)

if __name__ == '__main__':
    study_id = sys.argv[1]
    account_id = sys.argv[2]
    job_id = sys.argv[3]
    rule_id = sys.argv[4]
    version = sys.argv[5]
    rule = AEDR3(study_id, account_id, job_id, rule_id, version)
    rule.run()