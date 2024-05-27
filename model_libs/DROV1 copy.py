'''
Rule ID: DROV1
Release Version: R2.10M4.7
Changes: 
06-04-22 - MAKING THE ECADJ AS NOT COMPULSORY
29-04-22- [EDIT #2.0 & #2.1]- Change in Subjectid to subji due to 1) deeplink is not working as expected and 2)ov_function needs subjid
03-04-22- [EDIT #3.0 & #3.1] -Enclosing the conditions within the brackets and adding for loop,changing ec_fields to select only avilable columns 
02-01-2022 - CHANGING ALL THE NEWRECORD TO APPEND
'''

import re
import sys
import yaml
import logging
import warnings
import traceback
import numpy as np
import pandas as pd
try:
    from scripts.SubcatModels.model_libs.base import BaseSDQApi
    import scripts.SubcatModels.model_libs.dosing_utils_v1 as utils
    from  is_ovduplicate import DuplicateCheck
except:
    from  is_ovduplicate import DuplicateCheck
    from base import BaseSDQApi
    import dosing_utils_v1 as utils

warnings.filterwarnings('ignore')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
import os
from tqdm import tqdm


# from icecream import ic as debug
# debug.configureOutput(prefix='debug | ')


class DROV1(BaseSDQApi):
    domain_list = ['EC']
    
    curr_file_path = os.path.realpath(__file__)
    curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
    subcat_config_path = os.path.join(curr_path, 'subcate_config_dosing.yml')

    with open(subcat_config_path, 'r') as stream:
        config = yaml.safe_load(stream)
    
    def execute(self):
        study = self.study_id
        sub_cat = 'DROV1'
        subjects = self.get_subjects(study, domain_list = self.domain_list) # [88880002]# [13295007]#
        
        get_date = utils.get_date1
        norm = utils.normalise_df1
        gen_cols = utils.gen_columns 
        gen_cols={key:key.lower() for key,value in gen_cols.items()}
        ec_cols = utils.ec_columns
        fmt = utils.format_datetime
        basic_fields = self.config['FIELDS_FOR_UI']['BASIC']
        ec_fields = self.config['FIELDS_FOR_UI'][sub_cat]['EC']
        rename_features = self.config['FIELD_NAME_DICT']
        
        for subject in tqdm(subjects):
            
            try:
                flatten_data = self.get_flatten_data(study, subject, domain_list = self.domain_list)

                ec_df = pd.DataFrame(flatten_data['EC'])
                if {'SITENO', 'siteno'}.issubset(ec_df.columns):
                    ec_df.drop('SITENO', axis=1, inplace=True)
                ec_df = norm(ec_df, date_col=['ECSTDAT', 'ECENDAT'])
                
                if len(ec_df.FORM_INDEX.unique())>1:
                    index_col='FORM_IX'
                    index_idfier='FORM_INDEX'
                    index_flag=True
                else:
                    index_flag=False
                    index_col='ITEMSET_IX'
                    index_idfier='ITEMREPN'
                #[EDIT #2.0]
#                 ec_df['SUBJECTID'] = ec_df['SUBJID'].astype(float).astype(int)
                ec_df['FORMINDEX'] = ec_df[index_idfier]
                ec_df['SITENO'] = ec_df['SITENO'].astype(float).astype(int)
                
                for idx_row in ec_df.index:
                    cur_ec = ec_df.loc[[idx_row]]
                    formindex = cur_ec[index_idfier].iloc[0]
                    ec_itemsetix = cur_ec[index_col].iloc[0]
                    ecst_dat = cur_ec['ECSTDAT'].iloc[0]
                    ecend_dat = cur_ec['ECENDAT'].iloc[0]
                    # removing unused variable ECADJO
                    check_ectrt = cur_ec['ECTRT'].iloc[0]
                    modified_ts = cur_ec['MODIF_DTS'].iloc[0]
                    ec_visitnm = cur_ec['VISIT_NM'].iloc[0]
                    
                    
                    new_ec_record = pd.DataFrame()
                    ecrecord_flag, ec_records = utils.get_ecrecords(cur_ec, None, ec_df, None)
                    
                    if (ecrecord_flag == False):
                        continue
                    # if (all([date for date in [ecst_dat, ecend_dat] if date not in ['', ' ', 'null', pd.NaT, None] and not pd.isna(date)])):
                    if (all([True if (date not in ['', ' ', 'null', pd.NaT, None] and not pd.isna(date)) else False for date in [ecst_dat, ecend_dat]])):
                        
                        new_ec_records = ec_records[~(ec_records['ECSTDAT'].isin(['', ' ', 'null', pd.NaT, None]))]
                        new_ec_records = new_ec_records[~(new_ec_records['ECENDAT'].isin(['', ' ', 'null', pd.NaT, None]))]         
                        new_ec_records['ECSTDAT'] = new_ec_records['ECSTDAT'].apply(get_date)
                        new_ec_records['ECENDAT'] = new_ec_records['ECENDAT'].apply(get_date)
                        new_ec_records = new_ec_records[(new_ec_records[index_idfier] != formindex)]
                        
                        # same ec startdate(pivot) but lesser than ec endate in any of the all_ec_records
                        print('subject -->',subject)
                        print('ecend_dat --> ', ecend_dat)
                        print('ecst_dat --> ', ecst_dat)
                        condition1_df = new_ec_records[(new_ec_records['ECSTDAT'] == ecst_dat) & (new_ec_records['ECENDAT'] < ecend_dat)]
                        # same ec endate(pivot) but greater than ec startdate in any of the all_ec_records
                        condition2_df = new_ec_records[(new_ec_records['ECENDAT'] == ecend_dat) & (new_ec_records['ECSTDAT'] > ecst_dat)]
                        # pivotal ec startdate and endate are different but pivotal ec end date matches with \
                        # any of the ec start date in all_ec_records 
                        condition3_df = new_ec_records[(new_ec_records['ECSTDAT'] == ecend_dat)]
                        # pivotal ec startdate and endate are different but pivotal ec start date matches with \
                        # any of the ec end date in all_ec_records 
                        condition4_df = new_ec_records[(new_ec_records['ECENDAT'] == ecst_dat)]
                        
                        # pivotal ec startdate and endate are different but pivotal ec start date matches with \
                        # any of the ec start date in all_ec_records 
                        condition5_df = new_ec_records[(new_ec_records['ECSTDAT'] == ecst_dat) & (new_ec_records['ECENDAT'] != ecend_dat)]
                        # pivotal ec startdate and endate are different but pivotal ec end date matches with \
                        # any of the ec end date in all_ec_records
                        condition6_df = new_ec_records[(new_ec_records['ECENDAT'] == ecend_dat) & (new_ec_records['ECSTDAT'] != ecst_dat)]
                        # pivotal ec startdate and endate are same and matches with any of the ec startdate in all_ec_records
                        condition7_df = new_ec_records[(new_ec_records['ECSTDAT'] == ecst_dat) & (new_ec_records['ECENDAT'] != ecend_dat)]
                        #pivotal ec startdate and endate are same and matches with any of the ec endate in all_ec_records
                        condition8_df = new_ec_records[(new_ec_records['ECENDAT'] == ecend_dat) & (new_ec_records['ECSTDAT'] != ecst_dat)]
                        # pivotal ec startdate greater than any of the all_ec_records and pivotal ec endate lesser than any of the all_ec_records
                        condition9_df = new_ec_records[(new_ec_records['ECSTDAT'] > ecst_dat) & (new_ec_records['ECSTDAT'] < ecend_dat)]
                        #change in over lapping logic as per varshas comment "if start date is fall between anothers start and enddate"
#                          debug(condition1_df.shape[0], condition2_df.shape[0], condition3_df.shape[0], condition4_df.shape[0], \
#                                 condition5_df.shape[0], condition6_df.shape[0], condition7_df.shape[0], condition8_df.shape[0])
                        # import pdb;pdb.set_trace()
                        # Condition 1
                        #[EDIT #3.0]
                        #enclosing the conditions
                        new_ec_record=pd.DataFrame()
                        if((ecst_dat != ecend_dat) & (condition1_df.shape[0] >=1) ):
                            new_ec_record=new_ec_record.append(condition1_df)

                        # Condition 2
                        if ((ecst_dat != ecend_dat) & (condition2_df.shape[0] >= 1)):
                            new_ec_record=new_ec_record.append(condition2_df) 

                        # Condition 3
                        if ((ecst_dat != ecend_dat) & (condition3_df.shape[0] >= 1)):
                            new_ec_record=new_ec_record.append(condition3_df) 

                        # Condition 4
                        if ((ecst_dat != ecend_dat)&(condition4_df.shape[0] >= 1)):
                            new_ec_record=new_ec_record.append(condition4_df) 

                        # Condition 5
                        if ((ecst_dat != ecend_dat) & (condition5_df.shape[0] >= 1)):
                            new_ec_record=new_ec_record.append(condition5_df)

                        # Condition 6    
                        if ((ecst_dat != ecend_dat) & (condition6_df.shape[0] >= 1)):
                            new_ec_record=new_ec_record.append(condition6_df) 

                        # Condition 7
                        if ((ecst_dat == ecend_dat) & (condition7_df.shape[0] >= 1)):
                            new_ec_record=new_ec_record.append(condition7_df) 


                        # Condition 8
                        if ((ecst_dat == ecend_dat) & (condition8_df.shape[0] >= 1)):  
                            new_ec_record=new_ec_record.append(condition8_df) 


                        # Condition 9
                        if ((ecst_dat != ecend_dat) & (condition9_df.shape[0] >= 1)):
                            new_ec_record=new_ec_record.append(condition9_df)
                
                    elif(ecst_dat not in ['', ' ', 'null', pd.NaT, None] and ecend_dat in ['', ' ', 'null', pd.NaT, None]):
                        new_ec_record = ec_records[(ec_records['ECSTDAT'].notna())]
                        new_ec_record['ECSTDAT'] = new_ec_record['ECSTDAT'].apply(get_date)
                        new_ec_record = new_ec_record[(new_ec_record['ECSTDAT'] != ecst_dat)]
                        new_ec_record = new_ec_record[(new_ec_record['ECSTDAT'] > ecst_dat)]
                
                    
                    if (new_ec_record.shape[0] >= 1):
                        #adding For since firing preds for all the duplicates
                        
                        new_ec_record = new_ec_record[(new_ec_record[index_idfier] != formindex)]
                        new_ec_record.drop_duplicates(inplace=True)
                        for ind in range(new_ec_record.shape[0]):
                            total_valid_pred = 0
                            ex_duplicates = new_ec_record.iloc[[ind]]
                            report_dict = dict()
                            subcat_report = dict()
                            # gen_cols=[key for key,value in gen_cols.items()]
                            basic_fields=[key.lower() for key,value in gen_cols.items()]+['SUBJECTID']
                            print(basic_fields)
                            base_ec_df = cur_ec.rename(columns=gen_cols)[basic_fields]
                            
                            #[EDIT 3.1]
                            # Adding the below line to select only avilable columns from the ec_fields
                            ec_fields=[i for i in cur_ec.columns.tolist() if i in ec_fields]
                            ec_fields = ['EC_ITEMSETIDX', 'EC_VISITNAM', 'EC_FORMNAME'] + ec_fields
            
                            ec_col_df = cur_ec.rename(columns=ec_cols)[ec_fields]
                            ec_col_df['ECSTDAT'] = fmt(ec_col_df['ECSTDAT'].values[0])
                            ec_col_df['ECENDAT'] = fmt(ec_col_df['ECENDAT'].values[0])
                            ec_col_df = ec_col_df.rename(columns=rename_features)
                            ec_col_df['deeplink'] = utils.get_deeplink(study, base_ec_df)
                            report_dict['DR1'] = ec_col_df.to_json(orient='records')
                            # Taking only one index
                            for i, idx in enumerate(ex_duplicates.index):
                                to_add = ex_duplicates.loc[[idx]]
                                dup_itemsetix = to_add[index_col].iloc[0]
                                dup_visitnm = to_add['VISIT_NM'].iloc[0]
                                dup_ecstdat = to_add['ECSTDAT'].iloc[0]
                                dup_ecendat = to_add['ECENDAT'].iloc[0]
                                dup_formindex = to_add[index_idfier].iloc[0]
                                base_to_add = to_add.rename(columns=gen_cols)[basic_fields]
                                ec_to_add = to_add.rename(columns=ec_cols)[ec_fields]
                                ec_to_add['ECSTDAT'] = fmt(ec_to_add['ECSTDAT'].values[0])
                                ec_to_add['ECENDAT'] = fmt(ec_to_add['ECENDAT'].values[0])
                                ec_to_add['deeplink'] = utils.get_deeplink(study, base_to_add)
                                ec_to_add = ec_to_add.rename(columns=rename_features)
                                report_dict['DR2'] = ec_to_add.to_json(orient='records')
                                total_valid_pred += 1
                                break
                            subcat_report[sub_cat] = report_dict
                            duplicate_report=subcat_report.copy()
                            param_dict = {'EC_ITEMSETIDX': ec_itemsetix, 'ECSTDAT': fmt(ecst_dat), 'ECENDAT': fmt(ecend_dat), \
                                'EC_ITEMSETIDX1':dup_itemsetix, 'ECSTDAT1': fmt(dup_ecstdat), 'ECENDAT1': fmt(dup_ecendat), \
                                    'VISITNAME':ec_visitnm, 'VISITNAME1': dup_visitnm}
                            #[EDIT #2.1]
                            # print('True')
                            if index_flag:
                                duplicate_report['match_col']=index_idfier
                            # is_ov_duplicate = self.check_if_duplicate(study=study, subjid= int(cur_ec['SUBJID']), subcat= 'DROV1', index= int(cur_ec[index_idfier]), report= duplicate_report)
                            if(total_valid_pred == 1):
                                dup_flg = False
                                hash2 = final_dr_df[index_idfier].values[0]
                                if(len(dup_list)>0):
                                    if ((hash1, hash2) in dup_list) or ((hash2, hash1) in dup_list):
                                        dup_flg = True                                    
                                        continue
                                if(not dup_flg):
                                    dup_list.append((hash1, hash2))
                                    is_ov_duplicate=True


                            else:    
                                temp_mh = pd.DataFrame()
                                piv_rec = {'DR1' : mh_record.head(1)}
                                report_dict = {'DR2':dict()}
                                dup_rec_counter = None
                                count = 1
                                    
                                for mh_ind in final_dr_df.index.tolist():
                                    dup_flg = False
                                    temp_rec = final_dr_df.loc[[mh_ind]]
                                    hash2 = temp_rec[index_idfier].values[0]
                                    if(len(dup_list)>0):
                                        if ((hash1, hash2) in dup_list) or ((hash2, hash1) in dup_list):
                                            dup_flg = True                                    
                                            continue
                                    if(not dup_flg):
                                        dup_list.append((hash1,hash2))
                                        is_ov_duplicate=True
                                if(len(piv_rec) == 1):
                                    continue 
                                curr_mh_record = temp_mh.head(1)
                            curr_mh_record = curr_mh_record.fillna('blank')
                            
                            
                            is_ov_duplicate = False
                            if is_ov_duplicate == False:
                                payload = {
                                            "subcategory": sub_cat,
                                            'cdr_skey':str(cur_ec['CDR_SKEY'].values[0]),
                                            "query_text": self.get_model_query_text_json(study, sub_cat, params=param_dict), 
                                            "form_index": int(cur_ec['FORM_INDEX'].values[0]),
                                            "visit_nm": cur_ec['VISIT_NM'].values[0],
                                            "question_present": self.get_subcategory_json(study, sub_cat),
                                            "modif_dts": fmt(cur_ec['MODIF_DTS'].values[0]),
                                            "stg_ck_event_id": int(cur_ec['CK_EVENT_ID'].values[0]),
                                            "report" : subcat_report,
                                            "formrefname": cur_ec['FORMREFNAME'].values[0],
                                            "confid_score": np.random.uniform(0.7, 0.97),
                                            }
                                print(payload)
                                self.insert_query(study, subject, payload)

            except Exception as k:
                print(k)
                logging.info(traceback.format_exc())


                                                
if __name__ == '__main__':
    study_id = sys.argv[1]
    account_id = sys.argv[2]
    job_id = sys.argv[3]
    rule_id = sys.argv[4]
    version = sys.argv[5]
    rule = DROV1(study_id, account_id, job_id, rule_id, version)
    rule.run()
