'''
Rule ID: DROV2
Release Version: R2.10M4.6
Changes: 
06-04-22 - MAKING THE EDCADJ AS NON-MANDATORY
29-04-22- [EDIT #2.0 & #2.1]- Change in Subjectid to subji due to 1) deeplink is not working as expected and 2)ov_function needs subjid
03-04-22- [EDIT #3.0 & #3.1] -Enclosing the conditions within the brackets and adding for loop,changing ec_fields to select only avilable columns 

07-02-2023-[EDIT #4.0,#4.1 -REG. the primary index changes]
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
    from is_ovduplicate import DuplicateCheck
except:
    from is_ovduplicate import DuplicateCheck
    from base import BaseSDQApi
    import dosing_utils_v1 as utils

warnings.filterwarnings('ignore')
import os
import tqdm


class DROV2(BaseSDQApi):
    domain_list = ['EC']
    
    curr_file_path = os.path.realpath(__file__)
    curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
    subcat_config_path = os.path.join(curr_path, 'subcate_config_dosing.yml')

    with open(subcat_config_path, 'r') as stream:
        config = yaml.safe_load(stream)
    
    def execute(self):
        study = self.study_id
        ov_study_id = f'account_{self.account_id}_study_{self.study_id}'
        duplicate=DuplicateCheck(ov_study_id)
        check_if_duplicate=duplicate.check

        sub_cat = 'DROV2'
        subjects = self.get_subjects(study, domain_list = self.domain_list) # [70000028]#[12185001]#
        
        get_date = utils.get_date
        norm = utils.normalise_df
        gen_cols = utils.gen_columns
        gen_cols={key:key.lower() for key,value in gen_cols.items()} 
        ec_cols = utils.ec_columns
        fmt = utils.format_datetime
        basic_fields = self.config['FIELDS_FOR_UI']['BASIC']
        ec_fields = self.config['FIELDS_FOR_UI'][sub_cat]['EC']
        rename_features = self.config['FIELD_NAME_DICT']
        
        for subject in tqdm.tqdm(subjects):
            payload_list = []
        
            try:
                # import pdb; pdb.set_trace()
                flatten_data = self.get_flatten_data(study, subject, domain_list = self.domain_list)
                ec_df = pd.DataFrame(flatten_data['EC'])
                if {'SITENO', 'siteno'}.issubset(ec_df.columns):
                    ec_df.drop('SITENO', axis=1, inplace=True)
                ec_df = norm(ec_df, date_col=['ECSTDAT', 'ECENDAT'])
                ec_df['FORMINDEX'] = ec_df['ITEMREPN']
                #[EDIT #2.0]
#                 ec_df['SUBJECTID'] = ec_df['SUBJID'].astype(float).astype(int)
                ec_df['SITENO'] = ec_df['SITENO'].astype(float).astype(int)
                ec_df['ECSTTIME'] = 'null'
                ec_df['ECENDTIME'] = 'null'
                #[EDIT #4.0]
                if len(ec_df.FORM_INDEX.unique()) > 1:
                    index_col = 'FORM_IX'
                    index_idfier = 'FORM_INDEX'
                    index_flag = True
                    ec_cols['FORM_IX']=ec_cols.pop('ITEMSET_IX') if 'ITEMSET_IX' in ec_cols else ec_cols.pop('FORM_IX')
                else:
                    index_flag = False
                    index_col = 'ITEMSET_IX'
                    index_idfier = 'ITEMREPN'
                ec_df['FORMINDEX'] = ec_df[index_idfier]
                
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

                    ecrecord_flag, ec_records = utils.get_ecrecords(cur_ec, None, ec_df, None)
                    if (ecrecord_flag == False):
                        continue
                    #REMOVING THE MODIF_DTS CHECK SINCE NO NEED OF SAME MODIF_DTS
                    if (all([date for date in [ecst_dat, ecend_dat] if date not in ['', ' ', 'null']])):
                        new_ec_record = ec_records[(ec_records['ECSTDAT'] == ecst_dat) & (ec_records['ECENDAT'] == ecend_dat)]
                        ex_duplicates = new_ec_record[(new_ec_record[index_idfier] != formindex) ]
                        
                    else:
                        continue
                    if (ex_duplicates.shape[0] >= 1):
                        # import pdb;pdb.set_trace()
                        report_dict = dict()
                        subcat_report = dict()
                        #[EDIT 3.1]
                        # Adding the below line to select only avilable columns from the ec_fields
                        basic_fields =[key.lower() for key,value in gen_cols.items()]+['SUBJECTID']
                        base_ec_df = cur_ec.rename(columns=gen_cols)[basic_fields]
                        # basic_fields =[key.lower() for key,value in gen_cols.items()]+['SUBJECTID']
                        ec_fields=[i for i in cur_ec.columns.tolist() if i in ec_fields]
                        ec_fields = ['EC_ITEMSETIDX', 'EC_VISITNAM', 'EC_FORMNAME'] + ec_fields
                        ec_col_df = cur_ec.rename(columns=ec_cols)[ec_fields]
                        ec_col_df['ECSTDAT'] = fmt(ec_col_df['ECSTDAT'].values[0])
                        ec_col_df['ECENDAT'] = fmt(ec_col_df['ECENDAT'].values[0])
                        ec_col_df = ec_col_df.rename(columns=rename_features)
                        ec_col_df['deeplink'] = utils.get_deeplink(study, base_ec_df)
                        report_dict['DR1'] = ec_col_df.to_json(orient='records')
             
                        for i, idx in enumerate(ex_duplicates.index, 2):
                            to_add = ex_duplicates.loc[[idx]]
                            dup_itemsetix = to_add[index_col].iloc[0]
                            dup_itemsetix = to_add[index_col].iloc[0]
                            dup_formindex = to_add[index_idfier].iloc[0]
                            dup_visitnm = to_add['VISIT_NM'].iloc[0]
                            base_to_add = to_add.rename(columns=gen_cols)[basic_fields]
                            ec_to_add = to_add.rename(columns=ec_cols)[ec_fields]
                            ec_to_add['ECSTDAT'] = fmt(ec_to_add['ECSTDAT'].values[0])
                            ec_to_add['ECENDAT'] = fmt(ec_to_add['ECENDAT'].values[0])
                            ec_to_add = ec_to_add.rename(columns=rename_features)
                            ec_to_add['deeplink'] = utils.get_deeplink(study, base_to_add)
                            report_dict['DR'+str(i)] = ec_to_add.to_json(orient='records')
                            #[EDIT 3.1]
                            #commenting break since it cuts off predictions for only 2 records other dupls are neglated
                            break
                        subcat_report[sub_cat] = report_dict
                        
                        param_dict = {'EC_ITEMSETIDX': ec_itemsetix, 
                                        'EC_ITEMSETIDX1': dup_itemsetix,
                                        'VISITNAME': ec_visitnm,
                                        'VISITNAME1': dup_visitnm, 
                                        'ECSTDAT': fmt(ecst_dat),
                                        'ECENDAT': fmt(ecend_dat)}
                        #[EDIT #2.1]
                        #[EDIT #4.1]
                        duplicate_report = subcat_report.copy()
                        if index_flag:
                            duplicate_report['match_col'] = index_idfier
                            
                            
                        is_ov_duplicate = check_if_duplicate(subjid= int(cur_ec['SUBJID']), subcat= 'DROV2', index= int(cur_ec[index_idfier]), report= duplicate_report)
                        print('IS_OV_DUPLICATE -- ',is_ov_duplicate,subject)
                        # is_ov_duplicate = False

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
                            print(subject,payload)
                            self.insert_query(study, subject, payload)
                            if payload not in payload_list:
                                payload_list.append(payload)
     

            except Exception as k:
                logging.error(traceback.format_exc())

                                                
            if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                self.close_query(subject, payload_list)

if __name__ == '__main__':
    study_id = sys.argv[1]
    account_id = sys.argv[2]
    job_id = sys.argv[3]
    rule_id = sys.argv[4]
    version = sys.argv[5]
    rule = DROV2(study_id, account_id, job_id, rule_id, version)
    rule.run()
