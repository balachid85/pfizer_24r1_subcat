'''
Rule ID: CMOV1
Release Version: R2.10M4.6,[R3.6M5.3]
Changes: 
06-04-22 -date format(fill 'null' in 'nan' for dates)
[EDIT#2]-21-12-22 - EXCLUDING CMTERM LOGIC
[EDIT#3]-14-02-23 - ADDING CMDSTXT FOR FILED DOSING STUDIES 
           
'''
import pandas as pd
import sys
import re
import warnings
warnings.filterwarnings('ignore')
import traceback
import tqdm
import logging
import yaml
import numpy as np
import os
import datetime

try:
    from scripts.SubcatModels.model_libs.base import BaseSDQApi
    import scripts.SubcatModels.model_libs.utils as utils
    from  is_ovduplicate import DuplicateCheck
except:
    from  is_ovduplicate import DuplicateCheck
    from base import BaseSDQApi
    import utils as utils

curr_file_path = os.path.realpath(__file__)
curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
subcat_config_path = os.path.join(curr_path, 'subcate_config.yml')
subcate_config = yaml.safe_load(open(subcat_config_path, 'r'))


class CMOV1(BaseSDQApi):
    domain_list = ['CM']
    def execute(self):
        study = self.study_id
        domain_list = ['CM']
        sub_cat = 'CMOV1'
        subjects = self.get_subjects(study, domain_list = self.domain_list, per_page = 10000) #[16335003]#
        index_col = 'itemrepn' if sub_cat.startswith('DR') else 'form_index'
        #lambda function for changing the txt to integer value
        dose_extract=lambda x: ''.join(re.findall('[0-9]+',x))

        for subject in tqdm.tqdm(subjects):
            payload_list = []
            try:
                ov_study_id = f'account_{self.account_id}_study_{self.study_id}'
                duplicate=DuplicateCheck(ov_study_id)
                check_if_duplicate=duplicate.check
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list = self.domain_list)
                
                cm_df = pd.DataFrame(flatten_data['CM'])
                #ADDING CMDSTXT FOR TXT FIELD
                if 'CMDSTXT' in cm_df.columns.tolist():
                    cm_df['CMDSTXT']=cm_df['CMDSTXT'].astype('str')
                    cm_df['CMDSTXT_INT']=cm_df['CMDSTXT'].apply(dose_extract)
                    cm_df['CMDSTXT_INT']=cm_df['CMDSTXT_INT'].astype('int64',errors='ignore')
                    
                for ind in range(cm_df.shape[0]):
                    try:
                        
                        cm_record = cm_df.iloc[[ind]]
                        total_valid_present = 0
                        cmtrt = cm_record['CMTRT'].values[0]
                        cmdecod = cm_record['CMDECOD'].values[0]
                        cmstdt = cm_record['CMSTDAT'].values.tolist()[0]
                        if 'UNK' in str(cmstdt):
                            cmstdt = utils.remove_unk(cmstdt, combine = True)
                        cm_endt = cm_record['CMENDAT'].values[0]
                        cmongo = cm_record['CMONGO'].values.tolist()[0]
                        formidx = cm_record['form_ix'].values[0]
                        subjectid = cm_record['subjid'].values[0]
                        
                        cm_records = cm_df
                        
                        if cmongo.upper() == 'YES':
                            if type(cmstdt) == float:
                                cm_records = cm_records[cm_records['CMSTDAT'].notna()]
                                cm_records['CMSTDAT'] = pd.to_datetime(cm_records['CMSTDAT_DTR'].apply(utils.remove_unk, combine=True))
                                date_list = cm_record['CMSTDAT'].apply(utils.remove_unk).values[0]
                                if len(date_list) == 1: 
                                    new_cm_record = cm_records[(cm_records['CMSTDAT'].dt.year==int(date_list[0])) & (cm_records['CMONGO'] == cmongo)]
                                    cmstdt = cm_record['CMSTDAT_DTR'].values.tolist()[0]
                                elif len(date_list) == 2:
                                    new_cm_record = cm_records[(cm_records['CMSTDAT'].dt.year==int(date_list[1])) & (cm_records['CMSTDAT'].dt.month == int(date_list[0]))
                                                    & (cm_records['CMONGO'] == cmongo)]
                                    cmstdt = cm_record['CMSTDAT_DTR'].values.tolist()[0]
                            else:
                                cm_records = cm_records[cm_records['CMSTDAT'].notna()]
                                cm_records['CMSTDAT'] = pd.to_datetime(cm_records['CMSTDAT'])        
                                new_cm_record = cm_records[(cm_records['CMSTDAT'] == cmstdt) & (cm_records['CMONGO'] == cmongo)]
                                new_cm_record['CMSTDAT'] = new_cm_record['CMSTDAT'].dt.strftime("%d-%b-%Y")
                                cm_record['CMENDAT'] = cm_record['CMENDAT'].replace({np.nan : 'blank'})
                                new_cm_record['CMENDAT'] = new_cm_record['CMENDAT'].replace({np.nan : 'blank'})
                                
                        elif cmongo.upper() == 'NO' or type(cmongo) == float:
                            if type(cmstdt) == float:
                                cm_records = cm_records[cm_records['CMSTDAT'].notna()]
                                cm_records['CMSTDAT'] = pd.to_datetime(cm_records['CMSTDAT_DTR'].apply(utils.remove_unk, combine=True))
                                date_list = cm_record['CMSTDAT'].apply(utils.remove_unk).values[0]
                                if len(date_list) == 1: 
                                    new_cm_record = cm_records[(cm_records['CMSTDAT'].dt.year==int(date_list[0])) & (cm_records['CMENDAT'] == cm_endt)]
                                    cmstdt = cm_record['CMSTDAT_DTR'].values.tolist()[0]
                                elif len(date_list) == 2:
                                    new_cm_record = cm_records[(cm_records['CMSTDAT'].dt.year==int(date_list[1])) & (cm_records['CMSTDAT'].dt.month == int(date_list[0]))
                                                    & (cm_records['CMENDAT'] == cm_endt)]
                                    cmstdt = cm_record['CMSTDAT_DTR'].values.tolist()[0]
                                    
                            else:
                                new_cm_record = cm_records[(cm_records['CMSTDAT'] == cmstdt) & (cm_records['CMENDAT'] == cm_endt)]
                        
                        check = ['CMDOSTOT', 'CMDOSFRQ','CMROUTE','CMDSTXT_INT']
                        for c in check:
                            if c in new_cm_record.columns.tolist():
                                cm_record[c] = cm_record[c].astype(str)
                                new_cm_record[c] = new_cm_record[c].astype(str)
                                value = cm_record[c].values[0]
                                new_cm_record = new_cm_record[new_cm_record[c] == value]

                                if value in [np.nan, 'nan', 'null', 'NULL', '', ' ']:
                                    new_cm_record[c] = new_cm_record[c].replace({value : 'blank'})
                                    cm_record[c] = new_cm_record[c].replace({value : 'blank'})
                        
                        new_cm_record = new_cm_record[new_cm_record['form_ix'] != formidx]
                        
                        if len(new_cm_record) == 0:
                            continue
                        
                        elif len(new_cm_record) > 0:
                            for ind in new_cm_record.index.tolist():
                                curr_cmtrt = new_cm_record.loc[ind, 'CMTRT']
                                curr_cmdecod = new_cm_record.loc[ind, 'CMDECOD']
                                # cmtrt_flag = utils.check_similar_term_fuzzy(cmtrt, curr_cmtrt)
                                #[EDIT#2] COMMENTING OUT CMTERM LOGIC
                                cmdecod_flag = utils.check_similar_medication_verabtim2(cmdecod, curr_cmdecod)
                                #print(cm_record[['CMSTDAT', 'CMENDAT', 'CMTRT', 'CMDECOD', 'CMDOSTOT', 'CMDOSFRQ','CMROUTE', 'CMDOSU']])
                                #print(new_cm_record[['CMSTDAT', 'CMENDAT', 'CMTRT', 'CMDECOD', 'CMDOSTOT', 'CMDOSFRQ','CMROUTE', 'CMDOSU']])
                                #print(cmtrt_flag, cmdecod_flag)
                                # if cmtrt_flag and cmdecod_flag:
                                # [EDIT#2] commenting since no longer need in logic
                                if cmdecod_flag:
                                    
                                    ind1= ind
                                    total_valid_present+= 1
                                    break
                            if len(new_cm_record) > 0 and total_valid_present >= 1:
                                #print(True)
                                new_cm_record = new_cm_record.loc[[ind1]]
                                subcate_report_dict = {}
                                report_dict = {}

                                cm_record = cm_record.fillna('blank')
                                new_cm_record = new_cm_record.fillna('blank')

                                fmt = utils.format_datetime

                                cm_record['CMSTDAT'] = cm_record['CMSTDAT'].apply(fmt)
                                cm_record['CMENDAT'] = cm_record['CMENDAT'].apply(fmt)

                                new_cm_record['CMSTDAT'] = new_cm_record['CMSTDAT'].apply(fmt)
                                new_cm_record['CMENDAT'] = new_cm_record['CMENDAT'].apply(fmt)

                                new_cm_record['CMSTDAT'] = new_cm_record['CMSTDAT'].astype(str)
                                new_cm_record['CMENDAT'] = new_cm_record['CMENDAT'].astype(str)

                                piv_rec = {'CM1': cm_record.head(1),'CM2' : new_cm_record.head(1)}

                                for dom, cols in subcate_config['FIELDS_FOR_UI'][sub_cat].items():

                                    if len(cols) > 0:
                                        for k_dom in piv_rec.keys():
                                            piv_df = piv_rec[k_dom]
                                            present_col = [col for col in cols if col in piv_df.columns.tolist()]
                                            rep_df = piv_df[present_col]
                                            rep_df['deeplink'] = utils.get_deeplink(study, piv_df)
                                            rep_df = rep_df.rename(columns= subcate_config['FIELD_NAME_DICT'])
                                            rep_df.drop_duplicates(keep='first', inplace = True)
                                            report_dict[k_dom]= rep_df.to_json(orient= 'records')

                                subcate_report_dict[sub_cat] =  report_dict

                                formidx1 = new_cm_record['form_ix'].values[0]
                                cmtrt1 = new_cm_record['CMTRT'].values[0]

                                cm_record1 = cm_record.iloc[0]

                                # total_count += 1
                                if cm_endt in [np.nan, 'nan', 'null', 'NULL', '', ' ']:
                                    cm_endt = 'blank'
                                query_text_param = {
                                    'CM_FORMIDX':formidx,
                                    'CMTRT':cmtrt,
                                    'CM_FORMIDX1':formidx1,
                                    'CMTRT1':cmtrt1,
                                    'CMDECOD':cmdecod,
                                    'CMSTDAT':cmstdt,
                                    'CMENDAT':cm_endt,
                                }
                                
                                is_ov_duplicate = check_if_duplicate(subjid= int(cm_record1['subjid']), subcat= 'CMOV1', index= int(cm_record1['form_index']), report= subcate_report_dict)
                                # is_ov_duplicate =False
                                if is_ov_duplicate == False:
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
                                    # print(subject,payload)
                                    self.insert_query(study, subject, payload)
                                    if payload not in payload_list:
                                        payload_list.append(payload)

                                    
                    except Exception as exp:
                        print(exp)
                        print(traceback.format_exc())

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
    rule = CMOV1(study_id, account_id, job_id, rule_id, version)
    rule.run()
