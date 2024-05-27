import json
import logging
import os
import traceback

import pandas as pd
from sqlalchemy import create_engine, text

Db_url = os.environ.get('DATABASE_URI')
'''
Latest Release Version: R2.10M4.6,[R3.5M5.3]
Description: DuplicateCheck prevents exact duplicate predictions for OV subcats from getting inserted into dm_pred
Developer: Adheeban Manoharan
Change log: 
[EDIT #1] - [29Apr22] - Replacing EC with DR because we have the dosing subcats names starting with DR and not EC. Eg: DROV1, DROV2. 
[EDIT #2] - [19JAN23] - Replacing text method with fstring since its not finding the correct duplicates, including for loop to check all the similar preds for DROV1
                        note: if need to overide - match_cols pass it through the subcat report for example look at drov1
'''
class DuplicateCheck:
    DB_URL = Db_url

    def __init__(self,study):
        vol_eng = create_engine(self.DB_URL)
        self.study = study
        self.conn = vol_eng.connect()
    def check(self, subjid, subcat, index, report):
        is_duplicate = False
        try:
            domain = subcat[:2].upper()
            match_col = 'form_index' if domain in ['AE', 'CM'] else 'itemrepn'
            
            get_data_query = text(f'SELECT * from {self.study.lower()}.dm_pred where subjid= :sub and  {match_col} = :index and infer_subcat = :subcat')
            get_subcat_query = text(f'SELECT * from {self.study.lower()}.dm_pred_subcat_features where ck_event_id = :ck_event_id')
            domain_identifier_dict = {'AE': 'deeplink', 'CM': 'deeplink', 'DR': 'INTERNAL ID', 'LB': 'INTERNAL ID'}
            identifier_name = domain_identifier_dict[domain]
            #report = json.loads(report)
            # print(report)
            # [EDIT #2]
            if 'match_col' in report.keys():
                match_col=report['match_col'].lower()
            # print(match_col)
            #report = json.loads(report[subcat][f'{domain}2'])
            
            # [EDIT #1]
            if domain in ['AE', 'CM', 'DR']:
                if identifier_name == 'deeplink':
                    sec_index = json.loads(report[subcat][f'{domain}2'])[0][identifier_name]
                    sec_index = int(sec_index.split('!')[-1])
                else:
                    sec_index = json.loads(report[subcat][f'{domain}2'])[0][identifier_name]
                    sec_index = int(sec_index.split('~')[-1])
                    
                # print(f'Initial Sec index - {sec_index}')
                # print(f'SELECT * from {self.study.lower()}.dm_pred where subjid={subjid} and  {match_col} = {sec_index} and infer_subcat = {subcat}')
                # print(subjid,type(subjid),self.study,type(self.study),subcat,type(subcat))
                # dmpred_df = self.conn.execute(get_data_query,sub=subjid,index=sec_index,subcat=subcat)
                # [EDIT #2]
                dmpred_df = self.conn.execute(f"SELECT * from {self.study.lower()}.dm_pred where subjid='{subjid}' and  {match_col} = '{sec_index}' and infer_subcat = '{subcat}'")
                dmpred_df = pd.DataFrame(dmpred_df.fetchall(), columns=dmpred_df.keys())
                dmpred_df['infer_dts'] = pd.to_datetime(dmpred_df['infer_dts'])
                dmpred_df = dmpred_df.sort_values(by=['infer_dts'], ascending= False)
                # [EDIT #2]
                if dmpred_df.shape[0] > 0:
                    for ind in range(dmpred_df.shape[0]):
                        dmpred_df1=dmpred_df.iloc[[ind]]
                        ck_event_id = dmpred_df1['ck_event_id'].values[0]
                        # print(f'Matching ck id - {ck_event_id}')
                        subcat_report = self.conn.execute(get_subcat_query, ck_event_id=int(ck_event_id))
                        subcat_features = pd.DataFrame(subcat_report, columns=subcat_report.keys())['subcat_features'].values.tolist()[0]
                        # print(f'####Subcat feature - {subcat_features}')
                        #subcat_features = json.loads(subcat_features)
                        if identifier_name == 'deeplink':
                            report_sec_id = json.loads(subcat_features[subcat][f'{domain}2'])[0][identifier_name]
                            report_sec_id = int(report_sec_id.split('!')[-1])
                        else:
                            #this change is for handling cdr_skey and internal id 
                            print(f"#####MATCH_SEC_ identifier BEFORE  - {identifier_name}")
                            identifier_name1 = 'cdr_skey' if 'cdr_skey'  in json.loads(subcat_features[subcat][f'{domain}2'])[0].keys() else identifier_name
                            print(f"#####MATCH_SEC_ identifier - {identifier_name1}")
                            report_sec_id = json.loads(subcat_features[subcat][f'{domain}2'])[0][identifier_name1]
                            report_sec_id = int(report_sec_id.split('~')[-1])
                        # print(f'Primary_index - {index}, Secondary Index - {report_sec_id}')
                        if report_sec_id == int(index):
                            is_duplicate = True
                            break

            elif domain == 'LB':
                #Having a different logic for checking LBOV duplicates as it should also need to handle duplicate labtests
                curr_prim_index = json.loads(report[subcat][f'{domain}1'])[0][identifier_name]
                curr_sec_index = json.loads(report[subcat][f'{domain}2'])[0][identifier_name]

                def get_entity_values(cdr_skey):
                    cdr_key_splits = cdr_skey.split('~')
                    subjectid = int(cdr_key_splits[1])
                    visitid = int(cdr_key_splits[2])
                    visitix = int(cdr_key_splits[3])
                    formid = int(cdr_key_splits[4])
                    formix = int(cdr_key_splits[5])
                    return (subjectid, visitid, visitix, formid, formix)
                
                print(f"#####CURR_PRIM_CDR_SKEY - {curr_prim_index}")
                print(f"#####CURR_SEC_CDR_SKEY - {curr_sec_index}")
                curr_prim_subjectid, curr_prim_visitid, curr_prim_visitix, curr_prim_formid, curr_prim_formix = get_entity_values(curr_prim_index)
                curr_sec_subjectid, curr_sec_visitid, curr_sec_visitix, curr_sec_formid, curr_sec_formix = get_entity_values(curr_sec_index)

                get_match_dmpred_query = f"""SELECT * from {self.study.lower()}.dm_pred where subjid='{subjid}' and visit_id='{curr_sec_visitid}' and visit_ix='{curr_sec_visitix}' and form_id='{curr_sec_formid}' and form_index='{curr_sec_formix}' and infer_subcat = '{subcat}'"""
                get_match_dmpredsf_query = text(f"""SELECT * from {self.study.lower()}.dm_pred_subcat_features where ck_event_id = :ck_id""")
                sec_param_dict = {
                    'subjid': subjid,
                    'visitid': curr_sec_visitid,
                    'visitix': curr_sec_visitix,
                    'formid': curr_sec_formid,
                    'formix': curr_sec_formix,
                    'subcat': subcat
                }
                print(f"Matching values - {sec_param_dict}")
                match_df_res = self.conn.execute(get_match_dmpred_query)
                match_df = pd.DataFrame(match_df_res.fetchall(), columns=match_df_res.keys())
                print(f"Total matching records - {match_df.shape[0]}")
                if match_df.shape[0] > 0:
                    match_ck_ids = match_df['ck_event_id'].values.tolist()

                    for ck_id in match_ck_ids:
                        try:
                            match_dmpredsf = self.conn.execute(get_match_dmpredsf_query, ck_id=ck_id)
                            feat = pd.DataFrame(match_dmpredsf.fetchall(), columns=match_dmpredsf.keys())['subcat_features'].values.tolist()[0]
                            print(f'matched dmsubcat - {len(feat)}')
                            #this change is for handling cdr_skey and internal id 
                            identifier_name1 = 'cdr_skey' if 'cdr_skey'  in json.loads(feat[subcat][f'{domain}2'])[0].keys() else identifier_name
                            print(f"#####MATCH_SEC_ identifier - {identifier_name1}")
                            match_sec_index = json.loads(feat[subcat][f'{domain}2'])[0][identifier_name1]
                            print(f"#####MATCH_SEC_CDR_SKEY - {curr_sec_index}")
                            match_sec_subjectid, match_sec_visitid, match_sec_visitix, match_sec_formid, match_sec_formix = get_entity_values(match_sec_index)

                            if match_sec_visitid == curr_prim_visitid and match_sec_visitix == curr_prim_visitix and match_sec_formid == curr_prim_formid and match_sec_formix == curr_prim_formix:
                                print('#MATCHED')
                                is_duplicate =  True
                        except:
                            continue
            else:
                print(f"{domain} is not mapped. Domain should be in (AE, CM, EC, LB)")
        except Exception as exp:
            print(traceback.format_exc())
            return False
        return is_duplicate
       
class LabDuplicateCheck:
    DB_URL = Db_url
    def __init__(self, study):
        vol_eng = create_engine(self.DB_URL)
        self.study = study
        self.conn = vol_eng.connect()
    def check(self, subjid, visit_nm, visit_ix, formrefname, form_ix, subcat):
        is_duplicate = False
        try:
            get_data_query = f"""SELECT * from {self.study.lower()}.dm_pred 
                                   where subjid = '{subjid}' and visit_nm = '{visit_nm}'
                                   and formrefname = '{formrefname}' and visit_ix ='{visit_ix}'
                                   and form_ix = '{form_ix}' and infer_subcat = '{subcat}'"""
            params = {
                'subjid': subjid,
                'visit_nm': visit_nm,
                'visit_ix': visit_ix,
                'formrefname': formrefname,
                'form_ix': form_ix,
                'subcat': subcat
            }
            get_data_res = self.conn.execute(get_data_query)
            match_dmpred_df = pd.DataFrame(get_data_res.fetchall(), columns=get_data_res.keys())

            if match_dmpred_df.shape[0] >0:
                is_duplicate = True
        
        except Exception as exp:
            print(traceback.format_exc())
        
        return is_duplicate
