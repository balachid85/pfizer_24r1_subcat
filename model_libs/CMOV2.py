'''
Rule ID: CMOV2
Release Version: R2.10M4.6,[R3.6M5.3]
Changes: 
06-04-22 -  line 112 changing the cmdost and cmdosu as overlapping parameter
           -removing the print statement 
29-04-2022 - [EDIT #1] - Adding (one day over lap logic)
[EDIT#2] - 21-12-22 - EXCLUDING CMTERM LOGIC 
            
'''
import pandas as pd
import sys
import re
import warnings
warnings.filterwarnings('ignore')
import traceback
import numpy as np
import tqdm
import logging
import yaml
import os
from dateutil.parser import parse
import datetime
from copy import deepcopy

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


class CMOV2(BaseSDQApi):
    domain_list = ['CM']
    def execute(self):
        study = self.study_id
        ov_study_id = f'account_{self.account_id}_study_{self.study_id}'
        duplicate=DuplicateCheck(ov_study_id)
        check_if_duplicate=duplicate.check
        domain_list = ['CM']
        sub_cat = 'CMOV2'
        subjects = self.get_subjects(study, domain_list = domain_list, per_page = 10000) # [10251002]#[10525019]#
        index_col = 'itemrepn' if sub_cat.startswith('DR') else 'form_index'
        for subject in tqdm.tqdm(subjects):
            payload_list = []
            try:
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list = domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)

                cm_df = pd.DataFrame(flatten_data['CM'])
                for ind in range(cm_df.shape[0]):
                    try:

                        cm_record = cm_df.iloc[[ind]]
                        total_valid_present = 0
                        total_count = 0
                        cmdecod = cm_record['CMDECOD'].values[0]
                        cmtrt = cm_record['CMTRT'].values[0]
                        cmstdt = cm_record['CMSTDAT'].apply(utils.get_date)
                        cmstdt_dtr = cm_record['CMSTDAT'].values[0]
                        cmongo = cm_record['CMONGO'].values.tolist()[0]
                        subjectid = cm_record['SUBJECTID'].values[0]
                        form_ix = cm_record['form_ix'].values[0]
                        cmstdt1 = cm_record['CMSTDAT'].values[0]
                        cmendt1 = cm_record['CMENDAT'].values[0]
                        cmendt=cm_record['CMENDAT'].apply(utils.get_date)
                        if 'CMSTDTC_001' in cm_record.columns.tolist():
                            cmstdt = cm_record['CMSTDTC_001'].apply(utils.get_date)
                        if 'CMENDTC_001' in cm_record.columns.tolist():
                            cmendt = cm_record['CMENDTC_001'].apply(utils.get_date)
                        cm_records = deepcopy(cm_df)
                        unk_flag=stunk=endunk = False
                        if cmongo.upper() == 'YES':
                            if isinstance(cmstdt.values[0], float) == True:
                                stunk=True
                                cm_records = cm_records[cm_records['CMSTDAT_DTR'].notna()]
                                cm_records['CMSTDAT_DTR'] = pd.to_datetime(cm_records['CMSTDAT_DTR'].apply(utils.remove_unk, combine=True))
                                date_list = cm_record['CMSTDAT_DTR'].apply(utils.remove_unk_cmov).values[0]
                                if len(date_list) == 1:
                                    new_cm_record = cm_records[(cm_records['CMSTDAT_DTR'].dt.year > int(date_list[0]))]
                                elif len(date_list) == 2:
#                                     cm_records1 = cm_records[(cm_records['CMENDAT'].notna())]
                                    cm_records1 = cm_records
                                    new_cm_record = cm_records1[(cm_records['CMSTDAT_DTR'].apply(lambda dt:dt.replace(day=1)) >= pd.to_datetime(datetime.datetime(int(date_list[1]), int(date_list[0]), 1).date()))]
                            elif type(cmstdt.values.tolist()[0]) != float:
                                cm_records = cm_records[cm_records['CMSTDAT'].notna()]
#                                 cm_records1 = cm_records[(cm_records['CMENDAT'].notna())]
                                cm_records1 = cm_records
                                cm_records1['CMSTDAT'] = cm_records1['CMSTDAT'].apply(utils.get_date)
                                new_cm_record = cm_records1[(cm_records1['CMSTDAT'] >= cmstdt.values[0])]
                        elif cmongo.upper() == 'NO' or type(cmongo) == float:
                            
                            if isinstance(cmstdt.values[0], float) == True and isinstance(cmendt.values[0], float) != True:
                                
                                stunk=True
                                cm_endt = cm_record['CMENDAT'].apply(utils.get_date)
                                # handles UNK/UNK/2019 to 1/Jun/2020 and 19/Mar/2020 to 21/Mar/2020
                                cm_records = cm_records[cm_records['CMSTDAT_DTR'].notna()]

                                cm_records['CMSTDAT_DTR'] = pd.to_datetime(cm_records['CMSTDAT_DTR'].apply(utils.remove_unk, combine=True))
                                
                                date_list = cm_record['CMSTDAT_DTR'].apply(utils.remove_unk_cmov).values[0]
                                if len(date_list) == 1:
                                    new_cm_record = cm_records[(cm_records['CMSTDAT_DTR'].dt.year >= int(date_list[0])) & (cm_records['CMSTDAT_DTR'] < cm_endt.values[0])]
#                                     unk_flag = True
                                elif len(date_list) == 2:
                                    cm_records = cm_records[(cm_records['CMSTDAT_DTR'].notna())]
                                    #changed this to statisfy such conditions unk/dec/2017 >=01/may/2019
                                    # IMPUTING THE DATE UNK AS 01 SINCE WE ARE CONSIDERING ONLY THE YEAR AND MONTH
                                    new_cm_record = cm_records[(cm_records['CMSTDAT_DTR'].apply(lambda dt:dt.replace(day=1))>datetime.datetime(int(date_list[1]), int(date_list[0]), 1).date()) & (cm_records['CMSTDAT_DTR'] < cm_endt.values[0])]
                                    #handles unk in endate and crt date in stdat
                            elif  isinstance(cmstdt.values[0], float) != True and isinstance(cmendt.values[0], float) == True:
                                endunk=True
#                                 cm_endt = cm_record['CMENDAT'].apply(utils.get_date)
                                cm_records = cm_records[cm_records['CMSTDAT_DTR'].notna()]
                                
                                # handles UNK/UNK/2019 to 1/Jun/2020 and 19/Mar/2020 to 21/Mar/2020
                                cm_records['CMSTDAT_DTR'] = pd.to_datetime(cm_records['CMSTDAT_DTR'].apply(utils.remove_unk, combine=True))
                                cm_records['CMENDAT_DTR'] = pd.to_datetime(cm_records['CMENDAT_DTR'].apply(utils.remove_unk, combine=True))

                                date_list = cm_record['CMENDAT_DTR'].apply(utils.remove_unk_cmov).values[0]
                                if len(date_list) == 1:
                                    new_cm_record = cm_records[(cm_records['CMSTDAT_DTR'] >cmstdt.values[0]) & (cm_records['CMSTDAT_DTR'].dt.year <= int(date_list[0]))]
                                    unk_flag = True
                                elif len(date_list) == 2:
                                    cm_records = cm_records[(cm_records['CMSTDAT_DTR'].notna())]
                                    #IMPUTING THE END DAY AS WE GONNA COMPARE ONLY THE MONTH AND YEAR
                                    new_cm_record = cm_records[(cm_records['CMSTDAT_DTR'] > cmstdt.values[0]) & (cm_records['CMSTDAT_DTR'].apply(lambda dt:dt.replace(day=1)) < datetime.datetime(int(date_list[1]), int(date_list[0]), 1).date())]
                            elif  isinstance(cmstdt.values[0], float) == True and isinstance(cmendt.values[0], float) == True:
                                endunk=True
                                stunk=True
#                                 cm_endt = cm_record['CMENDAT'].apply(utils.get_date)
                                cm_records = cm_records[cm_records['CMSTDAT_DTR'].notna()]
                                cm_records = cm_records[cm_records['CMENDAT_DTR'].notna()]
                                
                                # handles UNK/UNK/2019 to 1/Jun/2020 and 19/Mar/2020 to 21/Mar/2020
                                cm_records['CMSTDAT_DTR'] = pd.to_datetime(cm_records['CMSTDAT_DTR'].apply(utils.remove_unk, combine=True))
                                cm_records['CMENDAT_DTR'] = pd.to_datetime(cm_records['CMENDAT_DTR'].apply(utils.remove_unk, combine=True))

                                date_list = cm_record['CMENDAT_DTR'].apply(utils.remove_unk_cmov).values[0]
                                date_list1 = cm_record['CMSTDAT_DTR'].apply(utils.remove_unk_cmov).values[0]

                                
                                if len(date_list) == 1 and len(date_list1) == 1:
                                    new_cm_record = cm_records[(cm_records['CMSTDAT_DTR'].dt.year  >= int(date_list1[0])) & (cm_records['CMSTDAT_DTR'].dt.year < int(date_list[0]))]
                                    unk_flag = True
                                elif len(date_list) == 2 and  len(date_list1) == 2:
                                    cm_records = cm_records[(cm_records['CMSTDAT_DTR'].notna())]
                                    #IMPUTING THE END DAY AS WE GONNA COMPARE ONLY THE MONTH AND YEAR
                                    new_cm_record = cm_records[(cm_records['CMSTDAT_DTR'].apply(lambda dt:dt.replace(day=1)) > datetime.datetime(int(date_list1[1]), int(date_list1[0]),1).date()) & (cm_records['CMSTDAT_DTR'].apply(lambda dt:dt.replace(day=1)) < datetime.datetime(int(date_list[1]), int(date_list[0]),1).date())]
                                #if end-date is unk/01/2014 and stdate unk/unk/2020
                                elif len(date_list) == 2 and  len(date_list1) == 1:
                                    cm_records = cm_records[(cm_records['CMSTDAT_DTR'].notna())]
                                    #IMPUTING THE END DAY AS WE GONNA COMPARE ONLY THE MONTH AND YEAR
                                    new_cm_record = cm_records[(cm_records['CMSTDAT_DTR'].dt.year  >= int(date_list1[0])) & (cm_records['CMSTDAT_DTR'].apply(lambda dt:dt.replace(day=1)) < datetime.datetime(int(date_list[1]), int(date_list[0]),1).date())]
                                elif len(date_list) == 1 and  len(date_list1) == 2:
                                    cm_records = cm_records[(cm_records['CMSTDAT_DTR'].notna())]
                                    #IMPUTING THE END DAY AS WE GONNA COMPARE ONLY THE MONTH AND YEAR
                                    new_cm_record = cm_records[(cm_records['CMSTDAT_DTR'].apply(lambda dt:dt.replace(day=1)) > datetime.datetime(int(date_list1[1]), int(date_list1[0]),1).date()) & (cm_records['CMSTDAT_DTR'].apply(lambda dt:dt.replace(day=1)).dt.year  < int(date_list1[0]))]
                                
                            elif type(cmstdt.values.tolist()[0]) != float:
                                cm_endt = cm_record['CMENDAT'].apply(utils.get_date)
                                cm_records = cm_records[cm_records['CMSTDAT'].notna()]
                                cm_records['CMSTDAT'] = cm_records['CMSTDAT'].apply(utils.get_date)
                                #cm_records = cm_records[cm_records['CMENDAT'].notna()]
                                cm_records['CMENDAT'] = cm_records['CMENDAT'].apply(utils.get_date)
                                cm_records = cm_records[~((cm_records['CMENDAT'] == cm_endt.values[0]) & (cm_records['CMSTDAT'] == cmstdt.values[0]))]
#                                 cm_records['CMENDAT'] = cm_records['CMENDAT'].dt.strftime("%d-%b-%Y")
                                if cm_records.shape[0] > 0:
                                    #[EDIT #1]
                                    # handles 1/Mar/2019 to 28/Mar/2019 and 1/Mar/2019 to 27/Mar/2019
                                    new_cm_record = cm_records[(cm_records['CMSTDAT'] < cm_endt.values[0]) & (cm_records['CMSTDAT'] > cmstdt.values[0]) & (cm_records['CMONGO'] == cmongo)]
                                    if len(new_cm_record) == 0:
                                    # handles 30/Nov/2019 to 8/Dec/2019 and 6/Dec/2019 to ongoing
                                    #[EDIT #1]
                                        new_cm_record = cm_records[(cm_records['CMSTDAT'] < cm_endt.values[0]) & (cm_records['CMSTDAT'] > cmstdt.values[0]) & (cm_records['CMONGO'] == 'YES')]
                                        if len(new_cm_record) == 0:
                                            if 'CMDOSFRQ' in new_cm_record.columns:
                                                cmdosfrq = new_cm_record['CMDOSFRQ'].values[0]
                                                #[EDIT #1]
                                                new_cm_record = cm_records[(cm_records['CMSTDAT'] < cm_endt.values[0]) & (cm_records['CMSTDAT'] > cmstdt.values[0]) & (cm_records['CMONGO'] == cmongo) & (cm_records['CMDOSFRQ'] == cmdosfrq)]
                                                if 'CMDOSTOT' in new_cm_record.columns and 'CMDOSU' in new_cm_record.columns:
                                                    cmdosu = new_cm_record['CMDOSU'].values[0]
                                                    cmdostot = new_cm_record['CMDOSTOT'].values[0]
                                                     #[EDIT #1]
                                                    # handles 1/Mar/2019 to 28/Mar/2019 and 2/Mar/2019 to 28/Mar/2019 and dose,unit,frequency
                                                    new_cm_record = cm_records[(cm_records['CMSTDAT'] < cm_endt.values[0]) & (cm_records['CMSTDAT'] > cmstdt.values[0]) & (cm_records['CMONGO'] == cmongo) & (cm_records['CMDOSU'] == cmdosu) & (cm_records['CMDOSTOT'] == cmdostot) & (cm_records['CMDOSFRQ'] == cmdosfrq)]
                        
                        
                        check = ['CMDOSTOT', 'CMDOSFRQ','CMROUTE']
                        for c in check:
                            if c in new_cm_record.columns.tolist():
                                cm_record[c] = cm_record[c].astype(str)
                                new_cm_record[c] = new_cm_record[c].astype(str)
                                value = cm_record[c].values[0]
                                new_cm_record = new_cm_record[new_cm_record[c] == value]

                                if value in [np.nan, 'nan', 'null', 'NULL', '', ' ']:
                                    new_cm_record[c] = new_cm_record[c].replace({value : 'blank'})
                                    cm_record[c] = new_cm_record[c].replace({value : 'blank'})
                                
                        
                        new_cm_record = new_cm_record[new_cm_record['form_ix'] != form_ix]   
                        if len(new_cm_record) == 0:
                            continue
                        
                        elif len(new_cm_record) > 0:
                            
                            if stunk:
                                #REMOVING THE DATE TIME STRP (IF UNK PRESENTS THAT NEEDS TO BE AS UNK)
                                cm_record['CMSTDAT'] = cm_record['CMSTDAT_DTR'].astype('str')
                            if endunk:
                                cm_record['CMENDAT'] = cm_record['CMENDAT_DTR'].astype('str')

#                                 new_cm_record['CMENDAT'] = new_cm_record['CMENDTC_001_DTR']
                            
                            for ind in new_cm_record.index.tolist():
                                
                                curr_cmtrt = new_cm_record.loc[ind, 'CMTRT']
                                curr_cmdecod = new_cm_record.loc[ind, 'CMDECOD']
                                # [EDIT#2] commenting since no longer need in logic
                                # cmtrt_flag = utils.check_similar_term_fuzzy(cmtrt, curr_cmtrt)
                                cmdecod_flag = utils.check_similar_medication_verabtim2(cmdecod, curr_cmdecod)
                                formindex=new_cm_record.loc[ind, 'form_index']
                                
                                # [EDIT#2] commenting since no longer need in logic
                                # if cmtrt_flag and cmdecod_flag:
                                if cmdecod_flag:
                                    
                                    ind1 = ind
                                    new_cm_record1 = cm_df.loc[cm_df['form_index']==formindex]
                                    subcate_report_dict = {}
                                    report_dict = {}
                             
                                    cm_record2 = cm_record.fillna('blank')
                                    new_cm_record1 = new_cm_record1.fillna('blank')
                                    #COMMENTING DATE TIME FORMAT SINCE (IF UNK PRESENTS THAT NEEDS TO BE AS UNK)
                                    try:
                                        new_cm_record1['CMSTDAT'] = new_cm_record1['CMSTDAT_DTR'].astype('str') if any([i for i in ['BLANK','UNK'] if i in new_cm_record1['CMSTDAT_DTR'].values[0].upper()]) else  new_cm_record1['CMSTDAT'].apply(utils.format_datetime)
                                        new_cm_record1['CMENDAT'] = new_cm_record1['CMENDAT_DTR'].astype('str') if any([i for i in ['BLANK','UNK'] if i in new_cm_record1['CMENDAT_DTR'].values[0].upper()])  else  new_cm_record1['CMENDAT'].apply(utils.format_datetime)
                                        #cm_record
                                        cm_record2['CMSTDAT'] = cm_record2['CMSTDAT_DTR'].astype('str') if 'blank' in cm_record2['CMSTDAT'].values[0]   else  cm_record2['CMSTDAT'].apply(utils.format_datetime)
                                        cm_record2['CMENDAT'] = cm_record2['CMENDAT_DTR'].astype('str') if 'blank' in cm_record2['CMENDAT'].values[0] else  cm_record2['CMENDAT'].apply(utils.format_datetime)
                                    except Exception as e:
                                        logging.error(e)
#                                     cm_record['CMSTDAT'] = cm_record['CMSTDAT'].apply(utils.format_datetime)
#                                     cm_record['CMENDAT'] = cm_record['CMENDAT'].apply(utils.format_datetime)
                                    
                                    piv_rec = {'CM1': cm_record2.head(1),'CM2' : new_cm_record1.head(1)}
    
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

                                    form_ix1 = new_cm_record1['form_ix'].values[0]
                                    cmtrt1 = new_cm_record1['CMTRT'].values[0]
                                    cmstdt2 = new_cm_record1['CMSTDAT'].values[0]
                                    cmendt2 = new_cm_record1['CMENDAT'].values[0]

                                    cm_record1 = cm_record.iloc[0]

                                    if cmendt1 == np.nan:
                                        cmendt1 = 'blank'
                                    total_count += 1
                                    query_text_param = {
                                        'CM_FORMIDX':form_ix,
                                        'CMTRT':cmtrt,
                                        'CM_FORMIDX1':form_ix1,
                                        'CMTRT1':cmtrt1,
                                        'CMDECOD':cmdecod,
                                        'CMSTDAT':cmstdt1,
                                        'CMENDAT':cmendt1,
                                        'CMSTDAT1':cmstdt2,
                                        'CMENDAT1':cmendt2,
                                    }
                                    
                                    is_ov_duplicate = check_if_duplicate(subjid= int(cm_record1['subjid']), subcat= 'CMOV2', index= int(cm_record1['form_index']), report= subcate_report_dict)
                                    print('is_ov_duplicate', is_ov_duplicate,subject)
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
                                        print(subject, payload)
                                        self.insert_query(study, subject, payload)
                                        if payload not in payload_list:
                                            payload_list.append(payload)

    
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
    rule = CMOV2(study_id, account_id, job_id, rule_id, version)
    rule.run()
