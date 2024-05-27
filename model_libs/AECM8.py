import pandas as pd
import sys
import re
import warnings
warnings.filterwarnings('ignore')
from base import BaseSDQApi
import utils as utils
from similarity_contents.drug_indication_search import check_valid_drug_indication
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

class AECM8(BaseSDQApi):
    domain_list = ['AE', 'CM']
    def execute(self):
        study = self.study_id
        
        domain_list = ['AE', 'CM']
        sub_cat = 'AECM8'
        index_col = 'itemrepn' if sub_cat.startswith('DR') else 'form_index'
        subjects = self.get_subjects(study, domain_list = domain_list, per_page = 10000) # ['12121233','12121234']#[10781007]#
        print(subjects)
        
        for subject in tqdm.tqdm(subjects):
            payload_list = []
            try:
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list = domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)

                ae_df = pd.DataFrame(flatten_data['AE'])
                cm_df = pd.DataFrame(flatten_data['CM'])
               
                for ind in range(ae_df.shape[0]):
                    try:
                        
                        ae_record = ae_df.iloc[[ind]]

                        ae_cm_records = utils.extractor(ae_record, ae_df, cm_df, 'AESPID', 'CMAENO')
                                    
                        if (type(ae_cm_records) == tuple) & (type(ae_cm_records) != bool): 
                            if len(ae_cm_records[0]) == 0: 
                            
                                for ind1 in range(cm_df.shape[0]):
                                    try:
                                        cm_record = cm_df.iloc[[ind1]]                                       
                                        ae_term = ae_record['AETERM'].values[0]
                                        cm_term = cm_record['CMTRT'].values[0]
                                        cmaer = cm_record['CMAER'].values[0]
                                        aecmgiv = ae_record['AECMGIV'].values[0]
                                        aespid = ae_record['AESPID'].values[0]
                                        cmaeno = cm_record['CMAENO'].values[0]
                                        aeongo = ae_record['AEONGO'].values[0]
                                        
                                        nullify = lambda x: None if x in utils.null_list else x
                                        
                                        aeptcd = ae_record['AEPTCD'].apply(nullify).values[0] if 'AEPTCD' in ae_record.columns else None
                                        aelltcd = ae_record['AELLTCD'].apply(nullify).values[0] if 'AELLTCD' in ae_record.columns else None
                                        cmcode = cm_record['CMCODE'].apply(nullify).values[0] if 'CMCODE' in cm_record.columns else None
                                        cmindc = cm_record['CMINDC'].apply(nullify).values[0] if 'CMINDC' in cm_record.columns else None
                                        cmdecod = cm_record['CMDECOD'].apply(nullify).values[0] if 'CMDECOD' in cm_record.columns else None
                                        aedecod = ae_record['AEDECOD'].apply(nullify).values[0] if 'AEDECOD' in ae_record.columns else None

                                        drug_flag = False
                                        if aecmgiv.upper() == 'YES':                                                
                                            ae_dtr_flag = False
                                            if 'AESTDAT_DTR' in ae_record.columns and 'AEENDAT_DTR' in ae_record.columns:
                                                ae_dtr_flag = True
                                                ae_record['AESTDAT_DTR'] = ae_record['AESTDAT_DTR'].astype('str')
                                                ae_stdt = ae_record['AESTDAT_DTR'].values[0]
                                                ae_record['AEENDAT_DTR'] = ae_record['AEENDAT_DTR'].astype('str')
                                                ae_endt = ae_record['AEENDAT_DTR'].values[0]
                                            else:
                                                ae_record['AESTDAT'] = ae_record['AESTDAT'].astype('str')
                                                ae_stdt = ae_record['AESTDAT'].values[0]
                                                ae_record['AEENDAT'] = ae_record['AEENDAT'].astype('str')
                                                ae_endt = ae_record['AEENDAT'].values[0]
                                            cm_record['CMSTDAT_DTR'] = cm_record['CMSTDAT_DTR'].astype('str')
                                            cm_stdt = cm_record['CMSTDAT_DTR'].values[0]
                                            cm_record['CMENDAT_DTR'] = cm_record['CMENDAT_DTR'].astype('str')
                                            cm_endt = cm_record['CMENDAT_DTR'].values[0]
                                            if aeongo.upper() == 'NO':
                                                unk_flag, year_not_unk, date_dict = utils.impute_unk({'aestdat':ae_stdt,
                                                                                 'cmstdat':cm_stdt,
                                                                                 'cmendat':cm_endt,
                                                                                 'aeendat':ae_endt}
                                                                                 )

                                                if not year_not_unk:
                                                    continue
                                                aestdat = date_dict['aestdat']
                                                cmstdat = date_dict['cmstdat']
                                                aeendat = date_dict['aeendat']
                                                cmendat = date_dict['cmendat']                                                
                                                if ((cmstdat >= aestdat) and (cmendat <= aeendat)):
                                                    drug_flag = True
                                                    
                                            elif aeongo.upper() == 'YES': 
                                                unk_flag, year_not_unk, date_dict = utils.impute_unk({'aestdat':ae_stdt,
                                                                                 'cmstdat':cm_stdt},
                                                                                  proper=False)
                                                if not year_not_unk:
                                                    continue

                                                aestdat = date_dict['aestdat']
                                                cmstdat = date_dict['cmstdat']
                                                
                                                print('***WIthin YES')
                                                
                                                print(aedecod, ae_term, cmdecod, cm_term, cmstdat, aestdat,)
                                                
                                                if (cmstdat >= aestdat):
                                                    print('Drug Flag - True')
                                                    drug_flag = True
                    
                                            print(subject)
                                            if drug_flag:
                                                print('Inside Drug flag -----')
                                                print(cm_term,ae_term)
                            
                                                valid_drug = check_valid_drug_indication(cmtrt=cm_term.upper(), 
                                                                                     aeterm=ae_term.upper(),
                                                                                     aeptcode=aeptcd,
                                                                                     aelltcode=aelltcd, 
                                                                                     cmcode=cmcode,
                                                                                     cmindc=cmindc,
                                                                                     aedecod=aedecod,
                                                                                     cmdecod=cmdecod
                                                                                     )
                                                print(valid_drug)
                                
                                                if valid_drug:
                                                    subcate_report_dict = {}
                                                    report_dict = {}                                                    
                                                    if unk_flag:
                                                            cm_record['CMSTDAT'] = cm_record['CMSTDAT_DTR']
                                                            cm_record['CMENDAT'] = cm_record['CMENDAT_DTR']
                                                            if ae_dtr_flag:
                                                                ae_record['AESTDAT'] = ae_record['AESTDAT_DTR']
                                                                ae_record['AEENDAT'] = ae_record['AEENDAT_DTR']
                                                            else:
                                                                ae_record['AESTDAT'] = ae_record['AESTDAT'].apply(utils.format_datetime)
                                                                ae_record['AEENDAT'] = ae_record['AEENDAT'].apply(utils.format_datetime)                                                                
                                                    else:
                                                        cm_record['CMSTDAT'] = cm_record['CMSTDAT'].apply(utils.format_datetime)
                                                        cm_record['CMENDAT'] = cm_record['CMENDAT'].apply(utils.format_datetime)
                                                        ae_record['AESTDAT'] = ae_record['AESTDAT'].apply(utils.format_datetime)
                                                        ae_record['AEENDAT'] = ae_record['AEENDAT'].apply(utils.format_datetime)
                                                        cm_record['CMAENO'] = cm_record['CMAENO']

                                                    piv_rec = {'AE' : ae_record, 'CM' : cm_record}
                                                    
                                                    for dom, cols in a['FIELDS_FOR_UI'][sub_cat].items():
                                                        piv_df = piv_rec[dom]
                                                        present_col = [col for col in cols if col in piv_df.columns.tolist()]
                                                        rep_df = piv_df[present_col]
                                                        rep_df['deeplink'] = utils.get_deeplink(study, piv_df)
                                                        rep_df = rep_df.rename(columns= a['FIELD_NAME_DICT'])
                                                        report_dict[dom]= rep_df.to_json(orient= 'records')

                                                    subcate_report_dict[sub_cat] =  report_dict

                                                    aespid = ae_record['AESPID'].values[0]
                                                    formidx = cm_record['form_ix'].values[0]
                                                    cmtrt = cm_record['CMTRT'].values[0]
                                                    cmongo = cm_record['CMONGO'].values[0]
                                                    cm_stdt = cm_record['CMSTDAT'].values[0]
                                                    cm_endt = cm_record['CMENDAT'].values[0]
                                                    if cm_endt == 'null':
                                                        cm_endt = 'blank'
                                                    ae_record1 = ae_record.iloc[0]
                                                    query_text_param = {
                                                       'CM_FORMIDX':formidx,
                                                        'CMTRT':cmtrt,
                                                        'CMSTDAT':cm_stdt,
                                                        'CMENDAT':cm_endt,
                                                        'CMONGO':cmongo,
                                                        'AESPID':aespid
                                                    }
                                                    payload = {
                                                        "cdr_skey" : str(ae_record1['cdr_skey']),
                                                        "subcategory": sub_cat,
                                                        "query_text": self.get_model_query_text_json(study, sub_cat, params= query_text_param),
                                                        "form_index": str(ae_record1['form_index']),
                                                        "question_present": self.get_subcategory_json(study, sub_cat),
                                                        "modif_dts": str(ae_record1['modif_dts']),  
                                                        "stg_ck_event_id": int(ae_record1['ck_event_id']),
                                                        "formrefname" : str(ae_record1['formrefname']),
                                                        "report" : subcate_report_dict,
                                                        "confid_score": np.random.uniform(0.7, 0.97)
                                                    }
                                                    print(payload)
                                                    self.insert_query(study, subject, payload)
                                                    if payload not in payload_list:
                                                        payload_list.append(payload)
                                
                                    except Exception as e:
                                        logging.exception(e)
                                        
                    except Exception as e:
                        logging.exception(e)

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
    rule = AECM8(study_id, account_id, job_id, rule_id, version)
    rule.run()
