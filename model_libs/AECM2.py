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
a = yaml.safe_load(open(subcat_config_path, 'r'))
class AECM2(BaseSDQApi):
    domain_list = ['AE', 'CM']
    def execute(self):
        study = self.study_id
        
        sub_cat = 'AECM2'
        index_col = 'itemrepn' if sub_cat.startswith('DR') else 'form_index'
        subjects = self.get_subjects(study, domain_list = self.domain_list, per_page = 10000) # ['70000057']#[11555001]#['11113003'] #
        
        for subject in tqdm.tqdm(subjects):
            payload_list = []
            try:
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list = self.domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)
                ae_df = pd.DataFrame(flatten_data['AE'])
                cm_df = pd.DataFrame(flatten_data['CM'])
                
                for ind in range(ae_df.shape[0]):
                    try:
                        ae_record = ae_df.iloc[[ind]]
                        o_aespid = ae_record['AESPID'].values[0]
                        
                        ae_cm_records = utils.extractor(ae_record, ae_df, cm_df, 'AESPID', 'CMAENO')
                        
                        if (type(ae_cm_records) == tuple) & (type(ae_cm_records) != bool): 
                            if len(ae_cm_records[0]) > 0:      
                                for ae_formindex, cm_records in ae_cm_records[0].items():
                                        for cm_record in cm_records:
                                            new_ae_records, new_cm_record = utils.get_cm_hierarchy(ae_record, ae_df, cm_record, key = 'AEENDAT')
                                            
                                            for aeptcd in new_ae_records['AEPTCD'].unique().tolist():
                                                new_ae_record = new_ae_records[new_ae_records['AEPTCD'] == aeptcd]
                                                
                                                new_ae_record['AEONGO'] = new_ae_record['AEONGO'].astype(str)
                                                aeongos = new_ae_record['AEONGO'].unique().tolist()
                                                aeongos = [i.upper() for i in aeongos]
                                                if 'YES' in aeongos:
                                                    continue
                                                
                                                if len(new_ae_record) > 1:
                                                    new_ae_record = new_ae_record.tail(1)
                                                                                                                             
                                                aespid = new_ae_record['AESPID'].values[0]
                                                new_ae_record1 = new_ae_record
                                                
                                                if aespid != o_aespid:
                                                    continue
                                                    
                                                if type(new_ae_record) != int:
                                                    aecmgiv = new_ae_record['AECMGIV'].values[0]
                                                    
                                                    dtr_null_flag=False
                                                    if 'AEENDAT_DTR' in new_ae_record.columns:
                                                        if new_ae_record['AEENDAT_DTR'].astype('str').values[0] in utils.null_list: 
                                                            dtr_null_flag=True
                                                    
#                                                     ae_dtr_flag = False
                                                    if 'AEENDAT_DTR' in new_ae_record.columns and not dtr_null_flag:
#                                                         ae_dtr_flag = True
                                                        new_ae_record['AEENDAT_DTR'] =  new_ae_record['AEENDAT_DTR'].astype('str')
                                                        ae_endt = new_ae_record['AEENDAT_DTR'].values[0]
                                                    else:
                                                        new_ae_record['AEENDAT'] = new_ae_record['AEENDAT'].astype('str')
                                                        ae_endt = new_ae_record['AEENDAT'].values[0]
                                                        
                                                    new_cm_record['CMSTDAT_DTR'] = new_cm_record['CMSTDAT_DTR'].astype('str')
                                                    cm_stdt = new_cm_record['CMSTDAT_DTR'].values[0]
                                                    
                                                    unk_flag, year_not_unk, date_dict = utils.impute_unk({'aeendat':ae_endt,
                                                                                 'cmstdat':cm_stdt})
                                                    
                                                    if not year_not_unk:
                                                        continue
                                                    
                                                    aeendat = date_dict['aeendat'].date()
                                                    cmstdat = date_dict['cmstdat'].date()
                                                    
                                                    aecmgiv = new_ae_record['AECMGIV'].values[0]
                                                    
                                                    if cmstdat > aeendat and aecmgiv.upper() == 'YES':
                                                        subcate_report_dict = {}
                                                        report_dict = {}
                                                       
                                                        new_cm_record['CMAENO'] = cm_record['CMAENO']
                                                        new_ae_record1,new_cm_record = utils.unk_format_datetime(unk_flag, new_ae_record1, new_cm_record)


                                                        piv_rec = {'AE' : new_ae_record1.head(1), 'CM' : new_cm_record.head(1)}

                                                        for dom, cols in a['FIELDS_FOR_UI'][sub_cat].items():
                                                            piv_df = piv_rec[dom]
                                                            present_col = [col for col in cols if col in piv_df.columns.tolist()]
                                                            rep_df = piv_df[present_col]
                                                            rep_df['deeplink'] = utils.get_deeplink(study, piv_df)
                                                            rep_df = rep_df.rename(columns= a['FIELD_NAME_DICT'])
                                                            report_dict[dom]= rep_df.to_json(orient= 'records')

                                                        subcate_report_dict[sub_cat] =  report_dict

                                                        aeterm = new_ae_record['AETERM'].values[0]
                                                        formidx = new_cm_record['form_ix'].values[0]
                                                        cmtrt = new_cm_record['CMTRT'].values[0]
                                                        cm_stdt = new_cm_record['CMSTDAT'].values[0]

                                                        ae_record1 = ae_record.iloc[0]
                                                        query_text_param = {
                                                                            'CM_FORMIDX':formidx, 
                                                                            'CMTRT':cmtrt, 
                                                                            'AESPID':aespid, 
                                                                            'AETERM':aeterm, 
                                                                            'CMSTDAT':cm_stdt, 
                                                                            'AEENDAT':new_ae_record1['AEENDAT'].values[0]}
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
                                                        print(subject, payload)
                                                        self.insert_query(study, subject, payload)
                                                        if payload not in payload_list:
                                                            payload_list.append(payload)
                                                        #break
                                
                    except Exception as exp:
                        logging.exception(exp)
                        #print(traceback.format_exc())
                        continue       

            except Exception as e:
                #print(traceback.format_exc())
                logging.exception(e)
                                                
            if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                self.close_query(subject, payload_list)

if __name__ == '__main__':
    study_id = sys.argv[1]
    account_id = sys.argv[2]
    job_id = sys.argv[3]
    rule_id = sys.argv[4]
    version = sys.argv[5]
    rule = AECM2(study_id, account_id, job_id, rule_id, version)
    rule.run()
