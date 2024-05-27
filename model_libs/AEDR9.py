import pandas as pd
import sys
import re
import datetime
from dateutil.relativedelta import relativedelta
import warnings
warnings.filterwarnings('ignore')
import os
from base import BaseSDQApi
import utils as utils
import traceback
import tqdm
import logging
import yaml
import numpy as np

# curr_file_path = os.path.realpath(__file__)
# curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
# subcat_config_path = os.path.join(curr_path, 'subcate_config.yml')
# subcate_config = yaml.load(open(subcat_config_path, 'r'))
class AEDR9(BaseSDQApi):
    domain_list = ['AE', 'EX']
    def execute(self):
        study = self.study_id
        
        sub_cat = self.__class__.__name__#'AEDR9'
        f_d = 'display_fields'
        f_c = 'fn_config'
        
        fields_dict = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_d))
        fn_config = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_c))
        fields_labels_1 = self.get_field_labels(self.account_id,self.domain_list[0])
        fields_labels_2 = self.get_field_labels(self.account_id,self.domain_list[1])
        fields_labels = {**fields_labels_1, **fields_labels_2 }
        #index_list = self.get_all_form_index_list(study, domain_list=self.domain_list, per_page=10000)
        index_col = 'itemrepn' if sub_cat.startswith('DR') else 'form_index'
        
        # match_config = fn_config['match']
        _cols = fn_config['cols']
        vals = fn_config['vals']
        dt_cols = fn_config['cols']['dt_cols']
        subjects = self.get_subjects(study, domain_list = self.domain_list, per_page = 10000)
        
        # subjects = ['122590']
        for subject in tqdm.tqdm(subjects):
            payload_list = []
            payload_list = []
            try:
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list = self.domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)

                ae_df = pd.DataFrame(flatten_data[self.domain_list[0]])
                ec_df = pd.DataFrame(flatten_data[self.domain_list[1]])
                #ae_df = ae_df[ae_df[index_col].isin(index_list)]
                
                for ind in range(ae_df.shape[0]):
                    try:
                        ae_record = ae_df.iloc[[ind]]

                        flag = -1

                        new_ec = ec_df.copy()
     
                        new_ec[dt_cols[2]+'1'] = new_ec[dt_cols[2]].apply(lambda x: utils.format_partial_date(x))
                        new_ec[dt_cols[3]+'1'] = new_ec[dt_cols[3]].apply(lambda x: utils.format_partial_date(x))
                        new_ec = new_ec.sort_values([dt_cols[2]+'1',dt_cols[3]+'1'])
                        
                        values = vals['aerel_val']
                        aetrt_dict = self.get_drugs_dict(study)
                        ec_dict = utils.get_ec_hierarchy(ae_record, new_ec, values, sub_cat, study, aetrt_dict)
                        print(ec_dict)
                        temp_lst = vals['adj_val']
                        if str(ae_record[_cols['aerel_col'][0]].values[0]).strip().upper() not in values or \
                            str(ae_record[_cols['aeacn_col'][0]].values[0]).strip().upper() not in vals['aeacn_val']:
                            continue

                        ecadj_flag, misdost_flag = utils.get_aereason_col(ae_record, ae_df, ec_df)
                        if ecadj_flag and misdost_flag:
                            new_ec1 = new_ec[~new_ec[_cols['adj_col'][0]].str.upper().isin(temp_lst)]
                            new_ec2 = new_ec[~new_ec[_cols['adj_col'][1]].str.upper().isin(temp_lst)]
                            if len(new_ec1) > len(new_ec2):
                                curr_ec_len = len(new_ec1)
                                new_ec = new_ec1
                            elif len(new_ec2) > len(new_ec1):
                                curr_ec_len = len(new_ec2)
                                new_ec = new_ec2
                        elif ecadj_flag and not misdost_flag:
                            new_ec = new_ec[~new_ec[_cols['adj_col'][0]].str.upper().isin(temp_lst)] 
                            curr_ec_len = len(new_ec)
                        elif misdost_flag and not ecadj_flag:
                            new_ec = new_ec[~new_ec[_cols['adj_col'][1]].str.upper().isin(temp_lst)]
                            curr_ec_len = len(new_ec)

                        # new_ec = new_ec[new_ec[_cols['aeacn_col'][0]].str.upper().isin(vals['aeacn_val'])]
                        if new_ec.empty:
                            continue

                        first_ec = new_ec.head(1)

                        if True:
                            subcate_report_dict = {}
                            report_dict = {}

                            # try:
                            #     new_ec['EXSTDTC'] = new_ec['EXSTDTC'].dt.strftime("%d-%b-%Y")
                            # except:
                            #     new_ec['EXSTDTC'] = pd.to_datetime(new_ec['EXSTDTC']).dt.strftime("%d-%b-%Y")
                            # new_ec = new_ec.head(1)
                            # ecendat = new_ec['EXENDTC'].values[0]
                            # if isinstance(ecendat, float) == False:
                            #     try:
                            #         new_ec['EXENDTC'] = new_ec['EXENDTC'].dt.strftime("%d-%b-%Y")
                            #     except:
                            #         new_ec['EXENDTC'] = pd.to_datetime(new_ec['EXENDTC']).dt.strftime("%d-%b-%Y")
                            ae_record = ae_record.replace({np.nan : 'blank'})
                            first_ec = first_ec.replace({np.nan : 'blank'})
                            piv_rec = {'AE' : ae_record, 'EX': first_ec}
                            
                            for dom, cols in fields_dict.items():
                                if dom not in piv_rec:
                                    continue
                                piv_df = piv_rec[dom]
                                present_col = [col for col in cols if col in piv_df.columns.tolist()]
                                rep_df = piv_df[present_col]
                                rep_df['deeplink'] = utils.get_deeplink(study, piv_df, self.get_study_source(), subject_id=self.get_map_subject_id(piv_df['subj_guid'].values[0]))
                                rep_df = rep_df.rename(columns= fields_labels)
                                rep_df = rep_df.loc[:,~rep_df.columns.duplicated()]
                                report_dict[dom]= rep_df.to_json(orient= 'records')
                                
                            subcate_report_dict[sub_cat] =  report_dict

                            ae_record1 = ae_record.iloc[0]

                            param_dict={}
                            try:
                                qt = fn_config['qt']
                                for i in qt:
                                    param_dict[i] = ae_record1[qt[i]]
                            except:
                                print('QT Error: ',traceback.format_exc())

                            payload = {
                                "subcategory": sub_cat,
                                "query_text": self.get_model_query_text_json(study, sub_cat, params = param_dict),
                                "form_index": str(ae_record1['form_index']),
                                "question_present": self.get_subcategory_json(study, sub_cat),
                                "modif_dts": str(ae_record1['modif_dts']),  
                                "stg_ck_event_id": int(ae_record1['ck_event_id']),
                                "formrefname" : str(ae_record1['formrefname']),
                                "report" : subcate_report_dict,
                                "cdr_skey": str(ae_record1['cdr_skey']),
                            }
                            
                            self.insert_query(study, subject, payload)
                            print(subject, payload)
                            if payload not in payload_list:
                                payload_list.append(payload)
                            
                    except:
                        print(traceback.format_exc())
                        continue       

            except Exception as e:
                logging.exception(e)
            if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                self.close_query(subject, payload_list)
                                                
if __name__ == '__main__':
    #(self.study_id, self.account_id, self.job_id, rule['ml_model_id'], 0.1
    # python AEDR9.py 1 1 2 1 0.1 > AEDR9log.txt
    study_id = sys.argv[1]
    account_id = sys.argv[2]
    job_id = sys.argv[3]
    rule_id = sys.argv[4]
    version = sys.argv[5]
    rule = AEDR9(study_id, account_id, job_id, rule_id, version)
    
    rule.run()
