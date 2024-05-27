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
# path = os.path.dirname(os.path.abspath(__file__))
# sys.path.append('/Users/ahamed.musthafa/Documents/spdm2.0/sdq_ingestion/Subcats/')
from base import BaseSDQApi
import utils as utils

# curr_file_path = os.path.realpath(__file__)
# curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
# subcat_config_path = os.path.join(curr_path, 'subcate_config.yml')
# subcate_config = yaml.load(open(subcat_config_path, 'r'))
class AEDR4(BaseSDQApi):
    domain_list = ['AE', 'EX']
    def execute(self):
        study = self.study_id
        
        sub_cat = self.__class__.__name__#'AEDR4'
        f_d = 'display_fields'
        f_c = 'fn_config'
        
        fields_dict = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_d))
        fn_config = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_c))
        fields_labels_1 = self.get_field_labels(self.account_id,self.domain_list[0])
        fields_labels_2 = self.get_field_labels(self.account_id,self.domain_list[1])
        fields_labels = {**fields_labels_1, **fields_labels_2 }
        #index_list = self.get_all_form_index_list(study, domain_list=self.domain_list, per_page=10000)
        index_col = 'itemrepn' if sub_cat.startswith('DR') else 'form_index'
        
        match_config = fn_config['match']
        match_terms = fn_config['match']
        match_cond = fn_config['match_condition']
        _cols = fn_config['cols']
        vals = fn_config['vals']
        dt_cols = fn_config['cols']['dt_cols']
        subjects = self.get_subjects(study, domain_list = self.domain_list, per_page = 10000)
        
        # subjects = ['9199964']
        for subject in tqdm.tqdm(subjects):
            payload_list = []
            payload_list = []
            try:
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list = self.domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)

                ae_df = pd.DataFrame(flatten_data[self.domain_list[0]])
                ex_df = pd.DataFrame(flatten_data[self.domain_list[1]])
                #ae_df = ae_df[ae_df[index_vol].isin(index_list)]
                for ind in range(ae_df.shape[0]):
                    try:
                        ae_record = ae_df.iloc[[ind]]

                        new_ec = pd.DataFrame()
                        for i in match_terms:
                            if i.strip().upper() == 'MATCH_DSL':
                                try:
                                    match_config_dsl = fn_config[i]
                                    match_flag, ex_match_df = utils.ae_cm_mapper(prim_rec=ae_record,
                                                sec_df = ex_df,
                                                subcat = sub_cat,
                                                match_type = 'dsl',
                                                match_DSL = match_config_dsl
                                                )
                                except Exception as ex:
                                    print(f' Exception in match_DSL : {ex}')
                                    print(traceback.format_exc())
                                    continue                                
                            else:
                                try:
                                    ex_match_df = pd.DataFrame()
                                    match_config = fn_config[i]
                                    match_flag, ex_match_df1 = utils.ae_cm_mapper(prim_rec=ae_record,
                                                        sec_df = ex_df,
                                                        subcat = sub_cat,
                                                        **match_config)
                                    if len(match_cond)>0 and match_cond[0].upper() == 'AND':
                                        ex_match_df = ex_match_df1
                                        if(len(ex_match_df1) > 0):
                                            ex_df = ex_match_df1
                                        else:
                                            break
                                    else:
                                        ex_match_df = ex_match_df.append(ex_match_df1)
                                except Exception as ex_match_exc:
                                    print(f'Exception in ex_match_df is : {ex_match_exc}')
                                    continue


                        new_ec = ex_match_df
                        ecadj_flag = False
                        misdost_flag = False
                        
                        curr_ec_len = -1

                        ae_cols = ae_record.columns.tolist()
                        ec_cols = ex_df.columns.tolist()
                        
                        ecadj_flag, misdost_flag = utils.get_aereason_col(ae_record, ae_df, ex_df)
                        

                        new_ec[dt_cols[2]+'1'] = new_ec[dt_cols[2]].apply(lambda x: utils.format_partial_date(x))
                        new_ec[dt_cols[3]+'1'] = new_ec[dt_cols[3]].apply(lambda x: utils.format_partial_date(x))
                        new_ec = new_ec.sort_values([dt_cols[2]+'1',dt_cols[3]+'1'])
                        
                            
                        values = vals['aeacn_val']
                        aetrt_dict = self.get_drugs_dict(study)
                        ec_dict = utils.get_ec_hierarchy(ae_record, new_ec, values, sub_cat, study, aetrt_dict)
                        temp_lst = vals['adj_val']

                        if bool(ec_dict) == False:
                            continue
                        print('1=',ec_dict.keys())
                        for i, (drug, records) in enumerate(ec_dict.items()):
                            new_ec = records

                            prev_ec_len = len(new_ec) 
                            
                            dose_flag = False
                            if ecadj_flag and misdost_flag:
                                new_ec1 = new_ec[(new_ec[_cols['adj_col'][0]].str.upper().isin(temp_lst))]
                                new_ec2 = new_ec[(new_ec[_cols['adj_col'][1]].str.upper().isin(temp_lst))]
                                if len(new_ec1) ==  prev_ec_len and len(new_ec2) ==  prev_ec_len and prev_ec_len != 0 and len(new_ec1) != 0 and len(new_ec2) != 0:
                                    dose_flag = True   
                                    new_ec = pd.merge(new_ec1, new_ec2) 
                            elif ecadj_flag and not misdost_flag:
                                new_ec = new_ec[(new_ec[_cols['adj_col'][0]].str.upper().isin(temp_lst))] 
                                curr_ec_len = len(new_ec)
                                if (prev_ec_len == curr_ec_len) and prev_ec_len != 0 and curr_ec_len != 0:
                                    dose_flag = True    
                            elif misdost_flag and not ecadj_flag:
                                new_ec = new_ec[(new_ec[_cols['adj_col'][1]].str.upper().isin(temp_lst))]
                                curr_ec_len = len(new_ec)
                                if (prev_ec_len == curr_ec_len) and prev_ec_len != 0 and curr_ec_len != 0:
                                    dose_flag = True    
                                
                            print('2=',dose_flag)
                            if dose_flag:
                                match_flag, ec_match_df = utils.ae_cm_mapper(prim_rec=ae_record,
                                                                sec_df=new_ec,
                                                                subcat=sub_cat,
                                                                **match_config)
                                print('3=',ec_match_df.shape)
                                if (match_flag is True):
                                    if len(ec_match_df) > 0:
                                        new_ec = ec_match_df
                                    else:
                                        continue
                                else:
                                    continue
                                for ix in range(new_ec.shape[0]):
                                    payload_flag = False
                                    ec_record = new_ec.iloc[[ix]]
                                    if utils.compare_partial_date(ae_record[dt_cols[0]].values[0],ec_record[dt_cols[2]].values[0],'>') or\
                                        utils.compare_partial_date(ae_record[dt_cols[1]].values[0],ec_record[dt_cols[2]].values[0],'<'):
                                        payload_flag = True
                                        if payload_flag:
                                            subcate_report_dict = {}
                                            report_dict = {}
                                            try:
                                                ec_record[dt_cols[2]] = pd.to_datetime(ec_record[dt_cols[2]]).dt.strftime("%d-%b-%Y")
                                            except:
                                                try:
                                                    ec_record[dt_cols[2]] = ec_record[dt_cols[2]].dt.strftime("%d-%b-%Y")
                                                except:
                                                    ec_record[dt_cols[2]] = ec_record[dt_cols[2]].apply(lambda x : str(x))
                                            ec_record = ec_record.head(1)
                                            ecendat = new_ec[dt_cols[3]].values[0]
                                            if isinstance(ecendat, float) == False:
                                                try:
                                                    ec_record[dt_cols[3]] = pd.to_datetime(ec_record[dt_cols[3]]).dt.strftime("%d-%b-%Y")
                                                except:
                                                    try:
                                                        ec_record[dt_cols[3]] = ec_record[dt_cols[3]].dt.strftime("%d-%b-%Y")
                                                    except:
                                                        ec_record[dt_cols[3]] = ec_record[dt_cols[3]].apply(lambda x : str(x))
                                            ae_record = ae_record.replace({np.nan : 'blank'})
                                            ec_record = ec_record.replace({np.nan : 'blank'})
                                            piv_rec = {'AE' : ae_record, 'EX' : ec_record}
                                            
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
                                                "query_text": self.get_model_query_text_json(study, sub_cat, params= param_dict),
                                                "form_index": str(ae_record1['form_index']),
                                                "question_present": self.get_subcategory_json(study, sub_cat),
                                                "modif_dts": str(ae_record1['modif_dts']),  
                                                "stg_ck_event_id": int(ae_record1['ck_event_id']),
                                                "formrefname" : str(ae_record1['formrefname']),
                                                "report" : subcate_report_dict,
                                                "confid_score": np.random.uniform(0.7, 0.97),
                                                "cdr_skey": str(ae_record1['cdr_skey']),
                                            }
                                            print(subject, payload)
                                            self.insert_query(study, subject, payload)
                                            if payload not in payload_list:
                                                payload_list.append(payload)
                                            break
                            
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
    #(self.study_id, self.account_id, self.job_id, rule['ml_model_id'], 0.1
    # python AEDR4.py 1 1 2 1 0.1 > AEDR4log.txt
    study_id = sys.argv[1]
    account_id = sys.argv[2]
    job_id = sys.argv[3]
    rule_id = sys.argv[4]
    version = sys.argv[5]
    rule = AEDR4(study_id, account_id, job_id, rule_id, version)
    
    rule.run()
