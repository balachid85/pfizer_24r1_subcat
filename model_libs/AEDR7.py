import pandas as pd
import sys
import re
import warnings
warnings.filterwarnings('ignore')
try:
    from scripts.SubcatModels.model_libs.base import BaseSDQApi
    import scripts.SubcatModels.model_libs.utils as utils
    import scripts.SubcatModels.model_libs.dosing_utils_v1 as dose_utils
except:
    from base import BaseSDQApi
    import utils as utils
    import dosing_utils_v1 as dose_utils
import traceback
import tqdm
import logging
import os
import yaml
import numpy as np

curr_file_path = os.path.realpath(__file__)
curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
subcat_config_path = os.path.join(curr_path, 'subcate_config.yml')
a = yaml.safe_load(open(subcat_config_path, 'r'))
class AEDR7(BaseSDQApi):
    domain_list = ['AE', 'EC']
    def execute(self):
        study = self.study_id
      
        domain_list = ['AE', 'EC']
        sub_cat = 'AEDR7'
        #index_list = self.get_all_form_index_list(study, domain_list=self.domain_list, per_page=10000)
        index_col = 'itemrepn' if sub_cat.startswith('DR') else 'form_index'
        subjects =self.get_subjects(study, domain_list = domain_list, per_page = 10000)##['10062003'] #
        fmt = utils.format_datetime
        for subject in tqdm.tqdm(subjects):
            payload_list = []
            try:
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list = domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)
                
                ae_df = pd.DataFrame(flatten_data['AE'])
                ec_df = pd.DataFrame(flatten_data['EC'])
                ec_df['ECSTDAT'] = ec_df['ECSTDAT'].apply(utils.get_date)
                ec_df['ECENDAT'] = ec_df['ECENDAT'].apply(utils.get_date)
                if len(ec_df.form_index.unique())>1:
                    index_col='form_ix'
                    index_idfier='form_index'
                    index_flag=True
                else:
                    index_flag=False
                    index_col='itemset_ix'
                    index_idfier='itemrepn'    
                #ae_df = ae_df[ae_df[index_col].isin(index_list)]
                for ind in range(ae_df.shape[0]):
                    try:
                        ae_record = ae_df.iloc[[ind]]

                        ecadj_flag = False
                        misdost_flag = False
                        
                        curr_ec_len = -1
                        flag = -1

                        ae_cols = ae_record.columns.tolist()
                        ec_cols = ec_df.columns.tolist()
                        new_ec = pd.DataFrame()
                        ae_ec_records = utils.extractor(ae_record, ae_df, ec_df, 'AESPID', 'ECAENO')
                        if (type(ae_ec_records) == tuple) & (type(ae_ec_records) != bool): 
                            if len(ae_ec_records[0]) > 0:  
                                for ae_formindex, ec_records in ae_ec_records[0].items():
                                    
                                    for ec_record in ec_records:
                                        
                                        new_ec = new_ec.append(ec_record, ignore_index = True)
                                        print(new_ec)

                            else:
                                continue
                        else:
                            continue
                        new_ec = new_ec.copy()
                        
                        ecadj_flag, misdost_flag = utils.get_aereason_col(ae_record, ae_df, new_ec)
                        dose_column = utils.get_dose_column(new_ec)
        
                        aest_dat = ae_record['AESTDAT'].apply(utils.get_date)
                        aeen_dat =  ae_record['AEENDAT'].apply(utils.get_date)
#                         new_ec = ec_df.copy()
#                         new_ec['ECSTDAT'] = new_ec['ECSTDAT'].apply(utils.get_date)
#                         new_ec['ECENDAT'] = new_ec['ECENDAT'].apply(utils.get_date)
#                         ec_df['ECSTDAT'] = ec_df['ECSTDAT'].apply(utils.get_date)
#                         ec_df['ECENDAT'] = ec_df['ECENDAT'].apply(utils.get_date) 
                        new_ec = new_ec.sort_values(['ECSTDAT', 'ECENDAT'])
                        
                        values = ['DOSE REDUCED']
                        aetrt_dict = self.get_drugs_dict(study)
                        
                        ec_dict = utils.get_ec_hierarchy(ae_record, new_ec, values, sub_cat, study, aetrt_dict)
                        temp_lst = ['Adverse Events:', 'ADVERSE EVENT(S)', 'ADVERSE EVENT'] 
                        
                        if bool(ec_dict) == False:
                            continue
                   
                        for i, (drug, records) in enumerate(ec_dict.items()):
                            new_ec = records 
                            new_ec = new_ec[(new_ec['ECSTDAT'] > aest_dat.values[0]) & (new_ec['ECSTDAT'] < aeen_dat.values[0])]
                            prev_ec_len = len(new_ec)
                            temp_lst = ['Adverse Events:', 'ADVERSE EVENT(S)', 'ADVERSE EVENT'] 
                            print(ecadj_flag,misdost_flag)
                            if ecadj_flag and misdost_flag:
                                new_ec1 = new_ec[new_ec['ECADJ'].isin(temp_lst)]
                                new_ec2 = new_ec[new_ec['MISDOST'].isin(temp_lst)]
                                
                                if len(new_ec1) > len(new_ec2):
                                    curr_ec_len = len(new_ec1)
                                elif len(new_ec2) > len(new_ec1):
                                    curr_ec_len = len(new_ec2)
                            elif ecadj_flag and not misdost_flag:
                                new_ec = new_ec[new_ec['ECADJ'].isin(temp_lst)] 
                                curr_ec_len = len(new_ec)
                            elif misdost_flag and not ecadj_flag:
                                new_ec = new_ec[new_ec['MISDOST'].isin(temp_lst)]
                                curr_ec_len = len(new_ec)
                            #changeing to ec_df from new_ec since the dose is should be compared to all the dose
#                             dose_values = ec_df[dose_column].values.tolist()
                            dose_uniques = ec_df[dose_column].unique().tolist()
                            dose_values = new_ec[dose_column].values.tolist()
                            
                            #changing the loop since the condidering the non zero dose
                            for  i in range(new_ec.shape[0]):
                                flag=-1
                                ec_dose_rec=new_ec.iloc[[i]] 
                                formindex={index_idfier:ec_dose_rec[index_idfier].values[0]}  
                                ecst_dat = ec_dose_rec['ECSTDAT'].iloc[0]
                                ecend_dat = ec_dose_rec['ECENDAT'].iloc[0]
                                
                                if float(ec_dose_rec[dose_column].values[0]) in [0,0.0]:
                                    
                                    dose_flag=True
                                    dose_info=None
                                    
                                else:
                                    dose_flag, dose_info = dose_utils.get_dose_values_v1_aedr7(ec_df, formindex, ecst_dat, ecend_dat)
                                

                                
                                print(subject)
                                if (dose_flag):
                                    if (dose_info):
                            
                                        *dose_values, dose_col, prev_itemsetix, prev_visitnm = dose_info
                                        current_dose_value, previous_dose_value = tuple(map(float, dose_values))
                                        if (prev_ec_len == curr_ec_len) and prev_ec_len != 0 and curr_ec_len != 0 and len(new_ec) >= 1:
            #                                     print(i)

                                                if ((float(previous_dose_value) <= float(current_dose_value)) and float(current_dose_value) != 0) or (len(dose_uniques) == 1):
            #                                         print(dose_values[i],dose_to_compare)
                                                    flag = 1
            #                                     else:
            #                                         dose_to_compare=dose_values[i]
            #                                         flag = 0
            #                                         continue
            #                                         break
                                    else:
                                        flag=1
                                    
                                    if flag == 1:
                                        
                                        subcate_report_dict = {}
                                        report_dict = {}

    #                                         new_ec['ECSTDAT'] = new_ec['ECSTDAT'].dt.strftime("%d-%b-%Y")
                                        if index_flag :

                                            if  len(dose_values)==1:
                                                new_ec1 = ec_df[(ec_df.form_index==(ec_dose_rec['form_index'].values[0]))]
                                            else:
                                                new_ec1 = ec_df[(ec_df.form_index==(ec_dose_rec['form_index'].values[0]))]
                                        else:
                                            if  len(dose_values)==1:
                                                new_ec1 = ec_df[ec_df.itemrepn==(ec_dose_rec['itemrepn'].values[0])]
                                            else:
                                                new_ec1 = ec_df[ec_df.itemrepn==(ec_dose_rec['itemrepn'].values[0])]

    #                                         ecendat = new_ec['ECENDAT'].values[0]
    # #                                         if isinstance(ecendat, float) == False:
                                        new_ec1['ECENDAT'] = new_ec1['ECENDAT'].apply(fmt)
                                        new_ec1['ECSTDAT'] = new_ec1['ECSTDAT'].apply(fmt)
                                        ae_record = ae_record.replace({np.nan : 'blank'})
                                        new_ec = new_ec.replace({np.nan : 'blank'})
                                        piv_rec = {'AE' : ae_record, 'EC' : new_ec1}
                                        for dom, cols in a['FIELDS_FOR_UI'][sub_cat].items():
                                            piv_df = piv_rec[dom]
                                            present_col = [col for col in cols if col in piv_df.columns.tolist()]
                                            rep_df = piv_df[present_col]
                                            rep_df['deeplink'] = utils.get_deeplink(study, piv_df)
                                            rep_df = rep_df.rename(columns= a['FIELD_NAME_DICT'])
                                            rep_df = rep_df.loc[:,~rep_df.columns.duplicated()]
                                            report_dict[dom]= rep_df.to_json(orient= 'records')

                                        subcate_report_dict[sub_cat] =  report_dict

                                        aespid = ae_record['AESPID'].values[0]
                                        aeterm = ae_record['AETERM'].values[0]
                                        aeacn_item = utils.get_drug_item(drug, sub_cat, study, aetrt_dict)
                                        aeacn = ae_record[aeacn_item].values[0]


                                        ae_record1 = ae_record.iloc[0]
                                        keys = {
                                            'AESPID' : aespid,
                                            'AETERM' : aeterm,
                                            'AEACN' : aeacn,
                                            'ECTRT' : drug
                                        }


                                        payload = {
                                            "subcategory": sub_cat,
                                            'cdr_skey':str(ae_record1['cdr_skey']),
                                            "query_text": self.get_model_query_text_json(study, sub_cat, params = keys),
                                            "form_index": str(ae_record1['form_index']),
                                            "question_present": self.get_subcategory_json(study, sub_cat),
                                            "modif_dts": str(ae_record1['modif_dts']),  
                                            "stg_ck_event_id": int(ae_record1['ck_event_id']),
                                            "formrefname" : str(ae_record1['formrefname']),
                                            "report" : subcate_report_dict
                                        }

                                        self.insert_query(study, subject, payload)
                                        print(subject,payload)
                                        if payload not in payload_list:
                                            payload_list.append(payload)
                    except:
                        print(traceback.format_exc())
                        continue       

            except Exception as e:
                print(traceback.format_exc())
                logging.exception(e)
                                                
            if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                self.close_query(subject, payload_list)

if __name__ == '__main__':
    study_id = sys.argv[1]
    account_id = sys.argv[2]
    job_id = sys.argv[3]
    rule_id = sys.argv[4]
    version = sys.argv[5]
    rule = AEDR7(study_id, account_id, job_id, rule_id, version)
    rule.run()
