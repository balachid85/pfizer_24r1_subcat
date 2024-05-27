# uncompyle6 version 3.8.0
# Python bytecode 3.6 (3379)
# Decompiled from: Python 3.8.10 (v3.8.10:3d8993a744, May  3 2021, 09:09:08) 
# [Clang 12.0.5 (clang-1205.0.22.9)]
# Embedded file name: /users/sdqdev/gowtham/pfizer_data_upload/dags/scripts/SubcatModels/model_libs/AEDR5.py
# Compiled at: 2021-10-22 13:38:06
# Size of source mod 2**32: 11453 bytes
import pandas as pd, sys, re, datetime
from dateutil.relativedelta import relativedelta
import warnings
warnings.filterwarnings('ignore')
try:
    from scripts.SubcatModels.model_libs.base import BaseSDQApi
    import scripts.SubcatModels.model_libs.utils as utils
except:
    from base import BaseSDQApi
    import utils

import traceback, tqdm, logging, yaml, numpy as np, os
curr_file_path = os.path.realpath(__file__)
curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
subcat_config_path = os.path.join(curr_path, 'subcate_config.yml')
subcate_config = yaml.safe_load(open(subcat_config_path, 'r'))

class AEDR5(BaseSDQApi):
    domain_list = [
     'AE', 'EC']

    def execute(self):
        study = self.study_id
        domain_list = [
         'AE', 'EC']
        sub_cat = 'AEDR5'
        index_col = 'itemrepn' if sub_cat.startswith('DR') else 'form_index'
        #subjects =['10021002'] #
        subjects = self.get_subjects(study, domain_list=domain_list, per_page=10000) #['40010127']#
        #import pdb;pdb.set_trace()
        for subject in tqdm.tqdm(subjects):
            payload_list = []
            try:
                # print(study, subject, domain_list)
                flatten_data = self.get_flatten_data(study, subject, domain_list=domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)

                if 'AE' not in flatten_data or 'EC' not in flatten_data:
                    continue
                else:
                    if len(flatten_data['AE'])==0 or len(flatten_data['EC'])==0:
                        continue
                        
                ae_df = pd.DataFrame(flatten_data['AE'])
                ec_df = pd.DataFrame(flatten_data['EC'])
                for ind in range(ae_df.shape[0]):
                    try:
                        ae_record = ae_df.iloc[[ind]]
                        ecadj_flag = False
                        misdost_flag = False
                        curr_ec_len = -1
                        ae_cols = ae_record.columns.tolist()
                        ec_cols = ec_df.columns.tolist()
                        new_ec = pd.DataFrame()
                        ae_ec_records = utils.extractor(ae_record, ae_df, ec_df, 'AESPID', 'ECAENO')
                        if (type(ae_ec_records) == tuple) & (type(ae_ec_records) != bool):
                            if len(ae_ec_records[0]) > 0:
                                for ae_formindex, ec_records in ae_ec_records[0].items():
                                    for ec_record in ec_records:
                                        new_ec = new_ec.append(ec_record, ignore_index=True)

                            else:
                                continue
                        else:
                            continue
                        new_ec = new_ec.copy()
                        ecadj_flag, misdost_flag = utils.get_aereason_col(ae_record, ae_df, new_ec)
                        if study.upper() in ('B7451015', 'C1061011'):
                            new_ec = new_ec[(new_ec['ECTRT'] == 'BLINDED THERAPY')]
                        new_ec['ECSTDAT'] = new_ec['ECSTDAT'].apply(utils.get_date)
                        new_ec = new_ec.sort_values(['ECSTDAT'])
                        aest_dat = ae_record['AESTDAT'].apply(utils.get_date)
                        aeongo = ae_record['AEONGO'].values[0]
                        values = [
                         'DOSE REDUCED', 'DRUG INTERRUPTED']
                        temp_lst = ['Adverse Events:', 'ADVERSE EVENT(S)', 'ADVERSE EVENT']
                        aetrt_dict = self.get_drugs_dict(study)
                        ec_dict = utils.get_ec_hierarchy(ae_record, new_ec, values, sub_cat, study, aetrt_dict)
                        #import pdb;pdb.set_trace()
                        if bool(ec_dict) == False:
                            continue
                        for i, (drug, records) in enumerate(ec_dict.items()):
                            for ec_ind in range(records.shape[0]):
                                #print(subject,"*********")
                                record = records.iloc[[ec_ind]]
                                stdt_flag = False
                                endt_flag = False
                                first_ec = record
                                prev_ec_len = len(first_ec)
                                if ecadj_flag and misdost_flag:
                                    new_ec1 = first_ec[first_ec['ECADJ'].isin(temp_lst)]
                                    new_ec2 = first_ec[first_ec['MISDOST'].isin(temp_lst)]
                                    if len(new_ec1) > len(new_ec2):
                                        curr_ec_len = len(new_ec1)
                                    else:
                                        if len(new_ec2) > len(new_ec1):
                                            curr_ec_len = len(new_ec2)
                                else:
                                    if ecadj_flag:
                                        if not misdost_flag:
                                            new_ec1 = first_ec[first_ec['ECADJ'].isin(temp_lst)]
                                            curr_ec_len = len(new_ec1)
                                if misdost_flag:
                                    if not ecadj_flag:
                                        new_ec1 = first_ec[first_ec['MISDOST'].isin(temp_lst)]
                                        curr_ec_len = len(new_ec1)
                                ecstdat = first_ec['ECSTDAT'].values[0]
                                if aest_dat.values[0] > ecstdat:
                                    stdt_flag = True
                                if stdt_flag == False:
                                    if type(aeongo) == str and aeongo.upper() == 'NO' or int(aeongo) == 0:
                                        aeen_dat = ae_record['AEENDAT'].apply(utils.get_date)
                                        print(aeen_dat,ecstdat)
                                        if aeen_dat.values[0] < ecstdat:
                                            endt_flag = True
                                
                                if stdt_flag or endt_flag:
                                    if curr_ec_len == prev_ec_len:
                                        if curr_ec_len != 0:
                                            if prev_ec_len != 0:
                                                subcate_report_dict = {}
                                                report_dict = {}
                                                ae_record1 = ae_record
                                                first_ec['ECSTDAT'] = first_ec['ECSTDAT'].dt.strftime('%d-%b-%Y')
                                                if type(aeongo) == str and aeongo.upper() == 'NO' or int(aeongo) == 0:
                                                    ae_record1['AEENDAT'] = ae_record1['AEENDAT'].astype(str)
                                                    ae_record1['AEENDAT'] = ae_record1['AEENDAT'].apply(utils.get_date)
                                                    ae_record1['AEENDAT'] = ae_record1['AEENDAT'].dt.strftime('%d-%b-%Y')
                                                if stdt_flag:
                                                    aestdat1 = ae_record1['AESTDAT'].values[0]
                                                    option = f"AE Start Date {aestdat1} is after the dose change date"
                                                if endt_flag:
                                                    aeendat1 = ae_record1['AEENDAT'].values[0]
                                                    option = f"AE End Date {aeendat1} is prior to the dose change date"
                                    ae_record1 = ae_record1.replace({np.nan: 'blank'})
                                    first_ec = first_ec.replace({np.nan: 'blank'})
                                    first_ec['ECAENO'] = str(first_ec['ECAENO'].values[0]).replace('[', '').replace(']', '')
                                    piv_rec = {'AE':ae_record1, 
                                     'EC':first_ec.head(1)}
                                    for dom, cols in subcate_config['FIELDS_FOR_UI'][sub_cat].items():
                                        #print(subject,"second for")
                                        piv_df = piv_rec[dom]
                                        present_col = [col for col in cols if col in piv_df.columns.tolist()]
                                        rep_df = piv_df[present_col]
                                        rep_df['deeplink'] = utils.get_deeplink(study, piv_df)
                                        rep_df = rep_df.rename(columns=(subcate_config['FIELD_NAME_DICT']))
                                        rep_df = rep_df.loc[:, ~rep_df.columns.duplicated()]
                                        report_dict[dom] = rep_df.to_json(orient='records')

                                    subcate_report_dict[sub_cat] = report_dict
                                    aespid = ae_record['AESPID'].values[0]
                                    aeterm = ae_record['AETERM'].values[0]
                                    aeacn_item = utils.get_drug_item(drug, sub_cat, study, aetrt_dict)
                                    aeacn = ae_record[aeacn_item].values[0]
                                    visit_nm = first_ec['visit_nm'].values[0]
                                    ae_record1 = ae_record.iloc[0]
                                    query_text_param = {'AESPID':aespid, 
                                     'AETERM':aeterm, 
                                     'AEACN':aeacn, 
                                     'visit_nm':visit_nm, 
                                     'option':option}
                                    payload = {'subcategory':sub_cat,
                                     'cdr_skey':str(ae_record1['cdr_skey']), 
                                     'query_text':self.get_model_query_text_json(study, sub_cat, params=query_text_param), 
                                     'form_index':str(ae_record1['form_index']), 
                                     'question_present':self.get_subcategory_json(study, sub_cat), 
                                     'modif_dts':str(ae_record1['modif_dts']), 
                                     'stg_ck_event_id':int(ae_record1['ck_event_id']), 
                                     'formrefname':str(ae_record1['formrefname']), 
                                     'report':subcate_report_dict, 
                                     'confid_score':np.random.uniform(0.7, 0.97)}
                                    #print(payload)
                                    self.insert_query(study, subject, payload)
                                    if payload not in payload_list:
                                        payload_list.append(payload)


                    except Exception as exp:
                        logging.exception(exp)
                        continue

            except Exception as e:
                logging.exception(e)
                continue


            if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                self.close_query(subject, payload_list)

if __name__ == '__main__':
    study_id = sys.argv[1]
    account_id = sys.argv[2]
    job_id = sys.argv[3]
    rule_id = sys.argv[4]
    version = sys.argv[5]
    rule = AEDR5(study_id, account_id, job_id, rule_id, version)
    rule.run()