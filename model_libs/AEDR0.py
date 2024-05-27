# uncompyle6 version 3.8.0
# Python bytecode 3.6 (3379)
# Decompiled from: Python 3.8.10 (v3.8.10:3d8993a744, May  3 2021, 09:09:08) 
# [Clang 12.0.5 (clang-1205.0.22.9)]
# Embedded file name: /users/sdqdev/murugesh/aug_release/karan/pfizer_data_upload/dags/scripts/SubcatModels/model_libs/AEDR0.py
# Compiled at: 2021-08-11 11:08:52
# Size of source mod 2**32: 7711 bytes
import pandas as pd, sys, re, warnings
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

with open(subcat_config_path, 'r') as stream:
    a = yaml.safe_load(stream)

class AEDR0(BaseSDQApi):
    domain_list = [
     'AE', 'EC']

    def execute(self):
        study = self.study_id
        domain_list = [
         'AE', 'EC']
        sub_cat = 'AEDR0'
        #subjects = ['10001010']
        subjects = self.get_subjects(study, domain_list=domain_list, per_page=10000)
        for subject in tqdm.tqdm(subjects):
            print('SUBJECT --> ', subject)
            payload_list = []
            try:
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list=domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)
 
                ae_df = pd.DataFrame(flatten_data['AE'])
                ec_df = pd.DataFrame(flatten_data['EC'])
                for ind in range(ae_df.shape[0]):
                    try:
                        ae_record = ae_df.iloc[[ind]]
                        ecadj_flag = False
                        misdost_flag = False
                        ae_cols = ae_record.columns.tolist()
                        ec_cols = ec_df.columns.tolist()
                        new_ec_df = pd.DataFrame()
                        #import pdb;pdb.set_trace();
                        ae_ec_records = utils.extractor(ae_record, ae_df, ec_df, 'AESPID', 'ECAENO')
                        if (type(ae_ec_records) == tuple) & (type(ae_ec_records) != bool):
                            if len(ae_ec_records[0]) > 0:
                                for ae_formindex, ec_records in ae_ec_records[0].items():
                                    for ec_record in ec_records:
                                        new_ec_df = new_ec_df.append(ec_record, ignore_index=True)

                            else:
                                continue
                        else:
                            continue
                        new_ec = new_ec_df.copy()
                        ecadj_flag, misdost_flag = utils.get_aereason_col(ae_record, ae_df, ec_df)
                        values = [
                         'NOT APPLICABLE', 'UNKNOWN', 'DOSE NOT CHANGED']
                        aetrt_dict = self.get_drugs_dict(study)
                        ec_dict = utils.get_ec_hierarchy(ae_record, new_ec, values, sub_cat, study, aetrt_dict)
                        temp_lst = ['Adverse Events:', 'ADVERSE EVENT(S)', 'ADVERSE EVENT']
                        if bool(ec_dict) == False:
                            continue
                        #import pdb;pdb.set_trace();
                        for i, (drug, records) in enumerate(ec_dict.items()):
                            for ec_ind in range(records.shape[0]):
                                print(subject)
                                try:
                                    record = records.iloc[[ec_ind]]
                                    payload_flag = False
                                    if ecadj_flag and misdost_flag:
                                        if record['ECADJ'].values[0] in temp_lst or record['MISDOST'].values[0] in temp_lst:
                                            payload_flag = True
                                    elif ecadj_flag and not misdost_flag:
                                        if record['ECADJ'].values[0] in temp_lst:
                                            payload_flag = True
                                    else:
                                        if misdost_flag:
                                            if not ecadj_flag:
                                                if record['MISDOST'].values[0] in temp_lst:
                                                    payload_flag = True
                                    if payload_flag:
                                        subcate_report_dict = {}
                                        report_dict = {}
                                        record['ECAENO'] = str(record['ECAENO'].values[0]).replace('[', '').replace(']', '')
                                        ae_record = ae_record.replace({np.nan: 'blank'})
                                        record = record.replace({np.nan: 'blank'})
                                        piv_rec = {'AE':ae_record,  'EC':record}
                                        for dom, cols in a['FIELDS_FOR_UI'][sub_cat].items():
                                            piv_df = piv_rec[dom]
                                            present_col = [col for col in cols if col in piv_df.columns.tolist()]
                                            rep_df = piv_df[present_col]
                                            rep_df['deeplink'] = utils.get_deeplink(study, piv_df)
                                            rep_df = rep_df.rename(columns=(a['FIELD_NAME_DICT']))
                                            rep_df = rep_df.loc[:, ~rep_df.columns.duplicated()]
                                            report_dict[dom] = rep_df.to_json(orient='records')

                                        subcate_report_dict[sub_cat] = report_dict
                                        aespid = ae_record['AESPID'].values[0]
                                        aeterm = ae_record['AETERM'].values[0]
                                        aeacn_item = utils.get_drug_item(drug, sub_cat, study, aetrt_dict)
                                        aeacn = ae_record[aeacn_item].values[0]
                                        ae_record1 = ae_record.iloc[0]
                                        ques_present = {'SRDM':[aeacn],  'INFORM':[aeacn]}
                                        query_text_param = {'AESPID':aespid, 
                                         'AETERM':aeterm, 
                                         'AEACN':aeacn}
                                        payload = {'subcategory':sub_cat,
                                         'cdr_skey':str(ae_record1['cdr_skey']), 
                                         'query_text':self.get_model_query_text_json(study, sub_cat, query_text_param), 
                                         'form_index':str(ae_record1['form_index']), 
                                         'question_present':self.get_subcategory_json(study, sub_cat), 
                                         'modif_dts':str(ae_record1['modif_dts']), 
                                         'stg_ck_event_id':int(ae_record1['ck_event_id']), 
                                         'formrefname':str(ae_record1['formrefname']), 
                                         'report':subcate_report_dict, 
                                         'confid_score':np.random.uniform(0.7, 0.97)}
                                        self.insert_query(study, subject, payload)
                                        if payload not in payload_list:
                                            payload_list.append(payload)


                                except Exception as e:
                                    logging.exception(e)
                                    continue

                    except Exception as e:
                        logging.exception(e)
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
    rule = AEDR0(study_id, account_id, job_id, rule_id, version)
    rule.run()