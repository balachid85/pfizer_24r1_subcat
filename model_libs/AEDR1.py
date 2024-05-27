# uncompyle6 version 3.8.0
# Python bytecode 3.6 (3379)
# Decompiled from: Python 3.8.10 (v3.8.10:3d8993a744, May  3 2021, 09:09:08) 
# [Clang 12.0.5 (clang-1205.0.22.9)]
# Embedded file name: /users/sdqdev/gowtham/pfizer_data_upload/dags/scripts/SubcatModels/model_libs/AEDR1.py
# Compiled at: 2021-10-21 17:46:25
# Size of source mod 2**32: 7799 bytes
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
subcate_config = yaml.safe_load(open(subcat_config_path, 'r'))

class AEDR1(BaseSDQApi):
    domain_list = [
     'AE', 'EC']

    def execute(self):
        study = self.study_id
        domain_list = [
         'AE', 'EC']
        sub_cat = 'AEDR1'
        index_col = 'itemrepn' if sub_cat.startswith('DR') else 'form_index'
        subjects = self.get_subjects(study, domain_list=(self.domain_list), per_page=10000)
        for subject in tqdm.tqdm(subjects):
            payload_list = []
            try:
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list=(self.domain_list))
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
                        curr_ec_len = -1
                        ae_cols = ae_record.columns.tolist()
                        ec_cols = ec_df.columns.tolist()
                        aest_dat = ae_record['AESTDAT'].apply(utils.get_date)
                        aeen_dat = ae_record['AEENDAT'].apply(utils.get_date)
                        new_ec = ec_df.copy()
                        new_ec['ECSTDAT'] = new_ec['ECSTDAT'].apply(utils.get_date)
                        new_ec['ECENDAT'] = new_ec['ECENDAT'].apply(utils.get_date)
                        new_ec = new_ec.sort_values(['ECSTDAT', 'ECENDAT'])
                        new_ec = new_ec[(new_ec['ECSTDAT'].notna() & new_ec['ECENDAT'].notna())]
                        if study.upper() in ('B7451015', 'C1061011'):
                            new_ec = new_ec[(new_ec['ECTRT'] == 'BLINDED THERAPY')]
                        values = ['NOT APPLICABLE']
                        aetrt_dict = self.get_drugs_dict(study)
                        ec_dict = utils.get_ec_hierarchy(ae_record, new_ec, values, sub_cat, study, aetrt_dict)
                        if bool(ec_dict) == False:
                            continue
                        for i, (drug, records) in enumerate(ec_dict.items()):
                            new_ec = records
                            payload_flag = False
                            if len(new_ec) > 1:
                                first_ec = new_ec.head(1)
                                last_ec = new_ec.tail(1)
                                ecstdat = first_ec['ECSTDAT'].values[0]
                                ecendat = last_ec['ECENDAT'].values[0]
                                if aest_dat.values[0] >= ecstdat:
                                    if aeen_dat.values[0] <= ecendat:
                                        ec = first_ec
                                        payload_flag = True
                            else:
                                if len(new_ec) == 1:
                                    first_ec = new_ec.head(1)
                                    ecstdat = first_ec['ECSTDAT'].values[0]
                                    ecendat = first_ec['ECENDAT'].values[0]
                                    if aest_dat.values[0] >= ecstdat:
                                        if aeen_dat.values[0] <= ecendat:
                                            ec = first_ec
                                            payload_flag = True
                            if payload_flag:
                                subcate_report_dict = {}
                                report_dict = {}
                                ec['ECSTDAT'] = ec['ECSTDAT'].dt.strftime('%d-%b-%Y')
                                ec = ec.tail(1)
                                ecendat = ec['ECENDAT'].values[0]
                                if isinstance(ecendat, float) == False:
                                    ec['ECENDAT'] = ec['ECENDAT'].dt.strftime('%d-%b-%Y')
                                ae_record = ae_record.replace({np.nan: 'blank'})
                                ec = ec.replace({np.nan: 'blank'})
                                piv_rec = {'AE':ae_record,  'EC':ec}
                                for dom, cols in subcate_config['FIELDS_FOR_UI'][sub_cat].items():
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
                                ae_record1 = ae_record.iloc[0]
                                query_text_param = {'AESPID':aespid, 
                                 'AETERM':aeterm, 
                                 'AEACN':aeacn}
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
                                self.insert_query(study, subject, payload)
                                if payload not in payload_list:
                                    payload_list.append(payload)

                    except Exception as e:
                        logging.exception(e)
                        continue

            except Exception as e:
                logging.exception(e)


            if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                self.close_query(subject, payload_list)

if __name__ == '__main__':
    study_id = sys.argv[1]
    rule_id = sys.argv[2]
    version = sys.argv[3]
    rule = AEDR1(study_id, rule_id, version)
    rule.run()