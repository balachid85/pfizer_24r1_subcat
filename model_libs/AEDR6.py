# uncompyle6 version 3.8.0
# Python bytecode 3.6 (3379)
# Decompiled from: Python 3.8.10 (v3.8.10:3d8993a744, May  3 2021, 09:09:08) 
# [Clang 12.0.5 (clang-1205.0.22.9)]
# Embedded file name: AEDR6.py
# Compiled at: 2021-05-10 16:21:51
# Size of source mod 2**32: 8445 bytes
import pandas as pd, sys, re, warnings
warnings.filterwarnings('ignore')
try:
    from scripts.SubcatModels.model_libs.base import BaseSDQApi
    import scripts.SubcatModels.model_libs.utils as utils
except:
    from base import BaseSDQApi
    import utils

import traceback, tqdm, logging, os, yaml, numpy as np
curr_file_path = os.path.realpath(__file__)
curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
subcat_config_path = os.path.join(curr_path, 'subcate_config.yml')
a = yaml.load(open(subcat_config_path, 'r'))

class AEDR6(BaseSDQApi):
    domain_list = [
     'AE', 'EC']

    def execute(self):
        study = self.study_id
        domain_list = [
         'AE', 'EC']
        sub_cat = 'AEDR6'
        index_col = 'itemrepn' if sub_cat.startswith('DR') else 'form_index'
        subjects = self.get_subjects(study, domain_list=domain_list, per_page=10000)
        for subject in tqdm.tqdm(subjects):
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
                        curr_ec_len = -1
                        flag = -1
                        ae_cols = ae_record.columns.tolist()
                        ec_cols = ec_df.columns.tolist()
                        ecadj_flag, misdost_flag = utils.get_aereason_col(ae_record, ae_df, ec_df)
                        dose_column = utils.get_dose_column(ec_df)
                        aest_dat = ae_record['AESTDAT'].apply(utils.get_date)
                        aeen_dat = ae_record['AEENDAT'].apply(utils.get_date)
                        new_ec = ec_df.copy()
                        new_ec['ECSTDAT'] = new_ec['ECSTDAT'].apply(utils.get_date)
                        new_ec['ECENDAT'] = new_ec['ECENDAT'].apply(utils.get_date)
                        new_ec = new_ec.sort_values(['ECSTDAT', 'ECENDAT'])
                        values = [
                         'DOSE INCREASED']
                        aetrt_dict = self.get_drugs_dict(study)
                        ec_dict = utils.get_ec_hierarchy(ae_record, new_ec, values, sub_cat, study, aetrt_dict)
                        temp_lst = ['Adverse Events:', 'ADVERSE EVENT(S)', 'ADVERSE EVENT']
                        if bool(ec_dict) == False:
                            continue
                        for i, (drug, records) in enumerate(ec_dict.items()):
                            new_ec = records
                            new_ec = new_ec[((new_ec['ECSTDAT'] > aest_dat.values[0]) & (new_ec['ECSTDAT'] < aeen_dat.values[0]))]
                            prev_ec_len = len(new_ec)
                            temp_lst = [
                             'Adverse Events:', 'ADVERSE EVENT(S)', 'ADVERSE EVENT']
                            if ecadj_flag and misdost_flag:
                                new_ec1 = new_ec[new_ec['ECADJ'].isin(temp_lst)]
                                new_ec2 = new_ec[new_ec['MISDOST'].isin(temp_lst)]
                                if len(new_ec1) > len(new_ec2):
                                    curr_ec_len = len(new_ec1)
                                else:
                                    if len(new_ec2) > len(new_ec1):
                                        curr_ec_len = len(new_ec2)
                            else:
                                if ecadj_flag:
                                    if not misdost_flag:
                                        new_ec = new_ec[new_ec['ECADJ'].isin(temp_lst)]
                                        curr_ec_len = len(new_ec)
                            if misdost_flag:
                                if not ecadj_flag:
                                    new_ec = new_ec[new_ec['MISDOST'].isin(temp_lst)]
                                    curr_ec_len = len(new_ec)
                            dose_values = new_ec[dose_column].values.tolist()
                            if prev_ec_len == curr_ec_len:
                                if prev_ec_len != 0:
                                    if curr_ec_len != 0:
                                        if len(new_ec) > 1:
                                            for i in range(len(dose_values) - 1):
                                                if dose_values[(i + 1)] > dose_values[i]:
                                                    if dose_values[i] != 0:
                                                        flag = 0
                                                        continue
                                                flag = 1
                                                break

                            if flag == 1:
                                subcate_report_dict = {}
                                report_dict = {}
                                new_ec['ECSTDAT'] = new_ec['ECSTDAT'].dt.strftime('%d-%b-%Y')
                                new_ec = new_ec.head(1)
                                ecendat = new_ec['ECENDAT'].values[0]
                                if isinstance(ecendat, float) == False:
                                    new_ec['ECENDAT'] = new_ec['ECENDAT'].dt.strftime('%d-%b-%Y')
                                ae_record = ae_record.replace({np.nan: 'blank'})
                                new_ec = new_ec.replace({np.nan: 'blank'})
                                piv_rec = {'AE':ae_record,  'EC':new_ec}
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
                                keys = {'AESPID':aespid, 
                                 'AETERM':aeterm, 
                                 'AEACN':aeacn}
                                payload = {'subcategory':sub_cat,
                                 'cdr_skey':str(ae_record1['cdr_skey']), 
                                 'query_text':self.get_model_query_text_json(study, sub_cat, params=keys), 
                                 'form_index':str(ae_record1['form_index']), 
                                 'question_present':self.get_subcategory_json(study, sub_cat), 
                                 'modif_dts':str(ae_record1['modif_dts']), 
                                 'stg_ck_event_id':int(ae_record1['ck_event_id']), 
                                 'formrefname':str(ae_record1['formrefname']), 
                                 'report':subcate_report_dict}
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
    rule_id = sys.argv[2]
    version = sys.argv[3]
    rule = AEDR6(study_id, rule_id, version)
    rule.run()