# uncompyle6 version 3.8.0
# Python bytecode 3.6 (3379)
# Decompiled from: Python 3.8.10 (v3.8.10:3d8993a744, May  3 2021, 09:09:08) 
# [Clang 12.0.5 (clang-1205.0.22.9)]
# Embedded file name: AECM0.py
# Compiled at: 2021-04-19 22:23:58
# Size of source mod 2**32: 5084 bytes
import pandas as pd, sys, re, warnings
warnings.filterwarnings('ignore')
try:
    from scripts.SubcatModels.model_libs.base import BaseSDQApi
    import scripts.SubcatModels.model_libs.utils as utils
except:
    from base import BaseSDQApi
    import utils

import traceback, tqdm, numpy as np, logging, yaml, os
curr_file_path = os.path.realpath(__file__)
curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
subcat_config_path = os.path.join(curr_path, 'subcate_config.yml')
a = yaml.safe_load(open(subcat_config_path, 'r'))

class AECM0(BaseSDQApi):
    domain_list = [
     'AE', 'CM']

    def execute(self):
        study = self.study_id
        domain_list = [
         'AE', 'CM']
        sub_cat = 'AECM0'
        index_col = 'itemrepn' if sub_cat.startswith('DR') else 'form_index'
        subjects = self.get_subjects(study, domain_list=domain_list, per_page=10000)
        print(subjects)
        for subject in tqdm.tqdm(subjects):
            payload_list = []
            try:
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list=domain_list)
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
                                # import pdb; pdb.set_trace()
                                for ind1 in range(cm_df.shape[0]):
                                    cm_record = cm_df.iloc[[ind1]]
                                    aecmgiv = ae_record['AECMGIV'].values[0]
                                    if aecmgiv.upper() == 'YES':
                                        subcate_report_dict = {}
                                        report_dict = {}
                                        ae_record['AESTDAT'] = ae_record['AESTDAT'].apply(utils.format_datetime)
                                        ae_record['AEENDAT'] = ae_record['AEENDAT'].apply(utils.format_datetime)
                                        piv_rec = {'AE': ae_record}
                                        for dom, cols in a['FIELDS_FOR_UI'][sub_cat].items():
                                            piv_df = piv_rec[dom]
                                            present_col = [col for col in cols if col in piv_df.columns.tolist()]
                                            rep_df = piv_df[present_col]
                                            rep_df = rep_df.rename(columns=(a['FIELD_NAME_DICT']))
                                            rep_df['deeplink'] = utils.get_deeplink(study, piv_df)
                                            report_dict[dom] = rep_df.to_json(orient='records')

                                        subcate_report_dict[sub_cat] = report_dict
                                        aespid = ae_record['AESPID'].values[0]
                                        aeterm = ae_record['AETERM'].values[0]
                                        ae_record1 = ae_record.iloc[0]
                                        query_text_param = {'AESPID':aespid, 
                                         'AETERM':aeterm}
                                        payload = {'subcategory':sub_cat,
                                         "cdr_skey" : str(ae_record1['cdr_skey']), 
                                         'query_text':self.get_model_query_text_json(study, sub_cat, params=query_text_param), 
                                         'form_index':str(ae_record1['form_index']), 
                                         'question_present':self.get_subcategory_json(study, sub_cat), 
                                         'modif_dts':str(ae_record1['modif_dts']), 
                                         'stg_ck_event_id':int(ae_record1['ck_event_id']), 
                                         'formrefname':str(ae_record1['formrefname']), 
                                         'report':subcate_report_dict, 
                                         'confid_score':np.random.uniform(0.7, 0.97)}
                                        print(subject, payload)
                                        self.insert_query(study, subject, payload)
                                        if payload not in payload_list:
                                            payload_list.append(payload)
                                        break
                                        

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
    rule = AECM0(study_id, account_id, job_id, rule_id, version)
    rule.run()