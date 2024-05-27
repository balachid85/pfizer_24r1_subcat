# uncompyle6 version 3.8.0
# Python bytecode 3.6 (3379)
# Decompiled from: Python 3.8.10 (v3.8.10:3d8993a744, May  3 2021, 09:09:08) 
# [Clang 12.0.5 (clang-1205.0.22.9)]
# Embedded file name: AECM3.py
# Compiled at: 2020-11-10 13:17:27
# Size of source mod 2**32: 7402 bytes
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
a = yaml.load(open(subcat_config_path, 'r'))

class AECM3(BaseSDQApi):
    domain_list = [
     'AE', 'CM']

    def execute(self):
        study = self.study_id
        domain_list = [
         'AE', 'CM']
        sub_cat = 'AECM3'
        index_col = 'itemrepn' if sub_cat.startswith('DR') else 'form_index'
        subjects = self.get_subjects(study, domain_list=domain_list, per_page=10000)
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
                        ae_cm_records = utils.extractor(ae_record, ae_df, cm_df, 'AESPID', 'CMAENO')
                        if (type(ae_cm_records) == tuple) & (type(ae_cm_records) != bool):
                            if len(ae_cm_records[0]) > 0:
                                for ae_formindex, cm_records in ae_cm_records[0].items():
                                    for cm_record in cm_records:
                                        aeongo = ae_record['AEONGO'].values[0]
                                        cm_endt = cm_record['CMENDAT'].values.tolist()[0]
                                        ae_endt = ae_record['AEENDAT'].values.tolist()[0]
                                        aecmgiv = ae_record['AECMGIV'].values[0]
                                        if type(cm_endt) != float:
                                            if cm_endt != 'null':
                                                if type(ae_endt) == float or aeongo.upper() == 'YES':
                                                    if aecmgiv.upper() == 'YES':
                                                        subcate_report_dict = {}
                                                        report_dict = {}
                                                        piv_rec = {'AE':ae_record,  'CM':cm_record}
                                                        for dom, cols in a['FIELDS_FOR_UI'][sub_cat].items():
                                                            piv_df = piv_rec[dom]
                                                            present_col = [col for col in cols if col in piv_df.columns.tolist()]
                                                            rep_df = piv_df[present_col]
                                                            rep_df['deeplink'] = utils.get_deeplink(study, piv_df)
                                                            rep_df = rep_df.rename(columns=(a['FIELD_NAME_DICT']))
                                                            report_dict[dom] = rep_df.to_json(orient='records')

                                                        subcate_report_dict[sub_cat] = report_dict
                                                        aespid = ae_record['AESPID'].values[0]
                                                        ae_stdt = ae_record['AESTDAT'].values[0]
                                                        aeterm = ae_record['AETERM'].values[0]
                                                        formidx = cm_record['form_ix'].values[0]
                                                        cmtrt = cm_record['CMTRT'].values[0]
                                                        cm_stdt = cm_record['CMSTDAT'].values[0]
                                                        cmongo = cm_record['CMONGO'].values[0]
                                                        if ae_endt == np.nan:
                                                            ae_endt = 'blank'
                                            ae_record1 = ae_record.iloc[0]
                                            query_text_param = {'CM_FORMIDX':formidx, 
                                             'CMTRT':cmtrt, 
                                             'AESPID':aespid, 
                                             'AETERM':aeterm, 
                                             'CMSTDAT':cm_stdt, 
                                             'AEENDAT':'blank', 
                                             'AEONGO':aeongo, 
                                             'CMONGO':cmongo, 
                                             'AESTDAT':ae_stdt, 
                                             'CMENDAT':cm_endt}
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
    rule = AECM3(study_id, rule_id, version)
    rule.run()