# uncompyle6 version 3.9.0
# Python bytecode version base 3.6 (3379)
# Decompiled from: Python 3.10.1 (v3.10.1:2cd268a3a9, Dec  6 2021, 14:28:59) [Clang 13.0.0 (clang-1300.0.29.3)]
# Embedded file name: AELB0.py
# Compiled at: 2021-02-26 20:51:39
# Size of source mod 2**32: 9840 bytes
import pandas as pd, sys, re, warnings
warnings.filterwarnings('ignore')
try:
    from scripts.SubcatModels.model_libs.base import BaseSDQApi
    import scripts.SubcatModels.model_libs.utilsk as utils
except:
    from base import BaseSDQApi
    import utilsk as utils

import traceback, tqdm, logging, yaml, numpy as np, os
curr_file_path = os.path.realpath(__file__)
curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
subcat_config_path = os.path.join(curr_path, 'subcate_config.yml')
a = yaml.load(open(subcat_config_path, 'r'))

class AELB0(BaseSDQApi):
    domain_list = [
     'AE', 'LB']

    def execute(self):
        study = self.study_id
        sub_cat = 'AELB0'
        subjects = self.get_subjects(study, domain_list=(self.domain_list), per_page=10000)
        for subject in tqdm.tqdm(subjects):
            payload_list = []
            try:
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list=(self.domain_list))
                if 'AE' not in flatten_data or 'LB' not in flatten_data:
                    continue
                ae_df = pd.DataFrame(flatten_data['AE'])
                ae_df = ae_df[~ae_df['AEPTCD'].isin(['', ' ', np.nan])]
                lb_df = pd.DataFrame(flatten_data['LB'])
                for ind in ae_df.index.tolist():
                    try:
                        ae_record = ae_df.loc[[ind]]
                        code = ae_record['AEPTCD'].values[0]
                        formindex = ae_record['form_index'].values[0]
                        aespid = ae_record['AESPID'].values[0]
                        lb_df['LBTEST'] = lb_df['LBTEST'].str.upper()
                        lb_df = lb_df[lb_df['LBTEST'] != 'HEMOGLOBIN_PX87']
                        tests = lb_df['LBTEST'].unique().tolist()
                        for test in tests:
                            if utils.check_aelab(float(code), test) == True:
                                lb_records = lb_df[lb_df['LBTEST'] == test]
                                lb_records = lb_records[lb_records['LBDAT'].notna()]
                                ae_record['AESTDAT'] = pd.to_datetime((ae_record['AESTDAT']), infer_datetime_format=True)
                                lb_records['LBDAT'] = pd.to_datetime((lb_records['LBDAT']), infer_datetime_format=True)
                                lb_records['DIFFERENCE'] = (ae_record['AESTDAT'].values[0] - lb_records['LBDAT']) / np.timedelta64(1, 'D')
                                diff = lb_records['DIFFERENCE'].unique().tolist()
                                diff1 = [i for i in diff if i >= 0]
                                diff2 = [i for i in diff if i < 0]
                                if len(diff1) > 0:
                                    minimum1 = min(diff1)
                                    final_minimum = minimum1
                                    if len(diff2) > 0:
                                        minimum2 = max(diff2)
                                        final_minimum = min(minimum1, abs(minimum2))
                                        if minimum1 == abs(minimum2):
                                            final_minimum = minimum1
                                        elif final_minimum == abs(minimum2):
                                            pass
                                        final_minimum = -final_minimum
                                elif len(diff2) > 0:
                                    pass
                                minimum2 = max(diff2)
                                final_minimum = minimum2
                                if len(diff1) > 0:
                                    minimum1 = min(diff1)
                                    final_minimum = min(minimum1, abs(minimum2))
                                    if minimum1 == abs(minimum2):
                                        final_minimum = minimum1
                                    elif final_minimum == abs(minimum2):
                                        final_minimum = -final_minimum
                                lb_records = lb_records[lb_records['DIFFERENCE'] == final_minimum]
                                length = len(lb_records)
                                for ind1 in range(lb_records.shape[0]):
                                    try:
                                        lb_record = lb_records.iloc[[ind1]]
                                        result = lb_record['LBORRES'].values[0]
                                        status = lb_record['LBSTAT'].values[0]
                                        low = lb_record['LNMTLOW'].values[0]
                                        high = lb_record['LNMTHIGH'].values[0]
                                        status = lb_record['LBSTAT'].values[0]
                                        payload_flag = False
                                        if status.upper() == 'NOT DONE':
                                            if result in ['', ' ', 'null', np.nan] or isinstance(result, float):
                                                new_lb = lb_records[lb_records['LBSTAT'] == status]
                                                if len(new_lb) == length:
                                                    payload_flag = True
                                        if float(result) >= float(low):
                                            if float(result) <= float(high):
                                                payload_flag = True
                                        if payload_flag:
                                            subcate_report_dict = {}
                                            report_dict = {}
                                            ae_record2 = ae_record
                                            lb_record2 = lb_record
                                            ae_record2['AESTDAT'] = ae_record2['AESTDAT'].dt.strftime('%d-%B-%Y')
                                            lb_record2['LBDAT'] = lb_record2['LBDAT'].dt.strftime('%d-%B-%Y')
                                            piv_rec = {'AE':ae_record2, 
                                             'LB':lb_record2}
                                            for dom, cols in a['FIELDS_FOR_UI'][sub_cat].items():
                                                piv_df = piv_rec[dom]
                                                present_col = [col for col in cols if col in piv_df.columns.tolist()]
                                                rep_df = piv_df[present_col]
                                                rep_df['deeplink'] = utils.get_deeplink(study, piv_df)
                                                rep_df = rep_df.rename(columns=(a['FIELD_NAME_DICT']))
                                                report_dict[dom] = rep_df.to_json(orient='records')

                                            subcate_report_dict[sub_cat] = report_dict
                                            aespid = ae_record2['AESPID'].values[0]
                                            aeterm = ae_record2['AETERM'].values[0]
                                            aestdat = ae_record2['AESTDAT'].values[0]
                                            ae_record1 = ae_record.iloc[0]
                                            keys = {'AESPID':aespid, 
                                             'AETERM':aeterm, 
                                             'AESTDAT':str(aestdat).split('T')[0]}
                                            payload = {'subcategory':sub_cat, 
                                             'query_text':self.get_model_query_text_json(study, sub_cat, params=keys), 
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
                        continue

            except Exception as e:
                logging.exception(e)


if __name__ == '__main__':
    study_id = sys.argv[1]
    rule_id = sys.argv[2]
    version = sys.argv[3]
    rule = AELB0(study_id, rule_id, version)
    rule.run()