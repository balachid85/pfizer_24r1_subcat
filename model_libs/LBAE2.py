# uncompyle6 version 3.9.0
# Python bytecode version base 3.6 (3379)
# Decompiled from: Python 3.10.1 (v3.10.1:2cd268a3a9, Dec  6 2021, 14:28:59) [Clang 13.0.0 (clang-1300.0.29.3)]
# Embedded file name: LBAE2.py
# Compiled at: 2020-11-10 13:17:27
# Size of source mod 2**32: 13463 bytes
import pandas as pd, sys, re, warnings
warnings.filterwarnings('ignore')
try:
    from scripts.SubcatModels.model_libs.base import BaseSDQApi
    import scripts.SubcatModels.model_libs.utilsk as utils
    from scripts.SubcatModels.model_libs.ctc_grades import *
except:
    from base import BaseSDQApi
    import utilsk as utils
    from ctc_grades import *

import traceback, tqdm, logging, numpy as np, yaml, os
curr_file_path = os.path.realpath(__file__)
curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
grades_path = os.path.join(curr_path, 'grade_lookup.yaml')
grades = yaml.load((open(grades_path, 'rb')), Loader=(yaml.FullLoader))
subcat_config_path = os.path.join(curr_path, 'subcate_config.yml')
a = yaml.load(open(subcat_config_path, 'r'))

class LBAE2(BaseSDQApi):
    domain_list = [
     'AE', 'LB']

    def execute(self):
        study = self.study_id
        sub_cat = 'LBAE2'
        subjects = self.get_subjects(study, domain_list=(self.domain_list), per_page=10000)
        index_col = 'itemrepn' if sub_cat.startswith('LB') else 'form_index'
        for subject in tqdm.tqdm(subjects):
            payload_list = []
            try:
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list=(self.domain_list))
                if 'AE' not in flatten_data or 'LB' not in flatten_data:
                    continue
                ae_df = pd.DataFrame(flatten_data['AE'])
                ae_df = ae_df[~ae_df['AEPTCD'].isin(['', ' ', np.nan])]
                codes = ae_df['AEPTCD'].unique().tolist()
                lb_df = pd.DataFrame(flatten_data['LB'])
                lb_df['LBTEST'] = lb_df['LBTEST'].str.upper()
                lb_df = lb_df[lb_df['LBTEST'] != 'HEMOGLOBIN_PX87']
                tests = lb_df['LBTEST'].unique().tolist()
                for test in tests:
                    for code in codes:
                        try:
                            if utils.check_aelab(float(code), test) == True:
                                lb_df1 = lb_df[lb_df['LBTEST'] == test]
                                for ind in range(lb_df1.shape[0]):
                                    try:
                                        ae_records = ae_df
                                        lb_record = lb_df1.iloc[[ind]]
                                        test = lb_record['LBTEST'].values[0]
                                        for code in codes:
                                            if utils.check_aelab(float(code), test) == True:
                                                ae_records = ae_records[ae_records['AEPTCD'] == code]
                                                ae_grades = ae_records['AETOXGR'].unique().tolist()
                                                if len(ae_grades) > 1:
                                                    continue
                                                ae_records['AESTDAT'] = pd.to_datetime((ae_records['AESTDAT']), infer_datetime_format=True)
                                                lb_record['LBDAT'] = pd.to_datetime((lb_record['LBDAT']), infer_datetime_format=True)
                                                ae_records['DIFFERENCE'] = (ae_records['AESTDAT'] - lb_record['LBDAT'].values[0]) / np.timedelta64(1, 'D')
                                                diff = ae_records['DIFFERENCE'].unique().tolist()
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
                                                ae_records = ae_records[ae_records['DIFFERENCE'] == final_minimum]
                                                for ind1 in range(ae_records.shape[0]):
                                                    try:
                                                        ae_record = ae_records.iloc[[ind1]]
                                                        aeterm = ae_record['AETERM'].values[0]
                                                        code = ae_record['AEPTCD'].values[0]
                                                        formindex = ae_record['form_index'].values[0]
                                                        aespid = ae_record['AESPID'].values[0]
                                                        aetoxgr = ae_record['AETOXGR'].values[0]
                                                        result = float(lb_record['LBORRES'].values[0])
                                                        status = lb_record['LBSTAT'].values[0]
                                                        labdate = lb_record['LBDAT'].values[0]
                                                        low = float(lb_record['LNMTLOW'].values[0])
                                                        high = float(lb_record['LNMTHIGH'].values[0])
                                                        unit = lb_record['LNMTUNIT'].values[0]
                                                        gap = (ae_record['AESTDAT'].values[0] - lb_record['LBDAT'].values[0]) / np.timedelta64(1, 'D')
                                                        func_name = grades['VERSION 4'][aeterm.upper()]
                                                        if type(func_name) == list:
                                                            func_name = func_name[0]
                                                        grade = globals()[func_name](result, low, high, unit)
                                                        grade = int(grade.split('~')[1])
                                                        if result < low or result > high:
                                                            if grade in (3, 4):
                                                                if gap >= -5:
                                                                    if gap <= 0:
                                                                        if int(float(aetoxgr)) != grade:
                                                                            if grade != 0:
                                                                                subcate_report_dict = {}
                                                                                report_dict = {}
                                                                                ae_record2 = ae_record
                                                                                lb_record2 = lb_record
                                                                                ae_record2['AESTDAT'] = ae_record2['AESTDAT'].dt.strftime('%d-%B-%Y')
                                                                                lb_record2['LBDAT'] = lb_record2['LBDAT'].dt.strftime('%d-%B-%Y')
                                                                                lb_record2['grade'] = grade
                                                                                piv_rec = {'LB':lb_record2, 
                                                                                 'AE':ae_record2}
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
                                                                                labdate = lb_record2['LBDAT'].values[0]
                                                                                lb_record1 = lb_record.iloc[0]
                                                                                keys = {'LBTEST':test.split('_')[0], 
                                                                                 'LBDAT':str(labdate).split('T')[0], 
                                                                                 'grade':grade, 
                                                                                 'AESPID':aespid, 
                                                                                 'AETERM':aeterm, 
                                                                                 'AESTDAT':aestdat, 
                                                                                 'AETOXGR':aetoxgr}
                                                                                payload = {'subcategory':sub_cat, 
                                                                                 'query_text':self.get_model_query_text_json(study, sub_cat, params=keys), 
                                                                                 'form_index':str(lb_record1['form_index']), 
                                                                                 'question_present':self.get_subcategory_json(study, sub_cat), 
                                                                                 'modif_dts':str(lb_record1['modif_dts']), 
                                                                                 'stg_ck_event_id':int(lb_record1['ck_event_id']), 
                                                                                 'formrefname':str(lb_record1['formrefname']), 
                                                                                 'report':subcate_report_dict, 
                                                                                 'confid_score':np.random.uniform(0.7, 0.97)}
                                                                                self.insert_query(study, subject, payload)
                                                    except:
                                                        continue

                                    except:
                                        continue

                        except:
                            continue

            except:
                continue


if __name__ == '__main__':
    study_id = sys.argv[1]
    rule_id = sys.argv[2]
    version = sys.argv[3]
    rule = LBAE2(study_id, rule_id, version)
    rule.run()