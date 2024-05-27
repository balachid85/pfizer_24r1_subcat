# uncompyle6 version 3.8.0
# Python bytecode 3.6 (3379)
# Decompiled from: Python 3.8.10 (v3.8.10:3d8993a744, May  3 2021, 09:09:08) 
# [Clang 12.0.5 (clang-1205.0.22.9)]
# Embedded file name: /users/sdqdev/Adheeban/pfizer_data_upload_old/dags/scripts/SubcatModels/model_libs/LBOV1.py
# Compiled at: 2021-11-12 13:52:46
# Size of source mod 2**32: 9260 bytes
import pandas as pd, sys, re, numpy as np, warnings
warnings.filterwarnings('ignore')
try:
    from scripts.SubcatModels.model_libs.base import BaseSDQApi
    import scripts.SubcatModels.model_libs.utilsk as utils
    from scripts.SubcatModels.model_libs.ctc_grades import *
    import scripts.SubcatModels.model_libs.utils as deeplink_utils
    from  is_ovduplicate import DuplicateCheck
    from  is_ovduplicate import LabDuplicateCheck
except:
    from base import BaseSDQApi
    import utilsk as utils
    from ctc_grades import *
    import utils as deeplink_utils
    from  is_ovduplicate import DuplicateCheck
    from  is_ovduplicate import LabDuplicateCheck

import traceback, tqdm, logging, numpy as np, yaml, os
curr_file_path = os.path.realpath(__file__)
curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
grades_path = os.path.join(curr_path, 'grade_lookup.yaml')
grades = yaml.load((open(grades_path, 'rb')), Loader=(yaml.FullLoader))
subcat_config_path = os.path.join(curr_path, 'subcate_config.yml')
a = yaml.safe_load(open(subcat_config_path, 'r'))

null_list = ['', ' ', None, 'null', 'nan', np.nan, 'NaN']
class LBOV1(BaseSDQApi):
    domain_list = [
     'LB']

    def execute(self):
        study = self.study_id
        sub_cat = 'LBOV1'
        subjects = self.get_subjects(study, domain_list=(self.domain_list), per_page=10000)
        #LabDuplicateCheck
        ov_study_id = f'account_{self.account_id}_study_{self.study_id}'
        duplicate=DuplicateCheck(ov_study_id)
        check_if_duplicate=duplicate.check
        
        lab_duplicate=LabDuplicateCheck(ov_study_id)
        check_if_lab_duplicate=lab_duplicate.check
        for subject in tqdm.tqdm(subjects):
            payload_list = []
            try:
                
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list=(self.domain_list))
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)
                if 'LB' not in flatten_data:
                    continue
                else:
                    lb_df = pd.DataFrame(flatten_data['LB'])
                    if len(lb_df)==0:
                        continue
                lb_df = pd.DataFrame(flatten_data['LB'])
                lbtim_flag = False
                if 'LBTIM' in lb_df.columns.tolist():
                    lb_df['LBTIM'] = lb_df['LBTIM'].fillna('blank')
                    lbtim_flag = True
                else:
                    labtime = 'blank'
                    labtime1 = 'blank'
                for ind in range(lb_df.shape[0]):
                    
                    try:
                        # print('LBORRES 70',lb_df['LBORRES'].values[0])
                        lbtstc_flag = False
                        lb_record = lb_df.iloc[[ind]]
                        if 'LBTEST_C' in lb_record.columns.tolist():
                            labtest_c = lb_record['LBTEST_C'].values[0]
                            if labtest_c in null_list:
                                lbtstc_flag = False
                            else:
                                lbtstc_flag = True
                        labtest = lb_record['LBTEST'].values[0]
                        labdate = lb_record['LBDAT'].values[0]
                        result = lb_record['LBORRES'].values[0]
                        # print('LBORRES 84',result)
                        # print('LBORRES dataframe',lb_df[['LBORRES','cdr_skey']].values)
                        formname = lb_record['formrefname'].values[0]
                        visit_nm = lb_record['visit_nm'].values[0]
                        cdr_skey = lb_record['cdr_skey'].values[0]
                        if lbtim_flag:
                            labtime = lb_record['LBTIM'].values[0]
                            new_lb = lb_df[((lb_df['LBTEST'] == labtest) & (lb_df['LBDAT'] == labdate) & (lb_df['LBTIM'] == labtime) & (lb_df['LBORRES'] == result))]
                        else:
                            new_lb = lb_df[((lb_df['LBTEST'] == labtest) & (lb_df['LBDAT'] == labdate) & (lb_df['LBORRES'] == result))]
                        if lbtstc_flag:
                            new_lb = new_lb[(new_lb['LBTEST_C'] == labtest_c)]
                        new_lb = new_lb[(new_lb['cdr_skey'] != cdr_skey)]
                        # print('LBORRES',result)
                        # print('cdr_skey',cdr_skey)
                        
                        if len(new_lb) > 0:
                            # print(' inside LBORRES',result)
                            # print('inside cdr_skey',cdr_skey)
                            # print('lab_data',new_lb.values)
                            subcate_report_dict = {}
                            report_dict = {}
                            piv_rec = {'LB1':lb_record,  'LB2':new_lb.head(1)}
                            for dom, cols in a['FIELDS_FOR_UI'][sub_cat].items():
                                piv_df = piv_rec[dom]
                                present_col = [col for col in cols if col in piv_df.columns.tolist()]
                                rep_df = piv_df[present_col]
                                rep_df['deeplink'] = deeplink_utils.get_deeplink(study, piv_df)
                                rep_df = rep_df.rename(columns=(a['FIELD_NAME_DICT']))
                                report_dict[dom] = rep_df.to_json(orient='records')

                            subcate_report_dict[sub_cat] = report_dict
                            visit = lb_record['visit_nm'].values[0]
                            labtest1 = new_lb['LBTEST'].values[0]
                            labdate1 = new_lb['LBDAT'].values[0]
                            visit1 = new_lb['visit_nm'].values[0]
                            if lbtim_flag:
                                labtime1 = new_lb['LBTIM'].values[0]
                            lb_record1 = lb_record.iloc[0]
                            replace = [
                             '', ' ', np.nan, 'null', 'blank']
                            if labtime in replace:
                                labtime = ''
                            if labtime1 in replace:
                                labtime1 = ''
                            labdate = f"{labdate} {labtime}".strip().replace('T', ' ')
                            labdate1 = f"{labdate1} {labtime1}".strip().replace('T', ' ')
                            query_text = {'LBDAT1':labdate, 
                             'VISIT1':visit_nm, 
                             'LBDAT2':labdate1, 
                             'VISIT2':visit1}
                            
                            is_ov_duplicate = check_if_duplicate(subjid=(int(lb_record1['subjid'])), subcat='LBOV1', index=(int(lb_record1['itemrepn'])), report=subcate_report_dict)
                            is_lab_duplicate = check_if_lab_duplicate(subjid=(int(lb_record1['subjid'])), subcat=sub_cat, visit_nm=(lb_record1['visit_nm']),
                               visit_ix=(int(lb_record1['visit_ix'])),
                               formrefname=(lb_record1['formrefname']),
                              form_ix=(int(lb_record1['form_ix'])))
                            # is_ov_duplicate=False
                            # is_lab_duplicate=False
                            print('is_ov_duplicate',subject,is_ov_duplicate)
                            print('is_lab_duplicate',subject,is_lab_duplicate)
                           
                            if is_ov_duplicate == False:
                                payload = {'subcategory':sub_cat,
                                     'cdr_skey':str(lb_record1['cdr_skey']), 
                                     'query_text':self.get_model_query_text_json(study, sub_cat, params=query_text), 
                                     'form_index':str(lb_record1['form_index']), 
                                     'question_present':self.get_subcategory_json(study, sub_cat), 
                                     'modif_dts':str(lb_record1['modif_dts']), 
                                     'stg_ck_event_id':int(lb_record1['ck_event_id']), 
                                     'formrefname':str(lb_record1['formrefname']), 
                                     'report':subcate_report_dict, 
                                     'confid_score':np.random.uniform(0.7, 0.97)}
                                if is_lab_duplicate == False:
                                    self.insert_query(study, subject, payload)
                                if payload not in payload_list:
                                    print('payload is appended')
                                    payload_list.append(payload)
                                        
                    except Exception as e:
                        logging.exception(e)

            except Exception as e:
                logging.exception(e)
                
            if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                self.close_query(subject, payload_list)


if __name__ == '__main__':
    study_id = sys.argv[1]
    rule_id = sys.argv[2]
    version = sys.argv[3]
    rule = LBOV1(study_id, rule_id, version)
    rule.run()