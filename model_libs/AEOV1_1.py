'''
Rule ID: AEOV1_1
Release Version: R3.8M5.5
Changes: 
[EDIT#1]15-03-23 - UPDATING THE FUZZYWUZZY LOGIC

'''

import pandas as pd, sys, re, warnings
warnings.filterwarnings('ignore')
try:
    from scripts.SubcatModels.model_libs.base import BaseSDQApi
    import scripts.SubcatModels.model_libs.utils as utils
    from  is_ovduplicate import DuplicateCheck
except:
    from  is_ovduplicate import DuplicateCheck
    from base import BaseSDQApi
    import utils

import traceback, tqdm, logging, numpy as np, yaml, os, json
curr_file_path = os.path.realpath(__file__)
curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
subcat_config_path = os.path.join(curr_path, 'subcate_config.yml')
subcate_config = yaml.safe_load(open(subcat_config_path, 'r'))

class AEOV1_1(BaseSDQApi):
    domain_list = [
     'AE']

    def execute(self):
        study = self.study_id
        ov_study_id = f'account_{self.account_id}_study_{self.study_id}'
        duplicate=DuplicateCheck(ov_study_id)
        check_if_duplicate=duplicate.check

        domain_list = ['AE']
        sub_cat = 'AEOV1_1'
        subjects = self.get_subjects(study, domain_list=(self.domain_list), per_page=10000) #[88880012]#
        for subject in tqdm.tqdm(subjects):
            payload_list = []
            try:
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list = self.domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)
                ae_df = pd.DataFrame(flatten_data['AE'])
                for ind in range(ae_df.shape[0]):
                    try:
                        ae_record = ae_df.iloc[[ind]]
                        total_valid_present = 0
                        total_count = 0
                        aeterm = ae_record['AETERM'].values[0]
                        aespid = ae_record['AESPID'].values[0]
                        aedecod = ae_record['AEDECOD'].values[0]
                        prim_subj = ae_record['subjid'].values[0]
                        aest_dt = ae_record['AESTDAT'].values.tolist()[0]
                        aeen_dt = ae_record['AEENDAT'].values.tolist()[0]
                        formidx = ae_record['form_ix'].values[0]
                        aeongo = ae_record['AEONGO'].values[0]
                        ae_records = ae_df
                        aesev_present = False
                        ae_cols = ae_record.columns.tolist()
                        if 'AESEV' in ae_cols:
                            aesev_present = True
                            aesev = ae_record['AESEV'].values[0]
                        if type(aest_dt) != float:
                            if type(aeen_dt) != float:
                                if aesev_present:
                                    new_ae_record = ae_records[((ae_records['AESTDAT'] == aest_dt) & (ae_records['AEENDAT'] == aeen_dt) & (ae_records['AESEV'] == aesev) & (ae_records['AEONGO'] == aeongo))]
                                else:
                                    new_ae_record = ae_records[((ae_records['AESTDAT'] == aest_dt) & (ae_records['AEENDAT'] == aeen_dt) & (ae_records['AEONGO'] == aeongo))]
                        if type(aest_dt) != float:
                            if type(aeen_dt) == float:
                                if aesev_present:
                                    new_ae_record = ae_records[((ae_records['AESTDAT'] == aest_dt) & (ae_records['AESEV'] == aesev) & (ae_records['AEONGO'] == aeongo))]
                                else:
                                    new_ae_record = ae_records[((ae_records['AESTDAT'] == aest_dt) & (ae_records['AEONGO'] == aeongo))]
                        new_ae_record = new_ae_record[(new_ae_record['form_ix'] != formidx)]
                        if len(new_ae_record) == 0:
                            continue
                        else:
                            if len(new_ae_record) >= 1:
                                for ind in new_ae_record.index.tolist():
                                    curr_aeterm = new_ae_record.loc[(ind, 'AETERM')]
                                    curr_aedecod = new_ae_record.loc[(ind, 'AEDECOD')]
                                    #EDIT#1 INCLUDING FUZZY WUZZY LOGIC 
                                    aeterm_flag = utils.check_similar_term_fuzzy(aeterm, curr_aeterm)
                                    aedecod_flag = utils.check_similar_medication_verabtim2(aedecod, curr_aedecod)
                                    if aeterm_flag and aedecod_flag:
                                        ind1 = ind
                                        total_valid_present += 1
                                        break

                                if total_valid_present > 0:
                                    subcate_report_dict = {}
                                    report_dict = {}
                                    new_ae_record = new_ae_record.loc[[ind1]]
                                    piv_rec = {'AE1':ae_record.head(1),  'AE2':new_ae_record.head(1)}
                                    for dom, cols in subcate_config['FIELDS_FOR_UI'][sub_cat].items():
                                        if len(cols) > 0:
                                            for k_dom in piv_rec.keys():
                                                piv_df = piv_rec[k_dom]
                                                present_col = [col for col in cols if col in piv_df.columns.tolist()]
                                                rep_df = piv_df[present_col]
                                                # import pdb;pdb.set_trace()
                                                rep_df['deeplink'] = utils.get_deeplink(study, piv_df)
                                                rep_df = rep_df.rename(columns=(subcate_config['FIELD_NAME_DICT']))
                                                #COMMENING OUT SINCE IT DROPING DATES COLS IF VALUES ARE SAME
                                                # rep_df = rep_df.T.drop_duplicates().T
                                                report_dict[k_dom] = rep_df.to_json(orient='records')

                                    subcate_report_dict[sub_cat] = report_dict
                                    aespid1 = new_ae_record['AESPID'].values[0]
                                    aeterm1 = new_ae_record['AETERM'].values[0]
                                    aestdat1 = new_ae_record['AESTDAT'].values[0]
                                    aeendat1 = new_ae_record['AEENDAT'].values[0]
                                    aeongo1 = new_ae_record['AEONGO'].values[0]
                                    if aeen_dt == np.nan:
                                        aeen_dt = 'blank'
                                    ae_record1 = ae_record.iloc[0]
                                    total_count += 1
                                    if aesev_present == False:
                                        aesev = 'blank'
                                    query_text_param = {'AESPID':aespid,  'AETERM':aeterm, 
                                     'AESPID1':aespid1, 
                                     'AETERM1':aeterm1, 
                                     'AESTDAT1':aestdat1, 
                                     'AEENDAT1':aeendat1, 
                                     'AEONGO1':aeongo1, 
                                     'AEDECOD':aedecod, 
                                     'AESTDAT':aest_dt, 
                                     'AEENDAT':aeen_dt, 
                                     'AESEV':aesev, 
                                     'AEONGO':aeongo}
                                    is_ov_duplicate = check_if_duplicate(subjid=(int(ae_record1['subjid'])), subcat='AEOV1_1', index=(int(ae_record1['form_index'])), report=subcate_report_dict)
                                    if is_ov_duplicate == False:
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
                        print(e)
                        continue

            except Exception as e:
                print(e)
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
    rule = AEOV1_1(study_id, account_id, job_id, rule_id, version)
    rule.run()