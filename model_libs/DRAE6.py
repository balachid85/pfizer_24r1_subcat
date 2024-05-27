'''
Rule ID: DRAE6
Release Version: R2.10M4.6
Changes: 
06-04-22 - CHANGES FOR FIRING PREDS FOR ALL THE ECNOS
03-05-2022-[EDIT 2.0]- "not" should be there Since aeendat should not be null and its not firing for 2nd scenario
'''    
import re
import sys
import pdb
import yaml
import logging
import warnings
import traceback
import numpy as np
import pandas as pd
import tqdm
try:
    from scripts.SubcatModels.model_libs.base import BaseSDQApi
    import scripts.SubcatModels.model_libs.dosing_utils_v1 as utils
except:
    from base import BaseSDQApi
    import dosing_utils_v1 as utils
# from icecream import ic as debug
warnings.filterwarnings('ignore')
# debug.configureOutput(prefix='debug | ')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
import os


class DRAE6(BaseSDQApi):
    domain_list = ['EC', 'AE']
    
    curr_file_path = os.path.realpath(__file__)
    curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
    subcat_config_path = os.path.join(curr_path, 'subcate_config_dosing.yml')

    with open(subcat_config_path, 'r') as stream:
        config = yaml.safe_load(stream)
    
    def execute(self):
        study = self.study_id
        sub_cat = 'DRAE6'
        subcat_aeacn_list = ['DRUG INTERRUPTED']
        subjects = self.get_subjects(study, domain_list = self.domain_list) # [10001011, 90000018]['90000019']#
        
        get_date = utils.get_date
        norm = utils.normalise_df
        gen_cols = utils.gen_columns 
        gen_cols={key:key.lower() for key,value in gen_cols.items()}
        ec_cols = utils.ec_columns
        fmt = utils.format_datetime
        basic_fields = self.config['FIELDS_FOR_UI']['BASIC']
        ec_fields = self.config['FIELDS_FOR_UI'][sub_cat]['EC']
        ae_fields = self.config['FIELDS_FOR_UI'][sub_cat]['AE']
        rename_features = self.config['FIELD_NAME_DICT'] 
        
        for subject in tqdm.tqdm(subjects):
            payload_list = []

            try:
                flatten_data = self.get_flatten_data(study, subject, domain_list = self.domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)
                
                ec_df = pd.DataFrame(flatten_data['EC'])
                if {'SITENO', 'siteno'}.issubset(ec_df.columns):
                    ec_df.drop('SITENO', axis=1, inplace=True)
                ec_df = norm(ec_df, date_col=['ECSTDAT', 'ECENDAT'])
                ec_df['FORMINDEX'] = ec_df['ITEMREPN']
#                 ec_df['SUBJECTID'] = ec_df['SUBJID'].astype(float).astype(int)
                ec_df['SITENO'] = ec_df['SITENO'].astype(float).astype(int)
                
                ae_df = pd.DataFrame(flatten_data['AE'])
                if {'SITENO', 'siteno'}.issubset(ae_df.columns):
                    ae_df.drop('SITENO', axis=1, inplace=True)
                ae_df = norm(ae_df, date_col=['AESTDAT', 'AEENDAT'])
                ae_df['FORMINDEX'] = ae_df['FORM_INDEX']
#                 ae_df['SUBJECTID'] = ae_df['SUBJID'].astype(float).astype(int)
                ae_df['SITENO'] = ae_df['SITENO'].astype(float).astype(int)
                for i_row in range(ec_df.shape[0]):
                    cur_ec = ec_df.iloc[[i_row]]
                    
                    formindex = cur_ec['FORMINDEX'].iloc[0]
                    ecst_dat = cur_ec['ECSTDAT'].iloc[0]
                    ecend_dat = cur_ec['ECENDAT'].iloc[0]
                    
                    ext_result = utils.extractor(cur_ec, ec_df, ae_df, 'ECAENO', 'AESPID')

                    
                    if (type(ext_result) == tuple) & (type(ext_result) != bool):
                        if len(ext_result[0]) == 0:
                            continue
                        for k, v in ext_result[0].items():
                            for cur_ae in v:
                                aetrt_dict = self.get_drugs_dict(study)
                                ecadj_flag, ecadj_col, _ = utils.check_ecadj(cur_ec, rule_flag='ae')
                                aeacn_flag, aeacn_tuple = utils.get_aeacn(cur_ec, cur_ae, subcat_aeacn_list, aetrt_dict)
                                print('ecadj_flag',aeacn_flag,ecadj_flag,aeacn_tuple)
                                if (ecadj_flag) and (aeacn_flag):
                                    ecrecord_flag, ec_values = utils.get_ecrecords(cur_ec, cur_ae, ec_df, ae_df)
                                    if (ecrecord_flag == False):
                                        continue
                                    else:
                                        ec_records, aest_dat, aend_dat = ec_values
                                    dose_flag, dose_info = utils.get_dose_values_v2(ec_records, formindex, ecst_dat, ecend_dat)
                                    print('dose_flag',dose_flag,dose_info)
                                    if (dose_flag):
                                        dose_values, dose_col = dose_info
                                        current_dose_value=float(dose_values)
                                        query_flag = False
                                        print(subject)
                                        if (current_dose_value == 0.0):
                                            if (ecst_dat < aest_dat):
                                                query_flag = True
                                            elif not pd.isnull(aend_dat) and (ecst_dat > aend_dat):#[EDIT 2.0]
                                                query_flag = True
                                            if query_flag:
                                                print(True, '*'*25)
#                                                 debug(formindex, aeacn_tuple, current_dose_value, previous_dose_value, ecst_dat, aest_dat, aend_dat)
#                                                 debug((ecst_dat >= aest_dat), (ecst_dat <= aend_dat))
                                                cur_ec['ECDOSTOT'] = cur_ec[dose_col].values[0]
                                                cur_ec['ECADJ'] = cur_ec[ecadj_col].values[0]
                                                subcat_report = dict()
                                                report_dict = dict()
                                                basic_fields=[key.lower() for key,value in gen_cols.items()]+['SUBJECTID']
                                                print(basic_fields)
                                                reports_map = {
                                                                "EC": cur_ec,
                                                                "AE": cur_ae,
                                                            }

                                                param_dict = {'EC_ITEMSETIDX': cur_ec['ITEMSET_IX'].values[0], \
                                                                'VISITNAME': cur_ec['VISIT_NM'].values[0],\
                                                              'AESPID': cur_ae['AESPID'].values[0], 'AETERM': cur_ae['AETERM'].values[0],
                                                              'AESTDAT': fmt(cur_ae['AESTDAT'].values[0]), 'AEENDAT': fmt(cur_ae['AEENDAT'].values[0]),
                                                              'ECSTDAT': fmt(cur_ec['ECSTDAT'].values[0]), 'ECENDAT': fmt(cur_ec['ECENDAT'].values[0])}
                                                for dom in self.domain_list:
                                                    if dom == 'EC':
                                                        base_ec_df = reports_map[dom].rename(columns=gen_cols)[basic_fields]
                                                        ec_col_df = reports_map[dom].rename(columns=ec_cols)[ec_fields]
                                                        ec_col_df['ECSTDAT'] = fmt(ec_col_df['ECSTDAT'].values[0])#[:10]
                                                        ec_col_df['ECENDAT'] = fmt(ec_col_df['ECENDAT'].values[0])#[:10]
                                                        ec_col_df['deeplink'] = utils.get_deeplink(study, base_ec_df)
                                                        ec_col_df = ec_col_df.rename(columns=rename_features)
                                                        report_dict[dom] = ec_col_df.to_json(orient='records')
                                                    elif dom == 'AE':
                                                        base_ae_df = reports_map[dom].rename(columns=gen_cols)[basic_fields]
                                                        ae_col_df = reports_map[dom].loc[:, reports_map[dom].columns.isin(ae_fields)]
                                                        ae_col_df['AESTDAT'] = fmt(ae_col_df['AESTDAT'].values[0])#[:10]
                                                        ae_col_df['AEENDAT'] = fmt(ae_col_df['AEENDAT'].values[0])#[:10]
                                                        ae_col_df['deeplink'] = utils.get_deeplink(study, base_ae_df)
                                                        ae_col_df = ae_col_df.rename(columns=rename_features)
                                                        report_dict[dom] = ae_col_df.to_json(orient='records')
                                                subcat_report[sub_cat] = report_dict
                                                
                                                payload = {
                                                            "subcategory": sub_cat,
                                                            # 'cdr_skey':str(cur_ec['CDR_SKEY']),
                                                            'cdr_skey':str(cur_ec['CDR_SKEY'].values[0]),
                                                            "query_text": self.get_model_query_text_json(study, sub_cat, params=param_dict), 
                                                            "form_index": int(cur_ec['FORM_INDEX'].values[0]),
                                                            "visit_nm": cur_ec['VISIT_NM'].values[0],
                                                            "question_present": self.get_subcategory_json(study, sub_cat),
                                                            "modif_dts": fmt(cur_ec['MODIF_DTS'].values[0]),
                                                            "stg_ck_event_id": int(cur_ec['CK_EVENT_ID'].values[0]),
                                                            "report" : subcat_report,
                                                            "formrefname": cur_ec['FORMREFNAME'].values[0],
                                                            "confid_score": np.random.uniform(0.7, 0.97),
                                                            }
                                                # print(subject, payload)
                                                self.insert_query(study, subject, payload)
                                                if payload not in payload_list:
                                                    payload_list.append(payload)

     

            except KeyError as k:
                logging.info(k)
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
    rule = DRAE6(study_id, account_id, job_id, rule_id, version)
    rule.run()