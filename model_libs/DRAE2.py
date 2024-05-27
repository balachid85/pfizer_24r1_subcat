'''
Rule ID: DRAE1
Release Version: R2.10M4.6
Changes: 
06-04-22 - CHANGES FOR FIRING PREDS FOR ALL THE ECNOS
            
'''
import re
import sys
import yaml
import logging
import warnings
import traceback
import pandas as pd
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
import tqdm



class DRAE2(BaseSDQApi):
    domain_list = ['EC', 'AE']
    
    curr_file_path = os.path.realpath(__file__)
    curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
    subcat_config_path = os.path.join(curr_path, 'subcate_config_dosing.yml')

    with open(subcat_config_path, 'r') as stream:
        config = yaml.safe_load(stream)
    
    def execute(self):
        study = self.study_id
        sub_cat = 'DRAE2'
        subcat_aeacn_list = ['DOSE INCREASED']
        subjects =self.get_subjects(study, domain_list = self.domain_list)# [70000040]#['40010133'] #
        
        get_date = utils.get_date
        norm = utils.normalise_df
        gen_cols = utils.gen_columns 
        gen_cols={key:key.lower() for key,value in gen_cols.items()}
        ec_cols = utils.ec_columns
        fmt = utils.format_datetime
        basic_fields = self.config['FIELDS_FOR_UI']['BASIC']
        ec_fields = self.config['FIELDS_FOR_UI'][sub_cat]['EC']
        rename_features = self.config['FIELD_NAME_DICT']
#         ae_fields = self.config['FIELDS_FOR_UI'][sub_cat]['AE']
        
        for subject in subjects: #[10861001, 10511002, 10441003]
            payload_list = []
            try:
                flatten_data = self.get_flatten_data(study, subject, domain_list = self.domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)

                ec_df = norm(pd.DataFrame(flatten_data['EC']), date_col=['ECSTDAT', 'ECENDAT'])
                ec_df['FORMINDEX'] = ec_df['ITEMREPN']
                # ec_df['SUBJECTID'] = ec_df['SUBJID'].astype(int)
                ae_flag=False
                # ae_df=pd.DataFrame()
                #import pdb;pdb.set_trace()
                if 'AE' in flatten_data:
                    ae_df = pd.DataFrame(flatten_data['AE'])
                    print('subjectid ==----> ', subject, ae_df.shape)
                    if len(ae_df)>0:
                        ae_df = norm(pd.DataFrame(flatten_data['AE']), date_col=['AESTDAT', 'AEENDAT'])
                        ae_df['FORMINDEX'] = ae_df['FORM_INDEX']
                    else:
                        ae_flag = True
                    
                    # ae_df['SUBJECTID'] = ae_df['SUBJID'].astype(int)
                else:
                    ae_flag=True
                
#                 debug(subject)
                
                for i_row in range(ec_df.shape[0]):
                    cur_ec = ec_df.iloc[[i_row]]
                    
                    formindex = cur_ec['FORMINDEX'].iloc[0]
                    ecst_dat = cur_ec['ECSTDAT'].iloc[0]
                    ecend_dat = cur_ec['ECENDAT'].iloc[0]
                    
#                     ext_result = utils.extractor(cur_ec, ec_df, ae_df, 'ECAENO', 'AESPID')
#                     import pdb;pdb.set_trace()
#                     if (type(ext_result) == tuple) & (type(ext_result) != bool):

                    print('subjectid ==> ', subject)
                    if  ae_flag:
                           
                        ecadj_flag = utils.check_ecadj(cur_ec, rule_flag='ae')
                        print('ecadj_flag ==> ', ecadj_flag)
                        print('ecaeno ==> ', cur_ec['ECAENO'].values)
                        if (ecadj_flag[0]) and cur_ec['ECAENO'].notna().values:
                            subcat_report = dict()
                            df = pd.DataFrame({})
                            reports_map = {
                                            "EC": cur_ec,
#                                                     "AE": cur_ae,
                                           }
                            print('CUR_EC COLUMNS 1 **********> ', cur_ec.columns)
                            print('CUR_EC ROWS LENGTH **********> ', cur_ec.shape)
                            basic_fields=[key.lower() for key,value in gen_cols.items()]+['SUBJECTID']
                            df = cur_ec.rename(columns=gen_cols)[basic_fields]
                            report_dict={}
                            
                            # print(basic_fields)
                            for dom in self.domain_list:
                                if dom == 'EC':
                                    ec_col_df = reports_map[dom].rename(columns=ec_cols)[ec_fields]
#                                             df = pd.concat([df, ec_col_df], axis=1)
                                    ec_col_df=ec_col_df.fillna('null')
                                    ec_col_df['ECSTDAT'] = str(ec_col_df['ECSTDAT'].dt.strftime("%d-%b-%Y").values[0])#[:10]
                                    ec_col_df['ECENDAT'] = str(ec_col_df['ECENDAT'].dt.strftime("%d-%b-%Y").values[0])
                                    ec_col_df['deeplink'] = utils.get_deeplink(study, df)
                                    ec_col_df = ec_col_df.rename(columns=rename_features)
                                    ec_col_df=ec_col_df.fillna('null')
                                    report_dict[dom] = ec_col_df.to_json(orient='records')
                            subcat_report[sub_cat] = report_dict
                            query_text_param = {
                                "ECAENO":cur_ec['ECAENO'].values[0]
                                }
           
                            payload = {
                                        'subcategory':sub_cat,
                                         'cdr_skey':str(cur_ec['CDR_SKEY'].values[0]),
                                        #  'cdr_skey':str(cur_ec['CDR_SKEY']), 
                                         'query_text':self.get_model_query_text_json(study, sub_cat, params=query_text_param), 
                                         'form_index':int(cur_ec['FORM_INDEX'].values[0]), 
                                         'visit_nm':cur_ec['VISIT_NM'].values[0], 
                                         'question_present':self.get_subcategory_json(study, sub_cat), 
                                         'modif_dts':fmt(cur_ec['MODIF_DTS'].values[0]), 
                                         'formrefname':str(cur_ec['FORMREFNAME'].values[0]), 
                                         'stg_ck_event_id':int(cur_ec['CK_EVENT_ID'].values[0]), 
                                         'report':subcat_report
                                        }
                            # print(subject, payload)
                            self.insert_query(study, subject, payload)
                            if payload not in payload_list:
                                payload_list.append(payload)

     

            except Exception as k:
                logging.exception(k)

                                                    
            if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                self.close_query(subject, payload_list)                                            
if __name__ == '__main__':
    study_id = sys.argv[1]
    account_id = sys.argv[2]
    job_id = sys.argv[3]
    rule_id = sys.argv[4]
    version = sys.argv[5]
    rule = DRAE2(study_id, account_id, job_id, rule_id, version)
    rule.run()
