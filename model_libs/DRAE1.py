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
curr_file_path = os.path.realpath(__file__)
curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
#slogging.info("subcat-curpath",curr_path)
subcat_config_path = os.path.join(curr_path, 'subcate_config_dosing.yml')
#logging.info("subcat_config_path",subcat_config_path)
config = yaml.safe_load(open(subcat_config_path, 'r'))

    


class DRAE1(BaseSDQApi):
    domain_list = ['EC', 'AE']
    
    # curr_file_path = os.path.realpath(__file__)
    # curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
    # subcat_config_path = os.path.join(curr_path, 'subcate_config_dosing.yml')

    # with open(subcat_config_path, 'r') as stream:
    #     config = yaml.safe_load(stream)
    def execute(self):
        study = self.study_id
        sub_cat = 'DRAE1'
        subjects =self.get_subjects(study, domain_list = self.domain_list)#7000008,[30000014]#[10001011]#[14215041]#
        fmt = utils.format_datetime
        get_date = utils.get_date
        norm = utils.normalise_df
        gen_cols = utils.gen_columns 
        gen_cols={key:key.lower() for key,value in gen_cols.items()} 
        ec_cols = utils.ec_columns
        basic_fields = config['FIELDS_FOR_UI']['BASIC']
        ec_fields = config['FIELDS_FOR_UI'][sub_cat]['EC']
        # ae_fields = config['FIELDS_FOR_UI'][sub_cat]['AE']
        rename_features = config['FIELD_NAME_DICT']
        
        for subject in tqdm.tqdm(subjects):
            payload_list = []

            try:
                flatten_data = self.get_flatten_data(study, subject, domain_list = self.domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)
                
                ec_df = norm(pd.DataFrame(flatten_data['EC']), date_col=['ECSTDAT', 'ECENDAT'])
                ec_df['FORMINDEX'] = ec_df['ITEMREPN']
                # ec_df['SUBJECTID'] = ec_df['SUBJID'].astype(int)

                
                ae_df = norm(pd.DataFrame(flatten_data['AE']), date_col=['AESTDAT', 'AEENDAT'])
                ae_df['FORMINDEX'] = ae_df['FORM_INDEX']
                # ae_df['SUBJECTID'] = ae_df['SUBJID'].astype(int)                
#                 debug(subject)
                
                for i_row in range(ec_df.shape[0]):
                    payload_flag=False
                    cur_ec = ec_df.iloc[[i_row]]
#                     ecadj_flag=
                    formindex = cur_ec['FORMINDEX'].iloc[0]
                    ecst_dat = cur_ec['ECSTDAT'].iloc[0]
                    ecend_dat = cur_ec['ECENDAT'].iloc[0]
                    
                    ext_result = utils.extractor(cur_ec, ec_df, ae_df, 'ECAENO', 'AESPID')
                    ecno_list=utils.id_handler(cur_ec['ECAENO'].values[0])
                    print(subject, ' == ext_result ==> ', ext_result)
                    if (type(ext_result) == tuple) & (type(ext_result) != bool):
                        if len(ext_result[0]) == 0:
#                             continue
#                             for k, v in ext_result[0].items():
#                             for ind in range(ae_df.shape[0]):
#                                 cur_ae=ae_df.iloc[[ind]]
                               
                            ecadj_flag = utils.check_ecadj(cur_ec, rule_flag='ae')
                            if (ecadj_flag[0]):
                                
                                payload_flag=True
                        elif len(ext_result[0])>0:
                            
                            new_ae=pd.DataFrame()
                            aeids=[]
                            for ae_formindex, ae_records in ext_result[0].items():
                                for ae_record in ae_records:
                                    new_ae = new_ae.append(ae_record, ignore_index = True)
                                    aeids.append(ae_record['AESPID'].astype('int64').values[0])
                                    
                                if len(new_ae) < len(ecno_list):
                                    ecadj_flag = utils.check_ecadj(cur_ec, rule_flag='ae')
                                    if (ecadj_flag[0]):
                                        ecno_list=list(set(ecno_list)-set(aeids))
                                        payload_flag=True
                                    
                        if payload_flag:               
                            subcat_report = dict()
                            df = pd.DataFrame({})
                            reports_map = {
                                            "EC": cur_ec,
    #                                        "AE": cur_ae,
                                            }
                            report_dict={}
                            basic_fields=[key.lower() for key,value in gen_cols.items()]+['SUBJECTID']
                            # print(basic_fields)
                            for ecno in ecno_list:
                                for dom in self.domain_list:
                                    df = cur_ec.rename(columns=gen_cols)[basic_fields]
#                                     ae_df_d=cur_ae.rename(columns=gen_cols)[basic_fields]
                                    if dom == 'EC':
                                        ec_col_df = reports_map[dom].rename(columns=ec_cols)[ec_fields]
                                        # pres_fields = [c for c in ec_col_df.columns if c in ec_fields]
                                        # ec_col_df = ec_col_df[pres_fields]
    #                                             df = pd.concat([df, ec_col_df], axis=1)
                                        ec_col_df=ec_col_df.fillna('null')
                                        ec_col_df['ECSTDAT'] = str(ec_col_df['ECSTDAT'].dt.strftime("%d-%b-%Y").values[0])#[:10]
                                        ec_col_df['ECENDAT'] = str(ec_col_df['ECENDAT'].dt.strftime("%d-%b-%Y").values[0])
                                        ### ADDING HANDLER FOR DATES TO HANDLES NULL - 10Nov23 - this is not fixed in 1.0
                                        # ec_col_df['ECSTDAT'] = ec_col_df['ECSTDAT'].apply(fmt).values[0]
                                        # ec_col_df['ECENDAT'] = ec_col_df['ECENDAT'].apply(fmt).values[0]
                                        ec_col_df['deeplink'] =utils.get_deeplink(study, df)
                                        ec_col_df = ec_col_df.rename(columns=rename_features)
                                        report_dict[dom] = ec_col_df.to_json(orient='records')
                            #COMMENTING AE AS PER COMMENTS FROM PFIZER(AE IS NOT ADD ANY MEANING TO THOSE PREDS)
    #                                         elif dom == 'AE':
    #                                             ae_col_df = reports_map[dom].loc[:, reports_map[dom].columns.isin(ae_fields)]
    #                                             ae_col_df=ae_col_df.fillna('null')
    #                                             ae_col_df['AESTDAT'] = str(ae_col_df['AESTDAT'].values[0])#[:10]
    #                                             ae_col_df['AEENDAT'] = str(ae_col_df['AEENDAT'].values[0])
    #                                             ae_col_df['deeplink'] = utils.get_deeplink(study, ae_df_d,domain=dom)
    #                                             ae_col_df = ae_col_df.rename(columns=rename_features)
    #                                             report_dict[dom]=ae_col_df.to_json(orient='records')

                                subcat_report[sub_cat] = report_dict
    #                                         debug(subcat_report)
    #                                     param={
    #                                             'ECAENO':cur_ec['ECAENO'].values[0]                                    
    #                                             }
    #                                     import pdb;pdb.set_trace()
                                payload = {
                                 'subcategory':sub_cat,
                                 'cdr_skey':str(cur_ec['CDR_SKEY'].values[0]), 
                                 'query_text':self.get_query_text_json(study, sub_cat, params=[ecno]), 
                                 'form_index':int(cur_ec['FORM_INDEX'].values[0]), 
                                 'visit_nm':cur_ec['VISIT_NM'].values[0], 
                                 'question_present':self.get_subcategory_json(study, sub_cat), 
                                 'modif_dts':fmt(cur_ec['MODIF_DTS'].values[0]), 
                                 'formrefname':str(cur_ec['FORMREFNAME'].values[0]), 
                                 'stg_ck_event_id':int(cur_ec['CK_EVENT_ID'].values[0]), 
                                 'report':subcat_report
                                }
        
                                # print(subject,payload)

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
    rule = DRAE1(study_id, account_id, job_id, rule_id, version)
    rule.run()
