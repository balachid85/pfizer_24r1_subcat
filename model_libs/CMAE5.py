# uncompyle6 version 3.8.0
# Python bytecode 3.6 (3379)
# Decompiled from: Python 3.8.10 (v3.8.10:3d8993a744, May  3 2021, 09:09:08) 
# [Clang 12.0.5 (clang-1205.0.22.9)]
# Embedded file name: CMAE5.py
# Compiled at: 2020-11-10 13:17:27
# Size of source mod 2**32: 6438 bytes
import pandas as pd, sys, re, warnings
warnings.filterwarnings('ignore')
try:
    from scripts.SubcatModels.model_libs.base import BaseSDQApi
    import scripts.SubcatModels.model_libs.utils as utils
    from similarity_contents.drug_indication_search import check_valid_drug_indication
except:
    from base import BaseSDQApi
    import utils
    from similarity_contents.drug_indication_search import check_valid_drug_indication

import traceback, tqdm, logging, yaml, numpy as np, os
curr_file_path = os.path.realpath(__file__)
curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
subcat_config_path = os.path.join(curr_path, 'subcate_config.yml')
a = yaml.safe_load(open(subcat_config_path, 'r'))

class CMAE5(BaseSDQApi):
    domain_list = ['AE', 'CM']

    def execute(self):
        study = self.study_id
        domain_list = [
         'AE', 'CM']
        sub_cat = 'CMAE5'

        subjects =self.get_subjects(study, domain_list=domain_list, per_page=10000)#['10031002']# ['10513008'] #
        print(subjects)
        index_col = 'itemrepn' if sub_cat.startswith('DR') else 'form_index'
        for subject in tqdm.tqdm(subjects):
            payload_list = []
            try:

                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list = self.domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)
                ae_df = pd.DataFrame(flatten_data['AE'])
                cm_df = pd.DataFrame(flatten_data['CM'])
                for ind in range(cm_df.shape[0]):
                    try:
                        cm_record = cm_df.iloc[[ind]]
                        ae_cm_records = utils.extractor(cm_record, cm_df, ae_df, 'CMAENO', 'AESPID')
                        print(ae_cm_records)
                        if (type(ae_cm_records) == tuple) & (type(ae_cm_records) != bool):
                            if len(ae_cm_records[0]) > 0:
                                for cm_formindex, ae_records in ae_cm_records[0].items():
                                    for ae_record in ae_records:
                                        ae_stdt = ae_record['AESTDAT'].apply(utils.get_date)
                                        cm_stdt = cm_record['CMSTDAT'].apply(utils.get_date)
                                        ae_endt = ae_record['AEENDAT'].apply(utils.get_date)
                                        cm_endt = cm_record['CMENDAT'].apply(utils.get_date)
                                        cm_term = cm_record['CMTRT'].values[0]
                                        ae_term = ae_record['AETERM'].values[0]
                                        cmongo = cm_record['CMONGO'].values[0]
                                        cmaer = cm_record['CMAER'].values[0]
                                        
                                        nullify = lambda x: None if x in utils.null_list else x
                                                                                
                                        aeptcd = ae_record['AEPTCD'].apply(nullify).values[0] if 'AEPTCD' in ae_record.columns else None
                                        aelltcd = ae_record['AELLTCD'].apply(nullify).values[0] if 'AELLTCD' in ae_record.columns else None
                                        cmcode = cm_record['CMCODE'].apply(nullify).values[0] if 'CMCODE' in cm_record.columns else None
                                        cmindc = cm_record['CMINDC'].apply(nullify).values[0] if 'CMINDC' in cm_record.columns else None
                                        cmdecod = cm_record['CMDECOD'].apply(nullify).values[0] if 'CMDECOD' in cm_record.columns else None
                                        aedecod = ae_record['AEDECOD'].apply(nullify).values[0] if 'AEDECOD' in ae_record.columns else None
                                        drug_flag = False
                                        
                                        # if cmaer.upper() == 'YES':
                                        #     if cmongo.upper() == 'NO':
                                        #         if cm_stdt.values[0] >= ae_stdt.values[0]:
                                        #             if cm_endt.values[0] <= ae_endt.values[0]:
                                        #                 drug_flag = True
                                        #     if drug_flag == True:
                                        valid_drug = check_valid_drug_indication(cmtrt=cm_term.upper(), 
                                                                             aeterm=ae_term.upper(),
                                                                             aeptcode=aeptcd,
                                                                             aelltcode=aelltcd, 
                                                                             cmcode=cmcode,
                                                                             cmindc=cmindc,
                                                                             aedecod=aedecod,
                                                                             cmdecod=cmdecod
                                                                             )
                                        print('valid_drug',valid_drug,ae_term,cm_term)
                                        if valid_drug == False:
                                            print('valid_drug',valid_drug)
                                            subcate_report_dict = {}
                                            report_dict = {}
                                            piv_rec = {'CM':cm_record, 
                                                'AE':ae_record}
                                            for dom, cols in a['FIELDS_FOR_UI'][sub_cat].items():
                                                piv_df = piv_rec[dom]
                                                present_col = [col for col in cols if col in piv_df.columns.tolist()]
                                                rep_df = piv_df[present_col]
                                                rep_df['deeplink'] = utils.get_deeplink(study, piv_df)
                                                rep_df = rep_df.rename(columns=(a['FIELD_NAME_DICT']))
                                                report_dict[dom] = rep_df.to_json(orient='records')

                                            subcate_report_dict[sub_cat] = report_dict
                                            aespid = ae_record['AESPID'].values[0]
                                            cm_record1 = cm_record.iloc[0]
                                            query_text_param = {'AESPID':aespid, 
                                                'AETERM':ae_term}
                                            payload = {'subcategory':sub_cat,
                                                'cdr_skey':str(cm_record1['cdr_skey']), 
                                                'query_text':self.get_model_query_text_json(study, sub_cat, params=query_text_param), 
                                                'form_index':str(cm_record1['form_index']), 
                                                'question_present':self.get_subcategory_json(study, sub_cat), 
                                                'modif_dts':str(cm_record1['modif_dts']), 
                                                'stg_ck_event_id':int(cm_record1['ck_event_id']), 
                                                'formrefname':str(cm_record1['formrefname']), 
                                                'report':subcate_report_dict, 
                                                'confid_score':np.random.uniform(0.7, 0.97)}
                                            print(payload)
                                            self.insert_query(study, subject, payload)
                                            if payload not in payload_list:
                                                payload_list.append(payload)


                    except Exception as e:
                        logging.exception(e)
                        continue

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
    rule = CMAE5(study_id, account_id, job_id, rule_id, version)
    rule.run()