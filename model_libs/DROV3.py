import sys
import logging
import yaml
import warnings
warnings.filterwarnings('ignore')
import traceback
import numpy as np
import pandas as pd
from base import BaseSDQApi
import utils as utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class DROV3(BaseSDQApi):
    domain_list= ['EX']

    def execute(self):
        study = self.study_id
        ov_study_id = f'account_{self.account_id}_study_{self.study_id}'
        duplicate=DuplicateCheck(ov_study_id)
        check_if_duplicate=duplicate.check

        
        sub_cat = self.__class__.__name__#'DROV3'
        f_d = 'display_fields'
        f_c = 'fn_config'
        try:
            fields_dict = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_d))
            fn_config = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_c))
            fields_labels = self.get_field_labels(self.account_id,self.domain_list[0])
            
            # match_config = fn_config['match']
            _cols = fn_config['cols']
            vals = fn_config['vals']
            dt_cols = fn_config['cols']['dt_cols']
            subjects = self.get_subjects(study, domain_list = self.domain_list)
            # deeplink_template = self.get_deeplink(study)
            get_date = utils.get_date
            # norm = utils.normalise_df
            fmt = utils.format_datetime
            # subjects = ['9999903']
        except Exception as e:
            print('e=',e)

        # subjects = ['9290098']
        for subject in tqdm.tqdm(subjects):
            payload_list = []
            try:
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list = self.domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)
                new_ec_df = pd.DataFrame(flatten_data[self.domain_list[0]])

                for dates in dt_cols:
                    new_ec_df = new_ec_df[new_ec_df[dates].notna()]
                subject_id = new_ec_df['subjid'].unique().tolist()
                for sid in subject_id:
                    new_exo_records = new_ec_df[new_ec_df['subjid']==sid]
                    new_exo_records = new_exo_records.sort_values(dt_cols[:2])
                    new_exo_records = new_exo_records.reset_index(drop=True)
                    for ix, ir in new_exo_records.iterrows():
                        try:    
                            if ix != 0:
                                prev_rec = new_exo_records.loc[[ix-1]]
                                cur_rec = new_exo_records.loc[[ix]]

                                dt1,dt2 = cur_rec[dt_cols[0]].values[0],prev_rec[dt_cols[1]].values[0]
                                dt_diff = (get_date(dt1)-get_date(dt2)).days
                                if dt_diff > vals['ex_dt_gap']:
                                

                                    subcate_report_dict = {}
                                    report_dict = {}

                                    piv_rec = {'EX1': cur_rec,'EX2' : prev_rec}
                                    for dom, cols in fields_dict.items():
                                        if len(cols) > 0:
                                            for k_dom in piv_rec.keys():
                                                piv_df = piv_rec[k_dom]
                                                present_col = [col for col in cols if col in piv_df.columns.tolist()]

                                                rep_df = piv_df[present_col]
                                                rep_df['deeplink'] = utils.get_deeplink(study, piv_df, self.get_study_source(), subject_id=self.get_map_subject_id(piv_df['subj_guid'].values[0]))
                                                rep_df = rep_df.rename(columns= fields_labels)
                                                rep_df = rep_df.loc[:,~rep_df.columns.duplicated()]
                                                report_dict[k_dom]= rep_df.to_json(orient= 'records')
                                    subcate_report_dict[sub_cat] = report_dict
                                    param_dict={} 
                                    try:
                                        qt = fn_config['qt']
                                        for i in qt:
                                            param_dict[i] = cur_rec[qt[i]].values[0]
                                    except:
                                        print('QT Error: ',traceback.format_exc())
                                    is_ov_duplicate=True
                                    payload = {
                                        "subcategory": sub_cat,
                                        "query_text": self.get_model_query_text_json(study, sub_cat, params=param_dict), 
                                        "form_index": int(cur_rec['form_index'].values[0]),
                                        "visit_nm": cur_rec['visit_nm'].values[0],
                                        "question_present": self.get_subcategory_json(study, sub_cat),
                                        "modif_dts": int(cur_rec['modif_dts'].values[0]),
                                        "stg_ck_event_id": int(cur_rec['ck_event_id'].values[0]),
                                        "report" : subcate_report_dict,
                                        "formrefname": cur_rec['formrefname'].values[0],
                                        "confid_score": np.random.uniform(0.7, 0.97),
                                        "cdr_skey": str(cur_rec['cdr_skey'].values[0]),
                                        }
                                    
                                    self.insert_query(study, subject, payload)
                                    print(subject, payload)
                                    if payload not in payload_list:
                                        payload_list.append(payload)
                                    # break
                        except:
                            print('===ecc:=',traceback.format_exc())
                            continue

            except:
                print('===ecc:=',traceback.format_exc())
                continue
            if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                self.close_query(subject, payload_list)




            if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                self.close_query(subject, payload_list)

if __name__ == '__main__':
    #(self.study_id, self.account_id, self.job_id, rule['ml_model_id'], 0.1
    # python DROV3.py 1 1 2 1 0.1 > DROV3log.txt
    study_id = sys.argv[1]
    account_id = sys.argv[2]
    job_id = sys.argv[3]
    rule_id = sys.argv[4]
    version = sys.argv[5]
    rule = DROV3(study_id, account_id, job_id, rule_id, version)
    
    rule.run()
