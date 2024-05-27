import sys
import logging
import traceback
import numpy as np
import pandas as pd
from base import BaseSDQApi
import utils as utils
import tqdm


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class DRAE3(BaseSDQApi):
    domain_list = ['EX', 'AE']

    def execute(self):
        study = self.study_id
        sub_cat = self.__class__.__name__#'DRAE3'
        f_d = 'display_fields'
        f_c = 'fn_config'
        
        fields_dict = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_d))
        fn_config = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_c))
        fields_labels_1 = self.get_field_labels(self.account_id,self.domain_list[0])
        fields_labels_2 = self.get_field_labels(self.account_id,self.domain_list[1])
        fields_labels = {**fields_labels_1, **fields_labels_2 }

        match_config = fn_config['match']
        match_terms = fn_config['match']
        match_cond = fn_config['match_condition']
        _cols = fn_config['cols']
        vals = fn_config['vals']
        dt_cols = fn_config['cols']['dt_cols']

        subjects = self.get_subjects(study, domain_list = self.domain_list) #['444-444-P1']#
        
        
        # subjects = ['9290128']
        for subject in tqdm.tqdm(subjects):
            payload_list = []
            try:
                temp_ec = pd.DataFrame()
                new_ec = pd.DataFrame()
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list = self.domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)
                ec_rec = pd.DataFrame(flatten_data[self.domain_list[0]])

                ae_rec = pd.DataFrame(flatten_data[self.domain_list[1]])

                try:
                    print('ecrec - cdr:',ec_rec['cdr_skey'].unique().tolist())
                    for items in _cols['adj_col']:
                        if items in ec_rec.columns.tolist():
                            new_ec = ec_rec[(ec_rec[items].str.upper().str.strip().isin(vals['adj_val']))]

                    temp_ec = new_ec.copy()
                    temp_ec[dt_cols[0]+'1'] = temp_ec[dt_cols[0]].apply(lambda x: utils.format_partial_date(x))
                    temp_ec[dt_cols[1]+'1'] = temp_ec[dt_cols[1]].apply(lambda x: utils.format_partial_date(x))
                    temp_ec = temp_ec.sort_values([dt_cols[0]+'1',dt_cols[1]+'1'])

                    for ind in temp_ec.reset_index(drop=True).index:
                        print('ecvalues:',temp_ec[dt_cols[:2]])
                        try:
                            dose_flag = False
                            current_dose = temp_ec.iloc[[ind]]
                            if current_dose['form_index'].values[0] not in new_ec['form_index'].values.tolist():
                                continue
                            new_ae = pd.DataFrame()
                            for i in match_terms:
                                if i.strip().upper() == 'MATCH_DSL':
                                    try:
                                        match_config_dsl = fn_config[i]
                                        match_flag, ae_match_df = utils.ae_cm_mapper(prim_rec=current_dose,
                                                    sec_df = ae_rec,
                                                    subcat = sub_cat,
                                                    match_type = 'dsl',
                                                    match_DSL = match_config_dsl
                                                    )
                                    except Exception as ex:
                                        print(f' Exception in match_DSL : {ex}')
                                        print(traceback.format_exc())
                                        continue                                
                                else:
                                    try:
                                        ae_match_df = pd.DataFrame()
                                        match_config = fn_config[i]
                                        match_flag, ae_match_df1 = utils.ae_cm_mapper(prim_rec=current_dose,
                                                            sec_df = ae_rec,
                                                            subcat = sub_cat,
                                                            **match_config)
                                        if len(match_cond)>0 and match_cond[0].upper() == 'AND':
                                            ae_match_df = ae_match_df1
                                            if(len(ae_match_df1) > 0):
                                                ae_rec = ae_match_df1
                                            else:
                                                break
                                        else:
                                            ae_match_df = ae_match_df.append(ae_match_df1)
                                    except Exception as ae_match_exc:
                                        print(f'Exception in ae_match_df is : {ae_match_exc}')
                                        continue

                            new_ae = ae_match_df
                            previous_dose = temp_ec.iloc[[ind-1]]
                            ecstdat = current_dose[dt_cols[0]].values[0]
                            try:
                                current_dose_value = int(float(current_dose[_cols['ex_dose_col'][0]].values[0]))
                            except:
                                current_dose_value = int(float(current_dose[_cols['ex_dose_col'][1]].values[0]))
                            try:
                                previous_dose_value = int(float(previous_dose[_cols['ex_dose_col'][0]].values[0]))
                            except:
                                previous_dose_value = int(float(previous_dose[_cols['ex_dose_col'][1]].values[0]))
                            

                            print('chkdose=',current_dose_value,previous_dose_value)
                            if current_dose_value > previous_dose_value and current_dose_value != 0 and previous_dose_value != 0:
                                dose_flag = True


                            extrt = current_dose[_cols['ex_trt_col'][0]].values[0].split('-')[-1].strip()
                            action = ''
                            temp_ae = new_ae
                            for col in temp_ae.columns.tolist():
                                if extrt.lower() in col.lower():
                                    action = col
                                    break
                            for idx in range(temp_ae.shape[0]):
                                aerow = temp_ae.iloc[[idx]]

                                if action == '':
                                    action = _cols['aeacn_col'][0]
                                if str(aerow[action].values[0]).strip().upper() not in vals['aeacn_val']:
                                    continue
                                
                                aestdat = aerow[dt_cols[2]].values[0]
                                aeendat = aerow[dt_cols[3]].values[0]

                                for dates in [aestdat, aeendat, ecstdat]:
                                    if pd.isnull(dates):
                                        continue

                                print('dates=',ecstdat, aestdat)
                                if (utils.compare_partial_date(ecstdat, aestdat, '<') or \
                                    utils.compare_partial_date(ecstdat, aeendat, '>')) and dose_flag:

                                    subcate_report_dict = {}
                                    report_dict = {}

                                    current_dose[dt_cols[0]] = pd.to_datetime(current_dose[dt_cols[0]]).dt.strftime("%d-%B-%Y")
                                    current_dose[dt_cols[1]] = pd.to_datetime(current_dose[dt_cols[1]]).dt.strftime("%d-%B-%Y")
                                    try:
                                        aerow[dt_cols[2]]= pd.to_datetime(aerow[dt_cols[2]]).dt.strftime("%d-%B-%Y")
                                        aerow[dt_cols[3]]= pd.to_datetime(aerow[dt_cols[3]]).dt.strftime("%d-%B-%Y")
                                    except:
                                        pass

                                    aerow = aerow.replace(np.nan, 'blank')
                                    current_dose = current_dose.replace(np.nan, 'blank')
                                    previous_dose = previous_dose.replace(np.nan, 'blank')

                                    piv_rec = {'EX1' : current_dose,'EX2':previous_dose,'AE':aerow}
                                
                                    for dom, cols in fields_dict.items():
                                        if dom not in piv_rec:
                                            continue
                                        piv_df = piv_rec[dom]
                                        present_col = [col for col in cols if col in piv_df.columns.tolist()]
                                        rep_df = piv_df[present_col]
                                        rep_df['deeplink'] = utils.get_deeplink(study, piv_df, self.get_study_source(), subject_id=self.get_map_subject_id(piv_df['subj_guid'].values[0]))
                                        rep_df = rep_df.rename(columns= fields_labels)
                                        rep_df = rep_df.loc[:,~rep_df.columns.duplicated()]
                                        report_dict[dom]= rep_df.to_json(orient= 'records')

                                    subcate_report_dict[sub_cat] = report_dict
                                    param_dict={}
                                    try:
                                        qt = fn_config['qt']
                                        for i in qt:
                                            param_dict[i] = current_dose[qt[i]].values[0]
                                    except:
                                        print('QT Error: ',traceback.format_exc())
                                    payload = {
                                        "subcategory": sub_cat,
                                        "query_text": self.get_model_query_text_json(study, sub_cat, params=param_dict), 
                                        "form_index": str(current_dose['form_index'].values[0]),
                                        "question_present": self.get_subcategory_json(study, sub_cat),
                                        "modif_dts": str(current_dose['modif_dts'].values[0]),
                                        "stg_ck_event_id": int(current_dose['ck_event_id'].values[0]),
                                        "report" : subcate_report_dict,
                                        "formrefname": str(current_dose['formrefname'].values[0]),
                                        "confid_score": np.random.uniform(0.7, 0.97),
                                        "cdr_skey": str(current_dose['cdr_skey'].values[0]),
                                        }
                                    self.insert_query(study, subject, payload)
                                    print(subject, payload)
                                    if payload not in payload_list:
                                        payload_list.append(payload)
                        except Exception:
                            print('---->>exc:',traceback.format_exc())

                except Exception:
                    print('---->>exc:',traceback.format_exc())

            except Exception:
                print('---->>exc:',traceback.format_exc())
            if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                self.close_query(subject, payload_list)


if __name__ == '__main__':
    #(self.study_id, self.account_id, self.job_id, rule['ml_model_id'], 0.1
    # python DRAE3.py 1 1 2 1 0.1 > DRAE3log.txt
    study_id = sys.argv[1]
    account_id = sys.argv[2]
    job_id = sys.argv[3]
    rule_id = sys.argv[4]
    version = sys.argv[5]
    rule = DRAE3(study_id, account_id, job_id, rule_id, version)
    
    rule.run()