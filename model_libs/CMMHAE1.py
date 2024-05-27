import os
import sys
import logging
import yaml
import traceback
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
import utils
from base import BaseSDQApi
import tqdm
from subprocess import Popen, PIPE

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class CMMHAE1(BaseSDQApi):
    domain_list = ['CM','MH','AE']

    def execute(self):
        study = self.study_id
        sub_cat = self.__class__.__name__#'CMMHAE1'

        f_d = 'display_fields'
        f_c = 'fn_config'

        try:
            fields_dict = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_d))
            fn_config = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_c))
            fields_labels_1 = self.get_field_labels(self.account_id,self.domain_list[0])
            fields_labels_2 = self.get_field_labels(self.account_id,self.domain_list[1])
            fields_labels_3 = self.get_field_labels(self.account_id,self.domain_list[2])
            fields_labels = {**fields_labels_1,**fields_labels_2, **fields_labels_3}
            cols = fn_config['cols']  
            subjects = self.get_subjects(study, domain_list=self.domain_list, per_page = 10000)
            match_terms_ae = fn_config['match_ae']
            match_terms_mh = fn_config['match_mh']
            match_cond_ae = fn_config['match_ae_condition']#non-DSL
            match_cond_mh = fn_config['match_mh_condition']#non-DSL
            smc = fn_config['use_smc'] if 'use_smc' in fn_config.keys() else False
            predict_med_code = None
        
        except Exception as fn_exc:
            print(f'Exception while fetching config/subjects : {fn_exc}')
            print(traceback.format_exc())  
    
        for subject in tqdm.tqdm(subjects):
            payload_list = []
            print('Subject is:', subject)
            payload_list = []
            try:
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list = self.domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)
                try:
                    cm_df = pd.DataFrame(flatten_data.get(self.domain_list[0],[]))
                    mh_df = pd.DataFrame(flatten_data.get(self.domain_list[1],[]))
                    ae_df = pd.DataFrame(flatten_data.get(self.domain_list[2],[]))
                    
                    if len(cm_df) == 0:
                        continue

                    cm_df = cm_df[cm_df[cols['cm_indc_col'][0]].notna()]   
                        
                except:
                    print(traceback.format_exc())
                    pass

                for ind in range(cm_df.shape[0]):
                    try:
                        mh_df1 = pd.DataFrame()
                        new_mh_df = pd.DataFrame()
                        new_ae_df = pd.DataFrame()
                        ae_df1 = pd.DataFrame()

                        cm_record = cm_df.iloc[[ind]]
                       
                        if len(mh_df) > 0:
                            mh_cols = mh_df.columns.tolist()
                            mhrefid = ''

                            for i in cols['mh_ref_id_col']:
                                if i in mh_cols:
                                    mhrefid = i
                                    break
                            try:
                                mh_df_new = mh_df.copy() 
                                for i in match_terms_mh:
                                    #print('The config (inside match_terms) is :', i)
                                    if(i.upper() == 'MATCH_MH_DSL'):
                                        match_mh_dsl = fn_config[i] 
                                        if smc:
                                            predict_med_code = self.predict_medical_code
        
                                        try:
                                            match_flag, new_mh_df = utils.ae_cm_mapper(prim_rec=cm_record,
                                                        sec_df = mh_df,
                                                        subcat = sub_cat,
                                                        match_type = 'dsl',
                                                        use_smc = smc,
                                                        func = predict_med_code,
                                                        match_DSL = match_mh_dsl
                                                        )
                                        except Exception as ex:
                                            print(f' Exception in match_DSL : {ex}')
                                            print(traceback.format_exc())
                                            continue                                    
                                    else:
                                        match_config_mh = fn_config[i]
                                        try:
                                            if i.upper() == 'MATCH_MH_TERM' and smc :
                                                predict_med_code = self.predict_medical_code
                                                sec_col = match_config_mh['sec_match_col']
                                                mh_df_new = mh_df_new[mh_df_new[sec_col].notna()]
                                            else:
                                                predict_med_code = None
                                            
                                            #print('match_config MH is :', subject, match_config_mh)
                                            match_flag_1, mh_df1 = utils.ae_cm_mapper(prim_rec=cm_record,
                                                                sec_df = mh_df_new,
                                                                subcat = sub_cat,
                                                                use_smc = smc,
                                                                func = predict_med_code,
                                                                **match_config_mh)
                                        except Exception as mat_exc:
                                            print(traceback.format_exc())
                                            print(f'Exception in match_config_mh is : {subject}, {mat_exc}')
                                            pass

                                        if len(match_cond_mh)>0 and match_cond_mh[0].upper() == 'AND':
                                            new_mh_df = mh_df1
                                            if(len(mh_df1) > 0):
                                                mh_df_new = mh_df1
                                            else:
                                                break
                                        else:
                                            new_mh_df = new_mh_df.append(mh_df1)
                        
                            except Exception as Ex3:
                                print(traceback.format_exc())
                                print(f'Exception in match_config_mh is : {subject},{Ex3}')
                                pass
                                                        
                            if(len(new_mh_df) > 0):
                                continue
                        #mh_df = new_mh_df

                        if len(ae_df) > 0:
                            ae_cols = ae_df.columns.tolist()
                            aerefid = ''

                            for i in cols['ae_ref_id_col']:
                                if i in ae_cols:
                                    aerefid = i
                                    break
                            try:
                                ae_df2 = ae_df.copy()
                                for i in match_terms_ae:
                                    if(i.upper() == 'MATCH_AE_DSL'):
                                        match_ae_dsl = fn_config[i]       
                                        try:
                                            if smc:
                                                predict_med_code = self.predict_medical_code
                                            match_flag, new_ae_df = utils.ae_cm_mapper(prim_rec=cm_record,
                                                        sec_df = ae_df,
                                                        subcat = sub_cat,
                                                        match_type = 'dsl',
                                                        use_smc = smc,
                                                        func = predict_med_code,
                                                        match_DSL = match_ae_dsl
                                                        )
                                        except Exception as ex:
                                            print(f' Exception in match_DSL : {ex}')
                                            print(traceback.format_exc())
                                            continue 
                                    else: 
                                        match_config_ae = fn_config[i]       
                                        try:
                                            if i.upper() == 'MATCH_AE_TERM' and smc :
                                                predict_med_code = self.predict_medical_code
                                                sec_col = match_config_ae['sec_match_col']
                                                ae_df2 = ae_df2[ae_df2[sec_col].notna()]
                                            else:
                                                predict_med_code = None
                                            match_flag_1, ae_df1 = utils.ae_cm_mapper(prim_rec=cm_record,
                                                            sec_df = ae_df2,
                                                            subcat = sub_cat,
                                                            use_smc = smc,
                                                            func = predict_med_code,
                                                            **match_config_ae)
                                        except Exception as match_exc:
                                            print(f'Exception while matching : {match_exc}')
                                            print(traceback.format_exc())
                                            continue

                                        if len(match_cond_ae)>0 and match_cond_ae[0].upper() == 'AND':
                                            new_ae_df = ae_df1
                                            if(len(ae_df1) > 0):
                                                ae_df2 = ae_df1
                                            else:
                                                break
                                        else:
                                            new_ae_df = new_ae_df.append(ae_df1)
                            except Exception as Ex1:
                                print(f'Exception in match_config1 is : {subject},{Ex1}')
                                print(traceback.format_exc())
                                continue                                     
                            
                            if(len(new_ae_df)>0):
                                continue

                        #ae_df = new_ae_df

                        if (len(new_mh_df) == 0 and len(new_ae_df) == 0):
                            subcate_report_dict = {}
                            report_dict = {}                             
                            
                            ae_blank = {}
                            mh_blank = {}
                            for ae_b_col in fields_dict[self.domain_list[2]]:
                                ae_blank[ae_b_col] = [' ']
                            ae_blank = pd.DataFrame.from_dict(ae_blank).head(1)

                            for mh_b_col in fields_dict[self.domain_list[1]]:
                                mh_blank[mh_b_col] = [' ']
                            mh_blank = pd.DataFrame.from_dict(mh_blank).head(1)
                            
                            piv_rec = {'CM': cm_record, 'MH': mh_blank, 'AE' : ae_blank}

                            for dom, columns in fields_dict.items():
                                piv_df = piv_rec[dom]
                                present_col = [col for col in columns if col in piv_df.columns.tolist()]
                                rep_df = piv_df[present_col]
                                try:
                                    rep_df['deeplink'] = utils.get_deeplink(study, piv_df, self.get_study_source(), subject_id=self.get_map_subject_id(piv_df['subj_guid'].values[0]))
                                except:
                                    rep_df['deeplink'] = '#'
                                rep_df = rep_df.rename(columns= fields_labels)
                                rep_df = rep_df.loc[:,~rep_df.columns.duplicated()]
                                report_dict[dom]= rep_df.to_json(orient= 'records') 
                            print('-*'*20,'\n')

                            subcate_report_dict[sub_cat] = report_dict
                            
                            query_text_param={
                                    cols['cm_trt_col'][0]: cm_record[cols['cm_trt_col'][0]].values[0]
                                    }

                            payload = {
                                    "subcategory": sub_cat,
                                    "cdr_skey" : str(cm_record['cdr_skey'].values[0]),
                                    "query_text": self.get_model_query_text_json(study, sub_cat, params =query_text_param),
                                    "form_index": str(cm_record['form_index'].values[0]),
                                    "question_present": self.get_subcategory_json(study, sub_cat),
                                    "modif_dts": str(cm_record['modif_dts'].values[0]),  
                                    "stg_ck_event_id": int(cm_record['ck_event_id'].values[0]),
                                    "formrefname" : str(cm_record['formrefname'].values[0]),
                                    "report" : subcate_report_dict,
                                    "confid_score": np.random.uniform(0.7, 0.97)
                                    }        
                            self.insert_query(study, subject, payload)
                            print('Subject and Payload is :', subject, payload)
                            if payload not in payload_list:
                                payload_list.append(payload)
                    except:
                        print(traceback.format_exc())
                        continue
            except:
                print(traceback.format_exc())
                continue
            if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                self.close_query(subject, payload_list)

if __name__ == '__main__':
    #(self.study_id, self.account_id, self.job_id, rule['ml_model_id'], 0.1
    # python CMMHAE1.py 1 1 14 1 0.1 > cmmhae1log.txt
    study_id = sys.argv[1]
    account_id = sys.argv[2]    
    rule_id = sys.argv[3]
    job_id = sys.argv[4]
    version = sys.argv[5]
    rule = CMMHAE1(study_id, account_id, job_id, rule_id, version)
    
    rule.run()
