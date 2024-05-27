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

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class CMMHAE0(BaseSDQApi):
    domain_list = ['CM','MH','AE']

    def execute(self):
        study = self.study_id
        sub_cat = self.__class__.__name__ #'CMMHAE0'

        f_d = 'display_fields'
        f_c = 'fn_config'
        try:
            fields_dict = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_d))           
            fn_config = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_c))
            fields_label_1 = self.get_field_labels(self.account_id,self.domain_list[0])
            fields_label_2 = self.get_field_labels(self.account_id,self.domain_list[1])
            fields_label_3 = self.get_field_labels(self.account_id,self.domain_list[2])
            fields_labels = {**fields_label_1, **fields_label_2, **fields_label_3}
                                    
            match_ae_terms = fn_config['match_ae']
            match_mh_terms = fn_config['match_mh']
            match_cond_ae = fn_config['match_ae_condition']#non-DSL
            match_cond_mh = fn_config['match_mh_condition']#non-DSL
            
            cols = fn_config['cols']
            dt_cols = cols['dt_cols']
            symptoms_list = fn_config['symptoms_list']
            match_dsl_ae_flg = False
            match_dsl_mh_flg = False

            smc = fn_config['use_smc'] if 'use_smc' in fn_config.keys() else False
            predict_med_code = None

            subjects = self.get_subjects(study, domain_list=self.domain_list, per_page = 10000)
        
        except Exception as fn_exc:
            print(f'Exception while fetching config/subjects : {fn_exc}')
            print(traceback.format_exc())  

        for subject in tqdm.tqdm(subjects):
            payload_list = []
            print('Subject is:', subject)
            payload_list = []
            mh_df = pd.DataFrame()
            ae_df = pd.DataFrame()
            cm_df = pd.DataFrame()
            try:
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list = self.domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)
                try:
                    cm_df = pd.DataFrame(flatten_data.get(self.domain_list[0],[]))
                    mh_df = pd.DataFrame(flatten_data.get(self.domain_list[1],[]))
                    ae_df = pd.DataFrame(flatten_data.get(self.domain_list[2],[]))
                    
                    if len(cm_df) == 0 or len(mh_df) == 0:
                        continue
                    
                    cm_df = cm_df[cm_df[cols['cm_trt_col'][0]].notna()]                    
                    cm_df = cm_df[cm_df[dt_cols[0]].notna()]
                    mh_df = mh_df[mh_df[dt_cols[3]].notna()]

                except:
                    pass

                for ind in range(cm_df.shape[0]):
                    try:
                        cm_record = cm_df.iloc[[ind]]
                        cmtrt = cm_record[cols['cm_trt_col'][0]].values[0]
                        cmstdt = cm_record[dt_cols[0]].values[0]
                            
                        ae_df1 = pd.DataFrame()
                        new_ae_df = pd.DataFrame()
                        mh_df1 = pd.DataFrame()
                        new_mh_df = pd.DataFrame()

                        if(len(ae_df) > 0):
                            ae_cols = ae_df.columns.tolist()
                            aerefid = ''

                            for i in cols['ae_ref_id_col']:
                                if i in ae_cols:
                                    aerefid = i
                                    break
                            
                            #ae_df_refid = ae_df[ae_df[aerefid].astype(dtype = np.float64).astype(dtype=np.int64) == cmaeno]
                            try:
                                ae_df2 = ae_df.copy()
                                #match_config1['sec_match_col'] = aerefid
                                for i in match_ae_terms:
                                    #print('i is :',i)
                                    if(i.upper() == 'MATCH_AE_DSL'):
                                        match_ae_dsl = fn_config[i]       
                                        match_dsl_ae_flg = True  
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
                                        
                                        if len(match_cond_ae)>0 and str(match_cond_ae[0]).upper() == 'AND':
                                            new_ae_df = ae_df1
                                            if(len(ae_df1) > 0):
                                                ae_df2 = ae_df1
                                            else:
                                                break
                                        else:
                                            new_ae_df = new_ae_df.append(ae_df1)

                            except Exception as Ex1:
                                print(f'Exception in match_config_ae is : {Ex1}')
                                pass                        

                        if(len(new_ae_df)>0):                            
                            if not match_dsl_ae_flg:                                
                                new_ae_df.drop_duplicates([cols['uniq_id_col'][0]],keep='first', inplace = True)
                            print('Len of ae_df is ',len(new_ae_df))
                         
                        if(len(mh_df) > 0):
                            mh_cols = mh_df.columns.tolist()
                            mhrefid = ''

                            for i in cols['mh_ref_id_col']:
                                if i in mh_cols:
                                    mhrefid = i
                                    break

                            #mh_df_refid = mh_df[mh_df[mhrefid].astype(dtype = np.float64).astype(dtype=np.int64)  == cmmhno]
                            try:
                                mh_df2 = mh_df.copy()
                                for x in match_mh_terms:                                    
                                    if(x.upper() == 'MATCH_MH_DSL'):
                                        match_mh_dsl = fn_config[x]  
                                        match_dsl_mh_flg = True       
                                        try:
                                            if smc:
                                                predict_med_code = self.predict_medical_code
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
                                        match_config_mh = fn_config[x]      
                                        try:
                                            if x.upper() == 'MATCH_MH_TERM' and smc :
                                                predict_med_code = self.predict_medical_code
                                                sec_col = match_config_mh['sec_match_col']
                                                mh_df2 = mh_df2[mh_df2[sec_col].notna()]
                                            else:
                                                predict_med_code = None
                                            
                                            match_flag, mh_df1 = utils.ae_cm_mapper(prim_rec=cm_record,
                                                                sec_df = mh_df2,
                                                                subcat=sub_cat,
                                                                use_smc = smc,
                                                                func = predict_med_code,
                                                                **match_config_mh)
                                            if len(match_cond_mh)>0 and str(match_cond_mh[0]).upper() == 'AND':
                                                new_mh_df = mh_df1
                                                if(len(mh_df1) > 0):
                                                    mh_df2 = mh_df1
                                                else:
                                                    break
                                            else:
                                                new_mh_df = new_mh_df.append(mh_df1)
                                        except Exception as mh_exc:
                                            print(f'Exception in mh_df1 match is : {mh_exc}')
                                            continue
                            except Exception as Ex3:
                                print(f'Exception in match_config_mh is : {Ex3}')
                                pass                          

                        if(len(new_mh_df))>0:
                            if not match_dsl_mh_flg:
                                new_mh_df.drop_duplicates([cols['uniq_id_col'][0]], keep = 'last',inplace= True)
                            new_mh_df = new_mh_df[new_mh_df[dt_cols[3]].apply(utils.compare_partial_date, args=(cmstdt, '<',))]
                            if(len(new_mh_df) == 0):
                                continue
                        else:
                            continue

                        worsening_flg = True
                        
                        if(len(new_ae_df) > 0):       
                            ae_term = str(new_ae_df[cols['ae_term_col'][0]].values[0]).upper()
                            for i in symptoms_list:
                                if i in ae_term.upper():
                                    worsening_flg = True
                                    break
                                else:
                                    worsening_flg = False

                            #worsening_flg = True if 'WORSENING' in ae_term else False
                        
                        #print('worsening flag is :', worsening_flg)

                        if not worsening_flg or len(new_ae_df) == 0:
                            for mhind in range(new_mh_df.shape[0]):
                                mh_record = new_mh_df.iloc[[mhind]]                                
                                '''
                                try:
                                    drug_validity = False
                                    #drug_validity = True # for testing                               
                                    drug_validity = check_valid_drug_indication(cmtrt.upper(), mh_term.upper()) 
                                    print('Drug Validity :', cmtrt, mh_term, drug_validity)
                                except Exception as drug_exc:
                                    print(f'Error in checking drug validity {drug_exc}')
                                    continue
                                '''
                                subcate_report_dict = {}
                                report_dict = {}   

                                if(len(new_ae_df) == 0):
                                    #fields_labels = {**fields_label_1, **fields_label_2}
                                    fields_labels = {**fields_label_1, **fields_label_2, **fields_label_3}
                                    ae_blank = {}
                                    for ae_b_col in fields_dict[self.domain_list[2]]:
                                        ae_blank[ae_b_col] = [' ']
                                    #ae_blank = {cols['ae_term_col'] : [' '], cols['ae_id_col'] : [' '], cols['ae_decod_col'] : [' '], cols['ae_endt_col'] : [' '],cols['ae_contrt_col'] : [' ']}
                                    ae_blank = pd.DataFrame.from_dict(ae_blank).head(1)
                                    piv_rec = {'CM': cm_record, 'MH' : mh_record, 'AE' : ae_blank}
                                else:              
                                    fields_labels = {**fields_label_1, **fields_label_2, **fields_label_3}                      
                                    piv_rec = {'CM': cm_record, 'MH' : mh_record,'AE' : new_ae_df.iloc[[0]] }
                                
                                for dom, columns in fields_dict.items():
                                    piv_df = piv_rec[dom]
                                    present_col = [col for col in columns if col in piv_df.columns.tolist()]
                                    rep_df = piv_df[present_col]
                                    try:
                                        rep_df['deeplink'] = utils.get_deeplink(study, piv_df, self.get_study_source(), subject_id=self.get_map_subject_id(piv_df['subj_guid'].values[0]))
                                    except:
                                        rep_df['deeplink'] = '#'
                                    rep_df = rep_df.rename(columns = fields_labels)
                                    rep_df = rep_df.loc[:,~rep_df.columns.duplicated()]
                                    report_dict[dom]= rep_df.to_json(orient= 'records') 
                                print('-*'*20,'\n')

                                subcate_report_dict[sub_cat] = report_dict                                
                                
                                query_text_param={
                                    cols['cm_trt_col'][0]: cm_record[cols['cm_trt_col'][0]].values[0],
                                    dt_cols[0]: cm_record[dt_cols[0]].values[0],
                                    dt_cols[3]: mh_record[dt_cols[3]].values[0]
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
    # python CMMHAE0.py 1 1 14 1 0.1 > cmmhae0log.txt
    study_id = sys.argv[1]
    account_id = sys.argv[2]    
    rule_id = sys.argv[3]
    job_id = sys.argv[4]
    version = sys.argv[5]
    rule = CMMHAE0(study_id, account_id, job_id, rule_id, version)
    
    rule.run()
