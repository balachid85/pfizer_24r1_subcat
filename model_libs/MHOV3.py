import sys
import logging
import warnings
warnings.filterwarnings('ignore')
import traceback
import numpy as np
import pandas as pd
import utils
from base import BaseSDQApi

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class MHOV3(BaseSDQApi):
    domain_list = ['MH']

    def execute(self):
        study= self.study_id
        sub_cat = self.__class__.__name__ #'MHOV3'
        f_c = 'fn_config'
        f_d = 'display_fields'
        
        try:
            fields_dict = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_d))
            fields_labels = self.get_field_labels(self.account_id,self.domain_list[0])
            fn_config = (self.get_field_dict(self.account_id, self.study_id, sub_cat, f_c))

            cols = fn_config['cols']
            dt_cols = cols['dt_cols']
            vals = fn_config['vals']
            flags = fn_config['flags']
            smc_flg = flags['smc_flag']
            mapper_flg = not(flags['llt_flag'])
            mhtox_present = False
            
            dup_list = []

            subjects = self.get_subjects(study, domain_list = self.domain_list, per_page = 10000)
        except Exception as fn_exc:
            print(f'Exception while fetching config/ retreiving subjects: {fn_exc}')
            print(traceback.format_exc()) 
        
        for subject in tqdm.tqdm(subjects):
            payload_list = []subjects:
            print('Subject is: ',subject)
            payload_list = []
            try:
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, domain_list = self.domain_list)
                if not flatten_data.get(self.domain_list[0],[]):
                    if self.has_auto_closure(self.study_id,  self.rule_id, self.version):
                        self.close_query(subject, payload_list)
                try:
                    mh_df = pd.DataFrame(flatten_data.get(self.domain_list[0],[]))
                    if(len(mh_df) > 0):
                        mh_df = mh_df[mh_df[dt_cols[0]].notna()]
                    if(len(mh_df)>0):
                        mh_cols = mh_df.columns.tolist()
                        if len(cols['mh_tox_col']) > 0  and cols['mh_tox_col'][0] in mh_cols:
                            mhtox_present = True
                        #mh_df = mh_df[mh_df[dt_cols[1]].notna()]
                except Exception as df_exc:
                    print('Exception while fetching df:', df_exc)
                    continue
                for ind in range(mh_df.shape[0]):
                    try:
                        dup_flg = False
                        mh_record = mh_df.iloc[[ind]]
                        total_valid_present = 0
                        hash1 = mh_record[cols['uniq_id_col'][0]].values[0]
                        if(len(dup_list)>0):
                            for (i,j) in dup_list:
                                if(i == hash1):
                                    dup_flg = True
                                    break
                        if(dup_flg):
                            continue
                        mhdecod = str(mh_record[cols['mh_decod_col'][0]].values[0])
                        mhst_dt = str(mh_record[dt_cols[0]].values[0])
                        mhen_dt = str(mh_record[dt_cols[1]].values[0])
                        mhongo = str(mh_record[cols['mh_ongo_col'][0]].values[0])
                        #mhtoxgr = mh_record[cols['mh_tox_col'][0]].values[0]            

                        if smc_flg:
                            if (pd.isna(mhdecod) or str(mhdecod).strip().upper() in ['NONE','NULL','NAN','']) and ((not mapper_flg) and pd.isna(mhllt) and str(mhllt).strip().upper() in ['NONE','NULL','NAN','']):
                                mhdecod, mhllt = self.get_decode_llt(mhterm, mhdecod, mhllt, mapper_flg, model = 'term')
                            elif (pd.isna(mhdecod) or str(mhdecod).strip().upper() in ['NONE','NULL','NAN','']):
                                print('Getting decod as mhdecod is null')
                                mhdecod, _ = self.get_decode_llt(mhterm, mhdecod, mhllt, mapper_flg, model = 'term')
                                print('Generated code is :', mhdecod,'for mh term :', mhterm)
                            elif ((mapper_flg) and pd.isna(mhllt) and str(mhllt).strip().upper() in ['NONE','NULL','NAN','']):
                                _,mhllt = self.get_decode_llt(mhterm, mhdecod, mhllt, mapper_flg, model = 'term')

                        mh_records = mh_df
                        mh_records = mh_records[mh_records[cols['uniq_id_col'][0]] != hash1]
                        new_mh_record = pd.DataFrame()
                        mh_record1 = pd.DataFrame()
                        mh_record2 = pd.DataFrame()
                        mh_records = mh_records[~((mh_records[dt_cols[0]] == mhst_dt) & (mh_records[dt_cols[1]] == mhen_dt))]
                        if pd.isna(mhongo) or mhongo == None or mhongo.upper() in vals['mh_ongo_val']['no_val']: 
                            new_mh_record = mh_records[(mh_records[dt_cols[0]].apply(utils.compare_partial_date, args = (mhst_dt, '>=',))) & \
                                            (mh_records[dt_cols[0]].apply(utils.compare_partial_date, args = (mhen_dt, '<',)))]
                               
                        elif (not pd.isna(mhongo)) and mhongo != None and mhongo.upper() in vals['mh_ongo_val']['yes_val']:
                            mh_record1 = mh_records[(mh_records[dt_cols[0]].apply(utils.compare_partial_date, args = (mhst_dt, '>',)))]
                            mh_record2 = mh_records[(mh_records[dt_cols[0]] == mhst_dt) & \
                                            (mh_records[cols['mh_ongo_col'][0]].str.upper() != str(mhongo).upper())]
                            new_mh_record = new_mh_record.append(mh_record1, ignore_index = True)
                            new_mh_record = new_mh_record.append(mh_record2, ignore_index = True)
                            if(len(new_mh_record)>0):
                                new_mh_record.drop_duplicates([cols['uniq_id_col'][0]],keep='first', inplace = True)
                        
                        if len(new_mh_record) == 0:
                            continue

                        if(mhtox_present):
                            mhtoxgr = mh_record[cols['mh_tox_col'][0]].values[0]  
                            new_mh_record[cols['mh_tox_col'][0]] = new_mh_record[cols['mh_tox_col'][0]].apply(lambda x: str(x).upper())
                            new_mh_record = new_mh_record[new_mh_record[cols['mh_tox_col'][0]] != str(mhtoxgr).upper()]
                        
                        total_valid_pred = 0
                        if len(new_mh_record) >= 1:
                            final_mh_df = pd.DataFrame()
                            total_valid_pred = 0
                            for new_ind in new_mh_record.index.tolist():
                                temp_mh_rec = new_mh_record.loc[[new_ind]]
                                curr_mhdecod = str(temp_mh_rec[cols['mh_decod_col'][0]].values[0]) 
                                mhdecod_flag = utils.check_similar_medication_verabtim2(mhdecod.upper(), curr_mhdecod.upper())
                                
                                if smc_flg:
                                    if (pd.isna(curr_mhdecod) or str(curr_mhdecod).strip().upper() in ['NONE','NULL','NAN','']) and ((not mapper_flg) and pd.isna(curr_mhllt) and str(curr_mhllt).strip().upper() in ['NONE','NULL','NAN','']):
                                        curr_mhdecod, curr_mhllt = self.get_decode_llt(curr_mhterm, curr_mhdecod, curr_mhllt, mapper_flg, model = 'term')
                                    elif (pd.isna(curr_mhdecod) or str(curr_mhdecod).strip().upper() in ['NONE','NULL','NAN','']):
                                        print('Getting decod as mhdecod is null')
                                        curr_mhdecod, _ = self.get_decode_llt(curr_mhterm, curr_mhdecod, curr_mhllt, mapper_flg, model = 'term')
                                        print('Generated code is :', curr_mhdecod,'for mh term :', curr_mhterm)
                                    elif ((mapper_flg) and pd.isna(curr_mhllt) and str(curr_mhllt).strip().upper() in ['NONE','NULL','NAN','']):
                                        _,curr_mhllt = self.get_decode_llt(curr_mhterm, curr_mhdecod, curr_mhllt, mapper_flg, model = 'term')

                                if mhdecod_flag:
                                    final_mh_df = final_mh_df.append(temp_mh_rec, ignore_index = True)
                                    total_valid_pred += 1

                        if(total_valid_pred == 0):
                            continue

                        report_dict = {}
                        subcate_report_dict = {}
                        mh_record = mh_record.fillna('blank') 

                        if(total_valid_pred == 1):
                            dup_flg = False
                            hash2 = final_mh_df[cols['uniq_id_col'][0]].values[0]
                            if(len(dup_list)>0):
                                if ((hash1, hash2) in dup_list) or ((hash2, hash1) in dup_list):
                                    dup_flg = True                                    
                                    continue
                            if(not dup_flg):
                                dup_list.append((hash1, hash2))
                                piv_rec = {'MH1' : mh_record, 'MH2' : final_mh_df.head(1)} 
                                report_dict = {'MH1':dict(),'MH2':dict()}
                                curr_mh_record = final_mh_df.head(1)

                        else:    
                            temp_mh = pd.DataFrame()
                            piv_rec = {'MH1' : mh_record.head(1)}
                            report_dict = {'MH1':dict()}
                            dup_rec_counter = None
                            count = 1
                                
                            for mh_ind in final_mh_df.index.tolist():
                                dup_flg = False
                                temp_rec = final_mh_df.loc[[mh_ind]]
                                hash2 = temp_rec[cols['uniq_id_col'][0]].values[0]
                                if(len(dup_list)>0):
                                    if ((hash1, hash2) in dup_list) or ((hash2, hash1) in dup_list):
                                        dup_flg = True                                    
                                        continue
                                if(not dup_flg):
                                    dup_list.append((hash1,hash2))
                                    count = count + 1  
                                    dup_rec_counter = 'MH' + str(count)
                                    piv_rec[dup_rec_counter] = temp_rec     
                                    report_dict[dup_rec_counter] = dict()    
                                    temp_rec = temp_rec.fillna('blank')    
                                    temp_mh = temp_mh.append(temp_rec, ignore_index = True)  
                            if(len(piv_rec) == 1):
                                continue 
                            curr_mh_record = temp_mh.head(1)
                        curr_mh_record = curr_mh_record.fillna('blank')                         

                        for dom, columns in fields_dict.items():
                            if len(columns) > 0:
                                for k_dom in piv_rec.keys():
                                    piv_df = piv_rec[k_dom]
                                    present_col = [col for col in columns if col in piv_df.columns.tolist()]
                                    rep_df = piv_df[present_col]
                                    rep_df['deeplink'] = utils.get_deeplink(study, piv_df, self.get_study_source(), subject_id=self.get_map_subject_id(piv_df['subj_guid'].values[0]))
                                    rep_df = rep_df.rename(columns= fields_labels)
                                    rep_df = rep_df.loc[:,~rep_df.columns.duplicated()]
                                    report_dict[k_dom]= rep_df.to_json(orient= 'records')
                                    
                        subcate_report_dict[sub_cat] = report_dict
                        query_text_param = {
                                            dt_cols[0]+'1': mh_record[dt_cols[0]].values[0],
                                            dt_cols[1]+'1': mh_record[dt_cols[1]].values[0],
                                            dt_cols[0]+'2': curr_mh_record[dt_cols[0]].values[0],
                                            dt_cols[1]+'2': curr_mh_record[dt_cols[1]].values[0]
                                        } 
                        if mhtox_present:
                            query_text_param[cols['mh_tox_col'][0]] =  mh_record[cols['mh_tox_col'][0]].values[0]
                        payload = {
                            "subcategory": sub_cat,
                            "cdr_skey" : str(mh_record['cdr_skey'].values[0]),
                            "query_text": self.get_model_query_text_json(study, sub_cat, params = query_text_param),
                            "form_index": str(mh_record['form_index'].values[0]),
                            "question_present": self.get_subcategory_json(study, sub_cat),
                            "modif_dts": str(mh_record['modif_dts'].values[0]),  
                            "stg_ck_event_id": int(mh_record['ck_event_id'].values[0]),
                            "formrefname" : str(mh_record['formrefname'].values[0]),
                            "report" : subcate_report_dict,
                            "confid_score": np.random.uniform(0.7, 0.97)
                        }
                        print('Subject and Payload are :', subject, payload)
                        self.insert_query(study, subject, payload)
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
if __name__ == "__main__":
    #(self.study_id, self.account_id, self.job_id, rule['ml_model_id'], 0.1
    #python MHOV3.py 1 1 1 84 0.1 > mhov3log.txt
    study_id = sys.argv[1]
    account_id = sys.argv[2]
    job_id = sys.argv[3]
    rule_id = sys.argv[4]
    version = sys.argv[5]
    rule = MHOV3(study_id, account_id, job_id, rule_id, version)
    
    rule.run()
