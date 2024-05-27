import os
from tqdm import tqdm
import numpy as np
import os
import ast
import re
try:
    import data_utils as utils
except:
    import scripts.dataIngestion.data_utils as utils

import pickle as pkl
import pandas as pd
import yaml
import logging
from tqdm import tqdm
path = os.path.dirname(os.path.abspath(__file__))
import configparser


class FeatureEngineering:
    def __init__(self, config, cln_df_dict , dict_df, subj_list = None):
        if not config:
          self.config = configparser.ConfigParser()
          self.config.read(path + '/' + "dataload_configs.ini")
        else:
          self.config = config

        self.col_metadata = ast.literal_eval(self.config['data_preprocess']['col_metadata'])
        self.cln_df_dict = cln_df_dict
        self.dict_df  = dict_df
        self.subj_list = subj_list

    def extract_metadata_info(self):
        dict_df = self.dict_df
        item_name_dict = {}
        item_dtype_dict = {}
        item_question_dict = {}
        item_set_dict = {}
        item_dtype_meta_dict = {1: 'string', 2: 'integer', 3: 'float', 4: 'date'}
        for rec_id in dict_df.index.tolist(): 
            record = dict_df.loc[rec_id, :] 
            item_set_presence = record['ITEMSET']
            item_id = record['ITEMID'] if item_set_presence == 0 else record['CHILDITEMID']
            item_name = record['ITEMREFNAME'] if item_set_presence == 0 else record['CHILDITEMREFNAME']
            item_set = record['ITEMSET']
            item_question = record['ITEMQUESTION']
            item_dtype = record['COLUMNDBTYPE']
            if isinstance(item_name, str): 
                item_name = utils.clean_item_name(item_name)
                item_name_dict[item_id] = item_name
                item_set_dict[item_name] = item_set
                item_dtype_dict[item_name] = item_dtype_meta_dict[item_dtype]
                item_question_dict[item_name] = item_question
            item_question_dict['AE'] = 'Adverse Event'
            item_question_dict['CM'] = 'Concominant Medication'
            item_question_dict['EC'] = 'Dosing'
            
        return item_name_dict, item_set_dict, item_question_dict, item_dtype_dict
    
    def flatten_answer(self, cln_df):
        answer_types = ['STRING', 'INTEGER', 'FLOAT', 'DATE', 'CATEGORY', 'CODE']
        for ans_type in answer_types:
            globals()[f"ANSWER_{ans_type}"] = list(np.repeat(np.nan, len(cln_df)))
        print(cln_df.columns.tolist())
        ans_val = cln_df['ANSWER'].values.tolist()
        ques_types = cln_df['QUESTION_TYPE'].values.tolist()
        #print(len(eval(f"ANSWER_{ques_types[0].upper()}")))
        for ind, (ques_type, ans) in enumerate(zip(ques_types, ans_val)):
            eval(f"ANSWER_{ques_type.upper()}")[ind] = ans
            
        for ans_type in answer_types:
            col_name = f"ANSWER_{ans_type}"
            cln_df[col_name] = eval(col_name)
        if 'ANSWER_DATE' not in self.col_metadata['DATE']:
            self.col_metadata['DATE'].append('ANSWER_DATE')
        return cln_df 

    def expand_date(self, cln_df):

        for col in self.col_metadata['DATE']:
            assert (col in cln_df.columns.tolist()), f"{col} column is not present in the data"

            if col  == 'MODIFYTS':
                cln_df= utils.process_date(cln_df, col)
            elif col == 'ANSWER_DATE':
                cln_df= utils.process_date(cln_df, col, date_only= True, answer_date= True)
            else:
                print(f"The column {col} is in the date column but does have any date handler.")
        return cln_df
    
    def unflatten(self, item_name_dict, item_set_dict,item_ques_dict, item_dtype_dict, limit_df= None):
        config = self.config
        full_unflatten = pd.DataFrame() 
        cln_dict = self.cln_df_dict
        item_id_dict = {val: key for key, val in item_name_dict.items()}

        needed_subcate_col_file = self.config['data_preprocess']['needed_subcat_cols_file']
        needed_subcate_cols = pkl.load(open(needed_subcate_col_file, 'rb'))

        base_cols = config['data_preprocess']['base_cols'].split(',')
        for domain in cln_dict.keys():
            dataF = cln_dict[domain]
            print(dataF.shape)
            print(domain)
            
            dataF.columns = map(str.upper, dataF.columns)
            if limit_df != None:
                if  dataF.shape[0] > limit_df:
                    dataF = dataF.sample(limit_df)
            dataF = utils.normalize_df_type(dataF, self.config)
            data_columns = dataF.columns.tolist()
            domain,total_ques_cols=  utils.get_ques_cols(dataF, domain)
            if dataF.shape[0] > 0:
                needed_subcate_cols = [col for col in needed_subcate_cols if col in data_columns]
                
                remove_cols = [col for col in data_columns if col.startswith('CDR')]
                valid_ques, c_ques, nd_ques, date_ques, time_ques = utils.get_ques(total_ques_cols)

                item_dtype_dict = utils.update_ddict(item_dtype_dict, valid_ques, c_ques, date_ques)
                remove_cols = remove_cols + nd_ques + c_ques + date_ques + time_ques
                
                remove_cols = list(set(remove_cols) - set(needed_subcate_cols))
                dataF.drop(remove_cols, axis =1, inplace= True)
                
                valid_ques.extend(needed_subcate_cols)
                valid_ques_num = len(valid_ques)
                other_cols = list(set(dataF.columns.tolist()) - set(valid_ques))
                data_full_df = pd.DataFrame()
                if len(valid_ques) > 0:
                    for ind in tqdm(dataF.index.tolist()):
                        curr_rec = dataF.loc[ind, :]
                        act_cols = pd.DataFrame(curr_rec[other_cols].to_dict(), index= [0])
                        act_df = pd.concat([act_cols] * valid_ques_num)
                        pivot_form_id = curr_rec['FORMID']
                        pivot_form_idx = curr_rec['FORMINDEX']
                        pivot_subject_id = curr_rec['SUBJECTID']
                        pivot_visit_id = curr_rec['VISITID']
                        
                        ans = list(curr_rec[valid_ques].values)
                        clean_valid_ques = [utils.clean_item_name(ques) for ques in valid_ques]
                        act_df['QUESTION'] = valid_ques
                        act_df['ITEMID'] = [item_id_dict[ques] if ques in item_id_dict else 0 for ques in clean_valid_ques]
                        act_df['ITEMSETID'] = [item_set_dict[ques] if ques in item_id_dict else 0 for ques in clean_valid_ques]
                        act_df['ANSWER'] = ans
                        act_df['DOMAIN'] = list(np.repeat(domain, valid_ques_num))
                        ##HAVE TO CHECK QUESTION TYPE AND TEXT
                        question_text = []
                        for ques in valid_ques:
                            if ques in item_ques_dict:
                                question_text.append(item_ques_dict[utils.clean_item_name(ques)])
                            else:
                                question_text.append(item_ques_dict[ques[:2]])
                        act_df['QUESTION_TEXT'] = question_text
                        question_type = []
                        item_id_dict = {name: i for i, name in item_name_dict.items()}
                        item_ids = []
                        for ques in clean_valid_ques:
                            if ques in item_dtype_dict:
                                question_type.append(item_dtype_dict[ques])
                            elif ques.endswith('DAT'):
                                question_type.append('date')
                            else:
                                question_type.append('string')
                            if ques in item_id_dict:
                                item_ids.append(item_id_dict[ques])
                            else:
                                item_ids.append(np.nan)

                        act_df['ITEM_ID'] = item_ids
                        act_df['QUESTION_TYPE'] = question_type

                        data_full_df = data_full_df.append(act_df, ignore_index= True)
                else:
                    data_full_df = pd.DataFrame()
                    # Store the data_full_df to the db
                full_unflatten = full_unflatten.append(data_full_df, ignore_index= True)
        full_unflatten = full_unflatten.reset_index()
        full_unflatten = self.flatten_answer(full_unflatten)
        full_unflatten = self.expand_date(full_unflatten)
        full_unflatten.to_csv('full_unflatten.csv', index = False)
        if 'QUESTION_TEXT' not in self.col_metadata['TEXT']:
            self.col_metadata['TEXT'].append('QUESTION_TEXT')
        return full_unflatten
