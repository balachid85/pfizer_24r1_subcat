import os
import numpy as np
import pandas as pd
import yaml
from datetime import datetime
import logging
import random
from collections import OrderedDict
import nltk
import seaborn as sn
import re
import ast
from dateutil.parser import parse
from matplotlib import pyplot as plt
from nltk.corpus import stopwords
from tqdm import tqdm
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import (accuracy_score, classification_report,
                             confusion_matrix, precision_score, recall_score)

import main_utils
logger = logging.getLogger(os.path.basename(__file__))


def check_dir(exp_name, use_exp= True):
    if not os.path.isdir(exp_name):
        os.system(f"mkdir {exp_name}")
    else:
        if use_exp == False:
            exp_name = exp_name + "_latest"
            check_dir(exp_name, use_exp= use_exp)
    return exp_name

#Clean the ITEM_NAME by removing the lower_case values, numeric values..etc.
def clean_item_name(name):
    if '_' in name:
        name = name.split('_')[0]
    name = re.sub("[a-z]+", '', name)
    name = re.sub("[0-9]+", '', name)
    if name == 'CMPAEREL':
        name = name.replace('CMP', '')
    return name

def create_item_dicts(dict_df):
    item_name_dict = {}
    item_dtype_dict = {}
    item_question_dict = {}
    for rec_id in dict_df.index.tolist():
        record = dict_df.loc[rec_id, :]
        item_set_presence = record['ITEMSET']
        item_id = record['ITEMID'] if item_set_presence == 0 else record['CHILDITEMID']
        item_name = record['ITEMREFNAME'] if item_set_presence == 0 else record['ITEMREFNAME']
        item_question = record['ITEMQUESTION']
        item_dtype = record['COLUMNDBTYPE']
        if isinstance(item_name, str):
            item_name = clean_item_name(item_name)
            item_name_dict[item_id] = item_name
            item_dtype_dict[item_name] = item_dtype
            item_question_dict[item_name] = item_question
    item_question_dict['AE'] = 'Adverse Event'
    item_question_dict['CM'] = 'Concominant Medication'
    return item_name_dict, item_question_dict, item_dtype_dict

def update_ddict(ddict, main_ques, c_ques, date_ques):
    for que in main_ques:
        if (que.endswith('CODE')) or (que.endswith('CD')):
            ddict[que] = 'CODE'
    for que in c_ques:

        que = que.split('_')[0]
        if que in ddict.keys():
            #el samp_dict[que]
            ddict[que] = 'CATEGORY'
    for que in date_ques:
        que = que.split('_')[0]
        if que in ddict.keys():
            #el samp_dict[que]
            ddict[que] = 'DATE'
    return ddict

def get_ques(columns):
    ques_cols = [col for col in columns if (col.startswith('AE') or (col.startswith('CM')))]
    c_ques = [ques for ques in ques_cols if ques.split('_')[-1] == 'C']
    nd_ques = [ques for ques in ques_cols if ques.split('_')[-1] == 'ND']
    date_ques = [ques for ques in ques_cols if (ques.split('_')[-1] == 'DTS') or (ques.split('_')[-1] == 'DTR')]
    time_ques = [ques for ques in ques_cols if (ques.split('_')[-1] == 'TMS') or (ques.split('_')[-1] == 'TMR')]
    main_ques = list(set(ques_cols) - set(c_ques + nd_ques + date_ques + time_ques))
    return (main_ques, c_ques, nd_ques, date_ques, time_ques)

def get_ques_cols(dataF, domain):
    domain = re.sub("\d+", "", domain).split('_')[1].upper()
    data_columns = [col for col in dataF.columns.tolist()]
    if domain == 'EC':
        ques_cols = [col for col in data_columns if col.startswith(domain) or col.startswith('MIS')]
    else:
        ques_cols = [col for col in data_columns if col.startswith(domain)]
    
    return domain, ques_cols

def normalize_df_type(dataF, config): 
    data = dataF.copy()
    id_cols = config['data_preprocess']['normalise_id_cols'].split(',')
    id_cols = [col for col in id_cols if col in data.columns.tolist()]
    data = data.dropna(subset= id_cols)

    if not data.empty:
      for col in id_cols:
          data[col] = [val if isinstance(val, str) else int(val) for val in data[col].values.tolist()]
          data[col] = data[col].astype(int)

    indx_col = 'ITEMREPN' if 'ITEMREPN' in data.columns else 'FORMINDEX'
    for col in id_cols:
        thresh_val = 1 if (col == indx_col) else 0
        data = data[data[col] > thresh_val]
    
    return data

def load_srdm_data(data_path, study_name, config, infer= False, select_files= None):
    srdm_config = config
    if os.path.isdir(data_path) == False:
        raise ValueError("Data Path is not present")

    if infer == False:
        query_file_name = srdm_config['query_file']
    else:
        disc_df = None

    metadata_file_name = srdm_config['metadata_file']
    study_path = os.path.join(data_path, study_name)
    
    if os.path.isdir(study_path) == False:
        assert("The study directory is not found")
    study_files = [fil for fil in [c for _,_,c in os.walk(study_path)][0] if fil.endswith('.txt')]
    clinical_files = [fil for fil in study_files if fil not in [query_file_name, metadata_file_name]]
    #cln_df_dict = {}
    if select_files != None:

        selected_clinical_files = []
        for fil in clinical_files:
            for select_file in select_files:
                if select_file in fil:
                    selected_clinical_files.append(fil)
    else:
        selected_clinical_files = clinical_files
    selected_clinical_files = [file for file in selected_clinical_files if len(file.split('.')[0]) == 8]
    cln_df_dict =  {domain_file: os.path.join(study_path, domain_file) for domain_file in selected_clinical_files}
    cln_df_dict = {domain: pd.read_csv(path, sep = '|') for domain, path in cln_df_dict.items()}
    
    if infer == False:
        if query_file_name in study_files:
            disc_df = pd.read_csv(os.path.join(study_path, query_file_name), sep = '|')
        else:
            raise ValueError(f"The file {query_file_name} is not present in {data_path}")

    if metadata_file_name in study_files:
        dict_df = pd.read_csv(os.path.join(study_path, metadata_file_name), sep = '|')
    else:
        raise ValueError(f"The file {metadata_file_name} is not present in {data_path}")
    
    return (cln_df_dict, disc_df, dict_df)

def process_date(dataF, col, date_only= False, answer_date= False):
    str_format =  "%d%b%Y" if date_only else "%d%b%Y:%H:%M:%S"
    
    #individual functions for expanding date
    def format_date(val):

        if isinstance(val, str):
            if (len(val) > 5) and (len(val) < 20):
                if len(val.split(':')[0]) == 7:
                    str_format_modif = "%d%b%y:%H:%M:%S"
                else:
                    str_format_modif =  "%d%b%Y:%H:%M:%S"
                return datetime.strptime(val, str_format_modif)
            
            else:
                return np.nan
        else:
            return np.nan
    #format_date = lambda x: datetime.strptime(x, str_format) if isinstance(x, str) else np.nan
    #answer_format_date = lambda x: parse(x) if isinstance(x, str) else np.nan
    def answer_format_date(val):
        try:
            if isinstance(val, str) == False:
                return np.nan

            else:
                if len(val) < 5:
                    return np.nan
                else:
                    if 'UNK' in val:
                        val = val.replace('UNK', '')
                    if ('-' in val) or ('/' in val):
                        return parse(val)
                    else:
                        return format_date(val)
        except:
            return np.nan
        
    get_hour = lambda x: x.hour if isinstance(x, datetime) else np.nan
    get_minute = lambda x: x.minute if isinstance(x, datetime) else np.nan
    get_month = lambda x: x.month if isinstance(x, datetime) else np.nan
    get_day = lambda x: x.day if isinstance(x, datetime) else np.nan
    get_year = lambda x: x.year if isinstance(x, datetime) else np.nan
    get_weekday = lambda x: x.weekday() if isinstance(x, datetime) else np.nan

    date_time_entities = ['day', 'month', 'year', 'hour', 'month', 'weekday']
    date_entities = ['day', 'month', 'year']

    try:
        if answer_date == True:
        
            dataF[col] = dataF[col].apply(answer_format_date)
        """
        else:
            dataF[col] = dataF[col].apply(format_date)"""
    except Exception as e:
        debug_dataF = dataF[~dataF[col].isnull()]
        raise ValueError(f"The date column - {col} is not in the  {str_format} format with the error - {e}")

    entities = date_entities if date_only else date_time_entities

    for entity in entities:
        new_col = f"{col}_{entity.upper()}"
        dataF[new_col] = dataF[col].apply(eval(f"get_{entity}"))

    return dataF

def group_cln_df(cln_dict):
    cln_df = pd.DataFrame()
    for df in list(cln_dict.values()):
        cln_df = cln_df.append(df, ignore_index= True)
    return cln_df

def create_cat_encoding(dataF, cat_cols):
    le = LabelEncoder()
    cat_encode_dict = OrderedDict()
    for col in tqdm(cat_cols):
        try:
            classes = le.fit(dataF[col]).classes_
        except:
            print(f'The Column Name {col} cannot be Label Encoded')

        col_label_dict = OrderedDict()
        for label, clas in enumerate(classes):
            col_label_dict[clas] = label
        cat_encode_dict[col] = col_label_dict
    return cat_encode_dict

def infer_cat_encoding(dataF, cat_encode_dict):
    for col in list(cat_encode_dict.keys()):
        col_label_dict= cat_encode_dict[col]
        new_val = []
        for val in dataF[col].values:
            if val in col_label_dict:
                new_val.append(col_label_dict[val] + 1)
            elif 'Null' in col_label_dict:
                new_val.append(col_label_dict['Null'] + 1)
            else:
                new_val.append(0)
        dataF[col] = new_val
    return dataF

#Customized logger
def clean_df_column_names(df):
    column_names = []
    for col in df.columns.tolist():
        if col.startswith('AE') or col.startswith('CM'):
            if (has_numbers(col) == True) and (col.split('_')[-1].isdigit() == True):
                column_names.append(col.split('_')[0])
            else:
                column_names.append(col)
        else:
            column_names.append(col)
    df.columns = column_names
    return df
def has_numbers(str_val):
    return any(char.isdigit() for char in str_val)
def get_weights(out_arr):
    vals, cnts = np.unique(out_arr, return_counts = True)
    weights = []
    total_len = len(out_arr)
    count_dict = OrderedDict((val, cnt) for val, cnt in zip(vals, cnts))
    count_dict = OrderedDict(sorted(count_dict.items()))
    for _, cnt in count_dict.items():
        cnt_per = (cnt/ total_len)
        ratio = 1 - cnt_per
        weights.append(ratio)
    return weights

def make_sample(dataF, sample_ratio= 2):
    df_0  = dataF[dataF['DISCREPANCY_PRESENCE'] == 0]
    df_1 = dataF[dataF['DISCREPANCY_PRESENCE'] == 1]
    one_count = df_1.shape[0]
    zero_count = one_count * sample_ratio
    df_0 = df_0.sample(zero_count)
    df = df_0.append(df_1, ignore_index= True)
    return df

def get_metrics(act_out, pred_out):
    class_report  = classification_report(act_out, pred_out, output_dict = True)
    precision = precision_score(act_out, pred_out, average = 'weighted')
    acc = accuracy_score(act_out, pred_out)
    recall = recall_score(act_out, pred_out, average = 'weighted')
    classes = [ent for ent in list(class_report.keys()) if len(ent) < 5]
    out_metric_text = f"Accuracy: {acc}"
    metrics = ['precision', 'recall']

    for metric in metrics:
        out_metric_text += f" {metric}"
        for clas in classes:
            out_metric_text += f" {clas}:{class_report[clas][metric]}"
        out_metric_text += "]"
    return out_metric_text, acc, precision, recall

def unflatten_df_process(full_unflatten, datadictionary_df, map_item_df, subject_id, config, stg_needed_columns):
    if len(full_unflatten) > 0:
        item_name_dict = {item_name: item_id for item_name, item_id in zip(map_item_df['item_nm'].values, map_item_df['item_id'].values)}
        get_item_id = lambda x: item_name_dict[x] if x in item_name_dict else 0
        full_unflatten['ITEMID'] = full_unflatten['QUESTION'].apply(get_item_id)
        print(f'FULL unflatten columns - {full_unflatten.columns.tolist()}')
        sub_df_with_str = full_unflatten[full_unflatten['SUBJECTNUMBERSTR'].str.startswith('SCR')]
        if sub_df_with_str.shape[0] > 0:
            full_unflatten = full_unflatten[~(full_unflatten['SUBJECTNUMBERSTR'].str.startswith('SCR'))]
        if 'FORMREFNAME' in full_unflatten.columns.tolist():
            formname_in_dict = datadictionary_df['FORMNAME'].values.tolist()
            formrefname_in_dict = datadictionary_df['FORMREFNAME'].values.tolist()
            formrefname_map_dict = {formname:formrefname for formname, formrefname in zip(formrefname_in_dict, formname_in_dict)}
            formrefname_map_func = lambda x: formrefname_map_dict[x] if x in formrefname_map_dict else np.nan
            full_unflatten['FORMNAME'] = full_unflatten['FORMREFNAME'].apply(formrefname_map_func)
            section_ref_name =[]
            for i in full_unflatten.index.tolist():
                rec = full_unflatten.loc[[i]]
                itemname = rec['QUESTION'].values[0]
                formrefname = rec['FORMREFNAME'].values[0]
                try:
                    sec_df = datadictionary_df[(datadictionary_df['ITEMREFNAME'].str.contains(itemname))& (datadictionary_df['FORMREFNAME'] == formrefname)]
                    if len(sec_df) > 0:
                        sec_name = sec_df['SECTIONREFNAME'].values[0]
                    else:
                        sec_name = 'null'
                
                except:
                    sec_name = np.nan
                section_ref_name.append(sec_name)
            full_unflatten['SECTIONREFNAME'] = section_ref_name

        #logger.info('ADPM SHAPE ' + str(full_unflatten.shape))
        ##print('ADPM SHAPE ' + str(full_unflatten.shape))
        adpm_structure = utils.data_for_stage(full_unflatten, config, stg_needed_columns)

    else:
        adpm_structure = pd.DataFrame()
        logger.info(f"{domain} for subject {subject_id} is either not in needed domain list or the length of the data in 0")
    
    return adpm_structure
