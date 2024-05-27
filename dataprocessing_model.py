import csv
import numpy as np
from datetime import datetime, timezone
import logging
import traceback
import pandas as pd
from dateutil.parser import parse
import datetime
import configparser
import requests
import json
import pandas as pd
import os
import ast
import yaml
from tqdm import tqdm
import re
from sqlalchemy import text

try:
    from scripts.dataIngestion import data
    from scripts.dataIngestion import database_connect
    
    from scripts.dataIngestion import data_utils
    from scripts.dataIngestion import utils
except Exception as e:
    logging.debug(e)
    import data
    import  database_connect
    import data_utils
    import main_utils

logger = logging.getLogger(os.path.basename(__file__))
    
class dataprocessing:
    def __init__(self, config, schema_name, sdq_connector=None):
        if not config:
            self.config = configparser.ConfigParser()
            self.config.read("dataload_configs.ini")
        else:
            self.config = config
        ######print(self.__class__.__name__)
        #logging.basicConfig(filename = self.__class__.__name__ + '_' + datetime.now().strftime('-%Y-%m-%d') + '.log', filemode = 'a', format='%(asctime)-15s : %(message)s', level = logging.DEBUG)
        self.adpm_structure =  pd.DataFrame()
        #col_metadata = eval((self.config['adpm']['col_metadata'])) 
        if sdq_connector is None:
            self.sdq_connector = database_connect.connection('sdq', 'database_connection', self.config)
        else:
            self.sdq_connector = sdq_connector

        self.sdq_conn = self.sdq_connector.connect()
        self.schema_name = schema_name
    def unflatten(self, clinical_dict_df, datadictionary_df, 
                    limit_df= None):
        schema = self.schema_name
        conn = self.sdq_conn
        created_dt = datetime.datetime.now()
        config = self.config
        stg_table_name = 'stg_covid_pred' if schema.lower() == 'c4591001' else 'stg_pred'
        adpm_structure = pd.DataFrame() 
        config = config['data_preprocess'] 
        cln_dict = clinical_dict_df
        common_cols = config['base_cols'].split(',')
        needed_domains= config['needed_domains'].split(',')
        ''' Needs to be changed ''' 
        stg_res = conn.execute(f"SELECT * from {schema.lower()}.{stg_table_name} limit 1")
        stg_df = pd.DataFrame(stg_res.fetchall(), columns= stg_res.keys())
        stg_needed_columns = stg_df.columns.tolist()

        map_item_res = conn.execute(f"SELECT item_id, item_nm from {schema.lower()}.map_item")
        map_item_df = pd.DataFrame(map_item_res, columns= map_item_res.keys())
        
        for domain in cln_dict.keys():
            #print(f"domain - {domain}")
            dataF = cln_dict[domain]
            
            form_columns = [col for col in dataF.columns.tolist() if col.startswith('FORM')]
            #####print(f"### Form columns - {form_columns}")
            domain_name = domain.split('_')[1].upper()
            if domain_name.startswith('LB003'):
                domain_name = 'CLB'
            
            domain_name = re.sub("\d+", "", domain_name)
  
            domain_name = 'TRIG' if domain_name == 'R' else domain_name
            if domain_name in ['VS', 'BEETRK']:
                limit_flag = False
            else:
                limit_flag = True
            if limit_df != None and limit_flag == True:
                if  dataF.shape[0] > limit_df:
                    dataF = dataF.sample(limit_df)
            vol_rename_dict = ast.literal_eval(self.config['data_preprocess']['base_col_vol_dict'])
            #print(vol_rename_dict)
            dataF = dataF.rename(columns= vol_rename_dict)
            #print(dataF.columns)
            if 'DATEDATACHANGED' not in dataF.columns.tolist() and "AUDIT_START_DATE" in dataF.columns.tolist():
                dataF['DATEDATACHANGED'] = dataF['AUDIT_START_DATE']
            
            try:
                dataF['DATEDATACHANGED'] = dataF['DATEDATACHANGED'].fillna('null')
                dataF['DATEDATACHANGED'] = dataF['DATEDATACHANGED'].astype(str)
            except:
                pass
            ###print(f'##### Changed audit start date DATAEDATECHANGED')
            dataF = dataF.T.drop_duplicates().T
            data_columns = dataF.columns.tolist()
            data_full_df = pd.DataFrame()
            #print(dataF.columns)
            if (dataF.shape[0] > 0) & (domain_name in needed_domains):

                remove_cols = [col for col in data_columns if col.startswith('CDR')]
                
                common_cols_present = [col for col in common_cols if col in dataF.columns]
                 
                remove_cols = [col for col in remove_cols if col not in common_cols_present] 
                dataF.drop(remove_cols, axis =1, inplace= True)
                ques_col = list(set(dataF.columns.tolist()) - set(common_cols_present))  
                valid_ques = ques_col
                valid_ques_num = len(valid_ques)
                data_full_df = pd.DataFrame()
                if valid_ques_num > 0:
                    subject_list = dataF['SUBJECTNUMBERSTR'].unique().tolist()
                    subj_count = len(subject_list)
                    logger.info(f" Total Subjects === {subj_count}")
                    for i, subject in enumerate(subject_list):
                        logger.info(f" RUNNING SUBJECT INDEX === {i}")
                        logger.info(f" RUNNING SUBJECT === {subject}")
                        subject_full_df = pd.DataFrame()
                        sub_dataF = dataF[dataF['SUBJECTNUMBERSTR'] == subject]
                        for ind in tqdm(sub_dataF.index.tolist()):
                            curr_rec = sub_dataF.loc[ind, :]
                            
                            act_cols = pd.DataFrame(curr_rec[common_cols_present].to_dict(), index= [0])
                            act_df = pd.concat([act_cols] * valid_ques_num)
                            ans = list(curr_rec[valid_ques].values)
                            clean_valid_ques = valid_ques
                            act_df['QUESTION'] = clean_valid_ques
                            act_df['ANSWER'] = ans
                            act_df['DOMAIN'] = list(np.repeat(domain_name, valid_ques_num))
                            subject_full_df = subject_full_df.append(act_df, ignore_index= True)
                        print(subject_full_df.shape)
                        subject_full_df['created_dt'] = created_dt
                        subj_adpm_df = data_utils.unflatten_df_process(subject_full_df, datadictionary_df, map_item_df, subject, config, stg_needed_columns)
                        subj_adpm_df = self.process_before_dbinsert(subj_adpm_df)
                        logger.info('ADPM SHAPE ' + str(subj_adpm_df.shape) + f"Subjnum == {i}/{subj_count}")
                
                        if (domain_name == 'AE') or (domain_name == 'CM'):
                            data_full_df = data_full_df.append(subj_adpm_df, ignore_index= True)
                        
                else:
                    data_full_df = pd.DataFrame()
                    logger.info(f"The number question for unflattening {domain} is 0")
                
        return data_full_df
        
    def process_before_dbinsert(self,adpm_structure):
        config = self.config       
        subjtable = config['sdq']['subjecttable']
        if len(adpm_structure) > 0:

            adpm_structure.drop(['subj_guid'], axis=1, inplace=True)
            df_add = pd.read_sql_query(
                f'SELECT subjid, subject_guid as subj_guid FROM {self.schema_name}.{subjtable}', self.sdq_conn)
            adpm_structure['subjid'] = adpm_structure['subjid'].astype(str)

            adpm_structure = pd.merge(adpm_structure, df_add.astype(str), on='subjid')

            adpm_structure.dropna(subset=['subjid'], inplace=True)

            id_nan_cols = ['form_ix', 'itemset_ix', 'form_index', 'itemrepn', 'subjid', 'siteno', 'sitemnemonic']
            str_cols = ['item_value', 'answer_in_text', 'subjid']
            def parse_date(x):
                if isinstance(x, str):
                    try:
                        return_val= parse(x)
                    except:
                        return_val = datetime.datetime.strptime('01JAN1900:12:40:25', '%d%b%Y:%H:%M:%S')
                else:
                    return_val = datetime.datetime.strptime('01JAN1900:12:40:25', '%d%b%Y:%H:%M:%S')
                return return_val

            adpm_structure['modif_dts'] = adpm_structure['modif_dts'].apply(parse_date)

            for col in id_nan_cols:
                adpm_structure[col] = adpm_structure[col].replace(np.inf, 0)
                adpm_structure[col] = adpm_structure[col].replace(np.nan, 0)

                adpm_structure[col] = adpm_structure[col].astype(int)

            for col in str_cols:
                adpm_structure[col] = adpm_structure[col].replace(np.inf, 'null')
                adpm_structure[col] = adpm_structure[col].replace(np.nan, 'null')

                adpm_structure[col] = adpm_structure[col].astype(str)

            adpm_structure['sectionref_nm'] = adpm_structure['sectionref_nm'].fillna('null')
            adpm_structure['last_run_dt'] = [datetime.datetime.now() for _ in range(len(adpm_structure))]
            #self.insert_stage_table(adpm_structure, self.sdq_conn)
        return adpm_structure

    def get_adpm_data(self, clinical_dict_df, datadictionary_df, limit_df= None):
        schema_name = self.schema_name
        adpm_structure = self.unflatten(clinical_dict_df, datadictionary_df, limit_df= limit_df)
        return adpm_structure
