import pandas as pd
import json
import numpy as np
from sqlalchemy import create_engine, text
import yaml
import os
import base

curr_file_path = os.path.realpath(__file__)
current_path = os.path.abspath(os.path.join(curr_file_path, '../'))
stream = open(os.path.join(current_path, 'studytype_mapping.yaml'), 'r')
studytype_mapping = yaml.load(stream, Loader=yaml.FullLoader)

def covid_flatten(unflatten_df, study_type_df, study_type):
    domain_dict = {}
    get_domain = lambda x: x[:2]
    # unflatten_df['DOMAIN'] = unflatten_df['item_nm'].apply(get_domain)
    
    exclude_form_vol_dict = {2: ['AE203*'], 3: ['AE001_1','EC001_5']}
    exclude_form = exclude_form_vol_dict[int(study_type)]
    #[EDIT #1.0]
    #adding this line to filter out the FORMREFNAME null from the records
    if len(unflatten_df) > 0:
        unflatten_df = unflatten_df[~(unflatten_df['formrefname'].isin(['null','NULL',np.nan,np.NaN,None]))]
    for form in exclude_form:
        if '*' in form:
            form = form.replace('*', '')
            unflatten_df = unflatten_df[~(unflatten_df['formrefname'].str.startswith(form))]
        else:
            unflatten_df = unflatten_df[~(unflatten_df['formrefname'] == form)]

    domains = unflatten_df['domain'].unique().tolist()
    unflatten_df['modif_dts'] = unflatten_df['modif_dts'].apply(pd.to_datetime)
    unflatten_df['modif_dts'] = unflatten_df['modif_dts'].astype(int)
    #print(unflatten_df['item_nm'].unique().tolist())
    unflatten_df['item_value'] = unflatten_df['item_value'].replace({'null': np.nan, 'nan' : np.nan, '': np.nan, 'NULL': np.nan})
    vol3_map_df = study_type_df.copy()
    vol3vol2_map = {vol3_col: vol2_col for vol3_col, vol2_col in zip(vol3_map_df['vol3_itemname'], vol3_map_df['vol2_itemname'])}
    match_col_dict = {
                        'AE': ['form_index'],
                        'CM': ['form_index'],
                        'DH' : ['cdr_skey'],
                        'EC': ['cdr_skey'],
                        'LB': ['cdr_skey'],
                        'MH': ['itemrepn'],
                        'BEETRK': ['itemrepn'],
                        'VS': ['itemrepn'],
                        'CECISR': ['itemrepn'],
                        'SV': ['visit_nm','visit_ix'],
                        'TRIG': ['formrefname'],
                        'DS':   ['formname'],
                        'DM':   ['formname'],
                        'MBMIB': ['visit_ix', 'form_index'],
                        'FAFUC': ['visit_nm','visit_ix'],
                        'CLB' : ['cdr_skey'],
                        'FAVSD' : ['cdr_skey'],
                        'IRT': ['subjid'],
                        'SLOPD': ['subjid'],
                        'IE': ['itemrepn'],
                        'CESOD' : ['itemrepn'],
                        'HOHCU': ['itemrepn']
                    }

    df = unflatten_df.copy()
    df_domains = df.domain.unique().tolist()
    items_available = df.item_nm.unique().tolist()
    for dom in domains:
        domain_dict[dom] = pd.DataFrame()
    for domain in domains:
        domain_dict[domain] = pd.DataFrame()
        match_columns = match_col_dict[domain.upper()]
        piv_domain = [dom for dom in df_domains if dom == domain]
        dom_df = df[df['domain'].isin(piv_domain)]
        #dom_df = dom_df[needed_cols]
        unique_match_seq_df = dom_df.groupby(match_columns).size().reset_index().rename(columns={0:'count'})
        unique_match_seq_df.drop(['count'], axis= 1, inplace= True)
        full_flatten_list = []
        for unique_dict in unique_match_seq_df.to_dict(orient= 'records'):
            piv_df = dom_df.copy()
            for col, value in unique_dict.items():
                piv_df = piv_df[piv_df[col] == value]

            if len(piv_df['modif_dts'].unique().tolist()) > 1:
                latest_modif_dts = piv_df['modif_dts'].max()
                piv_df = piv_df[piv_df['modif_dts'] == latest_modif_dts]

            piv_df = piv_df[piv_df['item_nm'].isin(items_available)]
            ques = piv_df['item_nm'].values.tolist()
            ans = piv_df['item_value'].values.tolist()
            ques_ans_dict = {q: a for q, a in zip(ques, ans)}
            single_record = piv_df.head(1)
            # single_record = piv_df[piv_df['modif_dts'] == piv_df['modif_dts'].max()]
            single_record.drop(['item_nm', 'item_value'], axis= 1, inplace= True)
            sing_dict = single_record.to_dict(orient= 'records')[0]
            sing_dict.update(ques_ans_dict)
            full_flatten_list.append(sing_dict)
        flatten_df = pd.DataFrame(full_flatten_list)
        flatten_df = flatten_df.rename(columns=vol3vol2_map)
        domain_dict[domain] = flatten_df.to_dict(orient= 'records')
        # domain_dict[domain] = json.loads(flatten_df.to_json(orient= 'records'))
    
    return domain_dict

def covid_flatten_v1(unflatten_df):
    domain_dict = {}
    get_domain = lambda x: x[:2]
    # unflatten_df['DOMAIN'] = unflatten_df['item_nm'].apply(get_domain)

    domains = unflatten_df['domain'].unique().tolist()
    unflatten_df['modif_dts'] = unflatten_df['modif_dts'].apply(pd.to_datetime).astype(int)
    unflatten_df['created_dt'] = unflatten_df['created_dt'].apply(pd.to_datetime).astype(int)
    
    # studytype_mapping_path = Path() / 'studytype_mapping.yaml'
    # with open(studytype_mapping_path,'r') as f:
    #     studytype_mapping = yaml.safe_load(f.read())
    
    if len(unflatten_df) > 0:
        unflatten_df = unflatten_df[~(unflatten_df['formrefname'].isin(['null','NULL',np.nan,np.NaN,None]))]
    
    '''
    match_col_dict = {
                        'AE': ['form_index'],
                        'CM': ['form_index'],
                        'EC': ['visit_nm'],
                        'LB': ['cdr_skey'],
                        'MH': ['itemrepn'],
                        'BEETRK': ['itemrepn'],
                        'VS': ['itemrepn'],
                        'CECISR': ['itemrepn'],
                        'SV': ['visit_nm','visit_ix'],
                        'TRIG': ['formrefname'],
                        'DS':   ['formname'],
                        'DM':   ['formname'],
                        'MBMIB': ['visit_ix', 'form_index'],
                        'FAFUC': ['visit_nm','visit_ix'],
                        'CLB' : ['cdr_skey'],
                        'FAVSD' : ['cdr_skey'],
                        'IRT': ['subjid'],
                        'SLOPD': ['subjid'],
                        'IE': ['itemrepn'],
                        'CESOD' : ['itemrepn'],
                        'HOHCU': ['itemrepn']
                    }
    '''

    df = unflatten_df.copy()
    df_domains = df.domain.unique().tolist()
    # items_available = df.item_nm.unique().tolist()
    # print('before flattern',df)
    for dom in domains:
        domain_dict[dom] = pd.DataFrame()
    for domain in domains:
        domain_dict[domain] = pd.DataFrame()
        # match_columns = match_col_dict[domain.upper()]
        # Changes for using CDR_SKEY for grouping data for all domain except IRT
        match_columns = ['subjid'] if domain.upper() == 'IRT' else ['cdr_skey']
        piv_domain = [dom for dom in df_domains if dom == domain]
        dom_df = df[df['domain'].isin(piv_domain)]
        # dom_df = dom_df[needed_cols]
        unique_match_seq_df = dom_df.groupby(match_columns).size().reset_index().rename(columns={0:'count'})
        unique_match_seq_df.drop(['count'], axis= 1, inplace= True)
        full_flatten_list = []
        for unique_dict in unique_match_seq_df.to_dict(orient= 'records'):
            piv_df = dom_df.copy()
            for col, value in unique_dict.items():
                piv_df = piv_df[piv_df[col] == value]

            if len(piv_df['modif_dts'].tolist()) > 1:
                latest_modif_dts = piv_df['modif_dts'].max()
                piv_df = piv_df[piv_df['modif_dts'] == latest_modif_dts]
                
                if piv_df.shape[0] > 1:
                    latest_created_dts = piv_df['created_dt'].max()
                    piv_df = piv_df[piv_df['created_dt'] == latest_created_dts]
            
            piv_df.drop(piv_df[piv_df['iudflag'] == 'D'].index, inplace=True)
            if not piv_df.shape[0]:
                continue
                        
            piv_df['modif_dts'] = piv_df['modif_dts'].apply(pd.to_datetime)
            piv_df['created_dt'] = piv_df['created_dt'].apply(pd.to_datetime)

            single_record = piv_df.head(1)
            ques_ans_dict = single_record['item_dataset'].values[0]
            if type(ques_ans_dict) == str:
                ques_ans_dict = json.loads(ques_ans_dict)
            for key,value in ques_ans_dict.items():
                if value in ['null','nan','','NULL']:
                    ques_ans_dict.update({key:np.nan})
            
            # single_record = piv_df[piv_df['modif_dts'] == piv_df['modif_dts'].max()]
            single_record.drop(['item_dataset'], axis= 1, inplace=True)
            sing_dict = single_record.to_dict(orient= 'records')[0]
            sing_dict.update(ques_ans_dict)
            full_flatten_list.append(sing_dict)
        flatten_df = pd.DataFrame(full_flatten_list)
        flatten_df = flatten_df.rename(columns=studytype_mapping)
        domain_dict[domain] = flatten_df.to_dict(orient= 'records')
        # domain_dict[domain] = json.loads(flatten_df.to_json(orient= 'records'))
    # print('after flattern',domain_dict)
    return domain_dict