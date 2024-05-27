import os
import re
import json
import traceback
from typing import List, Tuple, Union
import pandas as pd
import ast

import requests
import jellyfish

from similarity_contents.similarity_utils import fuzzy_search, get_int_id,null_check,remove_paranthesis,clean_indications,remove_splchars, google_search_helper, \
load_fallback,load_fdaopenlabel, load_meddra_dictionaries, load_full_meddra_precompute,load_flat_meddra_precompute, load_drug_lookup_files, load_who_dict_fb, load_who_dict, get_similarity_data

get_similarity_data()
    
llt_dict, pt_dict, hlt_dict, hlgt_dict, soc_dict, ptdf = load_meddra_dictionaries(version='Version 24.0')

who_drug_term, who_drug_code, who_drug_term_code = load_who_dict_fb()

def who_search(cmtrt_label: str) -> Union[List, None]:
    """WHO Drug dict search
    Args:

        cmtrt_label (str): Input Concomitant Medication(CMTRT) name
    Returns:
        Union[list, None]: CMTRT List if found else None
    """
    #who_drug_term, who_drug_code = load_who_dict()
    cmtrt_label = cmtrt_label.upper()
    if cmtrt_label in who_drug_term:
        #print('cmtrt_label is :', who_drug_term[cmtrt_label])
        return who_drug_term[cmtrt_label]
    return None


def who_search_with_code(cmtrt_label: str, 
                         cmcode: str) -> Union[List, None]:
    """WHO Drug dict search with cmcode
    Args:
        cmtrt_label (str): Input Concomitant Medication(CMTRT) name
        cmcode (str): CMTRT's code
    Returns:
        Union[List, None]: CMTRT List if found else None
    """
    #who_drug_term, who_drug_code = load_who_dict()
    cmtrt_label = cmtrt_label.upper()
    cmcode = cmcode.lstrip('0').replace(' ','')
    if cmtrt_label in who_drug_term:
        return who_drug_term[cmtrt_label]
    elif cmcode in who_drug_code:
        return who_drug_code[cmtrt_label]
    return None

# def who_search_with_code(cmtrt_label: str, 
#                          cmcode: str) -> Union[List, None]:
#     """WHO Drug dict search with cmcode

#     Args:
#         cmtrt_label (str): Input Concomitant Medication(CMTRT) name
#         cmcode (str): CMTRT's code

#     Returns:
#         Union[List, None]: CMTRT List if found else None
#     """
#     cmtrt_label = cmtrt_label.upper()
#     cmcode = cmcode.lstrip('0').replace(' ','')
#     if cmtrt_label in who_drug_term:
#         cmtrtl = who_drug_term[cmtrt_label]   
#         cmtrtl = cmtrtl.lower().split(';')    
#         return cmtrtl 
#     elif cmcode in who_drug_code: 
#         cmtrtc = who_drug_code[cmtrt_label]   
#         cmtrtc = cmtrtc.lower().split(';')    
#         return cmtrtc 
#     return None


def diagnosis_fda_openlabel(drug_name: Union[str, List]) -> List[str]:
    """Fetch list of diagnosis from FDA open label
    Args:
        drug_name (Union[str, List]): Input CMTRT
    Returns:
        List[str]: List of indications fetched from FDA Openlabel 
                   otherwise empty list
    """
    fda_openlabel = load_fdaopenlabel()
    result = list()
    drug_name = remove_splchars(drug_name.lower())
    #print('Inside FDA open label drug name :', drug_name,'len of result', len(fda_openlabel.items()))
    for index, values in fda_openlabel.items():
        try:
            brand_names = values.get("brand_name"," ")
            generic_names = values.get("generic_names"," ")
            generic_name = values.get("generic_name"," ")
            substance_name = values.get("substance_name", " ")
            if type(brand_names) == str:
                brand_list = list()
                brand_list.append(brand_names)
                brand_names = brand_list
            if type(generic_names) == str:
                generic_names_list = list()
                generic_names_list.append(generic_names)
                generic_names = generic_names_list
            if type(generic_name) == str:
                generic_name_list = list()
                generic_name_list.append(generic_name)
                generic_name = generic_name_list
            if type(substance_name) == str:
                substance_name_list = list()
                substance_name_list.append(substance_name)
                substance_name = substance_name_list
            if type(brand_names) == list:
                for brand in brand_names:
                    # search_drug_name = r"\b" + drug_name + r"\b"
                    # if (re.search(search_drug_name, brand, 
                    #     re.IGNORECASE)):
                    # * replaced regex search with fuzzysearch, might take more time but helps
                    # ! reverted to regex because fuzzywuzzy takes ages
                    if (re.search(drug_name, brand, re.IGNORECASE)):
                    # if fuzzy_search(brand, drug_name_list, threshold=90):
                        result = list(set(result + values['diagnosis']))
            if type(generic_names) == list:
                for generic_name in generic_names:
                    if (re.search(drug_name, generic_name, re.IGNORECASE)):
                    # if fuzzy_search(generic_name, drug_name_list, threshold=90):
                        result = list(set(result + values['diagnosis']))
            if type(generic_name) == list:
                for generic in generic_name:
                    if (re.search(drug_name, generic, re.IGNORECASE)):
                    # if fuzzy_search(generic, drug_name_list, threshold=90):
                        result = list(set(result + values['diagnosis']))
            if type(substance_name) == list:
                for substance in substance_name:
                    if (re.search(drug_name, substance, re.IGNORECASE)):
                    # if fuzzy_search(substance, drug_name_list, threshold=90):
                        result = list(set(result + values['diagnosis']))
        except Exception as e:
            print(traceback.format_exc())
    return result

# Extract MeDDRa hierarchy for the given indication
def extract_meddra_hierarchy(indication: str) -> Tuple[dict, str]:
    """Get Meddra hierarchy for the given indication from Meddra dictionary allowing no typos.
       dis <= 0. 
    Args:
        indication (str): Indication from FDA Openlabel
    Returns:
        _type_: Matched Meddra hierarchy and AETERM if matched.
    """
    #llt_dict, pt_dict, hlt_dict, hlgt_dict, soc_dict = load_meddra_dictionaries(version='Version 24.0')
    matched_term = None
    #print('Inside meddra hierarchy , aeterm is', indication, type(indication))

    ae_term = indication.lower()
    if ae_term in pt_dict:
        matched_term = ae_term
    else:
        for key, value in llt_dict.items():
            # * replacing fuzzy search here.
            # ! reverted to regex because fuzzywuzzy takes ages
            dis = jellyfish.levenshtein_distance(key, ae_term)
            if (dis <= 3):
                matched_term = key
            # if fuzzy_search(ae_term, key, threshold=90):
            #     matched_term = key

    return_dict = {}
    if matched_term:
        return_dict['llt_code'] = llt_dict[matched_term]['llt_code']
        return_dict['pt_code'] = llt_dict[matched_term]['pt_code']
        return_dict['hlt_code'] = pt_dict[return_dict['pt_code']]
        return_dict['hlgt_code'] = hlt_dict[return_dict['hlt_code']]
        return_dict['soc_code'] = hlgt_dict[return_dict['hlgt_code']]
    
    for key, value in return_dict.items():
        return_dict[key] = int(value)

    return return_dict, matched_term 

def cmindc_ptcode(cmindc):
    all_diagnosis_meddra_dict = load_full_meddra_precompute()
    full_flatten_meddra_hierarchy = load_flat_meddra_precompute()
    matched_term = None
    cm_term = cmindc.lower().strip()
    sep_list = ['/',',','and',';','-']
    seperator = []
    for i in sep_list:
        if i in cm_term:
            seperator = cm_term.split(i)
    

    # if cm_term in pt_dict:
    #     matched_term = cm_term
    # else:
    new_list=[]
    # print(seperator)
    seperator.append(cm_term)
    for val in seperator:
        
        for key, value in all_diagnosis_meddra_dict.items():
            # * replacing fuzzy search here.
            # ! reverted to regex because fuzzywuzzy takes ages
            # print(key)
            
            dis = jellyfish.levenshtein_distance(key.strip(), val)
            if (dis <= 3):
                matched_term = key

                for i in value:
                    for k,v in i.items(): 
                        for j in v.keys():
                            if v[j]['name']=='LLT':

                                if j not in new_list:
                                    new_list.append(j)
        for key, value in full_flatten_meddra_hierarchy.items():
            # * replacing fuzzy search here.
            # ! reverted to regex because fuzzywuzzy takes ages
            dis = jellyfish.levenshtein_distance(key.strip(), val)
            if (dis <= 3):
                matched_term = key

                # for i in value:
                for k,v in value.items():
                    # print('tasye',v)
                    # for j in v.keys():
                        # print('testing',j)
                    # if v['name']=='LLT':
                    #     print('hello')
                    if v['name']=='LLT':
                        # print('nameeeee',v['name'])
                        if k not in new_list:
                            new_list.append(k)

    return new_list


def map_diagnosis_meddra(aeptcode: str, 
                         aelltcode: str, 
                         diagnosis: str) -> bool:
    """Map diagnosis from FDA openlabel to 
       Meddra dictionary with the given AEPT or AELLT code.
    Args:
        aeptcode (str): AEPT code
        aelltcode (str): AELLT code
        diagnosis (str): Diagnosis from FDA
    Returns:
        bool: True if matches or otherwise
    """
    all_diagnosis_meddra_dict = load_full_meddra_precompute()
    code_match = False

    try:
        diagnosis_hierarchy = all_diagnosis_meddra_dict[diagnosis]
        for diag_suggestion in diagnosis_hierarchy:
            (suggestion_aeterm, suggestion_hie), = diag_suggestion.items()
            if (aeptcode in suggestion_hie) or (aelltcode in suggestion_hie):
                code_match = True
                break
    except:
        pass
    return code_match

def reverse_lookup(aeptcode: Union[int, str], 
                   cmtrt: Union[str, List], 
                   llr_flag: bool=True) -> bool:
    """Reverse lookup using AE PT Code(aeptcode) to check for 
       given Concomitant Medication(cmtrt)
    Args:
        aeptcode (str): AE PT Code
        cmtrt (Union[str, List]): Input CMTRT
        llr_flag(bool): _description_
    Returns:
        bool: Returns True if pt code matches or otherwise
    """
    
    faers, faers_male, faers_female, structures = load_drug_lookup_files()
    #print('Inside reverse lookup, len of all dfs are :', len(faers), len(faers_male), len(faers_female), len(structures))
    #print('aeptcode and cmtrt are :', aeptcode, cmtrt)
    # if not null_check(aeptcode):
    #     aeptcode = get_int_id(aeptcode)
    #     if aeptcode == None:
    #         return False
    # else:
    #     return False
    try:
        aeptcode = int(aeptcode) if isinstance(aeptcode, (str,float)) else aeptcode
    except Exception as aeptexc:
        pass

    cmtrt_list = cmtrt
    if isinstance(cmtrt, str):
        cmtrt_list = [cmtrt]
    
    if llr_flag:
        f = faers.query('meddra_code == @aeptcode and llr > llr_threshold')
        fm = faers_male.query('meddra_code == @aeptcode and llr > llr_threshold')
        ff = faers_female.query('meddra_code == @aeptcode and llr > llr_threshold')
    else:
        f = faers.query('meddra_code == @aeptcode')
        fm = faers_male.query('meddra_code == @aeptcode')
        ff = faers_female.query('meddra_code == @aeptcode')
    
    f_idx = f['struct_id'].unique().tolist()
    fm_idx = fm['struct_id'].unique().tolist()
    ff_idx = ff['struct_id'].unique().tolist()
    all_ids = set(f_idx).union(set(fm_idx)).union(set(ff_idx))

    '''print('f_idx', f['struct_id'].unique().tolist())    
    print('fm_idx', fm['struct_id'].unique().tolist())   
    print('fm_idx', ff['struct_id'].unique().tolist())   
    print('all_ids', all_ids)
    '''
    rv_flag = False
    '''
    for cmtrt in cmtrt_list:
        valid_cmtrts = list(set(structures[structures['id'].isin(all_ids)]['name'].unique().tolist()))
        #print('valid cmtrts', valid_cmtrts)
        # search_results = fuzzy_search(cmtrt, valid_cmtrts, threshold=90, debug=True)
        search_results = fuzzy_search(cmtrt, valid_cmtrts, threshold=90)
        if search_results:
            rv_flag = True
            return True
        else:
            rv_flag = False

    '''
    valid_cmtrts = structures[structures['id'].isin(all_ids)]['name'].unique().tolist()
    valid_cmtrt_2 = list()
    for cmtrt in structures[structures['id'].isin(all_ids)]['lname'].tolist():
        valid_cmtrt_2 += ast.literal_eval(cmtrt)
    valid_cmtrts += valid_cmtrt_2
    
    for cmtrt in cmtrt_list:    
        #print('valid cmtrts', valid_cmtrts)
        search_results = fuzzy_search(cmtrt, valid_cmtrts, threshold=90)
        if search_results:
            rv_flag = True
            return True
        else:
            rv_flag = False
    

    if rv_flag == False:
        return rv_flag
    else:
        return None

def str_fuzzy_check(term1, term2):
    try:
        ret_flg = False
        term1 = ast.literal_eval(term1)
        for i in term1:
            i = str(i)
            if(fuzzy_search(i.lower(), [term2])):
                ret_flg = True
                break
        return ret_flg
    except Exception as str_fuzzr_exc:
        print(f' Str fuzzy exception is : {str_fuzzr_exc} {term1} {term2} ')
        pass

def fallback(cmtrt: str, 
             aeterm: str, 
             aeptterm: str=None, 
             aeptcode: str=None, 
             aelltcode: str=None, 
             cmcode: str=None) -> bool:
    """Fallback lookup
    Args:
        cmtrt (str): _description_
        aeterm (str) : _description_
        cmcode (str, optional): _description_  Defaults to None.
        aeptterm (str, optional): _description_. Defaults to None.
        aeptcode (str, optional): _description_. Defaults to None.
        aelltcode (str, optional): _description_. Defaults to None.
    Returns:
        bool: _description_
    """
    fallback_df = load_fallback()
    #print('^^^^ The codes are : ',cmcode, '^', aeptcode,'^', aelltcode,'^',aeptterm,'^', aeterm,'^',cmtrt)
    try:
        if (cmcode is not None and aeptcode is not None and aeptterm is not None):  
            try:                                 
                cmcode = int(cmcode.lstrip('0').replace(' ',''))
            except Exception as cmexc:
                print(f'CM code is not an integer, {cmexc}')
                pass
            try:
                aeptcode = int(float(aeptcode))
            except Exception as aept_exc:
                print(f'aeptcode is not an integer {aept_exc}')
                pass
            threshold = fallback_df[(fallback_df['cmcode'] == cmcode) & \
                                    (fallback_df['aeptcode'] == aeptcode) & \
                                    #(fallback_df['aelltcode'] == aelltcode) & \
                                    (fallback_df['aeterm'].apply(str_fuzzy_check, args = (aeterm,))) & \
                                    (fallback_df['aeptterm'].apply(str_fuzzy_check, args = (aeptterm,))) & \
                                    (fallback_df['cmtrt'].apply(str_fuzzy_check,args = (cmtrt,)))]['valid']
            #print('Threshold inside cmcode is not None and aeptcode is not None and and aeptterm is not None is ', threshold)
            try:
                if not threshold.empty :
                    threshold = float(threshold.iloc[0])
                    #print('Threshold is (not inside str search) :', threshold)
                    if threshold > 0 :
                        return True
                    else:
                        return False
                else:
                    #print('No records in  Fallback.. Threshold is (not inside str search)')
                    return None
            except Exception as e:
                print(traceback.format_exc())            
        elif (cmcode is not None and aeptcode is not None):
            cmcode = int(cmcode.lstrip('0').replace(' ',''))
            aeptcode = int(float(aeptcode))
            #threshold = fallback_df[(fallback_df['cmcode'] == cmcode) & \
            #                        (fallback_df['aeptcode'] == aeptcode)]['valid']
            threshold = fallback_df[(fallback_df['cmcode'] == cmcode) & \
                            (fallback_df['aeterm'].apply(str_fuzzy_check, args = (aeterm,))) & \
                            (fallback_df['cmtrt'].apply(str_fuzzy_check,args = (cmtrt,))) & \
                            (fallback_df['aeptcode'] == aeptcode)]['valid']
            
            #print('Threshold inside cmcode is not None and aeptcode is not None is ', threshold)
            try:
                if not threshold.empty :
                    threshold = float(threshold.iloc[0])
                    #print('Threshold is (not inside fuzzy) :', threshold)
                    if threshold > 0 :
                        return True
                    else:
                        return False
                else:
                    print('No records in  Fallback.. Threshold is (not inside fuzzy)')
                    return None
            except Exception as e:
                print(f'Threshold not empty exception is : {e} ')
                print(traceback.format_exc())
        else:
            try:
                threshold_str = pd.DataFrame()
                threshold_str = fallback_df[fallback_df['aeterm'].apply(str_fuzzy_check, args = (aeterm,)) & \
                                fallback_df['cmtrt'].apply(str_fuzzy_check,args = (cmtrt,))]
                #print('Thresh hold string', threshold_str)
                if threshold_str.empty:
                    print(f'No records in Fallback for {aeterm} and {cmtrt}')
                    return None
            except Exception as Thr_exc:
                print('Threshold exception', Thr_exc)
                return None
            
            try:
                if not(threshold_str.empty):
                    threshold_str = float(threshold_str['valid'].values[0])
                    print('threshold is :',threshold_str)
                    if threshold_str > 0:
                        return True
                    else:
                        return False
            except Exception as e:
                print(traceback.format_exc())
        
    except Exception as e:
        print(traceback.format_exc())
    return None

def match_aeterm(aeterm: str, 
                 input_diagnosis: List, 
                 aeptterm: str = None, 
                 aeptcode: str = None, 
                 aelltcode:str = None) -> bool:
    """Match is done using 1) String search and 2) Meddra with given AE codes.
    Args:
        aeterm (str): AETERM
        input_diagnosis (List): Diagnosis list
        aeptterm (str): AEPTTERM.
        aeptcode (str): AEPTCODE.
        aelltcode (str): AELLTCODE.
    Returns:
        bool: True if matches or otherwise
    """
    ### string match to remove (INTERMITTENT) pattern from AETERM
    # temp1 = "(intermittent)"
    # if temp1 in ae_term:
    #     ae_term = ae_term.replace(temp1, '').strip()
    
    #print('Inside match ae_term :', aeterm, type(aeterm))
    match_flag = False
    aeterm = remove_paranthesis(aeterm) 

    rem_list = ['intermittent', 'worsening', ' in ', ' on ', ' with ', ' the ', ' and ', 'upper ', 'lower ', 'right ', 'left ']
    for i in rem_list:
        if i in aeterm:
            aeterm = aeterm.replace(i, ' ').strip()
    # Ignoring tokenizing aeterm considering we use Meddra code match.

    try:
        meddra_aeterm, _ = extract_meddra_hierarchy(aeterm)
        if meddra_aeterm:
            for diagnosis in input_diagnosis:
                # if (re.search(aeterm, diagnosis, re.IGNORECASE)):
                if not null_check(aeterm) and not null_check(diagnosis):
                    if fuzzy_search(aeterm, diagnosis):
                        match_flag = True
                        return True
                if not null_check(aeptterm) and not null_check(diagnosis):
                    # if (re.search(aeptterm, diagnosis, re.IGNORECASE)):
                    if fuzzy_search(aeptterm, diagnosis):
                        match_flag = True
                        return True
                meddra_diag, _ = extract_meddra_hierarchy(diagnosis)
                if meddra_diag and meddra_aeterm:
                    if (meddra_aeterm['llt_code'] == meddra_diag['llt_code']) \
                        or (meddra_aeterm['pt_code'] == meddra_diag['pt_code'])\
                        or (meddra_aeterm['hlt_code']==meddra_diag['hlt_code'])\
                        or (meddra_aeterm['hlgt_code']==meddra_diag['hlgt_code'])\
                        or (meddra_aeterm['soc_code'] == meddra_diag['soc_code']):
                        match_flag = True
                        return True
                
                if not null_check(aeptcode) and not null_check(aelltcode):
                    is_code_matching = map_diagnosis_meddra(aeptcode, aelltcode, diagnosis)
                    if is_code_matching:
                        match_flag = True
                        return True    
        return match_flag
    except:
        print(traceback.format_exc())
        pass

def google_search(study,subject,cmdecode,cm_term,aedecode,ae_term,AEPTCD='',AE_LLT_CODE=''):
    return google_search_helper(study,subject,cmdecode,cm_term,aedecode,ae_term,AEPTCD='',AE_LLT_CODE='')
    # fall_back_table = fallback_df
    # if cm_term in fall_back_table['cmtrt'].to_list():
    #     if type(ae_term)==float:
    #         ae_term='blank'
    #     if type(aedecode)==float:
    #         aedecode='blank'
    #     print('inside',fall_back_table[fall_back_table['cmtrt'].str.upper()==cm_term.upper()])
    #     print(fall_back_table[fall_back_table['cmtrt'].str.upper()==cm_term.upper()].to_dict())
    #     print('present',ae_term)
    #     data = fall_back_table[(fall_back_table['cmtrt'].str.upper()==cm_term.upper())&\
    #     ((fall_back_table['aeterm'].str.upper() == ae_term.upper())|(fall_back_table['aeptterm'].str.upper()== aedecode.upper())|\
    #         ((fall_back_table['aeterm'].str.upper() == aedecode.upper())|(fall_back_table['aeptterm'].str.upper() == ae_term.upper()))
    #     )]
    #     print('inside',data.shape)
    #     if data.shape[0]>0:
    #         for ind_i in range(data.shape[0]):
    #             data_rec=data.iloc[[ind_i]]
    #             print(data_rec['valid'])
    #             if   data_rec['valid'].values[0] in [1,'1','1.0',1.0]:
    #                 print('sucesss fallback')
    #                 similarity_flag=True
    #                 break
    

def check_valid_drug_indication(cmtrt: str,
                                cmindc : str, 
                                cmdecod : str,
                                aeterm: str,
                                aedecod : str, 
                                aeptterm: str=None,
                                aeptcode: str=None,
                                aelltcode: str=None, 
                                cmcode: str=None) -> bool:
    """
    Args:
        cmtrt (str): Concomitant medication(CM)
        aeterm (str): Adverse event(AE) term
        aeptterm (str, optional): Preferred AE term. Defaults to ''.
        aeptcode (str, optional): AE PT Code. Defaults to ''.
        aelltcode (str, optional): AE LLT Code. Defaults to ''.
        cmcode (str, optional): CM Code. Defaults to ''.
        # who_strict (bool, optional): Strictly check WHO dictionary. 
        #                              Set to False to ignore WHO search,
        #                              Defaults to True.
    Returns:
        bool: Returns true if CMTRT and AETERM matches or otherwise
    """
    print('***********CMTRT, CMINDC, CMDECOD, AETERM, AEDECOD',cmtrt, cmindc, cmdecod,aeterm, aedecod,'***********')
    try:    
        check_reverse_flg = None
        check_aeterm_flg = None

        if(type(cmtrt) != float and cmtrt != None): 
            cmtrt = cmtrt.lower().strip()
        if(type(cmdecod) != float and cmdecod != None): 
            cmdecod = cmdecod.lower().strip()
        if(type(aeterm) != float and aeterm != None):   
            aeterm = aeterm.lower().strip()
        if(type(aedecod) != float and aedecod != None):
            aedecod = aedecod.lower().strip()

        gen_cmtrt = None
        
        if aeptterm != None:    
            aeptterm = aeptterm.lower() 
        
        if cmcode != None:  
            gen_cmtrt = who_search_with_code(cmcode, cmtrt) 
        else:
            if(cmtrt != None):  
                gen_cmtrt = who_search(cmtrt)
            elif(cmdecod != None):
                gen_cmtrt = who_search(cmdecod)
            
        fallback_flg = None
        ccode= None

        if(isinstance(gen_cmtrt, str)):
            gen_cmtrt = gen_cmtrt.upper()
        if(isinstance(cmtrt, str)):
            c_cmtrt = cmtrt.upper()
                
        try:
            matched_term = aeterm
            if(aeptcode == None  or type(aeptcode) == float):
                aeptcode = llt_dict[aeterm]['pt_code']
                #print('aeptcode', aeptcode)
            if(aeptcode == None  or type(aeptcode) == float and aedecod.upper() != aeterm.upper()):
                aeptcode = llt_dict[aedecod]['pt_code']
                matched_term = aedecod
                #print('aeptcode', aeptcode)
            try:
                if(aeptcode != None):
                    new_pt_df = ptdf[ptdf[0] == aeptcode]
                    if len(new_pt_df)>0:
                        #print(len(new_pt_df), new_pt_df[0], new_pt_df[1])
                        if(aeptterm == None or type(aeptterm)==float):
                            aeptterm = new_pt_df[1].values[0]
                        try:
                            if(aelltcode == None or type(aelltcode) == float): 
                                aelltcode = int(llt_dict[matched_term]['llt_code'])
                            if(aeptcode == None or type(aeptcode) == float):
                                aeptcode = int(llt_dict[matched_term]['pt_code'])
                            print('aeptcode, aelltcode is ', aeptcode, aelltcode)
                        except Exception as ptcode_exc:
                            # print('Exception while getting llt code and ptcode from llt dict',ptcode_exc)
                                pass
                else:
                    print(f'No aeptcode for {aeterm}, {aedecod}')
            except Exception as ptterm_exc:
                print(f'No aeptterm for {aeterm}, {aedecod}, {aeptcode} :',ptterm_exc)
                pass
                #print('AE PT TERM is :',aeptterm)
            
        except Exception as ptcode_exc:
            print(f'No aeptcode for {aeterm}',ptcode_exc)
            pass
            
        if(cmcode == None):
            try:
                if gen_cmtrt != None and type(gen_cmtrt) != float and gen_cmtrt.upper() in who_drug_term_code:
                    ccode = who_drug_term_code[gen_cmtrt]
                    cmtrt = gen_cmtrt
                elif cmtrt != None and type(cmtrt) != float and cmtrt.upper() in who_drug_term_code:
                    ccode = who_drug_term_code[cmtrt]
                    cmtrt = cmtrt
                elif cmdecod != None and type(cmdecod)!= float and cmdecod.upper() in who_drug_term_code:
                    ccode = who_drug_term_code[cmdecod]  
                    cmtrt = cmdecod     
                print('CM code is ', ccode)
                
                if (len(ccode) == 0):
                    print(f'{c_cmtrt} not in who_drug_code')
                else:
                    cmcode = ccode
                print('Fetched cm code is', cmcode)
            except Exception as ccode_exc:
                print(f'No cmcode for {cmtrt}, {cmdecod}: {ccode_exc} ')
                pass
        #Checking fallback before match_aeterm and reverse lookup
        try:
            if(cmdecod != None and type(cmdecod) !=float):
                fallback_flg =  fallback(cmdecod, aeterm, aeptterm, aeptcode, aelltcode, cmcode)
            if(fallback_flg == None):
                if(cmtrt != None and type(cmtrt)!= float):
                    fallback_flg =  fallback(cmtrt, aeterm, aeptterm, aeptcode, aelltcode, cmcode)
                if(fallback_flg == None):
                    if(cmdecod != None and type(cmdecod) !=float):
                        fallback_flg =  fallback(cmdecod, aeterm)
                if(fallback_flg == None):
                    if(cmtrt != None and type(cmtrt)!= float):
                        fallback_flg =  fallback(cmtrt, aeterm)

        except Exception as fallback_exc:            
            print(f'Fall back exception is : {fallback_exc}')
            pass
        
        if(fallback_flg != None):
            print('Fallback flag is :', fallback_flg)
            return fallback_flg
        
        cmtrt_str = list()   
        if (aeptcode != None and type(aeptcode)!=float):    
            if(cmtrt != None) and type(cmtrt)!=float:
                cmtrt_str.append(cmtrt)               
                if(type(cmdecod) != float and cmdecod != None and cmtrt.upper() != cmdecod.upper()):
                    cmtrt_str.append(cmdecod)
                if(type(gen_cmtrt) != float  and gen_cmtrt!=None and cmtrt.upper() != gen_cmtrt.upper() and (cmdecod != None and type(cmdecod) != float and cmdecod.upper() != gen_cmtrt.upper())):
                    cmtrt_str.append(gen_cmtrt)  
            elif cmdecod != None and type(cmdecod)!=float:
                cmtrt_str.append(cmdecod)

        print('CMTRT list is:', cmtrt_str)

        if(aeptcode != None and len(cmtrt_str)>0):
            rv_look_flg = reverse_lookup(aeptcode, cmtrt_str)
            if rv_look_flg:
                print('Reverse look up is True for :', aeterm, aeptcode, cmtrt_str)
                check_reverse_flg = True
            else:
                if rv_look_flg == None:
                    check_reverse_flg = None
                else:
                    check_reverse_flg = False
                print('Reverse look up is False for :', aeterm, aeptcode, cmtrt_str)
        else:
            check_reverse_flg = None
        
        if(check_reverse_flg == None or not check_reverse_flg):
            try:
                diag_list = list()
                if(cmtrt != None and type(cmtrt) != float):
                    diag_list = diagnosis_fda_openlabel(cmtrt)
                    # print('cmtrt, diag_list is :',cmtrt, diag_list)
                #if(len(diag_list)==0 and cmdecod != None and type(cmdecod) != float):
                if (cmdecod != None and type(cmdecod) != float):
                    if(cmtrt != None and type(cmtrt) != float):
                        if(cmdecod.upper() != cmtrt.upper()):
                            diag_list = diag_list + diagnosis_fda_openlabel(cmdecod)
                            # print('cmdecod, diag_list is :',cmdecod, diag_list)
                    else:
                        diag_list = diagnosis_fda_openlabel(cmdecod)
                #if(len(diag_list)==0 and gen_cmtrt != None and type(gen_cmtrt) != float):
                if(gen_cmtrt != None and type(gen_cmtrt) != float):
                    if(len(diag_list) == 0):                
                        diag_list = diagnosis_fda_openlabel(gen_cmtrt)
                    else:
                        if (cmtrt != None and type(cmtrt) != float and cmtrt.upper() != gen_cmtrt.upper()) and (cmdecod != None and type(cmdecod) != float and cmdecod.upper() != gen_cmtrt.upper()):
                            diag_list = diag_list + diagnosis_fda_openlabel(gen_cmtrt)
                        elif (cmtrt != None and type(cmtrt) != float and cmtrt.upper() != gen_cmtrt.upper()):
                            diag_list = diag_list + diagnosis_fda_openlabel(gen_cmtrt)
                        elif(cmdecod != None and type(cmdecod) != float and cmdecod.upper() != gen_cmtrt.upper()):
                            diag_list = diag_list + diagnosis_fda_openlabel(gen_cmtrt)
                                   
            except Exception as diag_exc:
                print(f'Exception while retreiving diagnosis list :', diag_exc)     
                pass

            if(len(diag_list) > 0):
                valid_aedecod = None
                diag_list = list(set(diag_list))
                # print('diag_list :', diag_list)
                try:    
                    if(aeterm != None and type(aeterm)!= float):
                        valid_aeterm = match_aeterm(aeterm, diag_list, aeptterm, aeptcode, aelltcode)
                        if (not valid_aeterm or valid_aeterm == None) and aedecod != None and type(aedecod) != float:
                            if (aedecod.upper() != aeterm.upper()):
                                valid_aedecod = match_aeterm(aedecod, diag_list, aeptterm, aeptcode, aelltcode) 
                    elif aedecod != None and type(aedecod) != float:
                        valid_aeterm = match_aeterm(aedecod, diag_list, aeptterm, aeptcode, aelltcode)
                        
                except Exception as match_exc:
                    print(f'Exception while  matching aterm with Diagnosis {match_exc}')  
                    pass      

                if valid_aeterm:
                    print('AE term flag is :', valid_aeterm)
                    check_aeterm_flg = True
                else:
                    if valid_aedecod:
                        print('AE decod/term flag is :', valid_aedecod)
                        check_aeterm_flg = True
                    else:
                        print('AE term flag is False')
                        check_aeterm_flg = False
            else:            
                check_aeterm_flg = None

        print(f'*** Reverse lookup flag, match aeterm flag, fallback flag for {aeterm},{aedecod},{cmtrt},{cmdecod} ', check_reverse_flg, check_aeterm_flg, fallback_flg)
        if(fallback_flg == None):
            if check_reverse_flg == None and check_aeterm_flg == None:
                return None
            else:
                if check_aeterm_flg or check_reverse_flg:
                    return True
                else:
                    return False    
                    #return check_flg        
    except Exception as e:  
        print(traceback.format_exc())


def check_similar_indication(indication_1: str, 
                             indication_2: str) -> bool:
    """Check simlilar indication using Meddra dictionary
    Args:
        indication_1 (str): indication 1
        indication_2 (str): indication 2
    Returns:
        bool: True if matches or otherwise
    """
    ind_1 = extract_meddra_hierarchy(indication_1)[0]
    ind_2 = extract_meddra_hierarchy(indication_2)[0]
    if (ind_1 and ind_2):
            if (ind_1['llt_code'] == ind_2['llt_code']) or \
               (ind_1['pt_code'] == ind_2['pt_code']):
                return True
    return False



def check_similar_medication_verabtim(medication_1: str, 
                                      medication_2: str) -> bool:
    """Check simlilar medication with regex
    Args:
        medication_1 (str): medication 1
        medication_2 (str): medication 2
    Returns:
        bool: True if matches or otherwise
    """
    medication_1 = re.sub(r'[\W_]+', ' ', medication_1)
    medication_2 = re.sub(r'[\W_]+', ' ', medication_2)
    if re.search(medication_1, medication_2, re.IGNORECASE):
        return True
    return False

def check_similar_medication(medication_1: str, medication_2: str) -> bool:
    """Check simlilar medication with regex, who dict search followed by comparing it's diagnosis
       from FDA Openlabel.
    Args:
        medication_1 (str): medication 1
        medication_2 (str): medication 2
    Returns:
        bool: True if matches or otherwise
    """
    medication_1 = re.sub(r'[\W_]+', ' ', medication_1)
    medication_2 = re.sub(r'[\W_]+', ' ', medication_2)
    
#     search_medication = r"\b" + medication_1 + r"\b" 
#     this breaks the result
    if re.search(medication_1, medication_2, re.IGNORECASE):
        return True
    
    
    med1 = who_search(medication_1)
    med2 = who_search(medication_2)
    
    if med1 and med2:
        med1_diag_list = clean_indications(diagnosis_fda_openlabel(med1.lower()))
        med2_diag_list = clean_indications(diagnosis_fda_openlabel(med2.lower()))
        
        for diag1 in med1_diag_list:
            for diag2 in med2_diag_list:
                if diag1 == diag2:
                    
                    return True
    return False


#
# Experimental functions
#

model_url = 'http://10.20.102.114:8088/mapping'
headers = {'Content-type': 'application/json',}
def get_whodrug_model(cmtrt_label):
    
    cmtrt_suggestions = list()
    cmtrt_label = json.dumps(cmtrt_label)
    data = f'{{"vt": {cmtrt_label}, "task": "WHO"}}'
    
    response = requests.post(model_url, headers=headers, data=data)
    model_resp = response.json()
    
    if 'error' not in model_resp.keys():
        who_suggestion_dict = model_resp['results'][0]['suggestions']

        for idx in range(len(who_suggestion_dict)):
            cur_sugges = who_suggestion_dict[idx]
            distance_metric = cur_sugges['distance']
            cmtrt_term = cur_sugges['term']
            if distance_metric > 0:

                cmtrt_suggestions.append(remove_splchars(cmtrt_term))

        return cmtrt_suggestions


# Fetch diagnosis from FDA open label a different variation
def diagnosis_fda_openlabel1(drug_name):
    fda_openlabel = load_fdaopenlabel()
    result = list()
    if drug_name == None:
        return result
    drug_names = remove_splchars(drug_name).lower()
    
    if isinstance(drug_names, str):
        drug_names = [drug_names]
    elif (' ' in drug_names) == True:
        drug_names = drug_names.split(' ')
    
    for drug_name in drug_names:
        for index, values in fda_openlabel.items():
            try:
                brand_names = values.get("brand_name"," ")
                generic_names = values.get("generic_names"," ")
                generic_name = values.get("generic_name"," ")
                substance_name = values.get("substance_name", " ")
                if type(brand_names) == str:
                    brand_list = list()
                    brand_list.append(brand_names)
                    brand_names = brand_list
                if type(generic_names) == str:
                    generic_names_list = list()
                    generic_names_list.append(generic_names)
                    generic_names = generic_names_list
                if type(generic_name) == str:
                    generic_name_list = list()
                    generic_name_list.append(generic_name)
                    generic_name = generic_name_list
                if type(substance_name) == str:
                    substance_name_list = list()
                    substance_name_list.append(substance_name)
                    substance_name = substance_name_list
                if type(brand_names) == list:
                    for brand in brand_names:
                        # search_drug_name = r"\b" + drug_name + r"\b"
                        # if (re.search(search_drug_name, brand, 
                        #     re.IGNORECASE)): 
                        # # (check this if increases performance)
                        if (re.search(drug_name, brand, re.IGNORECASE)):
                            result = list(set(result + values['diagnosis']))
                if type(generic_names) == list:
                    for generic_name in generic_names:
                        # search_drug_name = r"\b" + drug_name + r"\b"
                        # if (re.search(search_drug_name, generic_name, 
                        #     re.IGNORECASE)): 
                        # # (check this if increases performance)
                        if (re.search(drug_name, generic_name, re.IGNORECASE)):
                            result = list(set(result + values['diagnosis']))
                if type(generic_name) == list:
                    for generic in generic_name:
                        # search_drug_name = r"\b" + drug_name + r"\b"
                        # if (re.search(drug_name, generic_name, 
                        #     re.IGNORECASE)): 
                        # # (check this if increases performance)
                        if (re.search(drug_name, generic, re.IGNORECASE)):
                            result = list(set(result + values['diagnosis']))
                if type(substance_name) == list:
                    for substance in substance_name:
                        # search_drug_name = r"\b" + drug_name + r"\b"
                        # if (re.search(drug_name, substance, 
                        #     re.IGNORECASE)): 
                        # # (check this if increases performance)
                        if (re.search(drug_name, substance, re.IGNORECASE)):
                            result = list(set(result + values['diagnosis']))
            except Exception as e:
                print(traceback.format_exc())
    return result