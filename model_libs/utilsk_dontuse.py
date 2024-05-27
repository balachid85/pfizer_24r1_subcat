import os
import pandas as pd
from dateutil.parser import parse
import logging
import numpy as np
import sys
from fuzzywuzzy import process
from fuzzywuzzy import fuzz

"""import yaml
config_file = "subcate_config.yml"
subcate_config = yaml.load(open(config_file, 'r'), Loader = yaml.FullLoader)
basic_cols = subcate_config['FIELDS_FOR_UI']['BASIC']
cate_cols_dict = subcate_config['FIELDS_FOR_UI']"""


def update_report(primary_df, secondary_df, category,report= dict()):
    primary_domain = category[:2]
    secondary_domain = category[2:4]
    needed_col_dict = cate_cols_dict[category]
        
    if 'OV' in category:
        df_dict = {f'{primary_domain}': primary_df, f'{primary_domain}_1': secondary_df}
    else:
        if category == 'AECM6' or category == 'CMAE6':
            df_dict = {primary_domain: primary_df}
        else:
             df_dict = {primary_domain: primary_df, secondary_domain: secondary_df}
    if "OV" in category: 
         for dom, df in df_dict.items():
            dom_base_cols = [col for col in basic_cols if col in df.columns.tolist()]
            suf_val = "_1" if "_1" in dom else ""
            for col in dom_base_cols:
                report[f"{primary_domain}_{col}{suf_val}"] = df[col]
                needed_cols = needed_col_dict[primary_domain]
                dom_needed_cols = [col for col in needed_cols if col in df.columns.tolist()]
                for col in dom_needed_cols:
                    report[f"{col}{suf_val}"] = df[col]
    else:
        for dom, df in df_dict.items():
            dom_base_cols = [col for col in basic_cols if col in df.columns.tolist()]
            for col in dom_base_cols:
                report[f"{dom}_{col}"] = df[col]
            needed_cols = needed_col_dict[dom]
            dom_needed_cols = [col for col in needed_cols if col in df.columns.tolist()]
            for col in dom_needed_cols:
                report[f"{col}"] = df[col]
    return report

        
def get_deeplink(study_id, rec):
    rec_columns = rec.columns.tolist()
    if 1:
        subj_id_col = [col for col in rec_columns if col.endswith('SUBJECTID')][0]
        siteno = [col for col in rec_columns if col.endswith('siteno')][0]
        visit_id = [col for col in rec_columns if col.endswith('visit_id')][0]
        visit_ix = [col for col in rec_columns if col.endswith('visit_ix')][0]
        form_id = [col for col in rec_columns if col.endswith('form_id')][0]
        form_index = [col for col in rec_columns if col.endswith('form_index')][0]
        subjid = rec[subj_id_col].values[0]
        siteno = rec[siteno].values[0]
        visit_id = rec[visit_id].values[0]
        visit_ix = rec[visit_ix].values[0]
        form_id = rec[form_id].values[0]
        form_index = rec[form_index].values[0]
        deeplink = f"https://pfizer-inform.oracleindustry.com/{study_id}/pfts.dll?S=2313B74C678711d29DAD00A0C9769FD8&C=TM_29&SI={siteno}&IP={subjid};{visit_id}!{visit_ix};{form_id}!{form_index}"
        return deeplink
    
def get_logger(logpath, filepath, package_files=[], displaying=False, saving=True, debug=False):
    logger = logging.getLogger()
    if debug:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logger.setLevel(level)
    if saving:
        info_file_handler = logging.FileHandler(logpath, mode="a")
        info_file_handler.setLevel(level)
        logger.addHandler(info_file_handler)
    if displaying:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        logger.addHandler(console_handler)
        logger.info(filepath)
    '''
    with open(filepath, "r") as f:
        logger.info(f.read())
    '''
    for f in package_files:
        logger.info(f)
        with open(f, "r") as package_f:
            logger.info(package_f.read())

    return logger

curr_file_path = os.path.realpath(__file__)
curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
main_path = curr_path
aelab = pd.read_csv(os.path.join(main_path, 'ae_lab_mapping-v2.csv'))


def get_ae(labtest):
    labtest = labtest.lower().strip().split('_')[0]
    for i, row in aelab.iterrows():
        aecod = row['aeptcd']
        valid_labs = [i.lower().strip() for i in row['lab_test'].split(',')]
        result = process.extract(labtest, valid_labs, scorer=fuzz.token_sort_ratio)
        flag = any([match_ratio for term_in_lablist, match_ratio in result if match_ratio >= 80])
        
        if flag == True:
            return int(float(aecod))
        else:
            return -1
        
def check_aelab(aecode: float, labtest: str):
    """Check aeterm code and labtest to be consistent
    Args:
        aecode (float): aeterm code, for now to test we can use AELLTCD
        labtest (str): labtest 
    """
    aecode = int(aecode)
    labtest = labtest.lower().strip().split('_')[0]
    for i, row in aelab.iterrows():
        aecod = row['aeptcd']
        valid_labs = [i.lower().strip() for i in row['lab_test'].split(',')]
        if (aecode == aecod):
            result = process.extract(labtest, valid_labs, scorer=fuzz.token_sort_ratio)
            # result = process.extract(labtest, valid_labs, scorer=fuzz.partial_token_sort_ratio)
            # print(result)
            return any([match_ratio for term_in_lablist, match_ratio in result if match_ratio >= 80])

def check_dir(dir_name):
    if not os.path.isdir(dir_name):
        os.system(f"mkdir {dir_name}")
    else:
        pass
    return True
d = {}
def update_dict(ae_form_ind, form_ind, ae_id, cm_id, d):
    tmp = {}
    tmp['ae_id'] = ae_id
    tmp['form_ind'] = form_ind
    tmp['cm_id'] = cm_id
    d[ae_form_ind].append(tmp)

get_date = lambda x : parse(str(x))

def extractor2(rec, piv_df, val1, val2, val3, val4):
    try:
        #print(ae_form_ind) 
        subjectid = piv_df['SUBJECTID'].values[0]
        rec = rec[rec['SUBJECTID'] == subjectid]
        piv_stdt = piv_df[val1].apply(get_date)
        piv_endt = piv_df[val2].apply(get_date)
        rec = rec[rec[val3].notna()]
        rec = rec[rec[val4].notna()]
        rec[val3] = rec[val3].apply(get_date)
        rec[val4] = rec[val4].apply(get_date)
        rec[val3] = rec[val3].apply(get_date)
        rec[val4] = rec[val4].apply(get_date)
        #print(len(rec))
        rec = rec[(rec[val3] >= piv_stdt.values[0]) & (rec[val4] <= piv_endt.values[0])]
        #print(rec[['CMSTDAT', 'CMENDAT']])
        if len(rec) > 0:
            return (rec, piv_df)
        else:
            return False
    except:
        #print(traceback.format_exc())
        return False

def extractor(record, ae_records, cm_records, question1, question2, d, valid_ind):
    pivot_form_ind = record['FORMINDEX'].values[0]
        
    k = {}
    pivot_ae_subjid = 0
    pivot_ae_subjid = record['SUBJECTID'].values[0]
    if 1:        
        pivot_ae_records = ae_records[ae_records['FORMINDEX'] == pivot_form_ind]
        ae_id = pivot_ae_records[question1].astype(str).values
        aeid = pivot_ae_records[question1].values[0]
        all_records = ae_records[(ae_records['SUBJECTID'] == pivot_ae_subjid) & (ae_records[question1] == aeid)]
        ae_id1 = ae_id
        #print('ae_id', ae_id)
        ae_flag = False
        # Checking ae_is shape is 1 and it's not 'nan' value
        if len(ae_id) >= 1 and pd.isnull(ae_id.any()) == False:
            ae_ids = id_handler(ae_id[0])
            if ae_ids == 'Other Category':
                if question1 == 'AESPID':                    
                    ae_id = ae_records['FORMIDX'].values[0]
                    ae_ids = id_handler(ae_id)
                else:
                    return 'Other Category'

            ae_flag = True
            pivot_cm_records = 0
            
        if ae_flag == True:
            cm_records1 = cm_records[cm_records['SUBJECTID'] == pivot_ae_subjid]['FORMINDEX'].unique().tolist()
            if len(cm_records1) > 1 and question2 != 'ECAENO':
                for cm_form_ind in cm_records1:
                    if 1:  
                        #print('cm form index', cm_form_ind)
                        pivot_cm_records = cm_records[cm_records['FORMINDEX'] == cm_form_ind]
                        pivot_cm_subjid = pivot_cm_records['SUBJECTID'].values[0]
                        #print('Pivot CM SubjectId', pivot_cm_subjid)
                        if pivot_ae_subjid == pivot_cm_subjid:
                            #print('CM SubjectID', pivot_cm_subjid)
                            cm_id = pivot_cm_records[question2].astype(str).values
                            cm_id1 = cm_id
                            #print('cm_id', cm_id)
                            if len(cm_id) >= 1 and pd.isnull(cm_id.any()) == False:
                                cm_ids = id_handler(cm_id[0])
                                #print('cm_ids', cm_ids)
                                if cm_ids == 'Other Category':
                                    #update_dict(pivot_form_ind, cm_form_ind, ae_id1[0], cm_id1[0], d)
                                    continue
                            else:
                                #update_dict(pivot_form_ind, cm_form_ind, ae_id1[0], cm_id1[0], d)
                                continue
                            for ae_id in ae_ids:
                                id_flag = 0 
                                if ae_id in cm_ids:
                                    id_flag = 1
                                    #print(pivot_ae_records['FORMINDEX'])
                                    if pivot_form_ind not in k:
                                        k[pivot_form_ind] = []
                                    k[pivot_form_ind].append(pivot_cm_records)
                                    valid_ind.add(pivot_form_ind)
                                    #print('cm_ids', cm_ids)
                                    break

                            if id_flag == 0:
                                #print()
                                pass
                                #update_dict(pivot_form_ind, cm_form_ind, ae_id1[0], cm_id1[0], d)

                        else:
                            continue
                return (k, pivot_ae_records, all_records)
            elif question2 == 'ECAENO':
                pivot_cm_records = cm_records[cm_records['SUBJECTID'] == pivot_ae_subjid]
                pivot_cm_records[question2] = pivot_cm_records[question2].astype(str).apply(id_handler)
                for q in range(pivot_cm_records.shape[0]):
                    pivot_cm_rec1 = pivot_cm_records.iloc[[q]]
                    ecaeno = pivot_cm_rec1[question2].values[0]
                    if type(ecaeno) == str:
                        continue
                    elif type(ecaeno) == list:
                        for ae_id in ae_ids:
                            if ae_id in ecaeno:
                                if pivot_form_ind not in k:
                                    k[pivot_form_ind] = []
                                k[pivot_form_ind].append(pivot_cm_rec1)
                                valid_ind.add(pivot_form_ind)
                                #print('cm_ids', ecaeno)
                                break
                return(k, pivot_ae_records, all_records)
            else:
                return False
            
        else:
            return False

def id_handler(ae_id):
    if type(ae_id) == float and np.isnan(ae_id) == True:
        return 'Other Category'
    elif type(ae_id) == float:
        return [int(ae_id)]
    elif type(ae_id) == int:
        return [ae_id]
    elif type(ae_id) == str:
        #print('IDs in string...')
        #print('Handling it...')
        flag = 0
        if ae_id.isnumeric():
            #print('String is a Number...')
            return [int(ae_id)]
        elif not ae_id.isnumeric():
            j = 0
            if 'and' in ae_id:
                ae_id = ae_id.replace('and', ',')
                flag = 1
                
            if 'AND' in ae_id:
                ae_id = ae_id.replace('AND', ',')
                flag = 1
            
            if '&' in ae_id:
                ae_id = ae_id.replace('&', ',')
                flag = 1
            
            if ';' in ae_id:
                ae_id = ae_id.replace(';', ',')
                flag = 1
            
            
            if flag == 0 and ',' not in ae_id:
                ae_id = ae_id.replace(" ", ",")
                #ae_id = ae_id.replace("  ", ",")
            
            ae_id = ae_id.split(',')
            ae_id =[uid for uid in ae_id if uid not in  [None, 'null', '', " ",]]     
            num = []
            arr_len = len(ae_id)
            for i in ae_id:
                try:
                    if i.isnumeric() == True or type(float(i)) == float:
                        j += 1
                        try:
                            num.append(int(i))
                        except:
                            num.append(int(float(i)))
                except:
                    if not i.isnumeric():
                        #print('Found String...')
                        return 'Other Category'
            if j == arr_len:
                #print('Handled All the Values in Array...')
                #print('Returning Array...')
                ae_id = num
                return ae_id
           
            
def combine_df(study):
    #print(study)
    start_files = ('sm_ae', 'sm_cm', 'sm_ec', 'sm_pr', 'sm_lb')
    study_files = [fil for fil in [c for _,_,c in os.walk(study)][0] if fil.startswith(start_files)]
    #print(study_files)
    study_files = [c for c in study_files if 'sm_ech' not in c]
    ae_rec = pd.DataFrame()
    cm_rec = pd.DataFrame()
    ec_rec = pd.DataFrame()
    pr_rec = pd.DataFrame()
    lb_rec = pd.DataFrame()
    for fil in study_files:
        df = pd.read_csv(os.path.join(study, fil), sep = '|')
        if len(df) > 0:
            if 'sm_ae' in fil:
                ae_rec = ae_rec.append(df, ignore_index=True)
            if 'sm_cm' in fil:
                cm_rec = cm_rec.append(df, ignore_index=True)
            if 'sm_ec' in fil:
                ec_rec = ec_rec.append(df, ignore_index=True)
            if 'sm_pr' in fil:
                pr_rec = pr_rec.append(df, ignore_index=True)
            if 'sm_lb' in fil:
                lb_rec = lb_rec.append(df, ignore_index=True)
    
    return ae_rec, cm_rec, ec_rec, pr_rec, lb_rec

def clean_columns(ae_rec, cm_rec, ec_rec, pr_rec):
    aecol_list = [col for col in ae_rec.columns.tolist() if '_' not in col]
    cmcol_list = [col for col in cm_rec.columns.tolist() if '_' not in col]
    eccol_list = [col for col in ec_rec.columns.tolist() if '_' not in col]
    prcol_list = [col for col in pr_rec.columns.tolist() if '_' not in col]
    
    ae_rec = ae_rec[ae_rec.columns.intersection(aecol_list)]
    cm_rec = cm_rec[cm_rec.columns.intersection(cmcol_list)]
    ec_rec = ec_rec[ec_rec.columns.intersection(eccol_list)]
    pr_rec = pr_rec[pr_rec.columns.intersection(prcol_list)]
    
    return ae_rec, cm_rec, ec_rec, pr_rec

def clean_numbers(final_cln_df, subjid = True):
    clean = lambda x : int(float(x))
    
    if subjid:   
        try:
            final_cln_df = final_cln_df[~final_cln_df['SUBJECTID'].str.contains("[a-zA-Z]").fillna(False)]
            final_cln_df = final_cln_df[~final_cln_df['SUBJECTID'].str.contains("-").fillna(False)]
            final_cln_df = final_cln_df[~final_cln_df['SUBJECTID'].str.contains(":").fillna(False)]
            final_cln_df = final_cln_df[final_cln_df['SUBJECTID'].notna()]
            final_cln_df['SUBJECTID'] = final_cln_df['SUBJECTID'].apply(clean)
        except:            
            final_cln_df = final_cln_df[final_cln_df['SUBJECTID'].notna()]
            final_cln_df['SUBJECTID'] = final_cln_df['SUBJECTID'].apply(clean)
            
    final_cln_df = final_cln_df[final_cln_df['FORMID'].notna()]
    final_cln_df = final_cln_df[final_cln_df['FORMINDEX'].notna()]
    
    final_cln_df['FORMINDEX'] = final_cln_df['FORMINDEX'].apply(clean)
    final_cln_df['FORMID'] = final_cln_df['FORMID'].apply(clean)
    
    #print('Before Cleaning Df shape', final_cln_df.shape)
    final_cln_df = final_cln_df[final_cln_df['FORMINDEX'] != -1]
    final_cln_df = final_cln_df[final_cln_df['FORMINDEX'] != 0]
    #print('After Cleaning Df shape', final_cln_df.shape)
    
    return final_cln_df
