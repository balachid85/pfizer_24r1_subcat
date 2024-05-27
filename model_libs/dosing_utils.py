try:
    import scripts.SubcatModels.model_libs.utils as utils
except:
    import utils as utils
import datetime
import numpy as np
import pandas as pd
from dateutil.parser import parse
from typing import List, Dict, Tuple, Union, NewType, Optional

aeacn_colums = ['AEACN', 'AEACN1', 'AEACN2', 'AEACN3', 'AEACN4', 'AEACN5', 'AEACN6', 'AEACN7', 'AEACN1_1', 'AEACN2_1', 'AEACN3_1']
aetrt_list = ['AETRT1', 'AETRT2', 'AETRT3', 'AETRT4', 'AETRT5', 'AETRT6', 'AETRT7']
aerel_map_dict = {'AETRT1': 'AEREL1', 'AETRT2': 'AEREL2', 'AETRT3': 'AEREL3'}
aeacn_map_dict = {'AETRT1': ['AEACN1', 'AEACN1_1'], 'AETRT2': ['AEACN2', 'AEACN2_1'], 'AETRT3': ['AEACN3', 'AEACN3_1']}

PandasDataFrame = NewType('pandas df', pd.core.frame.DataFrame)
DateTime = NewType('datetime', datetime.datetime)

# get_date = lambda x : parse(str(x)) if isinstance(x, str) else np.nan

###rename columns for report
gen_columns = {'SITENO': 'SITEID', 'VISIT_ID': 'VISITID', 'VISIT_IX': 'VISITINDEX', 'ITEMSET_IX': 'ITEMSETIDX', 'FORM_ID': 'FORMID', 'FORM_IX': 'FORMIDX'}
ec_columns = {'ITEMSET_IX': 'EC_ITEMSETIDX', 'VISIT_NM': 'EC_VISITNAM', 'FORMNAME': 'EC_FORMNAME'}

ecdose_studies = ['B9991032', 'B7461024', 'B7891015', 'C1061011']

def check_similar_aeterm(medication_1, medication_2):
    medication_1 = re.sub(r'[\W_]+', ' ', medication_1)
    medication_2 = re.sub(r'[\W_]+', ' ', medication_2)
    if re.search(medication_1, medication_2, re.IGNORECASE):
        return True
    return False

def get_date(date):
#     import ipdb;ipdb.set_trace()
    if (date in [None, 'null', '', " "]):
        return None
    elif not isinstance(date, float):
        date = str(date)
        try:
            if isinstance(date, str):
                if ':' not in date:
                    new_date = parse(date)
                    return new_date
                elif ':' in date:
                    date = date.split(':')[0]
                    new_date = parse(date)
                    return new_date
            else:
                return np.nan
        # exception added for this type of dates in study B9991032, "31JUL19:00:00:00"
        except ValueError as e:
            return np.nan
    else:
        return date

def format_datetime(timestamp, fmt="%d-%b-%Y", report=True):
    if isinstance(timestamp, pd.Timestamp):
        dt_obj = timestamp.to_pydatetime()
    elif isinstance(timestamp, datetime.date):
        dt_obj = timestamp
    elif isinstance(timestamp, np.datetime64):
        dt_obj = pd.Timestamp(timestamp).to_pydatetime()
    elif isinstance(timestamp, (int, str, np.int64)):
        dt_obj = pd.to_datetime(timestamp)#datetime.datetime.utcfromtimestamp(float(timestamp)/1000.) #datetime.datetime.fromtimestamp(19800801)
    str_ts = datetime.datetime.strftime(dt_obj, fmt)
    
    if (report==False):
        date_ts = datetime.datetime.strptime(str_ts, fmt).date() 
        return date_ts
    return str_ts
    
def normalise_df(df, date_col=None, **kwargs):
    if (isinstance(df, pd.DataFrame)):
        df.columns = list(map(str.upper, df.columns))
        if date_col:
            for col in date_col:
                df[col] = df[col].apply(get_date)
    else:
        raise TypeError('Input type is not a DataFrame')
    return df
def extractor(record, ae_records, cm_records, question1, question2):
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
#                                     valid_ind.add(pivot_form_ind)
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
#                                 valid_ind.add(pivot_form_ind)
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
            
def check_ecadj(primary_ec: PandasDataFrame, rule_flag: str) -> bool:
    """
    desc    :   Check for the ecadj value for the given primary ec record, \
                works for different conditions based on the rule_flag parameter.
                if rule_flag is 'ae' this checks if the ecadj equals adverse \
                events, else if rule_flag is 'other' checks if ecadj equals \
                others
    input   :   primary_ec: primary ec_record
                rule_flag: flag to switch between adverse events and others \
                condition
    output  :   Bool: True if the condition is met else False
    """
    
    ecadj = primary_ec['ECADJ'].values[0].strip()
    
    if rule_flag == 'ae':
        if (ecadj == 'ADVERSE EVENT(S)'):
            return True
        return False
    elif rule_flag == 'other':
        if (ecadj == 'OTHER'):
            return True
        return False
    
    

def get_aeacn(primary_ec: PandasDataFrame, corres_ae_rec: PandasDataFrame, subcate_last_actions: list) -> Tuple[bool, Tuple]:
    """
    desc    :   Get aeacn based on ectrt with the predefined mapping(study \
                specific) if the study is multidrug study or just will look for 
                the default field aeacn. This method is made to handle not in \
                checks too, in that case the first elem in subcate_last_actions
                is 'reverse'
    input   :   primary_ec: primary ec_record
                corres_ae_rec: corresponding ae_record to the primary ec record
                subcate_last_actions: list of last actions that are \
                allowed/not-allowed for a sub-category
    output  :   Bool: whether if the allowed/not-allowed last action is matched
                tuple: if the above bool value is true, then tuple will be name\
                 of the column which matched subcate_last_actions
                and the actual value in the column. if not the bool flag then \
                it returns None
    """

    doesnt_equal_flag = subcate_last_actions[0] == 'notequals'
    subjectectrt = primary_ec['ECTRT'].values[0]
    if not isinstance(subjectectrt, str):
        raise TypeError('input ectrt type should be str')
    
    bool_values_ae_ec = corres_ae_rec.isin([subjectectrt])
    collect_dict = dict()
    if (bool_values_ae_ec.values).any():
        ae_ec_match_col = bool_values_ae_ec.idxmax(1).values[0]
        related_col = aerel_map_dict[ae_ec_match_col]
        try:
            related_condition = corres_ae_rec[related_col].values[0].strip()
        except AttributeError as ae:
            print(f"SUBJECTID is {primary_ec.SUBJECTID.values[0]}, SUBJID {primary_ec.SUBJID.values[0]} and FORMINDEX is {primary_ec.FORMINDEX.values[0]}")
        if (related_condition == 'RELATED'):
            last_action_col = aeacn_map_dict[ae_ec_match_col][0]
            last_action_taken = corres_ae_rec[last_action_col].values[0]
        elif (related_condition == 'NOT RELATED'):
            last_action_col = aeacn_map_dict[ae_ec_match_col][1]
            last_action_taken = corres_ae_rec[last_action_col].values[0]
        
        if (doesnt_equal_flag):
            if (last_action_taken not in subcate_last_actions[1:]):
                return True, (last_action_col, last_action_taken)
            return False, (last_action_col, last_action_taken)
        else:
            if (last_action_taken in subcate_last_actions[1:]):
                return True, (last_action_col, last_action_taken)
            return False, (last_action_col, last_action_taken)
    else:
        col = 'AEACN'
        last_action_taken = corres_ae_rec[col].values[0]
        if (doesnt_equal_flag):
            if (last_action_taken not in subcate_last_actions[1:]):
                return True, (col, last_action_taken)
            return False, (col, last_action_taken)
        else:
            if (last_action_taken in subcate_last_actions):
                return True, (col, last_action_taken)
            return False, (col, last_action_taken)
    # return False, None
    
def get_dose_values(new_ec: PandasDataFrame, prim_formindex: int, \
    prim_ecstdat: DateTime, prim_ecendat: DateTime) -> Tuple[bool, Tuple]:
    
    """
    desc    :   Fetches the current dose and previous dose values for the given\
                sorted ec_records.
    input   :   new_ec: ec_records for the study
                prim_formindex: formindex of the primary ec record
                prim_ecstdat: ecstart date of the primary ec record
                prim_ecendat: ecend date of the primary ec record
    output  :   Bool: boolean flag if the dose valuse found
                Tuple:
                    current_ec_dose - current ec dose value
                    prev_ec_dose - previous ec dose value
    """

    current_ec_dose_rec = new_ec[(new_ec['FORMINDEX'] == prim_formindex) & \
                                         (new_ec['ECSTDAT'] == prim_ecstdat) & \
                                         (new_ec['ECENDAT'] == prim_ecendat)]
    
    if (len(current_ec_dose_rec)):
        current_ec_dose = current_ec_dose_rec['ECDOSTOT'].values[0]
        current_ec_dose_idx = current_ec_dose_rec.index.tolist()[0]
        new_ec_index = new_ec.index.tolist()
        if not (current_ec_dose_idx == new_ec_index[-1]):
            prev_index = new_ec_index[new_ec_index.index(current_ec_dose_idx)-1]
            next_index = new_ec_index[new_ec_index.index(current_ec_dose_idx)+1]
            prev_ec_dose = new_ec.loc[[prev_index]]['ECDOSTOT'].values[0]
            return True, (current_ec_dose, prev_ec_dose)
        elif (current_ec_dose_idx == new_ec_index[-1]):
            prev_index = new_ec_index[new_ec_index.index(current_ec_dose_idx)-1]
            prev_ec_dose = new_ec.loc[[prev_index]]['ECDOSTOT'].values[0]
            return True, (current_ec_dose, prev_ec_dose)
    return False, None

def sort_aerecords(ec_record, ae_record, ae_records):
    ae_record = ae_record.copy()
    ae_records = ae_records.copy()
    ec_record = ec_record.copy()
    ae_ids = []
    new_ae_records = pd.DataFrame()
    new_ae_records = new_ae_records.append(ae_record, ignore_index=True)
    subjectid = ae_record['SUBJECTID'].values[0]
    aespid = ae_record['AESPID'].astype(str).values[0]
    aespid = utils.id_handler(aespid)
    ae_records = ae_records[ae_records['SUBJECTID'] == subjectid]
    ec_record['ECAENO'] = ec_record['ECAENO'].astype(str).apply(utils.id_handler)
    ecaeno = ec_record['ECAENO'].values[0]
    for j in aespid:
        if type(ecaeno) != str:
            if j in ecaeno:
                if len(ecaeno) > 1:
                    pass
                for k in ecaeno:
                    if k not in aespid and k not in ae_ids:
                        ae_ids.append(k)
    for ae_id in ae_ids:
        temp = ae_records[ae_records['AESPID'] == ae_id]
        new_ae_records = new_ae_records.append(temp, ignore_index = True)
    
#     new_ae_records['AESTDAT'] = new_ae_records['AESTDAT'].apply(get_date)
#     new_ae_records['AEENDAT'] = new_ae_records['AEENDAT'].apply(get_date)
    new_ae_records = new_ae_records.sort_values(['AESTDAT', 'AEENDAT'])
    return new_ae_records

def get_aedates(ec_ectrt, pivot_ae, study):
    for ae_row_df in pivot_ae.itertuples():
        for aetrt_col in aetrt_list:
            try:
                aetrt = ae_record[dct[col]].values[0]
            except:
                if study.upper() in ['B7451015', 'B7931023', 'B7981015', 'C3671003', 'C1061011']:
                    aetrt = 'BLINDED THERAPY'
                elif study.upper() in ['B7461024']:
                    aetrt = 'LORLATINIB'
                    
#             if (ec_ectrt == getattr(ae_row_df, aetrt_col)):
                aest_dat = getattr(ae_row_df, 'AESTDAT').to_datetime64()
                aest_dat = get_date(getattr(ae_row_df, 'AESTDAT'))
                aend_dat = getattr(ae_row_df, 'AEENDAT')
                if not isinstance(aend_dat, (float, type(np.datetime64('NaT')))):
#                     aend_dat = aend_dat.to_datetime64()
                    aend_dat = get_date(aend_dat)
                form_index_temp = getattr(ae_row_df, 'FORM_IX')
        break
    return aest_dat, aend_dat, form_index_temp

def get_ecrecords(primary_ec: PandasDataFrame, \
                 corres_ae_rec: Optional[PandasDataFrame], \
                 all_ec_rec: PandasDataFrame, \
                 all_ae_rec: Optional[PandasDataFrame],
                 study: str) \
                     -> Tuple[PandasDataFrame, DateTime, DateTime]:
    """
    desc:   This is the cornerstone function which handles ectrt wise filtering \
            between studies(filters if there is more than 2 drugs) or just \
            returns the ec_records if the study just has one drug. 
            Not implemented for study with one drug and is single-blinded
    input:  primary_ec: primary ec_record
            corres_ae_rec: corresponding ae_record to the primary ec record
            all_ec_rec: all ec records dataframe
            all_ae_rec: all ae records dataframe
    output: Tuple
                new_ec_records - filtered if multidrug study else not filtered
                aest_dat - corresponding aest_dat
                aend_dat - corresponding aend_dat
    """
#     subjectid = primary_ec['SUBJECTID'].values[0]
#     subjectectrt = all_ec_rec['ECTRT'].values[0]
    
#     list_of_drugs = all_ec_rec['ECTRT'].unique().tolist()
#     list_of_drugs = [drug for drug in list_of_drugs if str(drug) != 'nan']
#     no_of_drugs = len(list_of_drugs)
#     print('no_of_drugs ', no_of_drugs)
#     # no blinded therapy, just one single drug
#     if (no_of_drugs == 1):
#         new_ec = all_ec_rec[(all_ec_rec['SUBJECTID'] == subjectid)]#, no_of_drugs#1
#         new_ec['ECSTDAT'] = new_ec['ECSTDAT'].apply(get_date)
#         new_ec['ECENDAT'] = new_ec['ECENDAT'].apply(get_date)
#         new_ec = new_ec.sort_values(['ECSTDAT', 'ECENDAT'])    
#         if (corres_ae_rec is not None) and (all_ae_rec is not None):
#             aest_dat = corres_ae_rec['AESTDAT'].apply(get_date)
#             aend_dat = corres_ae_rec['AESTDAT'].apply(get_date)
#             return new_ec, aest_dat, aend_dat
#         return new_ec
#     # blinded therapy along with one or more other drugs
#     elif ('BLINDED THERAPY' in list_of_drugs) and (no_of_drugs > 2):
#         new_ec = all_ec_rec[(all_ec_rec['SUBJECTID'] == subjectid) & (all_ec_rec['ECTRT'] == subjectectrt)]#, no_of_drugs#>2
#         new_ec['ECSTDAT'] = new_ec['ECSTDAT'].apply(get_date)
#         new_ec['ECENDAT'] = new_ec['ECENDAT'].apply(get_date)
#         new_ec = new_ec.sort_values(['ECSTDAT', 'ECENDAT'])
#         if (corres_ae_rec is not None) and (all_ae_rec is not None):
#             ae_records = sort_aerecords(primary_ec, corres_ae_rec, all_ae_rec)
#             aest_dat, aend_dat, _ = get_aedates(subjectectrt, corres_ae_rec)
#             return new_ec, aest_dat, aend_dat
#         return new_ec
#     # blinded therapy along with just one drug
#     elif ('BLINDED THERAPY' in list_of_drugs) and (no_of_drugs == 2):
#         raise NotImplementedError('Not implemented for single drug and blinded therapy')#2
        
    subjectid = primary_ec['SUBJECTID'].values[0]
    subjectectrt = all_ec_rec['ECTRT'].values[0]
    
    if (subjectectrt != np.nan) and (subjectectrt != None):
        new_ec = all_ec_rec[(all_ec_rec['SUBJECTID'] == subjectid) & (all_ec_rec['ECTRT'] == subjectectrt)]
    else:
        new_ec = all_ec_rec[(all_ec_rec['SUBJECTID'] == subjectid)]
    new_ec['ECSTDAT'] = new_ec['ECSTDAT'].apply(get_date)
    new_ec['ECENDAT'] = new_ec['ECENDAT'].apply(get_date)
    new_ec = new_ec.sort_values(['ECSTDAT', 'ECENDAT'])
    if (corres_ae_rec is not None) and (all_ae_rec is not None):
        ae_records = sort_aerecords(primary_ec, corres_ae_rec, all_ae_rec)
        aest_dat, aend_dat, _ = get_aedates(subjectectrt, corres_ae_rec, study)
        return new_ec, aest_dat, aend_dat
    return new_ec


def get_deeplink(study_id, rec, domain='EC'):
    rec_columns = rec.columns.tolist()
    subj_id_col = [col for col in rec_columns if col.endswith('SUBJID')][0]
    siteno = [col for col in rec_columns if col.endswith('SITEID')][0]
    visit_id = [col for col in rec_columns if col.endswith('VISITID')][0]
    visit_ix = [col for col in rec_columns if col.endswith('VISITINDEX')][0]
    form_id = [col for col in rec_columns if col.endswith('FORMID')][0]
    if domain == 'EC':
        form_index = [col for col in rec_columns if col.endswith('ITEMSETIDX')][0]
    else:
        form_index = [col for col in rec_columns if col.endswith('FORMINDEX')][0]
    
    subjid = rec[subj_id_col].values[0]
    siteno = rec[siteno].values[0]
    visit_id = rec[visit_id].values[0]
    visit_ix = rec[visit_ix].values[0]
    form_id = rec[form_id].values[0]
    form_index = '1' if domain == 'EC' else str(int(rec[form_index].values[0]))
    deeplink = f"https://pfizer-inform.oracleindustry.com/{study_id}/pfts.dll?S=2313B74C678711d29DAD00A0C9769FD8&C=TM_29&SI={siteno}&IP={subjid};{visit_id}!{visit_ix};{form_id}!{form_index}"
    return deeplink 