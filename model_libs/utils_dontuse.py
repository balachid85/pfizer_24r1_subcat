'''
Description: One of the common utils file for all the subcat files
Change log:
[EDIT #1] - 16May22 - Changing the UNK logic as per the new requirement - Adheeban
[EDIT #2] - 17May22 - Adding AE start date special case handling to UNk logic - Adheeban
[EDIT #3] - 28JUNE22 - Adding fuzzywuzzy logic for the OV subcats- Dhilip
[EDIT #4] - 08Jul22 - Editing UNK logic to handle AEENDAT also. Added handling for parser error
'''

from dateutil.parser import parse
import numpy as np
import datetime
from datetime import date
import re
import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

null_list = ['null',np.nan,'NULL',None,'nan'] 

yes_list = ['YES','Y','yes','y']
no_list = ['NO','N','no','n']

aeacn_list = ['AEACN', 'AEACN1', 'AEACN2', 'AEACN3', 'AEACN4'] 
aerel_list = ['AEREL', 'AEREL1', 'AEREL2', 'AEREL3', 'AEREL4']
misdost_list = [ 'MISDOST', 'MISDOST_C', 'MISDOST_ND']
ecadj_list = [ 'ECADJ', 'MISDOST']
ecadj_list = [ 'ECADJ', 'MISDOST']

aeacn_dict = {'AEACN1' : 'AETRT1', 'AEACN2' : 'AETRT2', 'AEACN3' : 'AETRT3', 'AEACN4' : 'AETRT4'}
aerel_dict = {'AEREL1' : 'AETRT1', 'AEREL2' : 'AETRT2', 'AEREL3' : 'AETRT3', 'AEREL4' : 'AETRT4'}

#Loading the study deeplink map_dict
study_deeplink = pd.read_csv('study_deeplink.csv')
study_deeplink_dict = {study: deeplink for study, deeplink in zip(study_deeplink.study_id, study_deeplink.deeplink)}

def impute_unk(date_dict:dict,proper=False):
    '''
    imputes both date and month if UNK is present in both fields, imputes only date if UNK is only present in date field.
    If there is no UNK present in the given dictionary in all the fields, the parsed date without imputing will be returned.
    '''
    
    def date_seperator_handler(date):
        date_and_time = re.split(' |T', date)
        if len(date_and_time) > 1:
            date_and_time[1] = date_and_time[1].replace('UNK','00')
        date = ' '.join(date_and_time)
        date = date.replace('NUL','00')
        date = re.sub('-$|/$|:$|^-|^/|^:', '', date)
        return date
    
    # Default date values to be imputed for the respective fields
    if proper:
        imp_default_date = {'cmstdat':2, 'aestdat':1, 'aeendat':4, 'cmendat': 3}
    elif not proper:
        imp_default_date = {'cmstdat':2, 'aestdat':3, 'aeendat':1, 'cmendat':4}
        
    month_flag, date_flag = False, False
        
    # Checking whether UNK in present in either date or month field or both
    unk_flag = False
    for date_col in date_dict.keys():
        date_value = split_date(date_dict[date_col])
        date_list = re.split('-|/', date_value)
        date_list = [i.upper() for i in date_list]
        unk_count = [i for i in date_list if 'UNK' in i.upper()]
        if 'UNK' in date_list:
            unk_flag = True
            date_flag = True
            # [EDIT #1]
            if len(unk_count) == 2:
                month_flag = True
            elif len(unk_count) > 2:
                return unk_flag,False,{}

    # If UNK is not present, parsed date to be returned without imputation
    if not unk_flag:
        parsed_date = {date_col:parse(date_seperator_handler(date_val)) for date_col,date_val in date_dict.items()}
        return unk_flag,True,parsed_date
          
    # [EDIT #2]
    # [EDIT #4]
    # Checking if the the day of AE start date or the AE end date is the first day of the month
    first_day_flag = False
    day = None
    if 'aestdat' in date_dict.keys(): 
        day = date_dict['aestdat'].upper()
    elif 'aeendat' in date_dict.keys():
        day = date_dict['aeendat'].upper()
    if day:
        day = date_seperator_handler(day.replace('UNK',''))
        day = parse(day).day
        if date.today().day != day and day == 1:
            first_day_flag = True
            
    imputed_date = {}
            
    # If UNK is present in either of the fields, it will get imputed
    if month_flag or date_flag:
        for date_col in date_dict.keys():
            date_val = date_dict.get(date_col)
            if date_flag:
                if isinstance(date_val, str):
                    date_val = date_val.upper()
                    date_val = date_seperator_handler(date_val.replace('UNK',''))
                    date_val = parse(date_val)
                    # If first_day_flag is true, CMSTDAT, AESTDAT, AEENDAT days are being imputed to 1. Else normal imputation is being done
                    if first_day_flag:
                        if date_col in ['aestdat','aeendat','cmstdat']:
                            date_val = date_val.replace(day=1)
                        else:
                            date_val = date_val.replace(day=imp_default_date[date_col])
                    else:
                        date_val = date_val.replace(day=imp_default_date[date_col])
                    if month_flag:
                        date_val = date_val.replace(month=1)

            imputed_date[date_col] = date_val

        return unk_flag,True,imputed_date
    
def check_similar_term_fuzzy(medication_1, medication_2):
    words=[ 'right lower','right upper','left lower','left upper','right', 'left', 'upper', 'lower', 'middle', 'front', 'back', 'anterior', 'posterior', 'superior', 'inferior']
    # medication_1 = re.sub(r'[\W_]+', '', medication_1)
    # medication_2 = re.sub(r'[\W_]+', '', medication_2)
    # print(medication_2,medication_1)
#     print(fuzz.WRatio(medication_1,medication_2))
    if fuzz.WRatio(medication_1,medication_2)>= 90:
#         print(fuzz.WRatio(medication_1,medication_2))
        for word in words:
            try:
                if any(re.search(word1,medication_1,re.IGNORECASE) for word1 in words) or  any(re.search(word1,medication_1,re.IGNORECASE) for word1 in words):
                    if (re.search(word,medication_1,re.IGNORECASE)) :
                        if  (re.search(word,medication_2,re.IGNORECASE)):
#                             print(word)
                            return True
                        else:
                            return False
                else:
                    return True
            except:
                return False
#                 print(medication_1,medication_2)
#                 import pdb;pdb.set_trace()
            
    return False

def format_datetime(timestamp, fmt="%d-%b-%Y", report=True):
    dt_obj = None
    if timestamp in null_list:
        return 'null'
    elif pd.notnull(timestamp):
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
    else:
        return 'null'
    
def unk_format_datetime(unk_flag, ae_df, cm_df):
    
    if unk_flag:
        cm_df['CMSTDAT'] = cm_df['CMSTDAT_DTR']
        cm_df['CMENDAT'] = cm_df['CMENDAT_DTR']
        
        if 'AESTDAT_DTR' in ae_df.columns: 
            ae_df['AESTDAT'] = ae_df['AESTDAT_DTR']
        else:
            ae_df['AESTDAT'] = ae_df['AESTDAT'].apply(format_datetime)
            
        if 'AEENDAT_DTR' in ae_df.columns: 
            ae_df['AEENDAT'] = ae_df['AEENDAT_DTR']
        else:
            ae_df['AEENDAT'] = ae_df['AEENDAT'].apply(format_datetime)

    else:
        cm_df['CMSTDAT'] = cm_df['CMSTDAT'].apply(format_datetime)
        try:
            cm_df['CMENDAT'] = cm_df['CMENDAT'].apply(format_datetime)
        except:
        ae_df['AESTDAT'] = ae_df['AESTDAT'].apply(format_datetime)
        ae_df['AEENDAT'] = ae_df['AEENDAT'].apply(format_datetime)
        
    return ae_df, cm_df
                                                    
def get_date(x):
    if isinstance(x, str):
        try:
            return_val = parse(x)
        except:
            try:
                return_val = datetime.datetime.strptime(x, '%d%b%Y:%H:%M:%S')
            except:
                #return_val = datetime.datetime.strptime('01JAN1900:12:40:25', '%d%b%Y:%H:%M:%S') 
                return_val = np.nan
    else:
        
        #return_val = datetime.datetime.strptime('01JAN1900:12:40:25', '%d%b%Y:%H:%M:%S') 
        return_val = np.nan
    return return_val
def remove_unk(x1, combine = False):
        if '/' in x1:
            a = str(x1).split('/')
        else:
            a = str(x1).split('-')
        a = [i for i in a if i != 'UNK']
        a = sorted(a, key = len)
        if combine:
            a = '/'.join(a)
        return a
def remove_unk_cmov(x1, combine = False):
        if '/' in x1:
            a = str(x1).split('/')
        else:
            a = str(x1).split('-')
        a = [i for i in a if i != 'UNK']
        a = sorted(a, key = len)
        if combine:
            a = '/'.join(a)
        try:
            a[0]=pd.to_datetime(a[0],format='%b').month
        except :
            try:
                a[0]=pd.to_datetime(a[0],format='%B').month
            except  :
                a=a
        return a

def check_similar_medication_verabtim2(medication_1, medication_2):
    medication_1 = str(medication_1)
    medication_2 = str(medication_2)
    
    remove_list = ['null', 'NULL', '', ' ', '  ', 'NAN', 'nan']
    if medication_1 in remove_list or medication_2 in remove_list:
        return False
    
    medication_1 = re.sub(r'[\W_]+', '', medication_1)
    medication_2 = re.sub(r'[\W_]+', '', medication_2)
    if medication_1.upper() == medication_2.upper():
        return True
    return False

def get_cm_hierarchy(ae_record, ae_records, cm_record, key):
    # import pdb; pdb.set_trace()
    ae_record = ae_record.copy()
    ae_records = ae_records.copy()
    cm_record = cm_record.copy()
    # get_date = lambda x : parse(str(x)) if isinstance(x, str) else np.nan    
    ae_ids = []
    new_ae_records = pd.DataFrame()
    new_ae_records = new_ae_records.append(ae_record, ignore_index=True)
    #subjectid = ae_record['SUBJECTID'].values[0]
    #print(f"SUBJECTID - {subjectid}")
    aespid = ae_record['AESPID'].astype(str).values[0]
    aespid = id_handler(aespid)
    #ae_records = ae_records[ae_records['SUBJECTID'] == subjectid]
    cm_record['CMAENO'] = cm_record['CMAENO'].astype(str).apply(id_handler)
    #print(f"CM RECORD - {cm_record.shape}")
    #print(f"AESPID - {aespid}")
    #print(f"All CMAENO - {cm_record['CMAENO']}")
    cmaeno = cm_record['CMAENO'].values[0]
    #print(f"CMAENO - {cmaeno}")        
    for j in aespid:
        if type(cmaeno) != str:
            if j in cmaeno:
                #print(f"{j} - {cmaeno} Present...")
                if len(cmaeno) > 1:
                    pass
                    #print(f"{j} - {cmaeno} More than 1 Present...")
                for k in cmaeno:
                    if k not in aespid and k not in ae_ids:
                        ae_ids.append(k)
    #print(f"NEW CM RECORDS SHAPE - {cm_record.shape}")
    #print(f"MISSING AEID'S - {ae_ids}")
    ae_ids = [str(i) for i in ae_ids]
    for ae_id in ae_ids:
        temp = ae_records[ae_records['AESPID'] == ae_id]
        #print(f"TEMP SHAPE - {temp.shape}")
        new_ae_records = new_ae_records.append(temp, ignore_index = True)
        
    """if key == 'AEENDAT' and len(new_ae_records) > 1:
        new_ae_records['AEONGO'] = new_ae_records['AEONGO'].astype(str)
        aeongos = new_ae_records['AEONGO'].unique().tolist()
        aeongos = [i.upper() for i in aeongos]
        if 'YES' in aeongos:
            return 0, 0"""
        
    new_ae_records[key] = new_ae_records[key].apply(get_date)
    new_ae_records = new_ae_records.sort_values(key)
    
    #aeptcd_len = len(new_ae_records['AEPTCD'].unique().tolist())
    #aetoxgr_len = len(new_ae_records['AETOXGR'].unique().tolist())
    #print(f"AEPTCD LENGTH - {aeptcd_len}")
    #print(f"AETOXGR LENGTH - {aetoxgr_len}")
    #print(f"AE RECORDS LENGTH - {new_ae_records.shape}")
    if len(new_ae_records) > 0:#if aeptcd_len == 1 and aetoxgr_len == len(new_ae_records):
        #new_ae_records = new_ae_records.head(1)
        return new_ae_records, cm_record
    else:
        return 0, 0

def get_ae_hierarchy(ae_records, key):
    ae_records = ae_records.copy()
    # get_date = lambda x : parse(str(x))    
    new_ae_records = pd.DataFrame()
    for record in ae_records:
        new_ae_records = new_ae_records.append(record, ignore_index=True)
        
    """if key == 'AEENDAT' and len(new_ae_records) > 1:
        new_ae_records['AEONGO'] = new_ae_records['AEONGO'].astype(str)
        aeongos = new_ae_records['AEONGO'].unique().tolist()
        aeongos = [i.upper() for i in aeongos]
        if 'YES' in aeongos:
            return 0"""
        
    new_ae_records[key] = new_ae_records[key].apply(get_date)
    new_ae_records = new_ae_records.sort_values(key)
    #aeptcd_len = len(new_ae_records['AEPTCD'].unique().tolist())
    #aetoxgr_len = len(new_ae_records['AETOXGR'].unique().tolist())
    #print(f"AEPTCD LENGTH - {aeptcd_len}")
    #print(f"AETOXGR LENGTH - {aetoxgr_len}")
    #print(f"AE RECORDS LENGTH - {len(new_ae_records)}")
    #print(new_ae_records[['AESTDAT', 'AETOXGR', 'AEPTCD', 'AETERM']])
    if len(new_ae_records) > 0:#aeptcd_len == 1 and aetoxgr_len == len(new_ae_records):
        return new_ae_records
    else:
        return 0 

def get_ec_ae_hierarchy(ae_df, record):
    ecaeno = record['ECAENO'].values[0]
    ecaeno = [str(i) for i in ecaeno]
    new_ae = ae_df
    new_ae = new_ae[new_ae['AESPID'].isin(ecaeno)]
    new_ae['AESTDAT'] = new_ae['AESTDAT'].apply(split_date)
    new_ae['AEENDAT'] = new_ae['AEENDAT'].apply(split_date)
    new_ae['AESTDAT'] = pd.to_datetime(new_ae['AESTDAT'], errors = 'coerce', infer_datetime_format=True)
    new_ae['AEENDAT'] = pd.to_datetime(new_ae['AEENDAT'], errors = 'coerce', infer_datetime_format=True)
    new_ae['AESTDAT'] = new_ae['AESTDAT'].dt.normalize()
    new_ae['AEENDAT'] = new_ae['AEENDAT'].dt.normalize()
    min_aestdat = new_ae['AESTDAT'].min()
    max_aeendat = new_ae['AEENDAT'].max()
    
    min_aestdat_record = new_ae[new_ae['AESTDAT'] == min_aestdat]
    max_aeendat_record = new_ae[new_ae['AEENDAT'] == max_aeendat]
    
    return min_aestdat, max_aeendat, min_aestdat_record, max_aeendat_record

def split_date(x):
    x = str(x)
    if ' ' in x:
        x = x.split()[0]
    elif 'T' in x:
        x = x.split('T')[0]
    elif ':' in x:
        x = x.split(':')[0]
    return x


def impute_unk(date_dict:dict,proper=False):
    '''
    imputes both date and month if UNK is present in both fields, imputes only date if UNK is only present in date field.
    If there is no UNK present in the given dictionary in all the fields, the parsed date without imputing will be returned.
    '''
    
    def date_seperator_handler(date):
        date_and_time = re.split(' |T', date)
        if len(date_and_time) > 1:
            date_and_time[1] = date_and_time[1].replace('UNK','00')
        date = ' '.join(date_and_time)
        date = date.replace('NUL','00')
        date = re.sub('-$|/$|:$|^-|^/|^:', '', date)
        return date
    
    # Default date values to be imputed for the respective fields
    if proper:
        imp_default_date = {'cmstdat':2, 'aestdat':1, 'aeendat':4, 'cmendat': 3}
    elif not proper:
        imp_default_date = {'cmstdat':2, 'aestdat':3, 'aeendat':1, 'cmendat':4}
        
    month_flag, date_flag = False, False
        
    # Checking whether UNK in present in either date or month field or both
    unk_flag = False
    for date_col in date_dict.keys():
        date_value = split_date(date_dict[date_col])
        date_list = re.split('-|/', date_value)
        date_list = [i.upper() for i in date_list]
        unk_count = [i for i in date_list if 'UNK' in i.upper()]
        if 'UNK' in date_list:
            unk_flag = True
            date_flag = True
            # [EDIT #1]
            if len(unk_count) == 2:
                month_flag = True
            elif len(unk_count) > 2:
                return unk_flag,False,{}

    # If UNK is not present, parsed date to be returned without imputation
    if not unk_flag:
        parsed_date = {date_col:parse(date_seperator_handler(date_val)) for date_col,date_val in date_dict.items()}
        return unk_flag,True,parsed_date
          
    # [EDIT #2]
    # [EDIT #4]
    # Checking if the the day of AE start date or the AE end date is the first day of the month
    first_day_flag = False
    day = None
    if 'aestdat' in date_dict.keys(): 
        day = date_dict['aestdat'].upper()
    elif 'aeendat' in date_dict.keys():
        day = date_dict['aeendat'].upper()
    if day:
        day = date_seperator_handler(day.replace('UNK',''))
        day = parse(day).day
        if date.today().day != day and day == 1:
            first_day_flag = True
            
    imputed_date = {}
            
    # If UNK is present in either of the fields, it will get imputed
    if month_flag or date_flag:
        for date_col in date_dict.keys():
            date_val = date_dict.get(date_col)
            if date_flag:
                if isinstance(date_val, str):
                    date_val = date_val.upper()
                    date_val = date_seperator_handler(date_val.replace('UNK',''))
                    date_val = parse(date_val)
                    # If first_day_flag is true, CMSTDAT, AESTDAT, AEENDAT days are being imputed to 1. Else normal imputation is being done
                    if first_day_flag:
                        if date_col in ['aestdat','aeendat','cmstdat']:
                            date_val = date_val.replace(day=1)
                        else:
                            date_val = date_val.replace(day=imp_default_date[date_col])
                    else:
                        date_val = date_val.replace(day=imp_default_date[date_col])
                    if month_flag:
                        date_val = date_val.replace(month=1)

            imputed_date[date_col] = date_val

        return unk_flag,True,imputed_date
        
        
def get_deeplink(study_id, rec):
    rec_columns = rec.columns.tolist()
    deeplink = study_deeplink_dict[study_id.upper()]
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

        param_dict = {'subjid': subjid,
                     'siteno':siteno,
                     'visit_id': visit_id,
                     'visit_ix': visit_ix,
                      'form_id' : form_id,
                     'form_index': form_index}
        deeplink = deeplink.format(**param_dict)
        return deeplink

def get_aereason_col(ae_record, ae_df, ec_records):
    ec_cols = ec_records.columns.tolist()
    ecadj_flag = False
    misdost_flag = False
    
    if 'ECADJ' in ec_cols and 'MISDOST' in ec_cols:
        ecadj_unique = ec_records['ECADJ'].unique().tolist()
        misdost_unique = ec_records['MISDOST'].unique().tolist()
        if len(ecadj_unique) > len(misdost_unique):
            ecadj_flag = True
        elif len(misdost_unique) > len(ecadj_unique):
            formind = ae_record['form_index'].values[0]
            result = extractor(ae_record, ae_df, ec_records, 'AESPID', 'ECAENO')
            if (type(result) == tuple) & (type(result) != bool):
                if len(result[0]) == 0:
                    misdost_flag = False
            else:
                misdost_flag = True
                         
    elif 'ECADJ' in ec_cols:
        ecadj_flag = True
            
    elif 'MISDOST' in ec_cols:
        formind = ae_record['form_index'].values[0]
        result =extractor(ae_record, ae_df, ec_records, 'AESPID', 'ECAENO')
        if (type(result) == tuple) & (type(result) != bool):
            if len(result[0]) == 0:
                misdost_flag = False
        else:
            misdost_flag = True
                             
    return ecadj_flag, misdost_flag    

def get_dose_column(ec_records):
    ec_cols = ec_records.columns.tolist()
    ecdose_list = [ 'ECDOSE', 'ECDOSTOT']
    
    if 'ECDOSE' in ec_cols and 'ECDOSTOT' in ec_cols:
        ecdose_unique = ec_records['ECDOSE'].unique().tolist()
        ecdostot_unique = ec_records['ECDOSTOT'].unique().tolist()
        if len(ecdose_unique) > len(ecdostot_unique):
            return 'ECDOSE'
        else:
            return 'ECDOSTOT'
    elif 'ECDOSE' in ec_cols:
        return 'ECDOSE'
    elif 'ECDOSTOT' in ec_cols:
        return 'ECDOSTOT'
    
def get_ec_hierarchy(ae_record, ec_rec, values, subcat, study, aetrt_dict):
    d = {}
    ec_records = {}
    subjectid = ae_record['SUBJECTID'].values[0]
    ec_rec = ec_rec[ec_rec['SUBJECTID'] == subjectid]

    if subcat == 'AEDR10':
        dct = aerel_dict
        lst = aerel_list
        aetrt_dict = {key.replace('AEACN','AEREL') : val for key,val in aetrt_dict.items()}
    else:
        dct = aeacn_dict
        lst = aeacn_list
        
    for col in ae_record.columns.tolist():
        #print(f"{col}")
        if col in lst:
            #print(f"{col}")
            aeacn = ae_record[col].values[0]
            aetrt = aetrt_dict[col]
            
            for val in values:
                if aeacn == val:
                    d[aetrt] = aeacn
    #print(d.keys())
    for key, val in d.items():
        new_ec = ec_rec[ec_rec['ECTRT'] == key]
        ec_records[key] = new_ec
        
    return ec_records

def extractor(record, ae_records, cm_records, question1, question2):
    # import pdb; pdb.set_trace()
    pivot_form_ind = record['form_index'].values[0]
        
    k = {}
    pivot_ae_subjid = 0
    pivot_ae_subjid = record['subjid'].values[0]
    if 1:        
        pivot_ae_records = ae_records[ae_records['form_index'] == pivot_form_ind]
        ae_id = pivot_ae_records[question1].astype(str).values
        aeid = pivot_ae_records[question1].values[0]
        all_records = ae_records[(ae_records['subjid'] == pivot_ae_subjid) & (ae_records[question1] == aeid)]
        ae_id1 = ae_id
        #print('ae_id', ae_id)
        ae_flag = False
        # Checking ae_is shape is 1 and it's not 'nan' value
        if len(ae_id) >= 1 and pd.isnull(ae_id.any()) == False:
            ae_ids = id_handler(ae_id[0])
            if ae_ids == 'Other Category':
                if question1 == 'AESPID':                    
                    ae_id = ae_records['form_ix'].values[0]
                    ae_ids = id_handler(ae_id)
                else:
                    return 'Other Category'

            ae_flag = True
            pivot_cm_records = 0
            
           
        if ae_flag == True:
            cm_records1 = cm_records[cm_records['subjid'] == pivot_ae_subjid]['form_index'].unique().tolist()
            cm_records1 = [i for i in cm_records1 if i not in [-1, '-1']]
            if len(cm_records1) >= 1 and question2 != 'ECAENO':
                for cm_form_ind in cm_records1:
                    if 1:  
                        #print('cm form index', cm_form_ind)
                        pivot_cm_records = cm_records[cm_records['form_index'] == cm_form_ind]
                        pivot_cm_subjid = pivot_cm_records['subjid'].values[0]
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
                pivot_cm_records = cm_records[cm_records['subjid'] == pivot_ae_subjid]
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

def get_drug_item(drug, subcat, study, aetrt_dict):
    new_aetrt_dict = {val:key for key, val in aetrt_dict.items()}    
    
    if subcat == 'AEDR10':
        new_aetrt_dict = {val:key.replace('AEACN', 'AEREL') for key, val in aetrt_dict.items()}
        piv_item_name = new_aetrt_dict[drug]
    else:
        piv_item_name = new_aetrt_dict[drug]
        
    return piv_item_name






#[EDIT #3]
def check_similar_term_fuzzy(medication_1, medication_2):
    words=[ 'right lower','right upper','left lower','left upper','right', 'left', 'upper', 'lower', 'middle', 'front', 'back', 'anterior', 'posterior', 'superior', 'inferior']
    # medication_1 = re.sub(r'[\W_]+', '', medication_1)
    # medication_2 = re.sub(r'[\W_]+', '', medication_2)
    # print(medication_2,medication_1)
#     print(fuzz.WRatio(medication_1,medication_2))
    if fuzz.WRatio(medication_1,medication_2)>= 90:
#         print(fuzz.WRatio(medication_1,medication_2))
        for word in words:
            try:
                if any(re.search(word1,medication_1,re.IGNORECASE) for word1 in words) or  any(re.search(word1,medication_1,re.IGNORECASE) for word1 in words):
                    if (re.search(word,medication_1,re.IGNORECASE)) :
                        if  (re.search(word,medication_2,re.IGNORECASE)):
#                             print(word)
                            return True
                        else:
                            return False
                else:
                    return True
            except:
                return False
#                 print(medication_1,medication_2)
#                 import pdb;pdb.set_trace()
            
    return False
#adding get date for cmov
def get_date1(date):
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