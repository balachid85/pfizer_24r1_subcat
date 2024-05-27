from dateutil.parser import parse
import numpy as np
import datetime
import re
import pandas as pd
from sqlalchemy import create_engine, text
aeacn_list = ['AEACN', 'AEACN1', 'AEACN2', 'AEACN3', 'AEACN4'] 
aerel_list = ['AEREL', 'AEREL1', 'AEREL2', 'AEREL3', 'AEREL4']
misdost_list = [ 'MISDOST', 'MISDOST_C', 'MISDOST_ND']
ecadj_list = [ 'ECADJ', 'MISDOST']
ecdose_list = [ 'ECDOSE', 'ECDOSTOT']

aeacn_dict = {'AEACN1' : 'AETRT1', 'AEACN2' : 'AETRT2', 'AEACN3' : 'AETRT3', 'AEACN4' : 'AETRT4'}
aerel_dict = {'AEREL1' : 'AETRT1', 'AEREL2' : 'AETRT2', 'AEREL3' : 'AETRT3', 'AEREL4' : 'AETRT4'}


def get_rule_name(rule_id, conn):
    rule_query = text(f"select * from sdq_common.ml_model where id= :s")
    map_rule_res = conn.execute(rule_query, s=rule_id)
    rule_details = [dict(row) for row in map_rule_res]
    return rule_details[0]['name']

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
        a = str(x1).split('/')
        a = [i for i in a if i != 'UNK']
        a = sorted(a, key = len)
        if combine:
            a = '/'.join(a)
        return a

def check_similar_medication_verabtim2(medication_1, medication_2):
    medication_1 = re.sub(r'[\W_]+', '', medication_1)
    medication_2 = re.sub(r'[\W_]+', '', medication_2)
    if medication_1.upper() == medication_2.upper():
        return True
    return False

def get_cm_hierarchy(ae_record, ae_records, cm_record):
    ae_record = ae_record.copy()
    ae_records = ae_records.copy()
    cm_record = cm_record.copy()
    get_date = lambda x : parse(str(x)) if isinstance(x, str) else np.nan    
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
    #print(f"All CMAENO - {cm_records['CMAENO']}")
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
    for ae_id in ae_ids:
        temp = ae_records[ae_records['AESPID'] == ae_id]
        new_ae_records = new_ae_records.append(temp, ignore_index = True)
    #print(f"NEW AE RECORDS SHAPE - {new_ae_records.shape}")
    new_ae_records['AESTDAT'] = new_ae_records['AESTDAT'].apply(get_date)
    new_ae_records = new_ae_records.sort_values('AESTDAT')
    aeptcd_len = len(new_ae_records['AEPTCD'].unique().tolist())
    aetoxgr_len = len(new_ae_records['AETOXGR'].unique().tolist())
    #print(f"AEPTCD LENGTH - {aeptcd_len}")
    #print(f"AETOXGR LENGTH - {aetoxgr_len}")
    #print(f"AE RECORDS LENGTH - {len(new_ae_records)}")
    if aeptcd_len == 1 and aetoxgr_len == len(new_ae_records):
        #new_ae_records = new_ae_records.head(1)
        return new_ae_records, cm_record
    else:
        return 0, 0

def get_ae_hierarchy(ae_records):
    ae_records = ae_records.copy()
    get_date = lambda x : parse(str(x))    
    new_ae_records = pd.DataFrame()
    for record in ae_records:
        new_ae_records = new_ae_records.append(record, ignore_index=True)
    new_ae_records['AESTDAT'] = new_ae_records['AESTDAT'].apply(get_date)
    new_ae_records = new_ae_records.sort_values('AESTDAT')
    aeptcd_len = len(new_ae_records['AEPTCD'].unique().tolist())
    aetoxgr_len = len(new_ae_records['AETOXGR'].unique().tolist())
    #print(f"AEPTCD LENGTH - {aeptcd_len}")
    #print(f"AETOXGR LENGTH - {aetoxgr_len}")
    #print(f"AE RECORDS LENGTH - {len(new_ae_records)}")
    #print(new_ae_records[['AESTDAT', 'AETOXGR', 'AEPTCD', 'AETERM']])
    if aeptcd_len == 1 and aetoxgr_len == len(new_ae_records):
        new_ae_records = new_ae_records.head(1)
        return new_ae_records
    else:
        return 0 


def get_ec_hierarchy(ae_record, ec_rec, values, subcat):
    d = {}
    ec_records = {}
    subjectid = ae_record['subjid'].values[0]
    ec_rec = ec_rec[ec_rec['subjid'] == subjectid]

    if subcat == 'AEDR10':
        dct = aerel_dict
        lst = aerel_list
    else:
        dct = aeacn_dict
        lst = aeacn_list
        
    for col in ae_record.columns.tolist():
        #print(f"{col}")
        if col in lst:
            #print(f"{col}")
            aeacn = ae_record[col].values[0]
            aetrt = ae_record[dct[col]].values[0]
            for val in values:
                if aeacn == val:
                    d[aetrt] = aeacn
    print(d.keys())
    for key, val in d.items():
        new_ec = ec_rec[ec_rec['ECTRT'] == key]
        ec_records[key] = new_ec
        
    return ec_records

def extractor(record, ae_records, cm_records, question1, question2):
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
            if len(cm_records1) > 1 and question2 != 'ECAENO':
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

def get_drug_item(drug, subcat):
    drug_dict = {'AEACN1' : 'BLINDED THERAPY',
                    'AEACN2' : 'DAUNORUBICIN',
                     'AEACN3': 'CYTARABINE',
                     'AEACN2_1': 'AZACITIDINE',
                    'AEREL1' : 'BLINDED THERAPY',
                    'AEREL2' : 'DAUNORUBICIN',
                    'AEREL3' : 'CYTARABINE',
                        'AEREL2_1': 'AZACITIDINE',
                    '': 'OVERALL DOSING'}
    
    
    if subcat == 'AEDR10':
        drug_dict = {key: val for key, val in drug_dict.items() if key.startswith('AEREL')}
        drug_dict = {val:key for key, val in drug_dict.items()}
        piv_item_name = drug_dict[drug]
    else:
        drug_dict = {key: val for key, val in drug_dict.items() if key.startswith('AEACN')}
        drug_dict = {val:key for key, val in drug_dict.items()}
        piv_item_name = drug_dict[drug]
        
    return piv_item_name