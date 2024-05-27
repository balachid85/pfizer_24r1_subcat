import traceback
from ruledb.db_service import get_db_engine, DBService
import pytz
from datetime import datetime
from math import ceil
from insert_utils import InsertQuery
import json
from constants import *
from query_utils_helper import *
from sqlalchemy import text


def get_rule_metadata(account_id, study_id, rule_id):
    #print('******* Inside get_rule_metadata************')
    #print('account_id, study_id, rule_id', account_id,study_id, rule_id)
    with get_db_engine().connect() as conn:
        service = DBService(conn)
        ctx = {
            'study_id': study_id,
            'rule_id': rule_id,
            'account_id': account_id
        }
        result = service.get_result('get_rule_meta', ctx)
        #print(result)
        result = [dict(_row) for _row in result]
        #print('**********Result in get_rule_meta_data function is :', result)
        result = result[0] if result else {}
        if not result:
            return {}
        UTC = pytz.UTC
        last_run_dt = result['last_run_dt'] if result[
            'last_run_dt'] else datetime.strptime(
            '1980-1-1', '%Y-%M-%d')
        rule_meta_data = {
            'rule_id': result['ec_id'],
            'study_id': study_id,
            'action_seq': str(result['version']),
            'version': str(result['version']),
            'status': result['status'],
            'act_uid': result['account_id'],
            'act_dts': str(result['updated_at']),
            'act_user_type': result['account_id'],
            'rule_name': result['name'],
            'rule_desc': result['description'],
            'rule_type': 'EditCheck',
            'query_text': result['query_text'],
            'last_run_dt': last_run_dt.astimezone(UTC).strftime("%a, %d %b %Y %H:%M:%S %Z"),
            'dynamic_panel_config': result.get('dynamic_panel_config', []),
            'account_id': result['account_id'],
            'query_target': result['query_target'],
            'current_batch_id': 0
        }
    return rule_meta_data

def get_subcatjson(account_id, study_id, rule_id):
    print('******* Inside get_subcatjson************')
    print('account_id, study_id, rule_id', account_id,study_id, rule_id)
    with get_db_engine().connect() as conn:
        service = DBService(conn)
        ctx = {
            'study_id': study_id,
            'rule_id': rule_id,
            'account_id': account_id
        }
        result = service.get_result('get_subcatjson', ctx)
        #print(result)
        result = [dict(_row) for _row in result]
        #print('**********Result in get_rule_meta_data function is :', result)
        result = result[0] if result else {}
        if not result:
            return {}
        UTC = pytz.UTC
        last_run_dt = result['last_run_dt'] if result[
            'last_run_dt'] else datetime.strptime(
            '1980-1-1', '%Y-%M-%d')
        rule_meta_data = {
            'rule_id': result['id'],
            'study_id': study_id,
            'account_id': result['account_id'],
            'preconf_item_name' : result['preconf_item_name'],
            'edc_item_name' : result['edc_item_name'],
        }
    return rule_meta_data


def get_reff_fields(account_id, study_id, rule_id, form_ref_name):
    reff_fields = {}
    with get_db_engine().connect() as conn:
        service = DBService(conn)
        ctx = {
            'study_id': study_id,
            'rule_id': rule_id,
            'form_ref_name': form_ref_name,
            'account_id': account_id
        }
        result = service.get_result('get_reff_fields', ctx)
        result = [dict(_row) for _row in result]
        result = result[0] if result else {}

        print('THE RESULT DICTIONARY IS',result)
        ## Query target might present in 2 different forms in EDC ( AE-> AE1, AE2 )
        ## Comma separated value will be stored in study_edit_check_dq_config and interpret the correct item based in indexing
        result_form_ref_name = result.get('form_ref_name', '')

        print("%%%%%%%%%% FRESULT_FORM_REF_NAME",result_form_ref_name)
        item_name_in_edc = result.get('item_name_in_edc', '')
        print('%%%%%%%%%%%%% ITEM NAME IN EDC LIST', item_name_in_edc)

        if result_form_ref_name and item_name_in_edc:
            form_ref_name_list = result_form_ref_name.split(',')
            item_name_in_edc_list = item_name_in_edc.split(',')
            #print(' DQ ERROR is caused by ' ,form_ref_name, form_ref_name_list)
            if form_ref_name in form_ref_name_list:
                form_name_index = form_ref_name_list.index(form_ref_name)
                
                reff_fields['formrefname'] = form_ref_name
                reff_fields['item_nm_edc'] = item_name_in_edc_list[form_name_index]
            else:
                print(f"DQ CONFIG ERROR :: Form Ref Name Not Matched with the dq config , given : {form_ref_name}, existing : {form_ref_name_list} ")
        else:
            print(f"DQ CONFIG ERROR :: Form Ref Name and Item name not present : form : {form_ref_name}, item : {item_name_in_edc} ")

        # Section is not used in rave , so sending the section as is
        reff_fields['sectionrefname'] = result.get('section_ref_name', '')
        reff_fields['query_target'] = result.get('query_target', '')
        print("REFF FIELDS:", reff_fields)

    return reff_fields

def get_field_labels(account_id, domain_name):
    stg_table = 'dynamic_field_label_config'
    with get_db_engine().connect() as conn:
        service = DBService(conn)
        ctx = {
            #'schema': SCHEMA,
            'table': stg_table,
            'account_id' : account_id,
            'domain_name' : domain_name
        }
        rs = service.get_result('get-field-labels', ctx)
        result = {}
        for row in rs.fetchall():
            result[row[0]] = row[1]
            
        #print('The result inside get_field_labels is :', result)
    return result

def get_fields_by_subcat(account_id, study_id, subcat, fieldname):
    #SCHEMA = get_study_id(account_id, study_id)
    stg_table = 'ml_model'
    with get_db_engine().connect() as conn:
        service = DBService(conn)
        ctx = {
            #'schema': SCHEMA,
            'table': stg_table,
            'account_id' : account_id,
            'study_id' : study_id,
            'subcat' : subcat,
            #'field_name' : fieldname
        }
        if(fieldname == 'display_fields'):
            rs = service.get_result('get-display-fields', ctx)
        elif(fieldname == 'fn_config') :
            rs = service.get_result('get-fn-config', ctx)
        elif(fieldname == 'field_labels'):
            rs = service.get_result('get-field-labels', ctx)

        #print(rs)
        if(fieldname == 'field_labels'):
            result = [row[0] for row in rs.fetchall()]  
        else:
            result = [row[0] for row in rs.fetchall()]   

        #print('The result inside get_fields_by_subcat is :', result)
    return result
   

def get_subjects_by_study_list(account_id, study_id, params):
   # print('inside get_subjects_by_study_list')
    SCHEMA = get_study_id(account_id, study_id)
    #print('**************************The Schema is : ****************',SCHEMA)
    request_args = params
    stg_table = 'stg_pred'
    task = request_args.get('task')
    page = request_args.get('page', 1)
    per_page = request_args.get('per_page', 50)
    rule_id = request_args.get('rule_id')
    rule_type = request_args.get('rule_type')
    offset = int(per_page) * (int(page) - 1)
    domain_filter = [p for p in params['domain'] if p.strip()]
    batch_id = params['batch_id']
    #print('account_id is:', account_id,'************************')
    #with get_db_engine(account_id).connect() as conn:
    with get_db_engine().connect() as conn:
        service = DBService(conn)
        ctx = {
            'schema': SCHEMA,
            'table': stg_table,
            'per_page': per_page,
            'offset': offset,
            'task': task,
            'domain': domain_filter,
            'batch_id': batch_id
        }

        rs = service.get_result('get-subjid', ctx)
        if task == 'count':
            count = rs.first()[0]
            result = {'count': count, 'pages': ceil(count / int(per_page))}
            return result

        result = [row[0] for row in rs.fetchall()]
    return result


def get_subject_data(account_id, study_id, subject_id, params):
    SCHEMA = get_study_id(account_id, study_id)
    request_args = params
    task = request_args.get('task')
    page = request_args.get('page', 1)
    per_page = request_args.get('per_page', 50)
    offset = int(per_page) * (int(page) - 1)
    batch_id = request_args.get('batch_id', 0)

    filters = []
    BASE_STG_PRED_FILTERS = ['formname', 'form_index', 'formrefname', 'site_id', 'domain']
    for filter in BASE_STG_PRED_FILTERS:
        if request_args.get(filter):
            filter_name = [p for p in request_args.get(filter) if p.strip()]
            filters.append({'col': filter, 'op': 'IN', 'val': filter_name})

    prev_ck_event_id = int(request_args.get('prev_ck_event_id') or 0)
    #with get_db_engine(account_id).connect() as conn:
    with get_db_engine().connect() as conn:

        service = DBService(conn)
        ctx = {
            'schema': SCHEMA,
            'table': 'stg_pred',
            'subjid': subject_id,
            'per_page': per_page,
            'offset': offset,
            'filters': filters,
            'prev_ck_event_id': prev_ck_event_id,
            'batch_id': batch_id
        }
        rs = service.get_result('stage-pred', ctx)
        result = []
        for row in rs.fetchall():
            _row = dict(row)
            result.append(_row)
        if task == 'count':
            count = len(result)
            result = {'count': count, 'pages': ceil(count / int(per_page))}
    return result


def create_query(account_id, study_id, payload):
    print('$%$%$%$%$%$%$%$% ',account_id, study_id, payload)   
    with get_db_engine(account_id).connect() as conn:
        query = InsertQuery(conn, account_id, study_id)
        if payload['query_text']:
            query.save_query(payload)


def update_rule_status(account_id, rule_id, payload):
    status = payload['status']
    subject = payload['subject']
    subject = int(subject)
    with get_db_engine().connect() as conn:
        service = DBService(conn)
        ctx = {
            'schema': COMMON_SCHEMA,
            'table': TEST_DATA_TABLE_MAP,
            'status': status,
            'subject': subject,
            'rule_id': rule_id,
            'account_id': account_id
        }
        service.execute('update_test_rule_status', ctx)


def save_query_metrics(account_id, study_id, payload):
    schema = get_study_id(account_id, study_id)
    rule_id = payload['rule_id']
    version = payload['version']
    end_dt = payload['end_dt']
    start_dt = payload['start_dt']
    disc_list = payload['disc_list']
    rule_type = payload.get('rule_type') or 'ml_subcat'
    values = ''
    f_flag = 0
    for track in disc_list:
        subjid = track['subjid']
        count = track['count']
        track = track['track']
        _row = f"('{rule_id}', '{rule_type}', '{int(float(version))}', '{subjid}', '{start_dt}', '{end_dt}', '{count}', '{json.dumps(track)}')"
        
        if(f_flag == 0):
            values += _row 
            f_flag = 1
        else:
            values += ',' + _row

    with get_db_engine(account_id).connect() as conn:
        service = DBService(conn)
        ctx = {
            'schema': schema,
            'table_name': SUBJECT_PROCESSED_REC_TABLE,
            'values': values
        }
        service.execute('insert-subject-processed-rec', ctx)


def get_domains(account_id, study_id, ck_event_ids):
    domain_dict = {}
    schema = get_study_id(account_id, study_id)
    with get_db_engine(account_id).connect() as conn:
        service = DBService(conn)
        ctx = {
            'schema': schema,
            'table_name': STAGE_TABLE_MAP,
            'ck_event_ids': tuple(ck_event_ids)
        }
        rs = service.get_result('get-domains', ctx)
        result = [dict(row) for row in rs.fetchall()]
        print("Result---->", result)
        for domain_set in result:
            if domain_set['domain'] in domain_dict:
                domain_dict[domain_set['domain']].append(domain_set['ck_event_id'])
            else:
                domain_dict[domain_set['domain']] = [domain_set['ck_event_id']]

    return domain_dict


def get_col_label(account_id, columns):
    label_dict = {}
    with get_db_engine().connect() as conn:
        service = DBService(conn)
        columns = str(columns).strip('[').strip(']')
        print("columns:::", columns)
        ctx = {
            'schema': COMMON_SCHEMA,
            'table_name': DYNAMIC_FIELD_LABEL_TABLE,
            'account_id': account_id,
            'item_names': columns
        }
        rs = service.get_result('get_item_nm_labels', ctx)
        result = [dict(row) for row in rs.fetchall()]
        print("Result---->", result)
        for item_set in result:
            label_dict[item_set['item_name']] = item_set['item_label']

    return label_dict


def update_edit_check_run_date(account_id, study_id, rule_id, version, payload):
    domain_list = payload['domain_list']
    print("DOMAIN LIST..........", domain_list)
    schema = get_study_id(account_id, study_id)
    with get_db_engine(account_id).connect() as conn:
        stg_pred_q = f"""select coalesce(max(created_dt), '1900-01-01 00:00:00') from {schema}.stg_pred
                 where domain in :domain_list"""
        stg_pred_rs = conn.execute(text(stg_pred_q), domain_list=tuple(domain_list))
        stg_pred_del_q = f"""select coalesce(max(created_dt), '1900-01-01 00:00:00') from {schema}.stg_pred_del
                 where domain in :domain_list"""
        stg_pred_del_rs = conn.execute(text(stg_pred_del_q), domain_list=tuple(domain_list))
        stg_pred_max = stg_pred_rs.fetchone()[0]
        stg_pred_del_max = stg_pred_del_rs.fetchone()[0]
        max_created_dt = max(stg_pred_del_max, stg_pred_max)

    with get_db_engine().connect() as conn:
        service = DBService(conn)
        ctx = {
            'schema': COMMON_SCHEMA,
            'table': 'study_edit_check',
            'ec_id': rule_id,
            'study_id': study_id,
            'last_run_dt': max_created_dt,
            'account_id': account_id
        }
        service.execute('update-edit-check-run-date', ctx)


def last_run_batch_id(payload):
    with get_db_engine().connect() as conn:
        service = DBService(conn)
        payload['schema'] = COMMON_SCHEMA
        payload['table'] = 'dataload_audit'
        rs = service.get_result('get_batch_id', payload)
        result = [dict(row).get('batch_id') for row in rs.fetchall()]
        print('batch_id is :', result)
    return list(set(result))


def update_batch_execution(payload):
    with get_db_engine().connect() as conn:
        service = DBService(conn)
        service.execute('insert_batch_rule_execution', payload)

def get_form_refname(account_id, study_id, stg_ck_event_id):
    SCHEMA = get_study_id(account_id, study_id)
    with get_db_engine(account_id).connect() as conn:
        service = DBService(conn)
        ctx = {
            'schema': SCHEMA,
            'table_name': 'stg_pred',
            'ck_event_id': stg_ck_event_id
        }
        rs = service.execute('get_form_refname', ctx)
        result = [dict(row) for row in rs]
        if result:
            print("FORMREFNAME:", result[0]['formrefname'])
            return result[0]['formrefname']
        else:
            return ''
