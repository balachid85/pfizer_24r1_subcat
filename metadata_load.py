import configparser
from datetime import datetime
import logging
import pandas
import pyodbc
import sqlalchemy
import traceback
import os
import getpass
import sys
path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, path)

try:
    from scripts.dataIngestion.database_connect import connection as dbconn  
except:
    from database_connect import connection as dbconn

logger = logging.getLogger(os.path.basename(__file__))

#path = os.path.dirname(os.path.abspath(__file__))

class MetadataLoad:
    def __init__(self, config):
        if not config:
            self.config = configparser.ConfigParser()
            self.config.read("dataload_configs.ini")
        else:
            self.config = config

        #record information for update sdq
        self.update_audit_table = {}
        self.fail_logs = {}
        #connection 

    def get_failed_tables(self, batchid, studyid, sdq_eng):
        config = self.config 
        ### study table from_ts, to_ts
        #sql = f""" 
        # SELECT source_tablename, load_start_ts, load_end_ts FROM common.dataload_detail_audit
        #WHERE (study_id,  coalesce(target_tablename, source_tablename), action_dts) IN 
        #    (SELECT study_id, coalesce(target_tablename, source_tablename), MAX(action_dts) FROM common.dataload_detail_audit  
        #    WHERE UPPER(study_id) = '{str(studyid).upper()}' GROUP BY study_id, coalesce(target_tablename, source_tablename)) 
        #AND UPPER(load_status) = 'FAILED' 
        #"""  
        
        sql = f""" SELECT UPPER(source_tablename), load_start_ts, load_end_ts FROM common.dataload_detail_audit
        WHERE (study_id,  source_tablename, action_dts) IN
            (SELECT study_id, source_tablename, MAX(action_dts) FROM common.dataload_detail_audit
            WHERE UPPER(study_id) = '{str(studyid).upper()}' GROUP BY study_id, source_tablename)
        AND UPPER(load_status) = 'FAILED'
        """
        result = sdq_eng.execute(sql)
        rs = result.fetchall()
        
        return rs #rs_fic  #drop dup
     
    def check_data(self, study_id, tablelist, end_ts):
        '''
        check from  job/batch run table
        '''
        table_timestamps = {} 
        auditdb_connector = dbconn('source', 'audit_database_connection', self.config) 
        auditdb_eng = auditdb_connector.engine

        self.update_audit_table[study_id] = {'tablelist':tablelist} 
        if str(end_ts).split(' ')[0] == '1900-01-01':#initial data transfer   
           sql = f"""SELECT table_name, 'Current', 'Data', MAX(batch_end_ts)
FROM  cdl_job_run cjr JOIN (SELECT study_id, batch_run_id, batch_end_ts FROM cdl_batch_run cbr WHERE batch_end_ts > '{end_ts}' AND study_id = '{study_id}'
AND batch_status = 'COMPLETED' AND conformed_crawler_job_status = 'COMPLETED' AND  upper(batch_id) != 'CDL_BATCH_SAS_LABELS_FULL_LOAD') temp ON cjr.study_id = temp.study_id AND cjr.batch_run_id = temp.batch_run_id
WHERE step_type = 'CONFORMANCE' AND LOWER(table_name) REGEXP lOWER('{'|'.join(list(map(lambda x: '^'+x if x.startswith('sm_') else x, tablelist)))}')
GROUP BY table_name """
                           
        else: 
            sql = f"""SELECT table_name, MIN(job_start_ts), MAX(job_end_ts), MAX(batch_end_ts)
FROM  cdl_job_run cjr JOIN (SELECT study_id, batch_run_id, batch_end_ts FROM cdl_batch_run cbr WHERE batch_end_ts > '{end_ts}' AND study_id = '{study_id}'
AND batch_status = 'COMPLETED' AND conformed_crawler_job_status = 'COMPLETED' AND  upper(batch_id) != 'CDL_BATCH_SAS_LABELS_FULL_LOAD') temp ON cjr.study_id = temp.study_id AND cjr.batch_run_id = temp.batch_run_id
WHERE step_type = 'CONFORMANCE' AND LOWER(table_name) REGEXP lOWER('{'|'.join(list(map(lambda x: '^'+x if x.startswith('sm_') else x, tablelist)))}')
GROUP BY table_name
            """
        result = auditdb_eng.execute(sql)
        temp_rs = result.fetchall() 
        if not temp_rs: 
            self.update_audit_table.pop(study_id, None)
            return {} #
        
        #extra info here
        table_timestamps = dict(map(lambda x: (x[0],[x[1] if str(x[1]).lower()=='current' else end_ts,x[2]]), temp_rs)) if temp_rs else {}
        new_end_ts =  max(map(lambda x: x[3], temp_rs)) if (temp_rs )  else end_ts
        sql = f""" SELECT batch_end_ts, conformed_crawler_end_ts, raw_crawler_end_ts, study_src_refresh_ts FROM {self.config['source']['audit_table']} 
               WHERE study_id = '{study_id}' and batch_end_ts = '{new_end_ts}'"""
        result = auditdb_eng.execute(sql)
        logger.info(sql) 
        keys = result.keys()
        other_info = result.fetchone()
        result.close() 
        for i in range(len(keys)):
            self.update_audit_table[study_id].update({keys[i]: other_info[i]})
 
        auditdb_eng.dispose()
    
        return  table_timestamps #{table_name: [from_ts, to_ts]}
    
    def get_domain_list(self, study_id):
        sdq_connector = dbconn('sdq', 'database_connection', self.config)
        sdq_eng = sdq_connector.engine
        sdq_audit_table = self.config['sdq']['audit_table']  
        sql = f"""
            select study_id, batch_id, tablelist from {sdq_audit_table} where (study_id, batch_id) in (
             select  distinct study_id , max(batch_id) over (partition  by study_id ) as batch_id from {sdq_audit_table} where study_id='{study_id}')
             and study_status = 'ACTIVE'
        """

        result = sdq_eng.execute(sql)
        rs = result.fetchall() 
        if rs:
            # 1st row + "tablelist"
            return rs[0][2]
        

    def setup_check_data(self, study=None):
        '''
        read data from audit table to get a array for this batch dataload
        '''
        config = self.config
        array = [] 
        sdq_audit_table = config['sdq']['audit_table']  
        try: 
            sdq_connector = None
            sdq_connector = dbconn('sdq', 'database_connection', self.config)
            sdq_eng = sdq_connector.engine
            '''Updated'''
            # get sdq (studyid, tablelist, end_ts) #to be honest, the batch_id is redundant if using studyid+end_ts for key
            sql = f""" 
            select batch_id , study_id , tablelist , source_batch_end_ts from {sdq_audit_table} 
            where (study_id , batch_id ) in 
            (select  distinct study_id , max(batch_id) over (partition  by study_id ) as batch_id from {sdq_audit_table} )
            and upper(study_status)  =  'ACTIVE' 
            """ 
  
            result = sdq_eng.execute(sql)
            rs = result.fetchall()  
            logger.info('result from sdq_audit: '+str(rs))
            for batch_id, study_id, tablelist, end_ts in rs: #each study  #check fail tables last batch
                if study and study != study_id: continue
                table_timestamps = self.check_data(study_id, tablelist, end_ts)
                if not table_timestamps: 
                    logger.warning(f'No available data was found in study {study_id} after timestamp {end_ts}.') 
                #get fail tables
                failed_tables = self.get_failed_tables(batch_id, study_id, sdq_eng) or []
                
                ###failed table "sdq table -> exact tablename in audit db"###
                logger.debug('result from fail table check: '+str(failed_tables))
                for tablename, from_ts, to_ts in failed_tables:
                    if str(from_ts).split(' ')[0] == '1900-01-01': # or 
                        table_timestamps.update({tablename: ['Current', 'Data']})
                    else:
                        from_ts = datetime.strptime(str(from_ts), '%Y-%m-%d %H:%M:%S')
                        to_ts = datetime.strptime(str(to_ts), '%Y-%m-%d %H:%M:%S')
                        table_timestamps.update({tablename: [min(from_ts, table_timestamps.get(tablename, [datetime.max, datetime.min])[0]),\
                                                             max(to_ts, table_timestamps.get(tablename, [datetime.max, datetime.min])[1])]}) 
                
                if not study_id in self.update_audit_table:  #use last batch info
                    sql = f""" SELECT tablelist, raw_end_ts, conformance_end_ts, source_batch_end_ts, study_src_refresh_ts FROM {sdq_audit_table} WHERE study_id = '{study_id}' AND  batch_id='{batch_id}'"""
                    result = sdq_eng.execute(sql)
                    self.update_audit_table.update({study_id:{}})
                    keys = result.keys()
                    other_info = result.fetchone()
                    result.close()

                    for i in range(len(keys)):
                        self.update_audit_table[study_id].update({keys[i]: other_info[i]})
                 
                array.append((batch_id, study_id, table_timestamps))            
        except:
            logger.error(traceback.format_exc())
            
        finally:
            if sdq_connector: sdq_connector.dispose()
                
        logger.info('metadata_load return array: '+str(array))
        return array
    
    def insert_audit_table(self, batch_id, study_id):
        data = self.update_audit_table[study_id]
        audit_table = self.config['sdq']['audit_table'] 
        batch_id += 1
        #check batch_id in table first, then do insert if no 
        sdq_connector = dbconn('sdq', 'database_connection',  self.config)
        sdq_eng = sdq_connector.engine
        sql = f"""SELECT 1 FROM {audit_table} WHERE study_id = '{study_id}' and batch_id={batch_id}"""
        result = sdq_eng.execute(sql)
        rs = result.fetchall()
        
        if rs:
            if sdq_connector: sdq_connector.dispose() 
            return
        data.update({"batch_id": batch_id, "study_id": study_id,"study_status":'ACTIVE',
                     "dataload_status": '',"action_dts": datetime.utcnow(), "act_uid": getpass.getuser()})
        audit_df = pandas.DataFrame(data = [data])
        audit_df = audit_df.rename(columns={'conformed_crawler_end_ts': 'conformance_end_ts',
                                                'batch_end_ts': 'source_batch_end_ts',
                                                'raw_crawler_end_ts': 'raw_end_ts',
                                                'study_src_refresh_ts' : 'study_src_refresh_ts'})
        logger.info('inserting audit table with empty dataload status:\n',audit_df)
        audit_df.to_sql(audit_table.split(".")[1], con = sdq_eng, schema = audit_table.split(".")[0], if_exists = 'append', index = False)
        
        if sdq_connector: sdq_connector.dispose()
        


    def get_active_studies(self):
        sdq_connector = None
        rs = []
        try:
            sdq_connector = dbconn('sdq', 'database_connection', self.config)
            sdq_eng = sdq_connector.engine
            sdq_audit_table = self.config['sdq']['audit_table'] 
            sql = f"""
              select batch_id , study_id , tablelist from {sdq_audit_table}
             where (study_id , batch_id ) in
             (select  distinct study_id , max(batch_id) over (partition  by study_id ) as batch_id from {sdq_audit_table})
             and upper(study_status)  =  'ACTIVE'
            """

            result = sdq_eng.execute(sql)
            rs = result.fetchall()  
        except Exception as e:
            logger.error(e)
        finally:
            if sdq_connector: sdq_connector.dispose()
        return [i for i in rs] if len(rs)>0 else []


if __name__ == "__main__":
    logger.info('Running in __main__')
    metadata_class_obj = MetadataLoad(None)
    a = metadata_class_obj.setup_check_data()
    print('Done metadata check\n',a)
