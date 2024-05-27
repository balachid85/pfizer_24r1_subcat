import yaml
import logging
from datetime import datetime
import os
import sys
from sqlalchemy import create_engine

import subinfer

current_path = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s(%(levelname)s): %(message)s',
                    datefmt='%y-%m-%d %H:%M',
                    filename=os.path.join(current_path, f"""load_data_{datetime.utcnow().strftime('%Y-%m-%d')}.log"""))

console = logging.StreamHandler()
console.setLevel(logging.INFO)  # CAN change to "WARNING" after tsting well
# set a format which is simpler for console use
formatter = logging.Formatter('%(levelname)s: %(name)-12s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

logger = logging.getLogger(os.path.basename(__file__))


def run(study_id, account_id, job_id, db_conn_url, config):
    try:
        eng = create_engine(db_url)
        conn = eng.connect()
        print(f"Calling subinfer - {study_id}, {config}")
        subcat_pred = subinfer.SubInfer(study_id, account_id, job_id, conn, config)
        subcat_pred.predict_subcat_with_probability()
        conn.close()

    except Exception as exp:
        logger.info(exp)
        print(exp)

    '''
    dataprocess = DataProcess(config)
    metadata_load_obj = metadata_load.MetadataLoad(config)
    array = metadata_load_obj.get_active_studies()
    domains = ['AE', 'CM', 'EC']
    for batch_id, ac_study, ac_table_list in array:
        if ac_study==study_id:
            #run(dataprocess, metadata_load_obj , 10, study_id, domains)
            study_data =  metadata_load_obj.setup_check_data(study_id) 
            if len(study_data) > 0: 
              batch_id, study_id, job_list = study_data[0]
              filtered_domain_list = {}
              for job in job_list:
                 for domain in domains:
                     if domain.upper() in job:
                          filtered_domain_list[job] = job_list[job]
              adpm_df, present_index_dict = dataprocess.construct_adpm_preprocess(batch_id, study_id, filtered_domain_list, metadata_load_obj)

              eng = connection('sdq', 'database_connection', config)
              conn = eng.connect()
              subcat_pred = subinfer.SubInfer(study_id.upper(), conn, config['subcat_model'])
              subcat_pred.predict_subcat_with_probability()
            else:
              return {} , {} 
    '''


if __name__ == "__main__":
    logger.info('Running in __main__')

    study_id = sys.argv[1]
    job_id = sys.argv[2]
    account_id = sys.argv[3]
    db_url = os.environ.get('DATABASE_URI')
    api_endpoint = os.environ.get('API_ENDPOINT')
    current_path = os.path.dirname(os.path.abspath(__file__))
    fname = os.path.join(current_path, "config.yaml")

    stream = open(fname, 'r')
    config = yaml.load(stream, Loader=yaml.FullLoader)

    print(config)
    config['API_ENDPOINT'] = api_endpoint

    with open(fname, 'w') as yaml_file:
        yaml_file.write(yaml.dump(config, default_flow_style=False))
    print("Config$$$$$$$$$", config)
    run(study_id, account_id, job_id, db_url, config)
