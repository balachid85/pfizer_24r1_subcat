import importlib
import sys
import pandas as pd
from sqlalchemy import create_engine, text
import configparser
import traceback
import json
import os
from main_utils import *
current_path = os.path.dirname(os.path.abspath(__file__))
#similarity_path = os.path.join(current_path, 'similarity')
#with open(os.path.join(similarity_path, 'fda_eu_label_dict_new_with_symptoms_2.json')) as f:
#    fda_eu_data_global = json.load(f)

#df_who_drug_global = pd.read_csv(os.path.join(similarity_path, 'AE_Consolidated_1.csv'), names=['a', 'b', 'c'])

sys.path.append('model_libs/')


class SubInfer:
    def __init__(self, study_id, account_id, job_id, conn, config):
        self.study_id = study_id
        self.conn = conn
        self.config = config
        self.account_id = account_id
        self.job_id = job_id
        self.domain_list = config['domains'].split(',')

    def get_subcat_list(self):
        print("getting subcats")
        map_rule_db = self.config['map_rule_db']
        present_subcates = {}
        # map_rule_query = text(f"SELECT rule_id, status, version, action_seq from common.{map_rule_db} where study_id= :s")
        # map_rule_query = text(f"""select rule_id, status, version, action_seq from common.{map_rule_db} mr1 where study_id = :s
        #                         and status = 'ACTIVE' and action_seq = (select max(action_seq) from common.map_rules mr2 where
        #                         mr2.study_id = mr1.study_id and mr2.rule_id = mr1.rule_id and mr1.version = mr2.version)
        #                         and version = (select max(version) from common.map_rules mr2 where mr2.study_id = mr1.study_id and
        #                         mr2.rule_id = mr1.rule_id)""")

        map_rule_query = text(f"select * from sdq_common.study_ml_model where study_id= :s and account_id= :account_id and status= 'active'")

        map_rule_res = self.conn.execute(map_rule_query, s=self.study_id, account_id=self.account_id)
        map_rule_df = pd.DataFrame(map_rule_res, columns=map_rule_res.keys())
        map_rule_df['ml_name'] = map_rule_df['ml_model_id'].apply(get_rule_name, conn=self.conn)
        '''
        max_version = max(map_rule_df['version'])
        map_rule_df = map_rule_df[map_rule_df['version'] == max_version]
        max_action_seq =  max(map_rule_df['action_seq'])
        max_rule_df = map_rule_df[map_rule_df['action_seq'] == max_action_seq]
        map_rule_df = map_rule_df[map_rule_df['status'] == 'ACTIVE']
        '''
        if len(map_rule_df) > 0:

            for domain in self.domain_list:
                present_subcates[domain] = map_rule_df[map_rule_df['ml_name'].str.startswith(domain)].to_dict(
                    orient='records')
        else:
            print(f"### No subcates present for {self.study_id}")
        return present_subcates

    def create_study_deeplink_map(self, for_study=None):
        map_studies_query = text(f"SELECT * from sdq_common.study where status = 'ACTIVE' and account_id= :account_id")
        map_studies_res = self.conn.execute(map_studies_query, account_id=self.account_id)
        map_studies_df = pd.DataFrame(map_studies_res.fetchall(), columns=map_studies_res.keys())
        study_deeplink_list = []
        replace_words = {
            '{site_id}': '{siteno}',
            '{subject_id}': '{subject_id}',
            '{visit_id}': '{visit_id}',
            '{visitindex}': '{visit_ix}',
            '{form_id}': '{form_id}',
            '{formindex}': '{form_index}',
            '{Datapageid}':'{Datapageid}'
        }
        for study, deeplink in zip(map_studies_df.id, map_studies_df.deep_link_url):
            rec_dict = {}
            if type(deeplink) == str:
                for match, replace in replace_words.items():
                    deeplink = deeplink.replace(match, replace)
                    if deeplink.endswith(';'):
                        deeplink = deeplink[:-1]
            rec_dict['id'] = study
            rec_dict['deep_link_url'] = deeplink
            study_deeplink_list.append(rec_dict)
        study_deeplink_df = pd.DataFrame(study_deeplink_list)
        current_path = os.path.dirname(os.path.abspath(__file__))
        # study_deeplink_df.to_csv(os.path.join(current_path, 'model_libs/%s/study_deeplink.csv'%(str(self.account_id))), index=False)
        study_deeplink_df.to_csv(os.path.join(current_path, 'study_deeplink.csv'), index=False)
        # deeplink = open(os.path.join(current_path, 'study_deeplink.csv'))
        # txt = deeplink.read()
        # deeplink.close()
        if for_study and for_study in study_deeplink_df.study_id.values:
            study_deeplink = study_deeplink_df[study_deeplink_df['study_id'] == for_study]['deeplink'].values[0]
            return study_deeplink

    def predict_subcat_with_probability(self):
        print("Running subcats")
        self.create_study_deeplink_map()
        subcate_dict = self.get_subcat_list()
        for domain in subcate_dict:
            print(f"Total subcats in {domain} - {len(subcate_dict[domain])}")
            #print("Domain.....", subcate_dict[domain])
            for rule in subcate_dict[domain]:
                #print(rule["ml_model_id"])
                rule_name = get_rule_name(rule['ml_model_id'], self.conn)
                print("Subcat name.......", rule_name)
                try:
                    catemodules = importlib.import_module(f"model_libs.{rule_name}")

                    cate_inst = getattr(catemodules, rule_name)(self.study_id, self.account_id, self.job_id, rule['ml_model_id'], 0.1)
                    cate_inst.run()
                except:
                    print(traceback.format_exc())


if __name__ == '__main__':
    eng = create_engine(DB_URL)
    conn = eng.connect()

    subcat_list = get_subcat_list(conn, STUDY_ID, VERSION, DOMAINS, MAP_RULE_DB)
    predict_subcat(subcat_list, STUDY_ID, VERSION)

