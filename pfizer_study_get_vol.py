import os
from sqlalchemy import text, create_engine
import os
from sqlalchemy import text, create_engine
import pandas as pd
import json
import logging

class Config:
    Db_url = os.environ.get('DATABASE_URI')


class pfizer_adapters:
    DB_URL = Config.Db_url
    
    def vol_info(self,study_id,account_id):
        print(study_id)
        vol_eng = create_engine(self.DB_URL)

        conn=vol_eng.connect()
        study_id = f'account_{account_id}_study_{study_id}'
        fetch_vol = conn.execute(f"select study_type from common.map_study ms where study_id in(select study_id from sdq_common.study where schema='{study_id}');")
        if fetch_vol:
            fetch_vol = fetch_vol.fetchone()[0]
            # print(fetch_vol, type(fetch_vol))
            return fetch_vol
        else:
            print(f'{study_id} - Study ID not found in common.map_study and sdq_common.study')
    
    def exclude_form(study_type,unflatten_df):
        try:
            if unflatten_df.shape[0]>0:
                exclude_form_vol_dict = {2: ['AE203*'], 3: ['AE001_1','EC001_5*']}
                exclude_form = exclude_form_vol_dict[int(study_type)]
                for form in exclude_form:
                    if '*' in form:
                        form = form.replace('*', '')
                        # print('exclude_form',unflatten_df.shape)
                        try:
                            unflatten_df = unflatten_df[~(unflatten_df['formrefname'].str.startswith(form))]
                        except:
                            # print('exclude_form',unflatten_df.shape,unflatten_df['formrefname'].values)
                            # print('exclude_form',unflatten_df.shape)
                            continue
                    else:
                        try:
                            unflatten_df = unflatten_df[~(unflatten_df['formrefname'] == form)]
                        except:
                            # print('exclude_form',unflatten_df.shape,unflatten_df['formrefname'].values)
                            continue

                return unflatten_df
            else:
                print('Data illa sir :)')
        except Exception as excep:
            print(excep)