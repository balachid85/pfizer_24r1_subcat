import os
import re
import json
import time
import traceback
from pathlib import Path
import zipfile
import numpy as np
import boto3
import pandas as pd
from rapidfuzz import fuzz, process
from functools import partial
from typing import List, Tuple, Union
from nltk.corpus import stopwords
import requests
import re
import time
import pickle

S3_BUCKET = os.environ['AWS_BUCKET_NAME']

curr_file_path = Path(__file__).resolve()
main_path = curr_file_path.resolve().parent
data_path = main_path / 'data'
fdalabels_path = data_path / 'fdaopenlabel_corpus'
whodrug_path = data_path / 'whodrug'
meddra_frozen_path = data_path / 'meddra_frozen_dumps'
reverse_lookup_file_path = data_path / 'reverse_lookup_files'
meddra_path = data_path / 'MedAscii'
fallback_path = data_path / 'fallback'


def load_fdaopenlabel(filepath: Path = fdalabels_path,
                      filename: str = 'fda_eu_openlabel_merged_March2021.json') -> json:
    """Load fdaopenlabel json
    Args:
        filepath (Path, optional): Filepath. Defaults to data_path.
        filename (str, optional): Filename. Defaults to 'fda_eu_openlabel_merged_March2021.json'.
    Returns:
        json: Loaded json object
    """


    with open(filepath/filename, encoding='utf-8') as f:
        # print('check type',f)
        fda_eu_data = json.load(f)
    # dbfile = open(filepath/filename, 'rb')
    # loaded = pickle.load(open(filepath/filename, 'rb'))
    
    return fda_eu_data#pickle.load(open(filepath/filename, 'rb'))


def load_who_dict_fb(filepath: Path = whodrug_path,
                     filename: str = 'AE_Consolidated_1.csv') -> Tuple[dict, dict]:
    """Loads WHO drug dictionary
    Args:
        filepath (Path): Input file path
        filename (str): Filename
    """

    df_who_drug = pd.read_csv(filepath/filename, names=['a', 'b', 'c'])
    who_drug_term = dict()
    who_drug_code = dict()
    who_drug_term_code = dict()
    # who_drug_sp_term = dict()

    for index, row in df_who_drug.iterrows():
        who_drug_term[row['a']] = row['c']
        who_drug_code[row['b']] = row['c']
        who_drug_term_code[row['c']] = row['b']
        '''if row['a'] not in who_drug_sp_term[row['c']]:
            who_drug_sp_term[row['c']].append()
        '''

    return who_drug_term, who_drug_code, who_drug_term_code


def load_who_dict(filepath: Path = whodrug_path,
                  filename: str = 'AE_Consolidated_1.csv') -> Tuple[dict, dict]:
    """Loads WHO drug dictionary
    Args:
        filepath (Path): Input file path
        filename (str): Filename
    """

    df_who_drug = pd.read_csv(filepath/filename, names=['a', 'b', 'c'])
    who_drug_term = dict()
    who_drug_code = dict()

    for index, row in df_who_drug.iterrows():
        who_drug_term[row['a']] = row['c']
        who_drug_code[row['b']] = row['c']
    return who_drug_term, who_drug_code


def load_full_meddra_precompute(filepath: Path = meddra_frozen_path,
                                filename: str = 'full_diagnosis_meddra_hierarchy.json') -> json:
    """Load Meddra precompute for all the list of unique diagnosis from fda openlabel.
    Args:
        filepath (Path, optional): Filepath. Defaults to data_path.
        filename (str, optional): Filename. Defaults to 'full_diagnosis_meddra_hierarchy.json'.
    Returns:
        json: Loaded Meddra for all unique diagnosis.
    """

    with open(filepath/filename, 'r') as jfile:
        # all_diagnosis_meddra_dict = json.load(jfile)
        return json.load(jfile)


load_flat_meddra_precompute = partial(load_full_meddra_precompute,
                                      filename='full_flatten_meddra_hierarchy.json')
load_flat_meddra_precompute.__doc__ = "Load Meddra precompute for fully \
                                       flattened all unique diagnosis"


def load_drug_lookup_files(filepath: Path = reverse_lookup_file_path) -> Tuple[
        pd.DataFrame,
        pd.DataFrame,
        pd.DataFrame,
        pd.DataFrame]:
    """_Load drug reverse lookup files
    Args:
        filepath (str): Filepath
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]: 
        faers df, faers_male df, faers_female df, structures df
    """
    all_csv_files = list(filepath.glob('*.csv'))
    for file in all_csv_files:
        if 'faers' in file.stem and ('male' not in file.stem and 'female' not in file.stem):
            faers = pd.read_csv(file)
        elif str(file.stem).startswith('faers_male'):
            faers_male = pd.read_csv(file)
        elif str(file.stem).startswith('faers_female'):
            faers_female = pd.read_csv(file)
        elif str(file.stem).startswith('structures'):
            structures = pd.read_csv(file)

    return faers, faers_male, faers_female, structures


def load_fallback(filepath: str = fallback_path, filename: str = 'fallback_corpus.csv') -> pd.DataFrame:
    """Load fallback dataframe
    Args:
        filepath (str, optional): _description_. Defaults to fallback_path.
        filename (str, optional): _description_. Defaults to 'fallback_corpus.csv'.
    Returns:
        pd.DataFrame: _description_
    """

    fallback_df = pd.read_csv(filepath/filename, delimiter='^')
    return fallback_df


def load_meddra_dictionaries_fb(filepath: Path = meddra_path,
                                version: str = 'Version 24.0') -> Tuple[dict, dict, dict, dict, dict]:
    """Load Meddra dictionaries of the given version

    Args:
        filepath (str, optional): Filepath. Defaults to meddra_path.
        version (str, optional): Version. Defaults to 'Version 24.0'.

    Returns:
        Tuple[dict, dict, dict, dict, dict]: Returns llt, pt, hlt, hlgt, soc dictionaries
    """

    version_filepath = filepath/version
    if not version_filepath.exists():
        raise NameError('Version not found error')
    else:
        llt = pd.read_csv(version_filepath/"llt.asc",
                          delimiter="$", header=None)
        pt = pd.read_csv(version_filepath/"pt.asc", delimiter="$", header=None)
        # soc = pd.read_csv(version_filepath/"soc.asc", delimiter="$", header=None)
        llt_dict = {}
        pt_dict = {}
        # llt.to_csv('fallback_additions/similarity/llt.csv')
        # pt.to_csv('fallback_additions/similarity/pt.csv')

        for i, rows in llt.iterrows():
            llt_dict[rows[1].lower()] = {
                "llt_code": rows[0], "pt_code": rows[2]}

        for i, rows in pt.iterrows():
            pt_dict[rows[1].lower()] = {
                "pt_code": rows[0], "soc_code": rows[3]}

        return llt_dict, pt_dict, pt


def load_meddra_dictionaries(filepath: Path = meddra_path,
                             version: str = 'Version 24.0') -> Tuple[dict, dict, dict, dict, dict]:
    """Load Meddra dictionaries of the given version
    Args:
        filepath (str, optional): Filepath. Defaults to meddra_path.
        version (str, optional): Version. Defaults to 'Version 24.0'.
    Returns:
        Tuple[dict, dict, dict, dict, dict]: Returns llt, pt, hlt, hlgt, soc dictionaries
    """

    version_filepath = filepath/version
    if not version_filepath.exists():
        raise NameError('Version not found error')
    else:
        llt = pd.read_csv(version_filepath/"llt.asc",
                          delimiter="$", header=None)
        pt = pd.read_csv(version_filepath/"pt.asc", delimiter="$", header=None)
        hlt_pt = pd.read_csv(version_filepath/"hlt_pt.asc",
                             delimiter="$", header=None)
        # hlt = pd.read_csv(version_filepath/"hlt.asc", delimiter="$", header=None)
        hlgt_hlt = pd.read_csv(
            version_filepath/"hlgt_hlt.asc", delimiter="$", header=None)
        # hlgt = pd.read_csv(version_filepath/"hlgt.asc", delimiter="$", header=None)
        soc_hlgt = pd.read_csv(
            version_filepath/"soc_hlgt.asc", delimiter="$", header=None)
        # soc = pd.read_csv(version_filepath/"soc.asc", delimiter="$", header=None)

        llt_dict = {}
        pt_dict = {}
        hlt_dict = {}
        hlgt_dict = {}
        soc_dict = {}

        for i, rows in llt.iterrows():
            llt_dict[rows[1].lower()] = {
                "llt_code": rows[0], "pt_code": rows[2]}

        for i, rows in hlt_pt.iterrows():
            pt_dict[rows[1]] = str(int(rows[0]))

        for i, rows in hlgt_hlt.iterrows():
            hlt_dict[str(int(rows[1]))] = str(int(rows[0]))

        for i, rows in soc_hlgt.iterrows():
            hlgt_dict[str(int(rows[1]))] = str(int(rows[0]))

        for i, rows in soc_hlgt.iterrows():
            soc_dict[str(int(rows[0]))] = str(int(rows[0]))

        return llt_dict, pt_dict, hlt_dict, hlgt_dict, soc_dict, pt


def clean_indications(indications_list: List) -> list:
    """Cleans indications from the given list of indication
    Args:
        indications_list (List): _description_
    Returns:
        list: _description_
    """

    final_return_list = list()
    user_defined_words = set(['uses'])

    eng_stopwords = set(stopwords.words('english'))
    eng_stopwords.update(user_defined_words)

    if (indications_list):
        def filter_stopwords(x): return [
            word for word in x if word not in eng_stopwords]
        diag_list_after_pre_clean = filter_stopwords(indications_list)

    if (diag_list_after_pre_clean):
        for sentence in diag_list_after_pre_clean:

            removed_substring = list()
            words = sentence.split(' ')
            removed_substring = filter_stopwords(words)

            final_return_list.append(' '.join(removed_substring).strip())

        return final_return_list


def remove_paranthesis(x): return re.sub(r'\(.*?\)', '', x).strip()


remove_paranthesis.__doc__ = """Removes paranthesis to the given str """


def remove_splchars(x): return re.sub('[^A-Za-z0-9]+', ' ', x).strip()


remove_splchars.__doc__ = """Removes special chars to the given str """


def null_check(value: Union[str, float, None]) -> Union[str, None]:
    """Basic null check function
    Args:
        value (Union[str, float, None]): Input value
    Returns:
        Union[str, None]: Returns not null str or None
    """
    if not isinstance(value, float):
        value = str(value)

    if not isinstance(value, str):
        value = str(value)

    value = value.lower().strip()
    null_list = ['nan', 'null', 'none']
    if pd.isnull(value) or value == '' or value in null_list:
        return None
    return value


def get_int_id(value: Union[str, float, None]) -> Union[int, None]:
    """Get id as int
    Args:
        value (Union[str, float, None]): Input id of str type
    Returns:
        Union[int, None]: Int id or None
    """
    if value is not None and not pd.isnull(value):
        try:
            number_as_float = float(value)
            number_as_int = int(number_as_float)
            return number_as_int
        except ValueError:
            return None
    else:
        return None


def fuzzy_search(string: str,
                 string_list: Union[str, List],
                 threshold: int = 85,
                 debug=False) -> bool:
    """_Do fuzzy search of a given substring over a list of strings
    Args:
        string (str): Input string
        string_list (Union[str, List]): List of possible substring for 
                                        given string or just another 
                                        string itself
        threshold (int, optional): _description_. Defaults to 85.
        debug (bool, optional): _description_. Defaults to False.
    Returns:
        bool: _description_
    """

    if isinstance(string_list, str):
        string_list = [string_list]

    result_term = process.extract(string, string_list,
                                  scorer=fuzz.token_sort_ratio)
    match_terms = list()
    # print('RESULT TERM IS :',result_term)
    for (term, match_ratio, x) in result_term:
        if match_ratio >= threshold:
            match = (term, match_ratio)
            match_terms.append(match)
    if any(match_terms):
        if debug:
            return True, match_terms
        else:
            return True
    else:
        if debug:
            return False, match_terms
        else:
            return False


def google_search_helper(study, subject, cmdecode, cm_term, aedecode, ae_term, AEPTCD='', AE_LLT_CODE=''):
    from bs4 import BeautifulSoup
    curr_file_path = Path(__file__).resolve()
    main_path = curr_file_path.resolve().parent
    filename = 'data/fallback/fallback_corpus.csv'
    fall_back_table = pd.read_csv(main_path/filename)
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
    headers = {'User-Agent': user_agent, 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9', 'Accept-Encoding': 'gzip',
               'Connection': 'keep-alive'}
    list_search = [' drug used for', ' used for', ' medicine given for', '']
    dict_temp = {}
    try:
        for i in list_search:
            time.sleep(.900)
            resp = requests.get('https://www.google.com/search?q={}'.format(
                cmdecode.lower().title() + i), headers=headers, verify=False)
            soup = BeautifulSoup(resp.text, 'lxml')
            print("inside", aedecode, cm_term)
            if re.search(aedecode.lower(), soup.decode("utf-8")) or re.search(ae_term.lower(), soup.decode("utf-8")):
                print("success")
                dict_temp['cmtrt'] = cm_term.upper()
                dict_temp['subject'] = subject
                dict_temp['study'] = study
                if type(ae_term) != float and ae_term != 'blank':
                    dict_temp['aeterm'] = ae_term.upper()
                if type(aedecode) != float and aedecode != 'blank':
                    dict_temp['aeptterm'] = aedecode.upper()

                if AEPTCD != '':
                    aept = AEPTCD
                    dict_temp['aeptcode'] = aept

                if AE_LLT_CODE != '':
                    aellt = AE_LLT_CODE
                    dict_temp['aelltcode'] = aellt
                dict_temp['valid'] = 1
                # print(fall_back_table.shape)
                print('google search', dict_temp)
                fall_back_table = fall_back_table.append(
                    dict_temp, ignore_index=True)
                print(fall_back_table.shape)
                fall_back_table.to_csv(main_path/filename, index=False)

                return True
        return False
    except Exception as e:
        print(traceback.format_exc())
        return False
    
def get_similarity_data():
    s3_path='subcats/similarity/data.zip'
    s3_client = boto3.client('s3')
    zip_file_path = main_path/'data.zip'
    
    if data_path.exists():
        print("Similarity - Local data folder is already present")
        return
    
    try:
        s3_client.download_file(Bucket=S3_BUCKET,Key=s3_path,Filename=str(zip_file_path))
        data_path.mkdir(parents=True, exist_ok=False)
    except Exception:
        print("Similarity - Data zip file is not found in S3")
        return
    else:
        print("Similarity - Local data folder is created")

    with zipfile.ZipFile(zip_file_path,'r') as zip_ref:
        zip_ref.extractall(data_path)

    zip_file_path.unlink()  


# def main():
#     load_fdaopenlabel(filepath=fdalabels_path)
#     load_who_dict(filepath=whodrug_path)
#     fda_openlabel = utils.load_fdaopenlabel()
#     who_drug_term, who_drug_code = utils.load_who_dict()
#     all_diagnosis_meddra_dict = utils.load_full_meddra_precompute()
#     full_flatten_meddra_hierarchy = utils.load_flat_meddra_precompute()
#     llt_dict, pt_dict, hlt_dict, hlgt_dict, soc_dict = utils.load_meddra_dictionaries(
#                                                                             version='Version 24.0')

# if __name__ == '__main__':
#     print('just print inside main function')
#     main()