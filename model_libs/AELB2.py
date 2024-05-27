# uncompyle6 version 3.9.0
# Python bytecode version base 3.6 (3379)
# Decompiled from: Python 3.10.1 (v3.10.1:2cd268a3a9, Dec  6 2021, 14:28:59) [Clang 13.0.0 (clang-1300.0.29.3)]
# Embedded file name: AELB2.py
# Compiled at: 2020-11-10 13:17:27
# Size of source mod 2**32: 8466 bytes
import pandas as pd, sys, re, numpy as np, warnings
warnings.filterwarnings('ignore')
try:
    from scripts.SubcatModels.model_libs.base import BaseSDQApi
    import scripts.SubcatModels.model_libs.utilsk as utils
except:
    from base import BaseSDQApi
    import utilsk as utils

import traceback, tqdm, logging, yaml, os
curr_file_path = os.path.realpath(__file__)
curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
subcat_config_path = os.path.join(curr_path, 'subcate_config.yml')
a = yaml.load(open(subcat_config_path, 'r'))

class AELB2(BaseSDQApi):
    domain_list = [
     'AE', 'LB']

    def execute--- This code section failed: ---

 L.  27         0  LOAD_FAST                'self'
                2  LOAD_ATTR                study_id
                4  STORE_FAST               'study'

 L.  29         6  LOAD_STR                 'AELB2'
                8  STORE_FAST               'sub_cat'

 L.  31        10  LOAD_FAST                'sub_cat'
               12  LOAD_ATTR                startswith
               14  LOAD_STR                 'DR'
               16  CALL_FUNCTION_1       1  '1 positional argument'
               18  POP_JUMP_IF_FALSE    24  'to 24'
               20  LOAD_STR                 'itemrepn'
               22  JUMP_FORWARD         26  'to 26'
               24  ELSE                     '26'
               24  LOAD_STR                 'form_index'
             26_0  COME_FROM            22  '22'
               26  STORE_FAST               'index_col'

 L.  32        28  LOAD_FAST                'self'
               30  LOAD_ATTR                get_subjects
               32  LOAD_FAST                'study'
               34  LOAD_FAST                'self'
               36  LOAD_ATTR                domain_list
               38  LOAD_CONST               10000
               40  LOAD_CONST               ('domain_list', 'per_page')
               42  CALL_FUNCTION_KW_3     3  '3 total positional and keyword args'
               44  STORE_FAST               'subjects'

 L.  34     46_48  SETUP_LOOP         1210  'to 1210'
               50  LOAD_GLOBAL              tqdm
               52  LOAD_ATTR                tqdm
               54  LOAD_FAST                'subjects'
               56  CALL_FUNCTION_1       1  '1 positional argument'
               58  GET_ITER         
            60_62  FOR_ITER           1208  'to 1208'
               64  STORE_FAST               'subject'

 L.  35     66_68  SETUP_EXCEPT       1158  'to 1158'

 L.  36        70  LOAD_FAST                'self'
               72  LOAD_ATTR                get_flatten_data
               74  LOAD_FAST                'study'
               76  LOAD_FAST                'subject'
               78  LOAD_CONST               10000
               80  LOAD_FAST                'self'
               82  LOAD_ATTR                domain_list
               84  LOAD_CONST               ('per_page', 'domain_list')
               86  CALL_FUNCTION_KW_4     4  '4 total positional and keyword args'
               88  STORE_FAST               'flatten_data'

 L.  38        90  LOAD_STR                 'AE'
               92  LOAD_FAST                'flatten_data'
               94  COMPARE_OP               not-in
               96  POP_JUMP_IF_TRUE    106  'to 106'
               98  LOAD_STR                 'LB'
              100  LOAD_FAST                'flatten_data'
              102  COMPARE_OP               not-in
            104_0  COME_FROM            96  '96'
              104  POP_JUMP_IF_FALSE   108  'to 108'

 L.  39       106  CONTINUE_LOOP        60  'to 60'
            108_0  COME_FROM           104  '104'

 L.  41       108  LOAD_GLOBAL              pd
              110  LOAD_ATTR                DataFrame
              112  LOAD_FAST                'flatten_data'
              114  LOAD_STR                 'AE'
              116  BINARY_SUBSCR    
              118  CALL_FUNCTION_1       1  '1 positional argument'
              120  STORE_FAST               'ae_df'

 L.  43       122  LOAD_GLOBAL              pd
              124  LOAD_ATTR                DataFrame
              126  LOAD_FAST                'flatten_data'
              128  LOAD_STR                 'LB'
              130  BINARY_SUBSCR    
              132  CALL_FUNCTION_1       1  '1 positional argument'
              134  STORE_FAST               'lb_df'

 L.  45       136  LOAD_FAST                'ae_df'
              138  LOAD_FAST                'ae_df'
              140  LOAD_STR                 'AESTDAT'
              142  BINARY_SUBSCR    
              144  LOAD_ATTR                notna
              146  CALL_FUNCTION_0       0  '0 positional arguments'
              148  LOAD_FAST                'ae_df'
              150  LOAD_STR                 'AEENDAT'
              152  BINARY_SUBSCR    
              154  LOAD_ATTR                notna
              156  CALL_FUNCTION_0       0  '0 positional arguments'
              158  BINARY_AND       
              160  BINARY_SUBSCR    
              162  STORE_FAST               'ae_df'

 L.  46       164  LOAD_FAST                'lb_df'
              166  LOAD_FAST                'lb_df'
              168  LOAD_STR                 'LBDAT'
              170  BINARY_SUBSCR    
              172  LOAD_ATTR                notna
              174  CALL_FUNCTION_0       0  '0 positional arguments'
              176  BINARY_SUBSCR    
              178  STORE_FAST               'lb_df'

 L.  47       180  LOAD_FAST                'lb_df'
              182  LOAD_STR                 'LBTEST'
              184  BINARY_SUBSCR    
              186  LOAD_ATTR                str
              188  LOAD_ATTR                upper
              190  CALL_FUNCTION_0       0  '0 positional arguments'
              192  LOAD_FAST                'lb_df'
              194  LOAD_STR                 'LBTEST'
              196  STORE_SUBSCR     

 L.  48       198  LOAD_FAST                'lb_df'
              200  LOAD_FAST                'lb_df'
              202  LOAD_STR                 'LBTEST'
              204  BINARY_SUBSCR    
              206  LOAD_STR                 'HEMOGLOBIN_PX87'
              208  COMPARE_OP               !=
              210  BINARY_SUBSCR    
              212  STORE_FAST               'lb_df'

 L.  50   214_216  SETUP_LOOP         1154  'to 1154'
              218  LOAD_GLOBAL              range
              220  LOAD_FAST                'ae_df'
              222  LOAD_ATTR                shape
              224  LOAD_CONST               0
              226  BINARY_SUBSCR    
              228  CALL_FUNCTION_1       1  '1 positional argument'
              230  GET_ITER         
          232_234  FOR_ITER           1152  'to 1152'
              236  STORE_FAST               'ind'

 L.  51   238_240  SETUP_EXCEPT       1136  'to 1136'

 L.  52       242  LOAD_FAST                'ae_df'
              244  LOAD_ATTR                iloc
              246  LOAD_FAST                'ind'
              248  BUILD_LIST_1          1 
              250  BINARY_SUBSCR    
              252  STORE_FAST               'ae_record'

 L.  53       254  LOAD_FAST                'ae_df'
              256  LOAD_FAST                'ae_df'
              258  LOAD_STR                 'AEPTCD'
              260  BINARY_SUBSCR    
              262  LOAD_ATTR                isin
              264  LOAD_STR                 ''
              266  LOAD_STR                 ' '
              268  LOAD_GLOBAL              np
              270  LOAD_ATTR                nan
              272  BUILD_LIST_3          3 
              274  CALL_FUNCTION_1       1  '1 positional argument'
              276  UNARY_INVERT     
              278  BINARY_SUBSCR    
              280  STORE_FAST               'ae_df'

 L.  54       282  LOAD_FAST                'ae_record'
              284  LOAD_STR                 'AEPTCD'
              286  BINARY_SUBSCR    
              288  LOAD_ATTR                values
              290  LOAD_CONST               0
              292  BINARY_SUBSCR    
              294  STORE_FAST               'code'

 L.  55       296  LOAD_FAST                'ae_record'
              298  LOAD_STR                 'form_index'
              300  BINARY_SUBSCR    
              302  LOAD_ATTR                values
              304  LOAD_CONST               0
              306  BINARY_SUBSCR    
              308  STORE_FAST               'formindex'

 L.  56       310  LOAD_FAST                'ae_record'
              312  LOAD_STR                 'AESPID'
              314  BINARY_SUBSCR    
              316  LOAD_ATTR                values
              318  LOAD_CONST               0
              320  BINARY_SUBSCR    
              322  STORE_FAST               'aespid'

 L.  58       324  LOAD_FAST                'lb_df'
              326  LOAD_STR                 'LBTEST'
              328  BINARY_SUBSCR    
              330  LOAD_ATTR                unique
              332  CALL_FUNCTION_0       0  '0 positional arguments'
              334  LOAD_ATTR                tolist
              336  CALL_FUNCTION_0       0  '0 positional arguments'
              338  STORE_FAST               'tests'

 L.  60   340_342  SETUP_LOOP         1132  'to 1132'
              344  LOAD_FAST                'tests'
              346  GET_ITER         
          348_350  FOR_ITER           1130  'to 1130'
              352  STORE_FAST               'test'

 L.  62       354  LOAD_GLOBAL              utils
              356  LOAD_ATTR                check_aelab
              358  LOAD_GLOBAL              float
              360  LOAD_FAST                'code'
              362  CALL_FUNCTION_1       1  '1 positional argument'
              364  LOAD_FAST                'test'
              366  CALL_FUNCTION_2       2  '2 positional arguments'
              368  LOAD_CONST               True
              370  COMPARE_OP               ==
          372_374  POP_JUMP_IF_FALSE   348  'to 348'

 L.  63       376  LOAD_FAST                'lb_df'
              378  LOAD_FAST                'lb_df'
              380  LOAD_STR                 'LBTEST'
              382  BINARY_SUBSCR    
              384  LOAD_FAST                'test'
              386  COMPARE_OP               ==
              388  BINARY_SUBSCR    
              390  STORE_FAST               'lb_records'

 L.  65   392_394  SETUP_LOOP         1126  'to 1126'
              396  LOAD_GLOBAL              range
              398  LOAD_FAST                'lb_records'
              400  LOAD_ATTR                shape
              402  LOAD_CONST               0
              404  BINARY_SUBSCR    
              406  CALL_FUNCTION_1       1  '1 positional argument'
              408  GET_ITER         
          410_412  FOR_ITER           1124  'to 1124'
              414  STORE_FAST               'ind1'

 L.  66   416_418  SETUP_EXCEPT       1104  'to 1104'

 L.  67       420  LOAD_FAST                'lb_records'
              422  LOAD_ATTR                iloc
              424  LOAD_FAST                'ind1'
              426  BUILD_LIST_1          1 
              428  BINARY_SUBSCR    
              430  STORE_FAST               'lb_record'

 L.  69       432  LOAD_FAST                'ae_record'
              434  LOAD_STR                 'AESTDAT'
              436  BINARY_SUBSCR    
              438  LOAD_ATTR                apply
              440  LOAD_GLOBAL              utils
              442  LOAD_ATTR                get_date
              444  CALL_FUNCTION_1       1  '1 positional argument'
              446  STORE_FAST               'aestdat'

 L.  70       448  LOAD_FAST                'ae_record'
              450  LOAD_STR                 'AEENDAT'
              452  BINARY_SUBSCR    
              454  LOAD_ATTR                apply
              456  LOAD_GLOBAL              utils
              458  LOAD_ATTR                get_date
              460  CALL_FUNCTION_1       1  '1 positional argument'
              462  STORE_FAST               'aeendat'

 L.  71       464  LOAD_FAST                'ae_record'
              466  LOAD_STR                 'AEONGO'
              468  BINARY_SUBSCR    
              470  LOAD_ATTR                values
              472  LOAD_CONST               0
              474  BINARY_SUBSCR    
              476  STORE_FAST               'aeongo'

 L.  72       478  LOAD_GLOBAL              float
              480  LOAD_FAST                'lb_record'
              482  LOAD_STR                 'LBORRES'
              484  BINARY_SUBSCR    
              486  LOAD_ATTR                values
              488  LOAD_CONST               0
              490  BINARY_SUBSCR    
              492  CALL_FUNCTION_1       1  '1 positional argument'
              494  STORE_FAST               'result'

 L.  73       496  LOAD_FAST                'lb_record'
              498  LOAD_STR                 'LBSTAT'
              500  BINARY_SUBSCR    
              502  LOAD_ATTR                values
              504  LOAD_CONST               0
              506  BINARY_SUBSCR    
              508  STORE_FAST               'status'

 L.  74       510  LOAD_FAST                'lb_record'
              512  LOAD_STR                 'LBDAT'
              514  BINARY_SUBSCR    
              516  LOAD_ATTR                apply
              518  LOAD_GLOBAL              utils
              520  LOAD_ATTR                get_date
              522  CALL_FUNCTION_1       1  '1 positional argument'
              524  STORE_FAST               'labdate'

 L.  75       526  LOAD_GLOBAL              float
              528  LOAD_FAST                'lb_record'
              530  LOAD_STR                 'LNMTLOW'
              532  BINARY_SUBSCR    
              534  LOAD_ATTR                values
              536  LOAD_CONST               0
              538  BINARY_SUBSCR    
              540  CALL_FUNCTION_1       1  '1 positional argument'
              542  STORE_FAST               'low'

 L.  76       544  LOAD_GLOBAL              float
              546  LOAD_FAST                'lb_record'
              548  LOAD_STR                 'LNMTHIGH'
              550  BINARY_SUBSCR    
              552  LOAD_ATTR                values
              554  LOAD_CONST               0
              556  BINARY_SUBSCR    
              558  CALL_FUNCTION_1       1  '1 positional argument'
              560  STORE_FAST               'high'

 L.  78       562  LOAD_FAST                'aeongo'
              564  LOAD_ATTR                upper
              566  CALL_FUNCTION_0       0  '0 positional arguments'
              568  LOAD_STR                 'YES'
              570  COMPARE_OP               ==
          572_574  POP_JUMP_IF_FALSE   598  'to 598'
              576  LOAD_FAST                'labdate'
              578  LOAD_ATTR                values
              580  LOAD_CONST               0
              582  BINARY_SUBSCR    
              584  LOAD_FAST                'aestdat'
              586  LOAD_ATTR                values
              588  LOAD_CONST               0
              590  BINARY_SUBSCR    
              592  COMPARE_OP               >=
            594_0  COME_FROM           572  '572'
          594_596  POP_JUMP_IF_TRUE    656  'to 656'
              598  LOAD_FAST                'aeongo'
              600  LOAD_ATTR                upper
              602  CALL_FUNCTION_0       0  '0 positional arguments'
              604  LOAD_STR                 'NO'
              606  COMPARE_OP               ==
          608_610  POP_JUMP_IF_FALSE  1100  'to 1100'
              612  LOAD_FAST                'labdate'
              614  LOAD_ATTR                values
              616  LOAD_CONST               0
              618  BINARY_SUBSCR    
              620  LOAD_FAST                'aestdat'
              622  LOAD_ATTR                values
              624  LOAD_CONST               0
              626  BINARY_SUBSCR    
              628  COMPARE_OP               >=
          630_632  POP_JUMP_IF_FALSE  1100  'to 1100'
              634  LOAD_FAST                'labdate'
              636  LOAD_ATTR                values
              638  LOAD_CONST               0
              640  BINARY_SUBSCR    
              642  LOAD_FAST                'aeendat'
              644  LOAD_ATTR                values
              646  LOAD_CONST               0
              648  BINARY_SUBSCR    
              650  COMPARE_OP               <
            652_0  COME_FROM           630  '630'
            652_1  COME_FROM           608  '608'
            652_2  COME_FROM           594  '594'
          652_654  POP_JUMP_IF_FALSE  1100  'to 1100'

 L.  79       656  LOAD_FAST                'result'
              658  LOAD_FAST                'low'
              660  COMPARE_OP               >=
          662_664  POP_JUMP_IF_FALSE  1100  'to 1100'
              666  LOAD_FAST                'result'
              668  LOAD_FAST                'high'
              670  COMPARE_OP               <=
          672_674  POP_JUMP_IF_FALSE  1100  'to 1100'

 L.  81       676  BUILD_MAP_0           0 
              678  STORE_FAST               'subcate_report_dict'

 L.  82       680  BUILD_MAP_0           0 
              682  STORE_FAST               'report_dict'

 L.  84       684  LOAD_FAST                'ae_record'
              686  LOAD_FAST                'lb_record'
              688  LOAD_CONST               ('AE', 'LB')
              690  BUILD_CONST_KEY_MAP_2     2 
              692  STORE_FAST               'piv_rec'

 L.  86       694  SETUP_LOOP          808  'to 808'
              696  LOAD_GLOBAL              a
              698  LOAD_STR                 'FIELDS_FOR_UI'
              700  BINARY_SUBSCR    
              702  LOAD_FAST                'sub_cat'
              704  BINARY_SUBSCR    
              706  LOAD_ATTR                items
              708  CALL_FUNCTION_0       0  '0 positional arguments'
              710  GET_ITER         
              712  FOR_ITER            806  'to 806'
              714  UNPACK_SEQUENCE_2     2 
              716  STORE_FAST               'dom'
              718  STORE_FAST               'cols'

 L.  87       720  LOAD_FAST                'piv_rec'
              722  LOAD_FAST                'dom'
              724  BINARY_SUBSCR    
              726  STORE_DEREF              'piv_df'

 L.  88       728  LOAD_CLOSURE             'piv_df'
              730  BUILD_TUPLE_1         1 
              732  LOAD_LISTCOMP            '<code_object <listcomp>>'
              734  LOAD_STR                 'AELB2.execute.<locals>.<listcomp>'
              736  MAKE_FUNCTION_CLOSURE        'closure'
              738  LOAD_FAST                'cols'
              740  GET_ITER         
              742  CALL_FUNCTION_1       1  '1 positional argument'
              744  STORE_FAST               'present_col'

 L.  89       746  LOAD_DEREF               'piv_df'
              748  LOAD_FAST                'present_col'
              750  BINARY_SUBSCR    
              752  STORE_FAST               'rep_df'

 L.  90       754  LOAD_GLOBAL              utils
              756  LOAD_ATTR                get_deeplink
              758  LOAD_FAST                'study'
              760  LOAD_DEREF               'piv_df'
              762  CALL_FUNCTION_2       2  '2 positional arguments'
              764  LOAD_FAST                'rep_df'
              766  LOAD_STR                 'deeplink'
              768  STORE_SUBSCR     

 L.  91       770  LOAD_FAST                'rep_df'
              772  LOAD_ATTR                rename
              774  LOAD_GLOBAL              a
              776  LOAD_STR                 'FIELD_NAME_DICT'
              778  BINARY_SUBSCR    
              780  LOAD_CONST               ('columns',)
              782  CALL_FUNCTION_KW_1     1  '1 total positional and keyword args'
              784  STORE_FAST               'rep_df'

 L.  92       786  LOAD_FAST                'rep_df'
              788  LOAD_ATTR                to_json
              790  LOAD_STR                 'records'
              792  LOAD_CONST               ('orient',)
              794  CALL_FUNCTION_KW_1     1  '1 total positional and keyword args'
              796  LOAD_FAST                'report_dict'
              798  LOAD_FAST                'dom'
              800  STORE_SUBSCR     
          802_804  JUMP_BACK           712  'to 712'
              806  POP_BLOCK        
            808_0  COME_FROM_LOOP      694  '694'

 L.  94       808  LOAD_FAST                'report_dict'
              810  LOAD_FAST                'subcate_report_dict'
              812  LOAD_FAST                'sub_cat'
              814  STORE_SUBSCR     

 L.  96       816  LOAD_FAST                'ae_record'
              818  LOAD_STR                 'AESPID'
              820  BINARY_SUBSCR    
              822  LOAD_ATTR                values
              824  LOAD_CONST               0
              826  BINARY_SUBSCR    
              828  STORE_FAST               'aespid'

 L.  97       830  LOAD_FAST                'ae_record'
              832  LOAD_STR                 'AETERM'
              834  BINARY_SUBSCR    
              836  LOAD_ATTR                values
              838  LOAD_CONST               0
              840  BINARY_SUBSCR    
              842  STORE_FAST               'aeterm'

 L.  98       844  LOAD_FAST                'ae_record'
              846  LOAD_STR                 'AESTDAT'
              848  BINARY_SUBSCR    
              850  LOAD_ATTR                values
              852  LOAD_CONST               0
              854  BINARY_SUBSCR    
              856  STORE_FAST               'aestdat'

 L.  99       858  LOAD_FAST                'ae_record'
              860  LOAD_STR                 'AEENDAT'
              862  BINARY_SUBSCR    
              864  LOAD_ATTR                values
              866  LOAD_CONST               0
              868  BINARY_SUBSCR    
              870  STORE_FAST               'aeendat'

 L. 100       872  LOAD_FAST                'lb_record'
              874  LOAD_STR                 'LBTEST'
              876  BINARY_SUBSCR    
              878  LOAD_ATTR                values
              880  LOAD_CONST               0
              882  BINARY_SUBSCR    
              884  STORE_FAST               'labtest'

 L. 101       886  LOAD_FAST                'lb_record'
              888  LOAD_STR                 'LBDAT'
              890  BINARY_SUBSCR    
              892  LOAD_ATTR                values
              894  LOAD_CONST               0
              896  BINARY_SUBSCR    
              898  STORE_FAST               'labdate'

 L. 102       900  LOAD_FAST                'aeongo'
              902  LOAD_GLOBAL              np
              904  LOAD_ATTR                nan
              906  COMPARE_OP               ==
          908_910  POP_JUMP_IF_FALSE   916  'to 916'

 L. 103       912  LOAD_STR                 'blank'
              914  STORE_FAST               'aeongo'
            916_0  COME_FROM           908  '908'

 L. 104       916  LOAD_FAST                'ae_record'
              918  LOAD_ATTR                iloc
              920  LOAD_CONST               0
              922  BINARY_SUBSCR    
              924  STORE_FAST               'ae_record1'

 L. 105       926  LOAD_FAST                'labtest'
              928  LOAD_ATTR                split
              930  LOAD_STR                 '_'
              932  CALL_FUNCTION_1       1  '1 positional argument'
              934  LOAD_CONST               0
              936  BINARY_SUBSCR    
              938  STORE_FAST               'labtest'

 L. 108       940  LOAD_FAST                'aespid'

 L. 109       942  LOAD_FAST                'aeterm'

 L. 110       944  LOAD_GLOBAL              str
              946  LOAD_FAST                'aestdat'
              948  LOAD_ATTR                values
              950  LOAD_CONST               0
              952  BINARY_SUBSCR    
              954  CALL_FUNCTION_1       1  '1 positional argument'
              956  LOAD_ATTR                split
              958  LOAD_STR                 'T'
              960  CALL_FUNCTION_1       1  '1 positional argument'
              962  LOAD_CONST               0
              964  BINARY_SUBSCR    

 L. 111       966  LOAD_GLOBAL              str
              968  LOAD_FAST                'aeendat'
              970  LOAD_ATTR                values
              972  LOAD_CONST               0
              974  BINARY_SUBSCR    
              976  CALL_FUNCTION_1       1  '1 positional argument'
              978  LOAD_ATTR                split
              980  LOAD_STR                 'T'
              982  CALL_FUNCTION_1       1  '1 positional argument'
              984  LOAD_CONST               0
              986  BINARY_SUBSCR    

 L. 112       988  LOAD_FAST                'aeongo'

 L. 113       990  LOAD_FAST                'labtest'

 L. 114       992  LOAD_FAST                'labdate'
              994  LOAD_CONST               ('AESPID', 'AETERM', 'AESTDAT', 'AEENDAT', 'AEONGO', 'LBTEST', 'LBDAT')
              996  BUILD_CONST_KEY_MAP_7     7 
              998  STORE_FAST               'keys'

 L. 118      1000  LOAD_FAST                'sub_cat'

 L. 119      1002  LOAD_FAST                'self'
             1004  LOAD_ATTR                get_model_query_text_json
             1006  LOAD_FAST                'study'
             1008  LOAD_FAST                'sub_cat'
             1010  LOAD_FAST                'keys'
             1012  LOAD_CONST               ('params',)
             1014  CALL_FUNCTION_KW_3     3  '3 total positional and keyword args'

 L. 120      1016  LOAD_GLOBAL              str
             1018  LOAD_FAST                'ae_record1'
             1020  LOAD_STR                 'form_index'
             1022  BINARY_SUBSCR    
             1024  CALL_FUNCTION_1       1  '1 positional argument'

 L. 121      1026  LOAD_FAST                'self'
             1028  LOAD_ATTR                get_subcategory_json
             1030  LOAD_FAST                'study'
             1032  LOAD_FAST                'sub_cat'
             1034  CALL_FUNCTION_2       2  '2 positional arguments'

 L. 122      1036  LOAD_GLOBAL              str
             1038  LOAD_FAST                'ae_record1'
             1040  LOAD_STR                 'modif_dts'
             1042  BINARY_SUBSCR    
             1044  CALL_FUNCTION_1       1  '1 positional argument'

 L. 123      1046  LOAD_GLOBAL              int
             1048  LOAD_FAST                'ae_record1'
             1050  LOAD_STR                 'ck_event_id'
             1052  BINARY_SUBSCR    
             1054  CALL_FUNCTION_1       1  '1 positional argument'

 L. 124      1056  LOAD_GLOBAL              str
             1058  LOAD_FAST                'ae_record1'
             1060  LOAD_STR                 'formrefname'
             1062  BINARY_SUBSCR    
             1064  CALL_FUNCTION_1       1  '1 positional argument'

 L. 125      1066  LOAD_FAST                'subcate_report_dict'

 L. 126      1068  LOAD_GLOBAL              np
             1070  LOAD_ATTR                random
             1072  LOAD_ATTR                uniform
             1074  LOAD_CONST               0.7
             1076  LOAD_CONST               0.97
             1078  CALL_FUNCTION_2       2  '2 positional arguments'
             1080  LOAD_CONST               ('subcategory', 'query_text', 'form_index', 'question_present', 'modif_dts', 'stg_ck_event_id', 'formrefname', 'report', 'confid_score')
             1082  BUILD_CONST_KEY_MAP_9     9 
             1084  STORE_FAST               'payload'

 L. 129      1086  LOAD_FAST                'self'
             1088  LOAD_ATTR                insert_query
             1090  LOAD_FAST                'study'
             1092  LOAD_FAST                'subject'
             1094  LOAD_FAST                'payload'
             1096  CALL_FUNCTION_3       3  '3 positional arguments'
             1098  POP_TOP          
           1100_0  COME_FROM           672  '672'
           1100_1  COME_FROM           662  '662'
           1100_2  COME_FROM           652  '652'
             1100  POP_BLOCK        
             1102  JUMP_FORWARD       1120  'to 1120'
           1104_0  COME_FROM_EXCEPT    416  '416'

 L. 130      1104  POP_TOP          
             1106  POP_TOP          
             1108  POP_TOP          

 L. 134  1110_1112  CONTINUE_LOOP       410  'to 410'
             1114  POP_EXCEPT       
             1116  JUMP_FORWARD       1120  'to 1120'
             1118  END_FINALLY      
           1120_0  COME_FROM          1116  '1116'
           1120_1  COME_FROM          1102  '1102'
         1120_1122  JUMP_BACK           410  'to 410'
             1124  POP_BLOCK        
           1126_0  COME_FROM_LOOP      392  '392'
         1126_1128  JUMP_BACK           348  'to 348'
             1130  POP_BLOCK        
           1132_0  COME_FROM_LOOP      340  '340'
             1132  POP_BLOCK        
             1134  JUMP_BACK           232  'to 232'
           1136_0  COME_FROM_EXCEPT    238  '238'

 L. 136      1136  POP_TOP          
             1138  POP_TOP          
             1140  POP_TOP          

 L. 138      1142  CONTINUE_LOOP       232  'to 232'
             1144  POP_EXCEPT       
             1146  JUMP_BACK           232  'to 232'
             1148  END_FINALLY      
             1150  JUMP_BACK           232  'to 232'
             1152  POP_BLOCK        
           1154_0  COME_FROM_LOOP      214  '214'
             1154  POP_BLOCK        
             1156  JUMP_BACK            60  'to 60'
           1158_0  COME_FROM_EXCEPT     66  '66'

 L. 140      1158  DUP_TOP          
             1160  LOAD_GLOBAL              Exception
             1162  COMPARE_OP               exception-match
         1164_1166  POP_JUMP_IF_FALSE  1204  'to 1204'
             1168  POP_TOP          
             1170  STORE_FAST               'e'
             1172  POP_TOP          
             1174  SETUP_FINALLY      1194  'to 1194'

 L. 141      1176  LOAD_GLOBAL              logging
             1178  LOAD_ATTR                exception
             1180  LOAD_FAST                'e'
             1182  CALL_FUNCTION_1       1  '1 positional argument'
             1184  POP_TOP          

 L. 143      1186  CONTINUE_LOOP        60  'to 60'
             1188  POP_BLOCK        
             1190  POP_EXCEPT       
             1192  LOAD_CONST               None
           1194_0  COME_FROM_FINALLY  1174  '1174'
             1194  LOAD_CONST               None
             1196  STORE_FAST               'e'
             1198  DELETE_FAST              'e'
             1200  END_FINALLY      
             1202  JUMP_BACK            60  'to 60'
             1204  END_FINALLY      
             1206  JUMP_BACK            60  'to 60'
             1208  POP_BLOCK        
           1210_0  COME_FROM_LOOP       46  '46'

Parse error at or near `POP_JUMP_IF_TRUE' instruction at offset 594_596


if __name__ == '__main__':
    study_id = sys.argv[1]
    rule_id = sys.argv[2]
    version = sys.argv[3]
    rule = AELB2(study_id, rule_id, version)
    rule.run()