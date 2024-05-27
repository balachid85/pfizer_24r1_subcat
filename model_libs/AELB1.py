# uncompyle6 version 3.9.0
# Python bytecode version base 3.6 (3379)
# Decompiled from: Python 3.10.1 (v3.10.1:2cd268a3a9, Dec  6 2021, 14:28:59) [Clang 13.0.0 (clang-1300.0.29.3)]
# Embedded file name: AELB1.py
# Compiled at: 2021-01-21 10:29:07
# Size of source mod 2**32: 10756 bytes
import pandas as pd, sys, re, warnings
warnings.filterwarnings('ignore')
try:
    from scripts.SubcatModels.model_libs.base import BaseSDQApi
    import scripts.SubcatModels.model_libs.utilsk as utils
except:
    from base import BaseSDQApi
    import utilsk as utils

import traceback, tqdm, logging, numpy as np, yaml, os
curr_file_path = os.path.realpath(__file__)
curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
subcat_config_path = os.path.join(curr_path, 'subcate_config.yml')
a = yaml.load(open(subcat_config_path, 'r'))

class AELB1(BaseSDQApi):
    domain_list = [
     'AE', 'LB']

    def execute--- This code section failed: ---

 L.  27         0  LOAD_FAST                'self'
                2  LOAD_ATTR                study_id
                4  STORE_FAST               'study'

 L.  29         6  LOAD_STR                 'AELB1'
                8  STORE_FAST               'sub_cat'

 L.  30        10  LOAD_FAST                'sub_cat'
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

 L.  31        28  LOAD_FAST                'self'
               30  LOAD_ATTR                get_subjects
               32  LOAD_FAST                'study'
               34  LOAD_FAST                'self'
               36  LOAD_ATTR                domain_list
               38  LOAD_CONST               10000
               40  LOAD_CONST               ('domain_list', 'per_page')
               42  CALL_FUNCTION_KW_3     3  '3 total positional and keyword args'
               44  STORE_FAST               'subjects'

 L.  33     46_48  SETUP_LOOP         1576  'to 1576'
               50  LOAD_GLOBAL              tqdm
               52  LOAD_ATTR                tqdm
               54  LOAD_FAST                'subjects'
               56  CALL_FUNCTION_1       1  '1 positional argument'
               58  GET_ITER         
            60_62  FOR_ITER           1574  'to 1574'
               64  STORE_FAST               'subject'

 L.  34     66_68  SETUP_EXCEPT       1526  'to 1526'

 L.  35        70  LOAD_FAST                'self'
               72  LOAD_ATTR                get_flatten_data
               74  LOAD_FAST                'study'
               76  LOAD_FAST                'subject'
               78  LOAD_CONST               10000
               80  LOAD_FAST                'self'
               82  LOAD_ATTR                domain_list
               84  LOAD_CONST               ('per_page', 'domain_list')
               86  CALL_FUNCTION_KW_4     4  '4 total positional and keyword args'
               88  STORE_FAST               'flatten_data'

 L.  36        90  LOAD_STR                 'AE'
               92  LOAD_FAST                'flatten_data'
               94  COMPARE_OP               not-in
               96  POP_JUMP_IF_TRUE    106  'to 106'
               98  LOAD_STR                 'LB'
              100  LOAD_FAST                'flatten_data'
              102  COMPARE_OP               not-in
            104_0  COME_FROM            96  '96'
              104  POP_JUMP_IF_FALSE   108  'to 108'

 L.  37       106  CONTINUE_LOOP        60  'to 60'
            108_0  COME_FROM           104  '104'

 L.  40       108  LOAD_GLOBAL              pd
              110  LOAD_ATTR                DataFrame
              112  LOAD_FAST                'flatten_data'
              114  LOAD_STR                 'AE'
              116  BINARY_SUBSCR    
              118  CALL_FUNCTION_1       1  '1 positional argument'
              120  STORE_FAST               'ae_df'

 L.  41       122  LOAD_FAST                'ae_df'
              124  LOAD_FAST                'ae_df'
              126  LOAD_STR                 'AEPTCD'
              128  BINARY_SUBSCR    
              130  LOAD_ATTR                isin
              132  LOAD_STR                 ''
              134  LOAD_STR                 ' '
              136  LOAD_GLOBAL              np
              138  LOAD_ATTR                nan
              140  BUILD_LIST_3          3 
              142  CALL_FUNCTION_1       1  '1 positional argument'
              144  UNARY_INVERT     
              146  BINARY_SUBSCR    
              148  STORE_FAST               'ae_df'

 L.  42       150  LOAD_GLOBAL              pd
              152  LOAD_ATTR                DataFrame
              154  LOAD_FAST                'flatten_data'
              156  LOAD_STR                 'LB'
              158  BINARY_SUBSCR    
              160  CALL_FUNCTION_1       1  '1 positional argument'
              162  STORE_FAST               'lb_df'

 L.  44   164_166  SETUP_LOOP         1522  'to 1522'
              168  LOAD_GLOBAL              range
              170  LOAD_FAST                'ae_df'
              172  LOAD_ATTR                shape
              174  LOAD_CONST               0
              176  BINARY_SUBSCR    
              178  CALL_FUNCTION_1       1  '1 positional argument'
              180  GET_ITER         
          182_184  FOR_ITER           1520  'to 1520'
              186  STORE_FAST               'ind'

 L.  45   188_190  SETUP_EXCEPT       1504  'to 1504'

 L.  46       192  LOAD_FAST                'ae_df'
              194  LOAD_ATTR                iloc
              196  LOAD_FAST                'ind'
              198  BUILD_LIST_1          1 
              200  BINARY_SUBSCR    
              202  STORE_FAST               'ae_record'

 L.  47       204  LOAD_FAST                'ae_record'
              206  STORE_FAST               'ae_record11'

 L.  49       208  LOAD_FAST                'ae_record'
              210  LOAD_STR                 'AEPTCD'
              212  BINARY_SUBSCR    
              214  LOAD_ATTR                values
              216  LOAD_CONST               0
              218  BINARY_SUBSCR    
              220  STORE_FAST               'code'

 L.  50       222  LOAD_FAST                'ae_record'
              224  LOAD_STR                 'form_index'
              226  BINARY_SUBSCR    
              228  LOAD_ATTR                values
              230  LOAD_CONST               0
              232  BINARY_SUBSCR    
              234  STORE_FAST               'formindex'

 L.  51       236  LOAD_FAST                'ae_record'
              238  LOAD_STR                 'AESPID'
              240  BINARY_SUBSCR    
              242  LOAD_ATTR                values
              244  LOAD_CONST               0
              246  BINARY_SUBSCR    
              248  STORE_FAST               'aespid'

 L.  53       250  LOAD_FAST                'lb_df'
              252  LOAD_STR                 'LBTEST'
              254  BINARY_SUBSCR    
              256  LOAD_ATTR                str
              258  LOAD_ATTR                upper
              260  CALL_FUNCTION_0       0  '0 positional arguments'
              262  LOAD_FAST                'lb_df'
              264  LOAD_STR                 'LBTEST'
              266  STORE_SUBSCR     

 L.  54       268  LOAD_FAST                'lb_df'
              270  LOAD_FAST                'lb_df'
              272  LOAD_STR                 'LBTEST'
              274  BINARY_SUBSCR    
              276  LOAD_STR                 'HEMOGLOBIN_PX87'
              278  COMPARE_OP               !=
              280  BINARY_SUBSCR    
              282  STORE_FAST               'lb_df'

 L.  55       284  LOAD_FAST                'lb_df'
              286  LOAD_STR                 'LBTEST'
              288  BINARY_SUBSCR    
              290  LOAD_ATTR                unique
              292  CALL_FUNCTION_0       0  '0 positional arguments'
              294  LOAD_ATTR                tolist
              296  CALL_FUNCTION_0       0  '0 positional arguments'
              298  STORE_FAST               'tests'

 L.  57   300_302  SETUP_LOOP         1500  'to 1500'
              304  LOAD_FAST                'tests'
              306  GET_ITER         
          308_310  FOR_ITER           1498  'to 1498'
              312  STORE_FAST               'test'

 L.  58       314  LOAD_GLOBAL              utils
              316  LOAD_ATTR                check_aelab
              318  LOAD_GLOBAL              float
              320  LOAD_FAST                'code'
              322  CALL_FUNCTION_1       1  '1 positional argument'
              324  LOAD_FAST                'test'
              326  CALL_FUNCTION_2       2  '2 positional arguments'
              328  LOAD_CONST               True
              330  COMPARE_OP               ==
          332_334  POP_JUMP_IF_FALSE   308  'to 308'

 L.  59       336  LOAD_FAST                'lb_df'
              338  LOAD_FAST                'lb_df'
              340  LOAD_STR                 'LBTEST'
              342  BINARY_SUBSCR    
              344  LOAD_FAST                'test'
              346  COMPARE_OP               ==
              348  BINARY_SUBSCR    
              350  STORE_FAST               'lb_records'

 L.  60       352  LOAD_FAST                'lb_records'
              354  LOAD_FAST                'lb_records'
              356  LOAD_STR                 'LBDAT'
              358  BINARY_SUBSCR    
              360  LOAD_ATTR                notna
              362  CALL_FUNCTION_0       0  '0 positional arguments'
              364  BINARY_SUBSCR    
              366  STORE_FAST               'lb_records'

 L.  62       368  LOAD_GLOBAL              pd
              370  LOAD_ATTR                to_datetime
              372  LOAD_FAST                'ae_record'
              374  LOAD_STR                 'AESTDAT'
              376  BINARY_SUBSCR    
              378  LOAD_CONST               True
              380  LOAD_CONST               ('infer_datetime_format',)
              382  CALL_FUNCTION_KW_2     2  '2 total positional and keyword args'
              384  LOAD_FAST                'ae_record'
              386  LOAD_STR                 'AESTDAT'
              388  STORE_SUBSCR     

 L.  63       390  LOAD_GLOBAL              pd
              392  LOAD_ATTR                to_datetime
              394  LOAD_FAST                'lb_records'
              396  LOAD_STR                 'LBDAT'
              398  BINARY_SUBSCR    
              400  LOAD_CONST               True
              402  LOAD_CONST               ('infer_datetime_format',)
              404  CALL_FUNCTION_KW_2     2  '2 total positional and keyword args'
              406  LOAD_FAST                'lb_records'
              408  LOAD_STR                 'LBDAT'
              410  STORE_SUBSCR     

 L.  64       412  LOAD_FAST                'ae_record'
              414  LOAD_STR                 'AESTDAT'
              416  BINARY_SUBSCR    
              418  LOAD_ATTR                values
              420  LOAD_CONST               0
              422  BINARY_SUBSCR    
              424  LOAD_FAST                'lb_records'
              426  LOAD_STR                 'LBDAT'
              428  BINARY_SUBSCR    
              430  BINARY_SUBTRACT  
              432  LOAD_GLOBAL              np
              434  LOAD_ATTR                timedelta64
              436  LOAD_CONST               1
              438  LOAD_STR                 'D'
              440  CALL_FUNCTION_2       2  '2 positional arguments'
              442  BINARY_TRUE_DIVIDE
              444  LOAD_FAST                'lb_records'
              446  LOAD_STR                 'DIFFERENCE'
              448  STORE_SUBSCR     

 L.  65       450  LOAD_FAST                'lb_records'
              452  LOAD_STR                 'DIFFERENCE'
              454  BINARY_SUBSCR    
              456  LOAD_ATTR                unique
              458  CALL_FUNCTION_0       0  '0 positional arguments'
              460  LOAD_ATTR                tolist
              462  CALL_FUNCTION_0       0  '0 positional arguments'
              464  STORE_FAST               'diff'

 L.  66       466  LOAD_LISTCOMP            '<code_object <listcomp>>'
              468  LOAD_STR                 'AELB1.execute.<locals>.<listcomp>'
              470  MAKE_FUNCTION_0          'Neither defaults, keyword-only args, annotations, nor closures'
              472  LOAD_FAST                'diff'
              474  GET_ITER         
              476  CALL_FUNCTION_1       1  '1 positional argument'
              478  STORE_FAST               'diff1'

 L.  67       480  LOAD_LISTCOMP            '<code_object <listcomp>>'
              482  LOAD_STR                 'AELB1.execute.<locals>.<listcomp>'
              484  MAKE_FUNCTION_0          'Neither defaults, keyword-only args, annotations, nor closures'
              486  LOAD_FAST                'diff'
              488  GET_ITER         
              490  CALL_FUNCTION_1       1  '1 positional argument'
              492  STORE_FAST               'diff2'

 L.  69       494  LOAD_GLOBAL              len
              496  LOAD_FAST                'diff1'
              498  CALL_FUNCTION_1       1  '1 positional argument'
              500  LOAD_CONST               0
              502  COMPARE_OP               >
          504_506  POP_JUMP_IF_FALSE   598  'to 598'

 L.  70       508  LOAD_GLOBAL              min
              510  LOAD_FAST                'diff1'
              512  CALL_FUNCTION_1       1  '1 positional argument'
              514  STORE_FAST               'minimum1'

 L.  71       516  LOAD_FAST                'minimum1'
              518  STORE_FAST               'final_minimum'

 L.  72       520  LOAD_GLOBAL              len
              522  LOAD_FAST                'diff2'
              524  CALL_FUNCTION_1       1  '1 positional argument'
              526  LOAD_CONST               0
              528  COMPARE_OP               >
          530_532  POP_JUMP_IF_FALSE   700  'to 700'

 L.  73       534  LOAD_GLOBAL              max
              536  LOAD_FAST                'diff2'
              538  CALL_FUNCTION_1       1  '1 positional argument'
              540  STORE_FAST               'minimum2'

 L.  74       542  LOAD_GLOBAL              min
              544  LOAD_FAST                'minimum1'
              546  LOAD_GLOBAL              abs
              548  LOAD_FAST                'minimum2'
              550  CALL_FUNCTION_1       1  '1 positional argument'
              552  CALL_FUNCTION_2       2  '2 positional arguments'
              554  STORE_FAST               'final_minimum'

 L.  75       556  LOAD_FAST                'minimum1'
              558  LOAD_GLOBAL              abs
              560  LOAD_FAST                'minimum2'
              562  CALL_FUNCTION_1       1  '1 positional argument'
              564  COMPARE_OP               ==
          566_568  POP_JUMP_IF_FALSE   576  'to 576'

 L.  76       570  LOAD_FAST                'minimum1'
              572  STORE_FAST               'final_minimum'
              574  JUMP_FORWARD        596  'to 596'
              576  ELSE                     '596'

 L.  77       576  LOAD_FAST                'final_minimum'
              578  LOAD_GLOBAL              abs
              580  LOAD_FAST                'minimum2'
              582  CALL_FUNCTION_1       1  '1 positional argument'
              584  COMPARE_OP               ==
          586_588  POP_JUMP_IF_FALSE   700  'to 700'

 L.  78       590  LOAD_FAST                'final_minimum'
              592  UNARY_NEGATIVE   
              594  STORE_FAST               'final_minimum'
            596_0  COME_FROM           574  '574'
              596  JUMP_FORWARD        700  'to 700'
              598  ELSE                     '700'

 L.  79       598  LOAD_GLOBAL              len
              600  LOAD_FAST                'diff2'
              602  CALL_FUNCTION_1       1  '1 positional argument'
              604  LOAD_CONST               0
              606  COMPARE_OP               >
          608_610  POP_JUMP_IF_FALSE   700  'to 700'

 L.  80       612  LOAD_GLOBAL              max
              614  LOAD_FAST                'diff2'
              616  CALL_FUNCTION_1       1  '1 positional argument'
              618  STORE_FAST               'minimum2'

 L.  81       620  LOAD_FAST                'minimum2'
              622  STORE_FAST               'final_minimum'

 L.  82       624  LOAD_GLOBAL              len
              626  LOAD_FAST                'diff1'
              628  CALL_FUNCTION_1       1  '1 positional argument'
              630  LOAD_CONST               0
              632  COMPARE_OP               >
          634_636  POP_JUMP_IF_FALSE   700  'to 700'

 L.  83       638  LOAD_GLOBAL              min
              640  LOAD_FAST                'diff1'
              642  CALL_FUNCTION_1       1  '1 positional argument'
              644  STORE_FAST               'minimum1'

 L.  84       646  LOAD_GLOBAL              min
              648  LOAD_FAST                'minimum1'
              650  LOAD_GLOBAL              abs
              652  LOAD_FAST                'minimum2'
              654  CALL_FUNCTION_1       1  '1 positional argument'
              656  CALL_FUNCTION_2       2  '2 positional arguments'
              658  STORE_FAST               'final_minimum'

 L.  85       660  LOAD_FAST                'minimum1'
              662  LOAD_GLOBAL              abs
              664  LOAD_FAST                'minimum2'
              666  CALL_FUNCTION_1       1  '1 positional argument'
              668  COMPARE_OP               ==
          670_672  POP_JUMP_IF_FALSE   680  'to 680'

 L.  86       674  LOAD_FAST                'minimum1'
              676  STORE_FAST               'final_minimum'
              678  JUMP_FORWARD        700  'to 700'
              680  ELSE                     '700'

 L.  87       680  LOAD_FAST                'final_minimum'
              682  LOAD_GLOBAL              abs
              684  LOAD_FAST                'minimum2'
              686  CALL_FUNCTION_1       1  '1 positional argument'
              688  COMPARE_OP               ==
          690_692  POP_JUMP_IF_FALSE   700  'to 700'

 L.  88       694  LOAD_FAST                'final_minimum'
              696  UNARY_NEGATIVE   
              698  STORE_FAST               'final_minimum'
            700_0  COME_FROM           690  '690'
            700_1  COME_FROM           678  '678'
            700_2  COME_FROM           634  '634'
            700_3  COME_FROM           608  '608'
            700_4  COME_FROM           596  '596'
            700_5  COME_FROM           586  '586'
            700_6  COME_FROM           530  '530'

 L.  91       700  LOAD_FAST                'lb_records'
              702  LOAD_FAST                'lb_records'
              704  LOAD_STR                 'DIFFERENCE'
              706  BINARY_SUBSCR    
              708  LOAD_FAST                'final_minimum'
              710  COMPARE_OP               ==
              712  BINARY_SUBSCR    
              714  STORE_FAST               'lb_records'

 L.  94   716_718  SETUP_LOOP         1494  'to 1494'
              720  LOAD_GLOBAL              range
              722  LOAD_FAST                'lb_records'
              724  LOAD_ATTR                shape
              726  LOAD_CONST               0
              728  BINARY_SUBSCR    
              730  CALL_FUNCTION_1       1  '1 positional argument'
              732  GET_ITER         
          734_736  FOR_ITER           1492  'to 1492'
              738  STORE_FAST               'ind1'

 L.  95   740_742  SETUP_EXCEPT       1472  'to 1472'

 L.  96       744  LOAD_FAST                'lb_records'
              746  LOAD_ATTR                iloc
              748  LOAD_FAST                'ind1'
              750  BUILD_LIST_1          1 
              752  BINARY_SUBSCR    
              754  STORE_FAST               'lb_record'

 L.  97       756  LOAD_FAST                'lb_record'
              758  STORE_FAST               'lb_record11'

 L.  98       760  LOAD_GLOBAL              float
              762  LOAD_FAST                'lb_record'
              764  LOAD_STR                 'LBORRES'
              766  BINARY_SUBSCR    
              768  LOAD_ATTR                values
              770  LOAD_CONST               0
              772  BINARY_SUBSCR    
              774  CALL_FUNCTION_1       1  '1 positional argument'
              776  STORE_FAST               'result'

 L.  99       778  LOAD_FAST                'lb_record'
              780  LOAD_STR                 'LBSTAT'
              782  BINARY_SUBSCR    
              784  LOAD_ATTR                values
              786  LOAD_CONST               0
              788  BINARY_SUBSCR    
              790  STORE_FAST               'status'

 L. 100       792  LOAD_FAST                'lb_record'
              794  LOAD_STR                 'LBDAT'
              796  BINARY_SUBSCR    
              798  LOAD_ATTR                values
              800  LOAD_CONST               0
              802  BINARY_SUBSCR    
              804  STORE_FAST               'labdate'

 L. 101       806  LOAD_GLOBAL              float
              808  LOAD_FAST                'lb_record'
              810  LOAD_STR                 'LNMTLOW'
              812  BINARY_SUBSCR    
              814  LOAD_ATTR                values
              816  LOAD_CONST               0
              818  BINARY_SUBSCR    
              820  CALL_FUNCTION_1       1  '1 positional argument'
              822  STORE_FAST               'low'

 L. 102       824  LOAD_GLOBAL              float
              826  LOAD_FAST                'lb_record'
              828  LOAD_STR                 'LNMTHIGH'
              830  BINARY_SUBSCR    
              832  LOAD_ATTR                values
              834  LOAD_CONST               0
              836  BINARY_SUBSCR    
              838  CALL_FUNCTION_1       1  '1 positional argument'
              840  STORE_FAST               'high'

 L. 104       842  LOAD_FAST                'ae_record'
              844  LOAD_STR                 'AESTDAT'
              846  BINARY_SUBSCR    
              848  LOAD_ATTR                values
              850  LOAD_CONST               0
              852  BINARY_SUBSCR    
              854  LOAD_FAST                'lb_record'
              856  LOAD_STR                 'LBDAT'
              858  BINARY_SUBSCR    
              860  LOAD_ATTR                values
              862  LOAD_CONST               0
              864  BINARY_SUBSCR    
              866  BINARY_SUBTRACT  
              868  LOAD_GLOBAL              np
              870  LOAD_ATTR                timedelta64
              872  LOAD_CONST               1
              874  LOAD_STR                 'D'
              876  CALL_FUNCTION_2       2  '2 positional arguments'
              878  BINARY_TRUE_DIVIDE
              880  STORE_FAST               'gap'

 L. 105       882  LOAD_GLOBAL              int
              884  LOAD_FAST                'gap'
              886  CALL_FUNCTION_1       1  '1 positional argument'
              888  STORE_FAST               'gap'

 L. 107       890  LOAD_FAST                'result'
              892  LOAD_FAST                'low'
              894  COMPARE_OP               <
          896_898  POP_JUMP_IF_TRUE    910  'to 910'
              900  LOAD_FAST                'result'
              902  LOAD_FAST                'high'
              904  COMPARE_OP               >
            906_0  COME_FROM           896  '896'
          906_908  POP_JUMP_IF_FALSE  1468  'to 1468'

 L. 108       910  LOAD_FAST                'gap'
              912  LOAD_CONST               -5
              914  COMPARE_OP               <
          916_918  POP_JUMP_IF_FALSE   930  'to 930'
              920  LOAD_FAST                'gap'
              922  LOAD_CONST               -30
              924  COMPARE_OP               >
            926_0  COME_FROM           916  '916'
          926_928  POP_JUMP_IF_TRUE    970  'to 970'
              930  LOAD_FAST                'ae_record'
              932  LOAD_STR                 'AESTDAT'
              934  BINARY_SUBSCR    
              936  LOAD_ATTR                values
              938  LOAD_CONST               0
              940  BINARY_SUBSCR    
              942  LOAD_FAST                'lb_record'
              944  LOAD_STR                 'LBDAT'
              946  BINARY_SUBSCR    
              948  LOAD_ATTR                values
              950  LOAD_CONST               0
              952  BINARY_SUBSCR    
              954  COMPARE_OP               >
          956_958  POP_JUMP_IF_FALSE  1468  'to 1468'
              960  LOAD_FAST                'gap'
              962  LOAD_CONST               30
              964  COMPARE_OP               <
            966_0  COME_FROM           956  '956'
            966_1  COME_FROM           926  '926'
          966_968  POP_JUMP_IF_FALSE  1468  'to 1468'

 L. 110       970  BUILD_MAP_0           0 
              972  STORE_FAST               'subcate_report_dict'

 L. 111       974  BUILD_MAP_0           0 
              976  STORE_FAST               'report_dict'

 L. 113       978  LOAD_FAST                'ae_record'
              980  STORE_FAST               'ae_record2'

 L. 114       982  LOAD_FAST                'lb_record'
              984  STORE_FAST               'lb_record2'

 L. 115       986  LOAD_FAST                'ae_record2'
              988  LOAD_STR                 'AESTDAT'
              990  BINARY_SUBSCR    
              992  LOAD_ATTR                dt
              994  LOAD_ATTR                strftime
              996  LOAD_STR                 '%d-%B-%Y'
              998  CALL_FUNCTION_1       1  '1 positional argument'
             1000  LOAD_FAST                'ae_record2'
             1002  LOAD_STR                 'AESTDAT'
             1004  STORE_SUBSCR     

 L. 116      1006  LOAD_FAST                'lb_record2'
             1008  LOAD_STR                 'LBDAT'
             1010  BINARY_SUBSCR    
             1012  LOAD_ATTR                dt
             1014  LOAD_ATTR                strftime
             1016  LOAD_STR                 '%d-%B-%Y'
             1018  CALL_FUNCTION_1       1  '1 positional argument'
             1020  LOAD_FAST                'lb_record2'
             1022  LOAD_STR                 'LBDAT'
             1024  STORE_SUBSCR     

 L. 118      1026  LOAD_FAST                'ae_record2'
             1028  LOAD_FAST                'lb_record2'
             1030  LOAD_CONST               ('AE', 'LB')
             1032  BUILD_CONST_KEY_MAP_2     2 
             1034  STORE_FAST               'piv_rec'

 L. 120      1036  SETUP_LOOP         1150  'to 1150'
             1038  LOAD_GLOBAL              a
             1040  LOAD_STR                 'FIELDS_FOR_UI'
             1042  BINARY_SUBSCR    
             1044  LOAD_FAST                'sub_cat'
             1046  BINARY_SUBSCR    
             1048  LOAD_ATTR                items
             1050  CALL_FUNCTION_0       0  '0 positional arguments'
             1052  GET_ITER         
             1054  FOR_ITER           1148  'to 1148'
             1056  UNPACK_SEQUENCE_2     2 
             1058  STORE_FAST               'dom'
             1060  STORE_FAST               'cols'

 L. 121      1062  LOAD_FAST                'piv_rec'
             1064  LOAD_FAST                'dom'
             1066  BINARY_SUBSCR    
             1068  STORE_DEREF              'piv_df'

 L. 122      1070  LOAD_CLOSURE             'piv_df'
             1072  BUILD_TUPLE_1         1 
             1074  LOAD_LISTCOMP            '<code_object <listcomp>>'
             1076  LOAD_STR                 'AELB1.execute.<locals>.<listcomp>'
             1078  MAKE_FUNCTION_CLOSURE        'closure'
             1080  LOAD_FAST                'cols'
             1082  GET_ITER         
             1084  CALL_FUNCTION_1       1  '1 positional argument'
             1086  STORE_FAST               'present_col'

 L. 123      1088  LOAD_DEREF               'piv_df'
             1090  LOAD_FAST                'present_col'
             1092  BINARY_SUBSCR    
             1094  STORE_FAST               'rep_df'

 L. 124      1096  LOAD_GLOBAL              utils
             1098  LOAD_ATTR                get_deeplink
             1100  LOAD_FAST                'study'
             1102  LOAD_DEREF               'piv_df'
             1104  CALL_FUNCTION_2       2  '2 positional arguments'
             1106  LOAD_FAST                'rep_df'
             1108  LOAD_STR                 'deeplink'
             1110  STORE_SUBSCR     

 L. 125      1112  LOAD_FAST                'rep_df'
             1114  LOAD_ATTR                rename
             1116  LOAD_GLOBAL              a
             1118  LOAD_STR                 'FIELD_NAME_DICT'
             1120  BINARY_SUBSCR    
             1122  LOAD_CONST               ('columns',)
             1124  CALL_FUNCTION_KW_1     1  '1 total positional and keyword args'
             1126  STORE_FAST               'rep_df'

 L. 126      1128  LOAD_FAST                'rep_df'
             1130  LOAD_ATTR                to_json
             1132  LOAD_STR                 'records'
             1134  LOAD_CONST               ('orient',)
             1136  CALL_FUNCTION_KW_1     1  '1 total positional and keyword args'
             1138  LOAD_FAST                'report_dict'
             1140  LOAD_FAST                'dom'
             1142  STORE_SUBSCR     
         1144_1146  JUMP_BACK          1054  'to 1054'
             1148  POP_BLOCK        
           1150_0  COME_FROM_LOOP     1036  '1036'

 L. 128      1150  LOAD_FAST                'report_dict'
             1152  LOAD_FAST                'subcate_report_dict'
             1154  LOAD_FAST                'sub_cat'
             1156  STORE_SUBSCR     

 L. 130      1158  LOAD_FAST                'ae_record2'
             1160  LOAD_STR                 'AESPID'
             1162  BINARY_SUBSCR    
             1164  LOAD_ATTR                values
             1166  LOAD_CONST               0
             1168  BINARY_SUBSCR    
             1170  STORE_FAST               'aespid'

 L. 131      1172  LOAD_FAST                'ae_record2'
             1174  LOAD_STR                 'AETERM'
             1176  BINARY_SUBSCR    
             1178  LOAD_ATTR                values
             1180  LOAD_CONST               0
             1182  BINARY_SUBSCR    
             1184  STORE_FAST               'aeterm'

 L. 132      1186  LOAD_FAST                'ae_record2'
             1188  LOAD_STR                 'AESTDAT'
             1190  BINARY_SUBSCR    
             1192  LOAD_ATTR                values
             1194  LOAD_CONST               0
             1196  BINARY_SUBSCR    
             1198  STORE_FAST               'aestdat'

 L. 133      1200  LOAD_FAST                'lb_record2'
             1202  LOAD_STR                 'LBTEST'
             1204  BINARY_SUBSCR    
             1206  LOAD_ATTR                values
             1208  LOAD_CONST               0
             1210  BINARY_SUBSCR    
             1212  STORE_FAST               'labtest'

 L. 134      1214  LOAD_FAST                'lb_record2'
             1216  LOAD_STR                 'LBDAT'
             1218  BINARY_SUBSCR    
             1220  LOAD_ATTR                values
             1222  LOAD_CONST               0
             1224  BINARY_SUBSCR    
             1226  STORE_FAST               'labdate'

 L. 135      1228  LOAD_FAST                'labtest'
             1230  LOAD_ATTR                split
             1232  LOAD_STR                 '_'
             1234  CALL_FUNCTION_1       1  '1 positional argument'
             1236  LOAD_CONST               0
             1238  BINARY_SUBSCR    
             1240  STORE_FAST               'labtest'

 L. 136      1242  LOAD_FAST                'ae_record'
             1244  LOAD_ATTR                iloc
             1246  LOAD_CONST               0
             1248  BINARY_SUBSCR    
             1250  STORE_FAST               'ae_record1'

 L. 138      1252  LOAD_FAST                'gap'
             1254  LOAD_CONST               -5
             1256  COMPARE_OP               <
         1258_1260  POP_JUMP_IF_FALSE  1278  'to 1278'
             1262  LOAD_FAST                'gap'
             1264  LOAD_CONST               -30
             1266  COMPARE_OP               >
         1268_1270  POP_JUMP_IF_FALSE  1278  'to 1278'

 L. 139      1272  LOAD_STR                 'But the AE start date is more than 5 days prior to lab sample collection date'
             1274  STORE_FAST               'option_text'
             1276  JUMP_FORWARD       1322  'to 1322'
           1278_0  COME_FROM          1258  '1258'

 L. 140      1278  LOAD_FAST                'ae_record'
             1280  LOAD_STR                 'AESTDAT'
             1282  BINARY_SUBSCR    
             1284  LOAD_ATTR                values
             1286  LOAD_CONST               0
             1288  BINARY_SUBSCR    
             1290  LOAD_FAST                'lb_record'
             1292  LOAD_STR                 'LBDAT'
             1294  BINARY_SUBSCR    
             1296  LOAD_ATTR                values
             1298  LOAD_CONST               0
             1300  BINARY_SUBSCR    
             1302  COMPARE_OP               >
         1304_1306  POP_JUMP_IF_FALSE  1322  'to 1322'
             1308  LOAD_FAST                'gap'
             1310  LOAD_CONST               30
             1312  COMPARE_OP               <
         1314_1316  POP_JUMP_IF_FALSE  1322  'to 1322'

 L. 141      1318  LOAD_STR                 'But the AE start date is after lab date'
             1320  STORE_FAST               'option_text'
           1322_0  COME_FROM          1314  '1314'
           1322_1  COME_FROM          1304  '1304'
           1322_2  COME_FROM          1276  '1276'

 L. 144      1322  LOAD_FAST                'aespid'

 L. 145      1324  LOAD_FAST                'aeterm'

 L. 146      1326  LOAD_GLOBAL              str
             1328  LOAD_FAST                'aestdat'
             1330  CALL_FUNCTION_1       1  '1 positional argument'
             1332  LOAD_ATTR                split
             1334  LOAD_STR                 'T'
             1336  CALL_FUNCTION_1       1  '1 positional argument'
             1338  LOAD_CONST               0
             1340  BINARY_SUBSCR    

 L. 147      1342  LOAD_FAST                'labtest'

 L. 148      1344  LOAD_GLOBAL              str
             1346  LOAD_FAST                'labdate'
             1348  CALL_FUNCTION_1       1  '1 positional argument'
             1350  LOAD_ATTR                split
             1352  LOAD_STR                 'T'
             1354  CALL_FUNCTION_1       1  '1 positional argument'
             1356  LOAD_CONST               0
             1358  BINARY_SUBSCR    

 L. 149      1360  LOAD_FAST                'option_text'
             1362  LOAD_CONST               ('AESPID', 'AETERM', 'AESTDAT', 'LBTEST', 'LBDAT', 'option')
             1364  BUILD_CONST_KEY_MAP_6     6 
             1366  STORE_FAST               'keys'

 L. 153      1368  LOAD_FAST                'sub_cat'

 L. 154      1370  LOAD_FAST                'self'
             1372  LOAD_ATTR                get_model_query_text_json
             1374  LOAD_FAST                'study'
             1376  LOAD_FAST                'sub_cat'
             1378  LOAD_FAST                'keys'
             1380  LOAD_CONST               ('params',)
             1382  CALL_FUNCTION_KW_3     3  '3 total positional and keyword args'

 L. 155      1384  LOAD_GLOBAL              str
             1386  LOAD_FAST                'ae_record1'
             1388  LOAD_STR                 'form_index'
             1390  BINARY_SUBSCR    
             1392  CALL_FUNCTION_1       1  '1 positional argument'

 L. 156      1394  LOAD_FAST                'self'
             1396  LOAD_ATTR                get_subcategory_json
             1398  LOAD_FAST                'study'
             1400  LOAD_FAST                'sub_cat'
             1402  CALL_FUNCTION_2       2  '2 positional arguments'

 L. 157      1404  LOAD_GLOBAL              str
             1406  LOAD_FAST                'ae_record1'
             1408  LOAD_STR                 'modif_dts'
             1410  BINARY_SUBSCR    
             1412  CALL_FUNCTION_1       1  '1 positional argument'

 L. 158      1414  LOAD_GLOBAL              int
             1416  LOAD_FAST                'ae_record1'
             1418  LOAD_STR                 'ck_event_id'
             1420  BINARY_SUBSCR    
             1422  CALL_FUNCTION_1       1  '1 positional argument'

 L. 159      1424  LOAD_GLOBAL              str
             1426  LOAD_FAST                'ae_record1'
             1428  LOAD_STR                 'formrefname'
             1430  BINARY_SUBSCR    
             1432  CALL_FUNCTION_1       1  '1 positional argument'

 L. 160      1434  LOAD_FAST                'subcate_report_dict'

 L. 161      1436  LOAD_GLOBAL              np
             1438  LOAD_ATTR                random
             1440  LOAD_ATTR                uniform
             1442  LOAD_CONST               0.7
             1444  LOAD_CONST               0.97
             1446  CALL_FUNCTION_2       2  '2 positional arguments'
             1448  LOAD_CONST               ('subcategory', 'query_text', 'form_index', 'question_present', 'modif_dts', 'stg_ck_event_id', 'formrefname', 'report', 'confid_score')
             1450  BUILD_CONST_KEY_MAP_9     9 
             1452  STORE_FAST               'payload'

 L. 164      1454  LOAD_FAST                'self'
             1456  LOAD_ATTR                insert_query
             1458  LOAD_FAST                'study'
             1460  LOAD_FAST                'subject'
             1462  LOAD_FAST                'payload'
             1464  CALL_FUNCTION_3       3  '3 positional arguments'
             1466  POP_TOP          
           1468_0  COME_FROM           966  '966'
           1468_1  COME_FROM           906  '906'
             1468  POP_BLOCK        
             1470  JUMP_FORWARD       1488  'to 1488'
           1472_0  COME_FROM_EXCEPT    740  '740'

 L. 165      1472  POP_TOP          
             1474  POP_TOP          
             1476  POP_TOP          

 L. 167  1478_1480  CONTINUE_LOOP       734  'to 734'
             1482  POP_EXCEPT       
             1484  JUMP_FORWARD       1488  'to 1488'
             1486  END_FINALLY      
           1488_0  COME_FROM          1484  '1484'
           1488_1  COME_FROM          1470  '1470'
         1488_1490  JUMP_BACK           734  'to 734'
             1492  POP_BLOCK        
           1494_0  COME_FROM_LOOP      716  '716'
         1494_1496  JUMP_BACK           308  'to 308'
             1498  POP_BLOCK        
           1500_0  COME_FROM_LOOP      300  '300'
             1500  POP_BLOCK        
             1502  JUMP_BACK           182  'to 182'
           1504_0  COME_FROM_EXCEPT    188  '188'

 L. 168      1504  POP_TOP          
             1506  POP_TOP          
             1508  POP_TOP          

 L. 170      1510  CONTINUE_LOOP       182  'to 182'
             1512  POP_EXCEPT       
             1514  JUMP_BACK           182  'to 182'
             1516  END_FINALLY      
             1518  JUMP_BACK           182  'to 182'
             1520  POP_BLOCK        
           1522_0  COME_FROM_LOOP      164  '164'
             1522  POP_BLOCK        
             1524  JUMP_BACK            60  'to 60'
           1526_0  COME_FROM_EXCEPT     66  '66'

 L. 172      1526  DUP_TOP          
             1528  LOAD_GLOBAL              Exception
             1530  COMPARE_OP               exception-match
         1532_1534  POP_JUMP_IF_FALSE  1570  'to 1570'
             1536  POP_TOP          
             1538  STORE_FAST               'e'
             1540  POP_TOP          
             1542  SETUP_FINALLY      1560  'to 1560'

 L. 174      1544  LOAD_GLOBAL              logging
             1546  LOAD_ATTR                exception
             1548  LOAD_FAST                'e'
             1550  CALL_FUNCTION_1       1  '1 positional argument'
             1552  POP_TOP          
             1554  POP_BLOCK        
             1556  POP_EXCEPT       
             1558  LOAD_CONST               None
           1560_0  COME_FROM_FINALLY  1542  '1542'
             1560  LOAD_CONST               None
             1562  STORE_FAST               'e'
             1564  DELETE_FAST              'e'
             1566  END_FINALLY      
             1568  JUMP_BACK            60  'to 60'
             1570  END_FINALLY      
             1572  JUMP_BACK            60  'to 60'
             1574  POP_BLOCK        
           1576_0  COME_FROM_LOOP       46  '46'

Parse error at or near `POP_JUMP_IF_TRUE' instruction at offset 926_928


if __name__ == '__main__':
    study_id = sys.argv[1]
    rule_id = sys.argv[2]
    version = sys.argv[3]
    rule = AELB1(study_id, rule_id, version)
    rule.run()