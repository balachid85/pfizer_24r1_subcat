# uncompyle6 version 3.9.0
# Python bytecode version base 3.6 (3379)
# Decompiled from: Python 3.10.1 (v3.10.1:2cd268a3a9, Dec  6 2021, 14:28:59) [Clang 13.0.0 (clang-1300.0.29.3)]
# Embedded file name: LBAE0.py
# Compiled at: 2020-11-10 13:17:27
# Size of source mod 2**32: 12873 bytes
import pandas as pd, sys, re, warnings
warnings.filterwarnings('ignore')
try:
    from scripts.SubcatModels.model_libs.base import BaseSDQApi
    import scripts.SubcatModels.model_libs.utilsk as utils
    from scripts.SubcatModels.model_libs.ctc_grades import *
except:
    from base import BaseSDQApi
    import utilsk as utils
    from ctc_grades import *

import traceback, tqdm, logging, numpy as np, yaml, os
curr_file_path = os.path.realpath(__file__)
curr_path = os.path.abspath(os.path.join(curr_file_path, '../'))
grades_path = os.path.join(curr_path, 'grade_lookup.yaml')
grades = yaml.load((open(grades_path, 'rb')), Loader=(yaml.FullLoader))
subcat_config_path = os.path.join(curr_path, 'subcate_config.yml')
a = yaml.load(open(subcat_config_path, 'r'))

class LBAE0(BaseSDQApi):
    domain_list = [
     'AE', 'LB']

    def execute--- This code section failed: ---

 L.  34         0  LOAD_FAST                'self'
                2  LOAD_ATTR                study_id
                4  STORE_FAST               'study'

 L.  36         6  LOAD_STR                 'LBAE0'
                8  STORE_FAST               'sub_cat'

 L.  37        10  LOAD_FAST                'self'
               12  LOAD_ATTR                get_subjects
               14  LOAD_FAST                'study'
               16  LOAD_FAST                'self'
               18  LOAD_ATTR                domain_list
               20  LOAD_CONST               10000
               22  LOAD_CONST               ('domain_list', 'per_page')
               24  CALL_FUNCTION_KW_3     3  '3 total positional and keyword args'
               26  STORE_FAST               'subjects'

 L.  39     28_30  SETUP_LOOP         1770  'to 1770'
               32  LOAD_GLOBAL              tqdm
               34  LOAD_ATTR                tqdm
               36  LOAD_FAST                'subjects'
               38  CALL_FUNCTION_1       1  '1 positional argument'
               40  GET_ITER         
            42_44  FOR_ITER           1768  'to 1768'
               46  STORE_FAST               'subject'

 L.  40     48_50  SETUP_EXCEPT       1752  'to 1752'

 L.  41        52  LOAD_FAST                'self'
               54  LOAD_ATTR                get_flatten_data
               56  LOAD_FAST                'study'
               58  LOAD_FAST                'subject'
               60  LOAD_CONST               10000
               62  LOAD_FAST                'self'
               64  LOAD_ATTR                domain_list
               66  LOAD_CONST               ('per_page', 'domain_list')
               68  CALL_FUNCTION_KW_4     4  '4 total positional and keyword args'
               70  STORE_FAST               'flatten_data'

 L.  43        72  LOAD_STR                 'AE'
               74  LOAD_FAST                'flatten_data'
               76  COMPARE_OP               not-in
               78  POP_JUMP_IF_TRUE     88  'to 88'
               80  LOAD_STR                 'LB'
               82  LOAD_FAST                'flatten_data'
               84  COMPARE_OP               not-in
             86_0  COME_FROM            78  '78'
               86  POP_JUMP_IF_FALSE    90  'to 90'

 L.  44        88  CONTINUE_LOOP        42  'to 42'
             90_0  COME_FROM            86  '86'

 L.  46        90  LOAD_GLOBAL              pd
               92  LOAD_ATTR                DataFrame
               94  LOAD_FAST                'flatten_data'
               96  LOAD_STR                 'AE'
               98  BINARY_SUBSCR    
              100  CALL_FUNCTION_1       1  '1 positional argument'
              102  STORE_FAST               'ae_df'

 L.  47       104  LOAD_FAST                'ae_df'
              106  LOAD_FAST                'ae_df'
              108  LOAD_STR                 'AEPTCD'
              110  BINARY_SUBSCR    
              112  LOAD_ATTR                isin
              114  LOAD_STR                 ''
              116  LOAD_STR                 ' '
              118  LOAD_GLOBAL              np
              120  LOAD_ATTR                nan
              122  BUILD_LIST_3          3 
              124  CALL_FUNCTION_1       1  '1 positional argument'
              126  UNARY_INVERT     
              128  BINARY_SUBSCR    
              130  STORE_FAST               'ae_df'

 L.  48       132  LOAD_FAST                'ae_df'
              134  LOAD_STR                 'AEPTCD'
              136  BINARY_SUBSCR    
              138  LOAD_ATTR                unique
              140  CALL_FUNCTION_0       0  '0 positional arguments'
              142  LOAD_ATTR                tolist
              144  CALL_FUNCTION_0       0  '0 positional arguments'
              146  STORE_FAST               'codes'

 L.  50       148  LOAD_GLOBAL              pd
              150  LOAD_ATTR                DataFrame
              152  LOAD_FAST                'flatten_data'
              154  LOAD_STR                 'LB'
              156  BINARY_SUBSCR    
              158  CALL_FUNCTION_1       1  '1 positional argument'
              160  STORE_FAST               'lb_df'

 L.  51       162  LOAD_FAST                'lb_df'
              164  LOAD_FAST                'lb_df'
              166  LOAD_STR                 'LBTEST'
              168  BINARY_SUBSCR    
              170  LOAD_ATTR                isin
              172  LOAD_STR                 ''
              174  LOAD_STR                 ' '
              176  LOAD_GLOBAL              np
              178  LOAD_ATTR                nan
              180  BUILD_LIST_3          3 
              182  CALL_FUNCTION_1       1  '1 positional argument'
              184  UNARY_INVERT     
              186  BINARY_SUBSCR    
              188  STORE_FAST               'lb_df'

 L.  52       190  LOAD_FAST                'lb_df'
              192  LOAD_STR                 'LBTEST'
              194  BINARY_SUBSCR    
              196  LOAD_ATTR                str
              198  LOAD_ATTR                upper
              200  CALL_FUNCTION_0       0  '0 positional arguments'
              202  LOAD_FAST                'lb_df'
              204  LOAD_STR                 'LBTEST'
              206  STORE_SUBSCR     

 L.  53       208  LOAD_FAST                'lb_df'
              210  LOAD_FAST                'lb_df'
              212  LOAD_STR                 'LBTEST'
              214  BINARY_SUBSCR    
              216  LOAD_STR                 'HEMOGLOBIN_PX87'
              218  COMPARE_OP               !=
              220  BINARY_SUBSCR    
              222  STORE_FAST               'lb_df'

 L.  54       224  LOAD_FAST                'lb_df'
              226  LOAD_STR                 'LBTEST'
              228  BINARY_SUBSCR    
              230  LOAD_ATTR                unique
              232  CALL_FUNCTION_0       0  '0 positional arguments'
              234  LOAD_ATTR                tolist
              236  CALL_FUNCTION_0       0  '0 positional arguments'
              238  STORE_FAST               'tests'

 L.  56   240_242  SETUP_LOOP         1748  'to 1748'
              244  LOAD_FAST                'tests'
              246  GET_ITER         
          248_250  FOR_ITER           1746  'to 1746'
              252  STORE_FAST               'test'

 L.  57   254_256  SETUP_LOOP         1744  'to 1744'
              258  LOAD_FAST                'codes'
              260  GET_ITER         
          262_264  FOR_ITER           1742  'to 1742'
              266  STORE_FAST               'code'

 L.  58   268_270  SETUP_EXCEPT       1722  'to 1722'

 L.  59       272  LOAD_GLOBAL              utils
              274  LOAD_ATTR                check_aelab
              276  LOAD_GLOBAL              float
              278  LOAD_FAST                'code'
              280  CALL_FUNCTION_1       1  '1 positional argument'
              282  LOAD_FAST                'test'
              284  CALL_FUNCTION_2       2  '2 positional arguments'
              286  LOAD_CONST               True
              288  COMPARE_OP               ==
          290_292  POP_JUMP_IF_FALSE  1718  'to 1718'

 L.  60       294  LOAD_FAST                'lb_df'
              296  LOAD_FAST                'lb_df'
              298  LOAD_STR                 'LBTEST'
              300  BINARY_SUBSCR    
              302  LOAD_FAST                'test'
              304  COMPARE_OP               ==
              306  BINARY_SUBSCR    
              308  STORE_FAST               'lb_df1'

 L.  61   310_312  SETUP_LOOP         1718  'to 1718'
              314  LOAD_GLOBAL              range
              316  LOAD_FAST                'lb_df1'
              318  LOAD_ATTR                shape
              320  LOAD_CONST               0
              322  BINARY_SUBSCR    
              324  CALL_FUNCTION_1       1  '1 positional argument'
              326  GET_ITER         
          328_330  FOR_ITER           1716  'to 1716'
              332  STORE_FAST               'ind'

 L.  62   334_336  SETUP_EXCEPT       1696  'to 1696'

 L.  63       338  LOAD_FAST                'ae_df'
              340  STORE_FAST               'ae_records'

 L.  64       342  LOAD_FAST                'lb_df1'
              344  LOAD_ATTR                iloc
              346  LOAD_FAST                'ind'
              348  BUILD_LIST_1          1 
              350  BINARY_SUBSCR    
              352  STORE_FAST               'lb_record'

 L.  65       354  LOAD_FAST                'lb_record'
              356  LOAD_STR                 'LBTEST'
              358  BINARY_SUBSCR    
              360  LOAD_ATTR                values
              362  LOAD_CONST               0
              364  BINARY_SUBSCR    
              366  STORE_FAST               'test'

 L.  66       368  LOAD_FAST                'ae_records'
              370  LOAD_FAST                'ae_records'
              372  LOAD_STR                 'AEPTCD'
              374  BINARY_SUBSCR    
              376  LOAD_FAST                'code'
              378  COMPARE_OP               ==
              380  BINARY_SUBSCR    
              382  STORE_FAST               'ae_records'

 L.  67       384  LOAD_GLOBAL              pd
              386  LOAD_ATTR                to_datetime
              388  LOAD_FAST                'ae_records'
              390  LOAD_STR                 'AESTDAT'
              392  BINARY_SUBSCR    
              394  LOAD_CONST               True
              396  LOAD_CONST               ('infer_datetime_format',)
              398  CALL_FUNCTION_KW_2     2  '2 total positional and keyword args'
              400  LOAD_FAST                'ae_records'
              402  LOAD_STR                 'AESTDAT'
              404  STORE_SUBSCR     

 L.  68       406  LOAD_GLOBAL              pd
              408  LOAD_ATTR                to_datetime
              410  LOAD_FAST                'ae_records'
              412  LOAD_STR                 'AEENDAT'
              414  BINARY_SUBSCR    
              416  LOAD_CONST               True
              418  LOAD_CONST               ('infer_datetime_format',)
              420  CALL_FUNCTION_KW_2     2  '2 total positional and keyword args'
              422  LOAD_FAST                'ae_records'
              424  LOAD_STR                 'AEENDAT'
              426  STORE_SUBSCR     

 L.  69       428  LOAD_GLOBAL              pd
              430  LOAD_ATTR                to_datetime
              432  LOAD_FAST                'lb_record'
              434  LOAD_STR                 'LBDAT'
              436  BINARY_SUBSCR    
              438  LOAD_CONST               True
              440  LOAD_CONST               ('infer_datetime_format',)
              442  CALL_FUNCTION_KW_2     2  '2 total positional and keyword args'
              444  LOAD_FAST                'lb_record'
              446  LOAD_STR                 'LBDAT'
              448  STORE_SUBSCR     

 L.  70       450  LOAD_FAST                'ae_records'
              452  LOAD_STR                 'AESTDAT'
              454  BINARY_SUBSCR    
              456  LOAD_FAST                'lb_record'
              458  LOAD_STR                 'LBDAT'
              460  BINARY_SUBSCR    
              462  LOAD_ATTR                values
              464  LOAD_CONST               0
              466  BINARY_SUBSCR    
              468  BINARY_SUBTRACT  
              470  LOAD_GLOBAL              np
              472  LOAD_ATTR                timedelta64
              474  LOAD_CONST               1
              476  LOAD_STR                 'D'
              478  CALL_FUNCTION_2       2  '2 positional arguments'
              480  BINARY_TRUE_DIVIDE
              482  LOAD_FAST                'ae_records'
              484  LOAD_STR                 'DIFFERENCE'
              486  STORE_SUBSCR     

 L.  72       488  LOAD_FAST                'ae_records'
              490  LOAD_STR                 'DIFFERENCE'
              492  BINARY_SUBSCR    
              494  LOAD_ATTR                unique
              496  CALL_FUNCTION_0       0  '0 positional arguments'
              498  LOAD_ATTR                tolist
              500  CALL_FUNCTION_0       0  '0 positional arguments'
              502  STORE_FAST               'diff'

 L.  73       504  LOAD_LISTCOMP            '<code_object <listcomp>>'
              506  LOAD_STR                 'LBAE0.execute.<locals>.<listcomp>'
              508  MAKE_FUNCTION_0          'Neither defaults, keyword-only args, annotations, nor closures'
              510  LOAD_FAST                'diff'
              512  GET_ITER         
              514  CALL_FUNCTION_1       1  '1 positional argument'
              516  STORE_FAST               'diff1'

 L.  74       518  LOAD_LISTCOMP            '<code_object <listcomp>>'
              520  LOAD_STR                 'LBAE0.execute.<locals>.<listcomp>'
              522  MAKE_FUNCTION_0          'Neither defaults, keyword-only args, annotations, nor closures'
              524  LOAD_FAST                'diff'
              526  GET_ITER         
              528  CALL_FUNCTION_1       1  '1 positional argument'
              530  STORE_FAST               'diff2'

 L.  76       532  LOAD_GLOBAL              len
              534  LOAD_FAST                'diff1'
              536  CALL_FUNCTION_1       1  '1 positional argument'
              538  LOAD_CONST               0
              540  COMPARE_OP               >
          542_544  POP_JUMP_IF_FALSE   636  'to 636'

 L.  77       546  LOAD_GLOBAL              min
              548  LOAD_FAST                'diff1'
              550  CALL_FUNCTION_1       1  '1 positional argument'
              552  STORE_FAST               'minimum1'

 L.  78       554  LOAD_FAST                'minimum1'
              556  STORE_FAST               'final_minimum'

 L.  79       558  LOAD_GLOBAL              len
              560  LOAD_FAST                'diff2'
              562  CALL_FUNCTION_1       1  '1 positional argument'
              564  LOAD_CONST               0
              566  COMPARE_OP               >
          568_570  POP_JUMP_IF_FALSE   738  'to 738'

 L.  80       572  LOAD_GLOBAL              max
              574  LOAD_FAST                'diff2'
              576  CALL_FUNCTION_1       1  '1 positional argument'
              578  STORE_FAST               'minimum2'

 L.  81       580  LOAD_GLOBAL              min
              582  LOAD_FAST                'minimum1'
              584  LOAD_GLOBAL              abs
              586  LOAD_FAST                'minimum2'
              588  CALL_FUNCTION_1       1  '1 positional argument'
              590  CALL_FUNCTION_2       2  '2 positional arguments'
              592  STORE_FAST               'final_minimum'

 L.  82       594  LOAD_FAST                'minimum1'
              596  LOAD_GLOBAL              abs
              598  LOAD_FAST                'minimum2'
              600  CALL_FUNCTION_1       1  '1 positional argument'
              602  COMPARE_OP               ==
          604_606  POP_JUMP_IF_FALSE   614  'to 614'

 L.  83       608  LOAD_FAST                'minimum1'
              610  STORE_FAST               'final_minimum'
              612  JUMP_FORWARD        634  'to 634'
              614  ELSE                     '634'

 L.  84       614  LOAD_FAST                'final_minimum'
              616  LOAD_GLOBAL              abs
              618  LOAD_FAST                'minimum2'
              620  CALL_FUNCTION_1       1  '1 positional argument'
              622  COMPARE_OP               ==
          624_626  POP_JUMP_IF_FALSE   738  'to 738'

 L.  85       628  LOAD_FAST                'final_minimum'
              630  UNARY_NEGATIVE   
              632  STORE_FAST               'final_minimum'
            634_0  COME_FROM           612  '612'
              634  JUMP_FORWARD        738  'to 738'
              636  ELSE                     '738'

 L.  86       636  LOAD_GLOBAL              len
              638  LOAD_FAST                'diff2'
              640  CALL_FUNCTION_1       1  '1 positional argument'
              642  LOAD_CONST               0
              644  COMPARE_OP               >
          646_648  POP_JUMP_IF_FALSE   738  'to 738'

 L.  87       650  LOAD_GLOBAL              max
              652  LOAD_FAST                'diff2'
              654  CALL_FUNCTION_1       1  '1 positional argument'
              656  STORE_FAST               'minimum2'

 L.  88       658  LOAD_FAST                'minimum2'
              660  STORE_FAST               'final_minimum'

 L.  89       662  LOAD_GLOBAL              len
              664  LOAD_FAST                'diff1'
              666  CALL_FUNCTION_1       1  '1 positional argument'
              668  LOAD_CONST               0
              670  COMPARE_OP               >
          672_674  POP_JUMP_IF_FALSE   738  'to 738'

 L.  90       676  LOAD_GLOBAL              min
              678  LOAD_FAST                'diff1'
              680  CALL_FUNCTION_1       1  '1 positional argument'
              682  STORE_FAST               'minimum1'

 L.  91       684  LOAD_GLOBAL              min
              686  LOAD_FAST                'minimum1'
              688  LOAD_GLOBAL              abs
              690  LOAD_FAST                'minimum2'
              692  CALL_FUNCTION_1       1  '1 positional argument'
              694  CALL_FUNCTION_2       2  '2 positional arguments'
              696  STORE_FAST               'final_minimum'

 L.  92       698  LOAD_FAST                'minimum1'
              700  LOAD_GLOBAL              abs
              702  LOAD_FAST                'minimum2'
              704  CALL_FUNCTION_1       1  '1 positional argument'
              706  COMPARE_OP               ==
          708_710  POP_JUMP_IF_FALSE   718  'to 718'

 L.  93       712  LOAD_FAST                'minimum1'
              714  STORE_FAST               'final_minimum'
              716  JUMP_FORWARD        738  'to 738'
              718  ELSE                     '738'

 L.  94       718  LOAD_FAST                'final_minimum'
              720  LOAD_GLOBAL              abs
              722  LOAD_FAST                'minimum2'
              724  CALL_FUNCTION_1       1  '1 positional argument'
              726  COMPARE_OP               ==
          728_730  POP_JUMP_IF_FALSE   738  'to 738'

 L.  95       732  LOAD_FAST                'final_minimum'
              734  UNARY_NEGATIVE   
              736  STORE_FAST               'final_minimum'
            738_0  COME_FROM           728  '728'
            738_1  COME_FROM           716  '716'
            738_2  COME_FROM           672  '672'
            738_3  COME_FROM           646  '646'
            738_4  COME_FROM           634  '634'
            738_5  COME_FROM           624  '624'
            738_6  COME_FROM           568  '568'

 L.  98       738  LOAD_FAST                'ae_records'
              740  LOAD_FAST                'ae_records'
              742  LOAD_STR                 'DIFFERENCE'
              744  BINARY_SUBSCR    
              746  LOAD_FAST                'final_minimum'
              748  COMPARE_OP               ==
              750  BINARY_SUBSCR    
              752  STORE_FAST               'ae_records'

 L. 100   754_756  SETUP_LOOP         1692  'to 1692'
              758  LOAD_GLOBAL              range
              760  LOAD_FAST                'ae_records'
              762  LOAD_ATTR                shape
              764  LOAD_CONST               0
              766  BINARY_SUBSCR    
              768  CALL_FUNCTION_1       1  '1 positional argument'
              770  GET_ITER         
          772_774  FOR_ITER           1690  'to 1690'
              776  STORE_FAST               'ind1'

 L. 101   778_780  SETUP_EXCEPT       1670  'to 1670'

 L. 102       782  LOAD_FAST                'ae_records'
              784  LOAD_ATTR                iloc
              786  LOAD_FAST                'ind1'
              788  BUILD_LIST_1          1 
              790  BINARY_SUBSCR    
              792  STORE_FAST               'ae_record'

 L. 103       794  LOAD_FAST                'ae_record'
              796  LOAD_STR                 'AETERM'
              798  BINARY_SUBSCR    
              800  LOAD_ATTR                values
              802  LOAD_CONST               0
              804  BINARY_SUBSCR    
              806  STORE_FAST               'aeterm'

 L. 104       808  LOAD_FAST                'ae_record'
              810  LOAD_STR                 'AEPTCD'
              812  BINARY_SUBSCR    
              814  LOAD_ATTR                values
              816  LOAD_CONST               0
              818  BINARY_SUBSCR    
              820  STORE_FAST               'code'

 L. 105       822  LOAD_FAST                'ae_record'
              824  LOAD_STR                 'form_index'
              826  BINARY_SUBSCR    
              828  LOAD_ATTR                values
              830  LOAD_CONST               0
              832  BINARY_SUBSCR    
              834  STORE_FAST               'formindex'

 L. 106       836  LOAD_FAST                'ae_record'
              838  LOAD_STR                 'AESPID'
              840  BINARY_SUBSCR    
              842  LOAD_ATTR                values
              844  LOAD_CONST               0
              846  BINARY_SUBSCR    
              848  STORE_FAST               'aespid'

 L. 107       850  LOAD_GLOBAL              float
              852  LOAD_FAST                'lb_record'
              854  LOAD_STR                 'LBORRES'
              856  BINARY_SUBSCR    
              858  LOAD_ATTR                values
              860  LOAD_CONST               0
              862  BINARY_SUBSCR    
              864  CALL_FUNCTION_1       1  '1 positional argument'
              866  STORE_FAST               'result'

 L. 108       868  LOAD_FAST                'lb_record'
              870  LOAD_STR                 'LBSTAT'
              872  BINARY_SUBSCR    
              874  LOAD_ATTR                values
              876  LOAD_CONST               0
              878  BINARY_SUBSCR    
              880  STORE_FAST               'status'

 L. 109       882  LOAD_FAST                'ae_record'
              884  LOAD_STR                 'AESTDAT'
              886  BINARY_SUBSCR    
              888  LOAD_ATTR                values
              890  LOAD_CONST               0
              892  BINARY_SUBSCR    
              894  STORE_FAST               'aestdat'

 L. 110       896  LOAD_FAST                'ae_record'
              898  LOAD_STR                 'AEENDAT'
              900  BINARY_SUBSCR    
              902  LOAD_ATTR                values
              904  LOAD_CONST               0
              906  BINARY_SUBSCR    
              908  STORE_FAST               'aeendat'

 L. 111       910  LOAD_FAST                'lb_record'
              912  LOAD_STR                 'LBDAT'
              914  BINARY_SUBSCR    
              916  LOAD_ATTR                values
              918  LOAD_CONST               0
              920  BINARY_SUBSCR    
              922  STORE_FAST               'labdate'

 L. 112       924  LOAD_GLOBAL              float
              926  LOAD_FAST                'lb_record'
              928  LOAD_STR                 'LNMTLOW'
              930  BINARY_SUBSCR    
              932  LOAD_ATTR                values
              934  LOAD_CONST               0
              936  BINARY_SUBSCR    
              938  CALL_FUNCTION_1       1  '1 positional argument'
              940  STORE_FAST               'low'

 L. 113       942  LOAD_GLOBAL              float
              944  LOAD_FAST                'lb_record'
              946  LOAD_STR                 'LNMTHIGH'
              948  BINARY_SUBSCR    
              950  LOAD_ATTR                values
              952  LOAD_CONST               0
              954  BINARY_SUBSCR    
              956  CALL_FUNCTION_1       1  '1 positional argument'
              958  STORE_FAST               'high'

 L. 114       960  LOAD_FAST                'lb_record'
              962  LOAD_STR                 'LNMTUNIT'
              964  BINARY_SUBSCR    
              966  LOAD_ATTR                values
              968  LOAD_CONST               0
              970  BINARY_SUBSCR    
              972  STORE_FAST               'unit'

 L. 116       974  LOAD_FAST                'ae_record'
              976  LOAD_STR                 'AESTDAT'
              978  BINARY_SUBSCR    
              980  LOAD_ATTR                values
              982  LOAD_CONST               0
              984  BINARY_SUBSCR    
              986  LOAD_FAST                'lb_record'
              988  LOAD_STR                 'LBDAT'
              990  BINARY_SUBSCR    
              992  LOAD_ATTR                values
              994  LOAD_CONST               0
              996  BINARY_SUBSCR    
              998  BINARY_SUBTRACT  
             1000  LOAD_GLOBAL              np
             1002  LOAD_ATTR                timedelta64
             1004  LOAD_CONST               1
             1006  LOAD_STR                 'D'
             1008  CALL_FUNCTION_2       2  '2 positional arguments'
             1010  BINARY_TRUE_DIVIDE
             1012  STORE_FAST               'gap'

 L. 117      1014  LOAD_GLOBAL              int
             1016  LOAD_FAST                'gap'
             1018  CALL_FUNCTION_1       1  '1 positional argument'
             1020  STORE_FAST               'gap'

 L. 118      1022  LOAD_GLOBAL              grades
             1024  LOAD_STR                 'VERSION 4'
             1026  BINARY_SUBSCR    
             1028  LOAD_FAST                'aeterm'
             1030  LOAD_ATTR                upper
             1032  CALL_FUNCTION_0       0  '0 positional arguments'
             1034  BINARY_SUBSCR    
             1036  STORE_FAST               'func_name'

 L. 119      1038  LOAD_GLOBAL              type
             1040  LOAD_FAST                'func_name'
             1042  CALL_FUNCTION_1       1  '1 positional argument'
             1044  LOAD_GLOBAL              list
             1046  COMPARE_OP               ==
         1048_1050  POP_JUMP_IF_FALSE  1060  'to 1060'

 L. 120      1052  LOAD_FAST                'func_name'
             1054  LOAD_CONST               0
             1056  BINARY_SUBSCR    
             1058  STORE_FAST               'func_name'
           1060_0  COME_FROM          1048  '1048'

 L. 121      1060  LOAD_GLOBAL              globals
             1062  CALL_FUNCTION_0       0  '0 positional arguments'
             1064  LOAD_FAST                'func_name'
             1066  BINARY_SUBSCR    
             1068  LOAD_FAST                'result'
             1070  LOAD_FAST                'low'
             1072  LOAD_FAST                'high'
             1074  LOAD_FAST                'unit'
             1076  CALL_FUNCTION_4       4  '4 positional arguments'
             1078  STORE_FAST               'grade'

 L. 122      1080  LOAD_GLOBAL              int
             1082  LOAD_FAST                'grade'
             1084  LOAD_ATTR                split
             1086  LOAD_STR                 '~'
             1088  CALL_FUNCTION_1       1  '1 positional argument'
             1090  LOAD_CONST               1
             1092  BINARY_SUBSCR    
             1094  CALL_FUNCTION_1       1  '1 positional argument'
             1096  STORE_FAST               'grade'

 L. 124      1098  LOAD_FAST                'result'
             1100  LOAD_FAST                'low'
             1102  COMPARE_OP               <
         1104_1106  POP_JUMP_IF_TRUE   1118  'to 1118'
             1108  LOAD_FAST                'result'
             1110  LOAD_FAST                'high'
             1112  COMPARE_OP               >
           1114_0  COME_FROM          1104  '1104'
         1114_1116  POP_JUMP_IF_FALSE  1666  'to 1666'

 L. 125      1118  LOAD_FAST                'grade'
             1120  LOAD_CONST               (3, 4)
             1122  COMPARE_OP               in
         1124_1126  POP_JUMP_IF_FALSE  1666  'to 1666'

 L. 126      1128  LOAD_FAST                'gap'
             1130  LOAD_CONST               -5
             1132  COMPARE_OP               <
         1134_1136  POP_JUMP_IF_FALSE  1148  'to 1148'
             1138  LOAD_FAST                'gap'
             1140  LOAD_CONST               -30
             1142  COMPARE_OP               <
           1144_0  COME_FROM          1134  '1134'
         1144_1146  POP_JUMP_IF_TRUE   1188  'to 1188'
             1148  LOAD_FAST                'ae_record'
             1150  LOAD_STR                 'AESTDAT'
             1152  BINARY_SUBSCR    
             1154  LOAD_ATTR                values
             1156  LOAD_CONST               0
             1158  BINARY_SUBSCR    
             1160  LOAD_FAST                'lb_record'
             1162  LOAD_STR                 'LBDAT'
             1164  BINARY_SUBSCR    
             1166  LOAD_ATTR                values
             1168  LOAD_CONST               0
             1170  BINARY_SUBSCR    
             1172  COMPARE_OP               >
         1174_1176  POP_JUMP_IF_FALSE  1666  'to 1666'
             1178  LOAD_FAST                'gap'
             1180  LOAD_CONST               30
             1182  COMPARE_OP               >
           1184_0  COME_FROM          1174  '1174'
           1184_1  COME_FROM          1144  '1144'
         1184_1186  POP_JUMP_IF_FALSE  1666  'to 1666'

 L. 127      1188  LOAD_FAST                'labdate'
             1190  LOAD_FAST                'aestdat'
             1192  COMPARE_OP               <
         1194_1196  POP_JUMP_IF_FALSE  1208  'to 1208'
             1198  LOAD_FAST                'labdate'
             1200  LOAD_FAST                'aeendat'
             1202  COMPARE_OP               <
           1204_0  COME_FROM          1194  '1194'
         1204_1206  POP_JUMP_IF_TRUE   1228  'to 1228'
             1208  LOAD_FAST                'labdate'
             1210  LOAD_FAST                'aestdat'
             1212  COMPARE_OP               >
         1214_1216  POP_JUMP_IF_FALSE  1666  'to 1666'
             1218  LOAD_FAST                'labdate'
             1220  LOAD_FAST                'aeendat'
             1222  COMPARE_OP               >
           1224_0  COME_FROM          1214  '1214'
           1224_1  COME_FROM          1204  '1204'
         1224_1226  POP_JUMP_IF_FALSE  1666  'to 1666'

 L. 129      1228  BUILD_MAP_0           0 
             1230  STORE_FAST               'subcate_report_dict'

 L. 130      1232  BUILD_MAP_0           0 
             1234  STORE_FAST               'report_dict'

 L. 132      1236  LOAD_FAST                'ae_record'
             1238  STORE_FAST               'ae_record2'

 L. 133      1240  LOAD_FAST                'lb_record'
             1242  STORE_FAST               'lb_record2'

 L. 134      1244  LOAD_FAST                'ae_record2'
             1246  LOAD_STR                 'AESTDAT'
             1248  BINARY_SUBSCR    
             1250  LOAD_ATTR                dt
             1252  LOAD_ATTR                strftime
             1254  LOAD_STR                 '%d-%B-%Y'
             1256  CALL_FUNCTION_1       1  '1 positional argument'
             1258  LOAD_FAST                'ae_record2'
             1260  LOAD_STR                 'AESTDAT'
             1262  STORE_SUBSCR     

 L. 135      1264  LOAD_FAST                'ae_record2'
             1266  LOAD_STR                 'AEENDAT'
             1268  BINARY_SUBSCR    
             1270  LOAD_ATTR                dt
             1272  LOAD_ATTR                strftime
             1274  LOAD_STR                 '%d-%B-%Y'
             1276  CALL_FUNCTION_1       1  '1 positional argument'
             1278  LOAD_FAST                'ae_record2'
             1280  LOAD_STR                 'AEENDAT'
             1282  STORE_SUBSCR     

 L. 136      1284  LOAD_FAST                'lb_record2'
             1286  LOAD_STR                 'LBDAT'
             1288  BINARY_SUBSCR    
             1290  LOAD_ATTR                dt
             1292  LOAD_ATTR                strftime
             1294  LOAD_STR                 '%d-%B-%Y'
             1296  CALL_FUNCTION_1       1  '1 positional argument'
             1298  LOAD_FAST                'lb_record2'
             1300  LOAD_STR                 'LBDAT'
             1302  STORE_SUBSCR     

 L. 137      1304  LOAD_FAST                'grade'
             1306  LOAD_FAST                'lb_record2'
             1308  LOAD_STR                 'grade'
             1310  STORE_SUBSCR     

 L. 139      1312  LOAD_FAST                'lb_record2'
             1314  LOAD_FAST                'ae_record2'
             1316  LOAD_CONST               ('LB', 'AE')
             1318  BUILD_CONST_KEY_MAP_2     2 
             1320  STORE_FAST               'piv_rec'

 L. 141      1322  SETUP_LOOP         1436  'to 1436'
             1324  LOAD_GLOBAL              a
             1326  LOAD_STR                 'FIELDS_FOR_UI'
             1328  BINARY_SUBSCR    
             1330  LOAD_FAST                'sub_cat'
             1332  BINARY_SUBSCR    
             1334  LOAD_ATTR                items
             1336  CALL_FUNCTION_0       0  '0 positional arguments'
             1338  GET_ITER         
             1340  FOR_ITER           1434  'to 1434'
             1342  UNPACK_SEQUENCE_2     2 
             1344  STORE_FAST               'dom'
             1346  STORE_FAST               'cols'

 L. 142      1348  LOAD_FAST                'piv_rec'
             1350  LOAD_FAST                'dom'
             1352  BINARY_SUBSCR    
             1354  STORE_DEREF              'piv_df'

 L. 143      1356  LOAD_CLOSURE             'piv_df'
             1358  BUILD_TUPLE_1         1 
             1360  LOAD_LISTCOMP            '<code_object <listcomp>>'
             1362  LOAD_STR                 'LBAE0.execute.<locals>.<listcomp>'
             1364  MAKE_FUNCTION_CLOSURE        'closure'
             1366  LOAD_FAST                'cols'
             1368  GET_ITER         
             1370  CALL_FUNCTION_1       1  '1 positional argument'
             1372  STORE_FAST               'present_col'

 L. 144      1374  LOAD_DEREF               'piv_df'
             1376  LOAD_FAST                'present_col'
             1378  BINARY_SUBSCR    
             1380  STORE_FAST               'rep_df'

 L. 145      1382  LOAD_GLOBAL              utils
             1384  LOAD_ATTR                get_deeplink
             1386  LOAD_FAST                'study'
             1388  LOAD_DEREF               'piv_df'
             1390  CALL_FUNCTION_2       2  '2 positional arguments'
             1392  LOAD_FAST                'rep_df'
             1394  LOAD_STR                 'deeplink'
             1396  STORE_SUBSCR     

 L. 146      1398  LOAD_FAST                'rep_df'
             1400  LOAD_ATTR                rename
             1402  LOAD_GLOBAL              a
             1404  LOAD_STR                 'FIELD_NAME_DICT'
             1406  BINARY_SUBSCR    
             1408  LOAD_CONST               ('columns',)
             1410  CALL_FUNCTION_KW_1     1  '1 total positional and keyword args'
             1412  STORE_FAST               'rep_df'

 L. 147      1414  LOAD_FAST                'rep_df'
             1416  LOAD_ATTR                to_json
             1418  LOAD_STR                 'records'
             1420  LOAD_CONST               ('orient',)
             1422  CALL_FUNCTION_KW_1     1  '1 total positional and keyword args'
             1424  LOAD_FAST                'report_dict'
             1426  LOAD_FAST                'dom'
             1428  STORE_SUBSCR     
         1430_1432  JUMP_BACK          1340  'to 1340'
             1434  POP_BLOCK        
           1436_0  COME_FROM_LOOP     1322  '1322'

 L. 149      1436  LOAD_FAST                'report_dict'
             1438  LOAD_FAST                'subcate_report_dict'
             1440  LOAD_FAST                'sub_cat'
             1442  STORE_SUBSCR     

 L. 151      1444  LOAD_FAST                'ae_record2'
             1446  LOAD_STR                 'AESPID'
             1448  BINARY_SUBSCR    
             1450  LOAD_ATTR                values
             1452  LOAD_CONST               0
             1454  BINARY_SUBSCR    
             1456  STORE_FAST               'aespid'

 L. 152      1458  LOAD_FAST                'ae_record2'
             1460  LOAD_STR                 'AETERM'
             1462  BINARY_SUBSCR    
             1464  LOAD_ATTR                values
             1466  LOAD_CONST               0
             1468  BINARY_SUBSCR    
             1470  STORE_FAST               'aeterm'

 L. 153      1472  LOAD_FAST                'ae_record2'
             1474  LOAD_STR                 'AESTDAT'
             1476  BINARY_SUBSCR    
             1478  LOAD_ATTR                values
             1480  LOAD_CONST               0
             1482  BINARY_SUBSCR    
             1484  STORE_FAST               'aestdat'

 L. 154      1486  LOAD_FAST                'lb_record2'
             1488  LOAD_STR                 'LBDAT'
             1490  BINARY_SUBSCR    
             1492  LOAD_ATTR                values
             1494  LOAD_CONST               0
             1496  BINARY_SUBSCR    
             1498  STORE_FAST               'labdate'

 L. 156      1500  LOAD_FAST                'lb_record'
             1502  LOAD_ATTR                iloc
             1504  LOAD_CONST               0
             1506  BINARY_SUBSCR    
             1508  STORE_FAST               'lb_record1'

 L. 158      1510  LOAD_FAST                'aespid'

 L. 159      1512  LOAD_FAST                'aeterm'

 L. 160      1514  LOAD_GLOBAL              str
             1516  LOAD_FAST                'aestdat'
             1518  CALL_FUNCTION_1       1  '1 positional argument'
             1520  LOAD_ATTR                split
             1522  LOAD_STR                 'T'
             1524  CALL_FUNCTION_1       1  '1 positional argument'
             1526  LOAD_CONST               0
             1528  BINARY_SUBSCR    

 L. 161      1530  LOAD_FAST                'test'
             1532  LOAD_ATTR                split
             1534  LOAD_STR                 '_'
             1536  CALL_FUNCTION_1       1  '1 positional argument'
             1538  LOAD_CONST               0
             1540  BINARY_SUBSCR    

 L. 162      1542  LOAD_GLOBAL              str
             1544  LOAD_FAST                'labdate'
             1546  CALL_FUNCTION_1       1  '1 positional argument'
             1548  LOAD_ATTR                split
             1550  LOAD_STR                 'T'
             1552  CALL_FUNCTION_1       1  '1 positional argument'
             1554  LOAD_CONST               0
             1556  BINARY_SUBSCR    

 L. 163      1558  LOAD_FAST                'grade'
             1560  LOAD_CONST               ('AESPID', 'AETERM', 'AESTDAT', 'LBTEST', 'LBDAT', 'grade')
             1562  BUILD_CONST_KEY_MAP_6     6 
             1564  STORE_FAST               'keys'

 L. 167      1566  LOAD_FAST                'sub_cat'

 L. 168      1568  LOAD_FAST                'self'
             1570  LOAD_ATTR                get_model_query_text_json
             1572  LOAD_FAST                'study'
             1574  LOAD_FAST                'sub_cat'
             1576  LOAD_FAST                'keys'
             1578  LOAD_CONST               ('params',)
             1580  CALL_FUNCTION_KW_3     3  '3 total positional and keyword args'

 L. 169      1582  LOAD_GLOBAL              str
             1584  LOAD_FAST                'lb_record1'
             1586  LOAD_STR                 'form_index'
             1588  BINARY_SUBSCR    
             1590  CALL_FUNCTION_1       1  '1 positional argument'

 L. 170      1592  LOAD_FAST                'self'
             1594  LOAD_ATTR                get_subcategory_json
             1596  LOAD_FAST                'study'
             1598  LOAD_FAST                'sub_cat'
             1600  CALL_FUNCTION_2       2  '2 positional arguments'

 L. 171      1602  LOAD_GLOBAL              str
             1604  LOAD_FAST                'lb_record1'
             1606  LOAD_STR                 'modif_dts'
             1608  BINARY_SUBSCR    
             1610  CALL_FUNCTION_1       1  '1 positional argument'

 L. 172      1612  LOAD_GLOBAL              int
             1614  LOAD_FAST                'lb_record1'
             1616  LOAD_STR                 'ck_event_id'
             1618  BINARY_SUBSCR    
             1620  CALL_FUNCTION_1       1  '1 positional argument'

 L. 173      1622  LOAD_GLOBAL              str
             1624  LOAD_FAST                'lb_record1'
             1626  LOAD_STR                 'formrefname'
             1628  BINARY_SUBSCR    
             1630  CALL_FUNCTION_1       1  '1 positional argument'

 L. 174      1632  LOAD_FAST                'subcate_report_dict'

 L. 175      1634  LOAD_GLOBAL              np
             1636  LOAD_ATTR                random
             1638  LOAD_ATTR                uniform
             1640  LOAD_CONST               0.7
             1642  LOAD_CONST               0.97
             1644  CALL_FUNCTION_2       2  '2 positional arguments'
             1646  LOAD_CONST               ('subcategory', 'query_text', 'form_index', 'question_present', 'modif_dts', 'stg_ck_event_id', 'formrefname', 'report', 'confid_score')
             1648  BUILD_CONST_KEY_MAP_9     9 
             1650  STORE_FAST               'payload'

 L. 180      1652  LOAD_FAST                'self'
             1654  LOAD_ATTR                insert_query
             1656  LOAD_FAST                'study'
             1658  LOAD_FAST                'subject'
             1660  LOAD_FAST                'payload'
             1662  CALL_FUNCTION_3       3  '3 positional arguments'
             1664  POP_TOP          
           1666_0  COME_FROM          1224  '1224'
           1666_1  COME_FROM          1184  '1184'
           1666_2  COME_FROM          1124  '1124'
           1666_3  COME_FROM          1114  '1114'
             1666  POP_BLOCK        
             1668  JUMP_FORWARD       1686  'to 1686'
           1670_0  COME_FROM_EXCEPT    778  '778'

 L. 181      1670  POP_TOP          
             1672  POP_TOP          
             1674  POP_TOP          

 L. 183  1676_1678  CONTINUE_LOOP       772  'to 772'
             1680  POP_EXCEPT       
             1682  JUMP_FORWARD       1686  'to 1686'
             1684  END_FINALLY      
           1686_0  COME_FROM          1682  '1682'
           1686_1  COME_FROM          1668  '1668'
         1686_1688  JUMP_BACK           772  'to 772'
             1690  POP_BLOCK        
           1692_0  COME_FROM_LOOP      754  '754'
             1692  POP_BLOCK        
             1694  JUMP_FORWARD       1712  'to 1712'
           1696_0  COME_FROM_EXCEPT    334  '334'

 L. 184      1696  POP_TOP          
             1698  POP_TOP          
             1700  POP_TOP          

 L. 186  1702_1704  CONTINUE_LOOP       328  'to 328'
             1706  POP_EXCEPT       
             1708  JUMP_FORWARD       1712  'to 1712'
             1710  END_FINALLY      
           1712_0  COME_FROM          1708  '1708'
           1712_1  COME_FROM          1694  '1694'
         1712_1714  JUMP_BACK           328  'to 328'
             1716  POP_BLOCK        
           1718_0  COME_FROM_LOOP      310  '310'
           1718_1  COME_FROM           290  '290'
             1718  POP_BLOCK        
             1720  JUMP_FORWARD       1738  'to 1738'
           1722_0  COME_FROM_EXCEPT    268  '268'

 L. 188      1722  POP_TOP          
             1724  POP_TOP          
             1726  POP_TOP          

 L. 190  1728_1730  CONTINUE_LOOP       262  'to 262'
             1732  POP_EXCEPT       
             1734  JUMP_FORWARD       1738  'to 1738'
             1736  END_FINALLY      
           1738_0  COME_FROM          1734  '1734'
           1738_1  COME_FROM          1720  '1720'
         1738_1740  JUMP_BACK           262  'to 262'
             1742  POP_BLOCK        
           1744_0  COME_FROM_LOOP      254  '254'
             1744  JUMP_BACK           248  'to 248'
             1746  POP_BLOCK        
           1748_0  COME_FROM_LOOP      240  '240'
             1748  POP_BLOCK        
             1750  JUMP_BACK            42  'to 42'
           1752_0  COME_FROM_EXCEPT     48  '48'

 L. 192      1752  POP_TOP          
             1754  POP_TOP          
             1756  POP_TOP          

 L. 194      1758  CONTINUE_LOOP        42  'to 42'
             1760  POP_EXCEPT       
             1762  JUMP_BACK            42  'to 42'
             1764  END_FINALLY      
             1766  JUMP_BACK            42  'to 42'
             1768  POP_BLOCK        
           1770_0  COME_FROM_LOOP       28  '28'

Parse error at or near `POP_JUMP_IF_TRUE' instruction at offset 1144_1146


if __name__ == '__main__':
    study_id = sys.argv[1]
    rule_id = sys.argv[2]
    version = sys.argv[3]
    rule = LBAE0(study_id, rule_id, version)
    rule.run()