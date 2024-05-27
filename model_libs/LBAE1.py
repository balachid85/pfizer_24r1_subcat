# uncompyle6 version 3.9.0
# Python bytecode version base 3.6 (3379)
# Decompiled from: Python 3.10.1 (v3.10.1:2cd268a3a9, Dec  6 2021, 14:28:59) [Clang 13.0.0 (clang-1300.0.29.3)]
# Embedded file name: LBAE1.py
# Compiled at: 2021-01-21 10:29:07
# Size of source mod 2**32: 13647 bytes
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

class LBAE1(BaseSDQApi):
    domain_list = [
     'AE', 'LB']

    def execute--- This code section failed: ---

 L.  33         0  LOAD_FAST                'self'
                2  LOAD_ATTR                study_id
                4  STORE_FAST               'study'

 L.  35         6  LOAD_STR                 'LBAE1'
                8  STORE_FAST               'sub_cat'

 L.  36        10  LOAD_FAST                'self'
               12  LOAD_ATTR                get_subjects
               14  LOAD_FAST                'study'
               16  LOAD_FAST                'self'
               18  LOAD_ATTR                domain_list
               20  LOAD_CONST               10000
               22  LOAD_CONST               ('domain_list', 'per_page')
               24  CALL_FUNCTION_KW_3     3  '3 total positional and keyword args'
               26  STORE_FAST               'subjects'

 L.  38     28_30  SETUP_LOOP         1842  'to 1842'
               32  LOAD_GLOBAL              tqdm
               34  LOAD_ATTR                tqdm
               36  LOAD_FAST                'subjects'
               38  CALL_FUNCTION_1       1  '1 positional argument'
               40  GET_ITER         
            42_44  FOR_ITER           1840  'to 1840'
               46  STORE_FAST               'subject'

 L.  39     48_50  SETUP_EXCEPT       1824  'to 1824'

 L.  40        52  LOAD_FAST                'self'
               54  LOAD_ATTR                get_flatten_data
               56  LOAD_FAST                'study'
               58  LOAD_FAST                'subject'
               60  LOAD_CONST               10000
               62  LOAD_FAST                'self'
               64  LOAD_ATTR                domain_list
               66  LOAD_CONST               ('per_page', 'domain_list')
               68  CALL_FUNCTION_KW_4     4  '4 total positional and keyword args'
               70  STORE_FAST               'flatten_data'

 L.  42        72  LOAD_STR                 'AE'
               74  LOAD_FAST                'flatten_data'
               76  COMPARE_OP               not-in
               78  POP_JUMP_IF_TRUE     88  'to 88'
               80  LOAD_STR                 'LB'
               82  LOAD_FAST                'flatten_data'
               84  COMPARE_OP               not-in
             86_0  COME_FROM            78  '78'
               86  POP_JUMP_IF_FALSE    90  'to 90'

 L.  43        88  CONTINUE_LOOP        42  'to 42'
             90_0  COME_FROM            86  '86'

 L.  45        90  LOAD_GLOBAL              pd
               92  LOAD_ATTR                DataFrame
               94  LOAD_FAST                'flatten_data'
               96  LOAD_STR                 'AE'
               98  BINARY_SUBSCR    
              100  CALL_FUNCTION_1       1  '1 positional argument'
              102  STORE_FAST               'ae_df'

 L.  46       104  LOAD_FAST                'ae_df'
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

 L.  47       132  LOAD_FAST                'ae_df'
              134  LOAD_STR                 'AEPTCD'
              136  BINARY_SUBSCR    
              138  LOAD_ATTR                unique
              140  CALL_FUNCTION_0       0  '0 positional arguments'
              142  LOAD_ATTR                tolist
              144  CALL_FUNCTION_0       0  '0 positional arguments'
              146  STORE_FAST               'codes'

 L.  49       148  LOAD_GLOBAL              pd
              150  LOAD_ATTR                DataFrame
              152  LOAD_FAST                'flatten_data'
              154  LOAD_STR                 'LB'
              156  BINARY_SUBSCR    
              158  CALL_FUNCTION_1       1  '1 positional argument'
              160  STORE_FAST               'lb_df'

 L.  50       162  LOAD_FAST                'lb_df'
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

 L.  51       190  LOAD_FAST                'lb_df'
              192  LOAD_STR                 'LBTEST'
              194  BINARY_SUBSCR    
              196  LOAD_ATTR                str
              198  LOAD_ATTR                upper
              200  CALL_FUNCTION_0       0  '0 positional arguments'
              202  LOAD_FAST                'lb_df'
              204  LOAD_STR                 'LBTEST'
              206  STORE_SUBSCR     

 L.  52       208  LOAD_FAST                'lb_df'
              210  LOAD_FAST                'lb_df'
              212  LOAD_STR                 'LBTEST'
              214  BINARY_SUBSCR    
              216  LOAD_STR                 'HEMOGLOBIN_PX87'
              218  COMPARE_OP               !=
              220  BINARY_SUBSCR    
              222  STORE_FAST               'lb_df'

 L.  53       224  LOAD_FAST                'lb_df'
              226  LOAD_STR                 'LBTEST'
              228  BINARY_SUBSCR    
              230  LOAD_ATTR                unique
              232  CALL_FUNCTION_0       0  '0 positional arguments'
              234  LOAD_ATTR                tolist
              236  CALL_FUNCTION_0       0  '0 positional arguments'
              238  STORE_FAST               'tests'

 L.  55   240_242  SETUP_LOOP         1820  'to 1820'
              244  LOAD_FAST                'tests'
              246  GET_ITER         
          248_250  FOR_ITER           1818  'to 1818'
              252  STORE_FAST               'test'

 L.  56   254_256  SETUP_LOOP         1816  'to 1816'
              258  LOAD_FAST                'codes'
              260  GET_ITER         
          262_264  FOR_ITER           1814  'to 1814'
              266  STORE_FAST               'code'

 L.  57   268_270  SETUP_EXCEPT       1794  'to 1794'

 L.  58       272  LOAD_GLOBAL              utils
              274  LOAD_ATTR                check_aelab
              276  LOAD_GLOBAL              float
              278  LOAD_FAST                'code'
              280  CALL_FUNCTION_1       1  '1 positional argument'
              282  LOAD_FAST                'test'
              284  CALL_FUNCTION_2       2  '2 positional arguments'
              286  LOAD_CONST               True
              288  COMPARE_OP               ==
          290_292  POP_JUMP_IF_FALSE  1790  'to 1790'

 L.  59       294  LOAD_FAST                'lb_df'
              296  LOAD_FAST                'lb_df'
              298  LOAD_STR                 'LBTEST'
              300  BINARY_SUBSCR    
              302  LOAD_FAST                'test'
              304  COMPARE_OP               ==
              306  BINARY_SUBSCR    
              308  STORE_FAST               'lb_df1'

 L.  60   310_312  SETUP_LOOP         1790  'to 1790'
              314  LOAD_GLOBAL              range
              316  LOAD_FAST                'lb_df1'
              318  LOAD_ATTR                shape
              320  LOAD_CONST               0
              322  BINARY_SUBSCR    
              324  CALL_FUNCTION_1       1  '1 positional argument'
              326  GET_ITER         
          328_330  FOR_ITER           1788  'to 1788'
              332  STORE_FAST               'ind'

 L.  61   334_336  SETUP_EXCEPT       1768  'to 1768'

 L.  62       338  LOAD_FAST                'ae_df'
              340  STORE_FAST               'ae_records'

 L.  63       342  LOAD_FAST                'lb_df1'
              344  LOAD_ATTR                iloc
              346  LOAD_FAST                'ind'
              348  BUILD_LIST_1          1 
              350  BINARY_SUBSCR    
              352  STORE_FAST               'lb_record'

 L.  64       354  LOAD_FAST                'lb_record'
              356  LOAD_STR                 'LBTEST'
              358  BINARY_SUBSCR    
              360  LOAD_ATTR                values
              362  LOAD_CONST               0
              364  BINARY_SUBSCR    
              366  STORE_FAST               'test'

 L.  65       368  LOAD_FAST                'ae_records'
              370  LOAD_FAST                'ae_records'
              372  LOAD_STR                 'AEPTCD'
              374  BINARY_SUBSCR    
              376  LOAD_FAST                'code'
              378  COMPARE_OP               ==
              380  BINARY_SUBSCR    
              382  STORE_FAST               'ae_records'

 L.  66       384  LOAD_GLOBAL              pd
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

 L.  67       406  LOAD_GLOBAL              pd
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

 L.  68       428  LOAD_GLOBAL              pd
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

 L.  69       450  LOAD_FAST                'ae_records'
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

 L.  71       488  LOAD_FAST                'ae_records'
              490  LOAD_STR                 'DIFFERENCE'
              492  BINARY_SUBSCR    
              494  LOAD_ATTR                unique
              496  CALL_FUNCTION_0       0  '0 positional arguments'
              498  LOAD_ATTR                tolist
              500  CALL_FUNCTION_0       0  '0 positional arguments'
              502  STORE_FAST               'diff'

 L.  72       504  LOAD_LISTCOMP            '<code_object <listcomp>>'
              506  LOAD_STR                 'LBAE1.execute.<locals>.<listcomp>'
              508  MAKE_FUNCTION_0          'Neither defaults, keyword-only args, annotations, nor closures'
              510  LOAD_FAST                'diff'
              512  GET_ITER         
              514  CALL_FUNCTION_1       1  '1 positional argument'
              516  STORE_FAST               'diff1'

 L.  73       518  LOAD_LISTCOMP            '<code_object <listcomp>>'
              520  LOAD_STR                 'LBAE1.execute.<locals>.<listcomp>'
              522  MAKE_FUNCTION_0          'Neither defaults, keyword-only args, annotations, nor closures'
              524  LOAD_FAST                'diff'
              526  GET_ITER         
              528  CALL_FUNCTION_1       1  '1 positional argument'
              530  STORE_FAST               'diff2'

 L.  75       532  LOAD_GLOBAL              len
              534  LOAD_FAST                'diff1'
              536  CALL_FUNCTION_1       1  '1 positional argument'
              538  LOAD_CONST               0
              540  COMPARE_OP               >
          542_544  POP_JUMP_IF_FALSE   636  'to 636'

 L.  76       546  LOAD_GLOBAL              min
              548  LOAD_FAST                'diff1'
              550  CALL_FUNCTION_1       1  '1 positional argument'
              552  STORE_FAST               'minimum1'

 L.  77       554  LOAD_FAST                'minimum1'
              556  STORE_FAST               'final_minimum'

 L.  78       558  LOAD_GLOBAL              len
              560  LOAD_FAST                'diff2'
              562  CALL_FUNCTION_1       1  '1 positional argument'
              564  LOAD_CONST               0
              566  COMPARE_OP               >
          568_570  POP_JUMP_IF_FALSE   738  'to 738'

 L.  79       572  LOAD_GLOBAL              max
              574  LOAD_FAST                'diff2'
              576  CALL_FUNCTION_1       1  '1 positional argument'
              578  STORE_FAST               'minimum2'

 L.  80       580  LOAD_GLOBAL              min
              582  LOAD_FAST                'minimum1'
              584  LOAD_GLOBAL              abs
              586  LOAD_FAST                'minimum2'
              588  CALL_FUNCTION_1       1  '1 positional argument'
              590  CALL_FUNCTION_2       2  '2 positional arguments'
              592  STORE_FAST               'final_minimum'

 L.  81       594  LOAD_FAST                'minimum1'
              596  LOAD_GLOBAL              abs
              598  LOAD_FAST                'minimum2'
              600  CALL_FUNCTION_1       1  '1 positional argument'
              602  COMPARE_OP               ==
          604_606  POP_JUMP_IF_FALSE   614  'to 614'

 L.  82       608  LOAD_FAST                'minimum1'
              610  STORE_FAST               'final_minimum'
              612  JUMP_FORWARD        634  'to 634'
              614  ELSE                     '634'

 L.  83       614  LOAD_FAST                'final_minimum'
              616  LOAD_GLOBAL              abs
              618  LOAD_FAST                'minimum2'
              620  CALL_FUNCTION_1       1  '1 positional argument'
              622  COMPARE_OP               ==
          624_626  POP_JUMP_IF_FALSE   738  'to 738'

 L.  84       628  LOAD_FAST                'final_minimum'
              630  UNARY_NEGATIVE   
              632  STORE_FAST               'final_minimum'
            634_0  COME_FROM           612  '612'
              634  JUMP_FORWARD        738  'to 738'
              636  ELSE                     '738'

 L.  85       636  LOAD_GLOBAL              len
              638  LOAD_FAST                'diff2'
              640  CALL_FUNCTION_1       1  '1 positional argument'
              642  LOAD_CONST               0
              644  COMPARE_OP               >
          646_648  POP_JUMP_IF_FALSE   738  'to 738'

 L.  86       650  LOAD_GLOBAL              max
              652  LOAD_FAST                'diff2'
              654  CALL_FUNCTION_1       1  '1 positional argument'
              656  STORE_FAST               'minimum2'

 L.  87       658  LOAD_FAST                'minimum2'
              660  STORE_FAST               'final_minimum'

 L.  88       662  LOAD_GLOBAL              len
              664  LOAD_FAST                'diff1'
              666  CALL_FUNCTION_1       1  '1 positional argument'
              668  LOAD_CONST               0
              670  COMPARE_OP               >
          672_674  POP_JUMP_IF_FALSE   738  'to 738'

 L.  89       676  LOAD_GLOBAL              min
              678  LOAD_FAST                'diff1'
              680  CALL_FUNCTION_1       1  '1 positional argument'
              682  STORE_FAST               'minimum1'

 L.  90       684  LOAD_GLOBAL              min
              686  LOAD_FAST                'minimum1'
              688  LOAD_GLOBAL              abs
              690  LOAD_FAST                'minimum2'
              692  CALL_FUNCTION_1       1  '1 positional argument'
              694  CALL_FUNCTION_2       2  '2 positional arguments'
              696  STORE_FAST               'final_minimum'

 L.  91       698  LOAD_FAST                'minimum1'
              700  LOAD_GLOBAL              abs
              702  LOAD_FAST                'minimum2'
              704  CALL_FUNCTION_1       1  '1 positional argument'
              706  COMPARE_OP               ==
          708_710  POP_JUMP_IF_FALSE   718  'to 718'

 L.  92       712  LOAD_FAST                'minimum1'
              714  STORE_FAST               'final_minimum'
              716  JUMP_FORWARD        738  'to 738'
              718  ELSE                     '738'

 L.  93       718  LOAD_FAST                'final_minimum'
              720  LOAD_GLOBAL              abs
              722  LOAD_FAST                'minimum2'
              724  CALL_FUNCTION_1       1  '1 positional argument'
              726  COMPARE_OP               ==
          728_730  POP_JUMP_IF_FALSE   738  'to 738'

 L.  94       732  LOAD_FAST                'final_minimum'
              734  UNARY_NEGATIVE   
              736  STORE_FAST               'final_minimum'
            738_0  COME_FROM           728  '728'
            738_1  COME_FROM           716  '716'
            738_2  COME_FROM           672  '672'
            738_3  COME_FROM           646  '646'
            738_4  COME_FROM           634  '634'
            738_5  COME_FROM           624  '624'
            738_6  COME_FROM           568  '568'

 L.  97       738  LOAD_FAST                'ae_records'
              740  LOAD_FAST                'ae_records'
              742  LOAD_STR                 'DIFFERENCE'
              744  BINARY_SUBSCR    
              746  LOAD_FAST                'final_minimum'
              748  COMPARE_OP               ==
              750  BINARY_SUBSCR    
              752  STORE_FAST               'ae_records'

 L.  99   754_756  SETUP_LOOP         1764  'to 1764'
              758  LOAD_GLOBAL              range
              760  LOAD_FAST                'ae_records'
              762  LOAD_ATTR                shape
              764  LOAD_CONST               0
              766  BINARY_SUBSCR    
              768  CALL_FUNCTION_1       1  '1 positional argument'
              770  GET_ITER         
          772_774  FOR_ITER           1762  'to 1762'
              776  STORE_FAST               'ind1'

 L. 100   778_780  SETUP_EXCEPT       1742  'to 1742'

 L. 101       782  LOAD_FAST                'ae_records'
              784  LOAD_ATTR                iloc
              786  LOAD_FAST                'ind1'
              788  BUILD_LIST_1          1 
              790  BINARY_SUBSCR    
              792  STORE_FAST               'ae_record'

 L. 102       794  LOAD_FAST                'ae_record'
              796  LOAD_STR                 'AETERM'
              798  BINARY_SUBSCR    
              800  LOAD_ATTR                values
              802  LOAD_CONST               0
              804  BINARY_SUBSCR    
              806  STORE_FAST               'aeterm'

 L. 103       808  LOAD_FAST                'ae_record'
              810  LOAD_STR                 'AEPTCD'
              812  BINARY_SUBSCR    
              814  LOAD_ATTR                values
              816  LOAD_CONST               0
              818  BINARY_SUBSCR    
              820  STORE_FAST               'code'

 L. 104       822  LOAD_FAST                'ae_record'
              824  LOAD_STR                 'form_index'
              826  BINARY_SUBSCR    
              828  LOAD_ATTR                values
              830  LOAD_CONST               0
              832  BINARY_SUBSCR    
              834  STORE_FAST               'formindex'

 L. 105       836  LOAD_FAST                'ae_record'
              838  LOAD_STR                 'AESPID'
              840  BINARY_SUBSCR    
              842  LOAD_ATTR                values
              844  LOAD_CONST               0
              846  BINARY_SUBSCR    
              848  STORE_FAST               'aespid'

 L. 106       850  LOAD_GLOBAL              float
              852  LOAD_FAST                'lb_record'
              854  LOAD_STR                 'LBORRES'
              856  BINARY_SUBSCR    
              858  LOAD_ATTR                values
              860  LOAD_CONST               0
              862  BINARY_SUBSCR    
              864  CALL_FUNCTION_1       1  '1 positional argument'
              866  STORE_FAST               'result'

 L. 107       868  LOAD_FAST                'lb_record'
              870  LOAD_STR                 'LBSTAT'
              872  BINARY_SUBSCR    
              874  LOAD_ATTR                values
              876  LOAD_CONST               0
              878  BINARY_SUBSCR    
              880  STORE_FAST               'status'

 L. 108       882  LOAD_FAST                'ae_record'
              884  LOAD_STR                 'AESTDAT'
              886  BINARY_SUBSCR    
              888  LOAD_ATTR                values
              890  LOAD_CONST               0
              892  BINARY_SUBSCR    
              894  STORE_FAST               'aestdat'

 L. 109       896  LOAD_FAST                'ae_record'
              898  LOAD_STR                 'AEENDAT'
              900  BINARY_SUBSCR    
              902  LOAD_ATTR                values
              904  LOAD_CONST               0
              906  BINARY_SUBSCR    
              908  STORE_FAST               'aeendat'

 L. 110       910  LOAD_FAST                'lb_record'
              912  LOAD_STR                 'LBDAT'
              914  BINARY_SUBSCR    
              916  LOAD_ATTR                values
              918  LOAD_CONST               0
              920  BINARY_SUBSCR    
              922  STORE_FAST               'labdate'

 L. 111       924  LOAD_GLOBAL              float
              926  LOAD_FAST                'lb_record'
              928  LOAD_STR                 'LNMTLOW'
              930  BINARY_SUBSCR    
              932  LOAD_ATTR                values
              934  LOAD_CONST               0
              936  BINARY_SUBSCR    
              938  CALL_FUNCTION_1       1  '1 positional argument'
              940  STORE_FAST               'low'

 L. 112       942  LOAD_GLOBAL              float
              944  LOAD_FAST                'lb_record'
              946  LOAD_STR                 'LNMTHIGH'
              948  BINARY_SUBSCR    
              950  LOAD_ATTR                values
              952  LOAD_CONST               0
              954  BINARY_SUBSCR    
              956  CALL_FUNCTION_1       1  '1 positional argument'
              958  STORE_FAST               'high'

 L. 113       960  LOAD_FAST                'lb_record'
              962  LOAD_STR                 'LNMTUNIT'
              964  BINARY_SUBSCR    
              966  LOAD_ATTR                values
              968  LOAD_CONST               0
              970  BINARY_SUBSCR    
              972  STORE_FAST               'unit'

 L. 115       974  LOAD_FAST                'ae_record'
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

 L. 116      1014  LOAD_GLOBAL              int
             1016  LOAD_FAST                'gap'
             1018  CALL_FUNCTION_1       1  '1 positional argument'
             1020  STORE_FAST               'gap'

 L. 117      1022  LOAD_GLOBAL              grades
             1024  LOAD_STR                 'VERSION 4'
             1026  BINARY_SUBSCR    
             1028  LOAD_FAST                'aeterm'
             1030  LOAD_ATTR                upper
             1032  CALL_FUNCTION_0       0  '0 positional arguments'
             1034  BINARY_SUBSCR    
             1036  STORE_FAST               'func_name'

 L. 118      1038  LOAD_GLOBAL              type
             1040  LOAD_FAST                'func_name'
             1042  CALL_FUNCTION_1       1  '1 positional argument'
             1044  LOAD_GLOBAL              list
             1046  COMPARE_OP               ==
         1048_1050  POP_JUMP_IF_FALSE  1060  'to 1060'

 L. 119      1052  LOAD_FAST                'func_name'
             1054  LOAD_CONST               0
             1056  BINARY_SUBSCR    
             1058  STORE_FAST               'func_name'
           1060_0  COME_FROM          1048  '1048'

 L. 120      1060  LOAD_GLOBAL              globals
             1062  CALL_FUNCTION_0       0  '0 positional arguments'
             1064  LOAD_FAST                'func_name'
             1066  BINARY_SUBSCR    
             1068  LOAD_FAST                'result'
             1070  LOAD_FAST                'low'
             1072  LOAD_FAST                'high'
             1074  LOAD_FAST                'unit'
             1076  CALL_FUNCTION_4       4  '4 positional arguments'
             1078  STORE_FAST               'grade'

 L. 121      1080  LOAD_GLOBAL              int
             1082  LOAD_FAST                'grade'
             1084  LOAD_ATTR                split
             1086  LOAD_STR                 '~'
             1088  CALL_FUNCTION_1       1  '1 positional argument'
             1090  LOAD_CONST               1
             1092  BINARY_SUBSCR    
             1094  CALL_FUNCTION_1       1  '1 positional argument'
             1096  STORE_FAST               'grade'

 L. 123      1098  LOAD_FAST                'result'
             1100  LOAD_FAST                'low'
             1102  COMPARE_OP               <
         1104_1106  POP_JUMP_IF_TRUE   1118  'to 1118'
             1108  LOAD_FAST                'result'
             1110  LOAD_FAST                'high'
             1112  COMPARE_OP               >
           1114_0  COME_FROM          1104  '1104'
         1114_1116  POP_JUMP_IF_FALSE  1738  'to 1738'

 L. 124      1118  LOAD_FAST                'grade'
             1120  LOAD_CONST               (3, 4)
             1122  COMPARE_OP               in
         1124_1126  POP_JUMP_IF_FALSE  1738  'to 1738'

 L. 125      1128  LOAD_FAST                'gap'
             1130  LOAD_CONST               -5
             1132  COMPARE_OP               <
         1134_1136  POP_JUMP_IF_FALSE  1148  'to 1148'
             1138  LOAD_FAST                'gap'
             1140  LOAD_CONST               -30
             1142  COMPARE_OP               >
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
         1174_1176  POP_JUMP_IF_FALSE  1738  'to 1738'
             1178  LOAD_FAST                'gap'
             1180  LOAD_CONST               30
             1182  COMPARE_OP               <
           1184_0  COME_FROM          1174  '1174'
           1184_1  COME_FROM          1144  '1144'
         1184_1186  POP_JUMP_IF_FALSE  1738  'to 1738'

 L. 126      1188  LOAD_FAST                'labdate'
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
         1214_1216  POP_JUMP_IF_FALSE  1738  'to 1738'
             1218  LOAD_FAST                'labdate'
             1220  LOAD_FAST                'aeendat'
             1222  COMPARE_OP               >
           1224_0  COME_FROM          1214  '1214'
           1224_1  COME_FROM          1204  '1204'
         1224_1226  POP_JUMP_IF_FALSE  1738  'to 1738'

 L. 128      1228  BUILD_MAP_0           0 
             1230  STORE_FAST               'subcate_report_dict'

 L. 129      1232  BUILD_MAP_0           0 
             1234  STORE_FAST               'report_dict'

 L. 131      1236  LOAD_FAST                'ae_record'
             1238  STORE_FAST               'ae_record2'

 L. 132      1240  LOAD_FAST                'lb_record'
             1242  STORE_FAST               'lb_record2'

 L. 133      1244  LOAD_FAST                'ae_record2'
             1246  LOAD_STR                 'AESTDAT'
             1248  BINARY_SUBSCR    
             1250  LOAD_ATTR                dt
             1252  LOAD_ATTR                strftime
             1254  LOAD_STR                 '%d-%B-%Y'
             1256  CALL_FUNCTION_1       1  '1 positional argument'
             1258  LOAD_FAST                'ae_record2'
             1260  LOAD_STR                 'AESTDAT'
             1262  STORE_SUBSCR     

 L. 134      1264  LOAD_FAST                'ae_record2'
             1266  LOAD_STR                 'AEENDAT'
             1268  BINARY_SUBSCR    
             1270  LOAD_ATTR                dt
             1272  LOAD_ATTR                strftime
             1274  LOAD_STR                 '%d-%B-%Y'
             1276  CALL_FUNCTION_1       1  '1 positional argument'
             1278  LOAD_FAST                'ae_record2'
             1280  LOAD_STR                 'AEENDAT'
             1282  STORE_SUBSCR     

 L. 135      1284  LOAD_FAST                'lb_record2'
             1286  LOAD_STR                 'LBDAT'
             1288  BINARY_SUBSCR    
             1290  LOAD_ATTR                dt
             1292  LOAD_ATTR                strftime
             1294  LOAD_STR                 '%d-%B-%Y'
             1296  CALL_FUNCTION_1       1  '1 positional argument'
             1298  LOAD_FAST                'lb_record2'
             1300  LOAD_STR                 'LBDAT'
             1302  STORE_SUBSCR     

 L. 136      1304  LOAD_FAST                'grade'
             1306  LOAD_FAST                'lb_record2'
             1308  LOAD_STR                 'grade'
             1310  STORE_SUBSCR     

 L. 138      1312  LOAD_FAST                'lb_record2'
             1314  LOAD_FAST                'ae_record2'
             1316  LOAD_CONST               ('LB', 'AE')
             1318  BUILD_CONST_KEY_MAP_2     2 
             1320  STORE_FAST               'piv_rec'

 L. 140      1322  SETUP_LOOP         1436  'to 1436'
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

 L. 141      1348  LOAD_FAST                'piv_rec'
             1350  LOAD_FAST                'dom'
             1352  BINARY_SUBSCR    
             1354  STORE_DEREF              'piv_df'

 L. 142      1356  LOAD_CLOSURE             'piv_df'
             1358  BUILD_TUPLE_1         1 
             1360  LOAD_LISTCOMP            '<code_object <listcomp>>'
             1362  LOAD_STR                 'LBAE1.execute.<locals>.<listcomp>'
             1364  MAKE_FUNCTION_CLOSURE        'closure'
             1366  LOAD_FAST                'cols'
             1368  GET_ITER         
             1370  CALL_FUNCTION_1       1  '1 positional argument'
             1372  STORE_FAST               'present_col'

 L. 143      1374  LOAD_DEREF               'piv_df'
             1376  LOAD_FAST                'present_col'
             1378  BINARY_SUBSCR    
             1380  STORE_FAST               'rep_df'

 L. 144      1382  LOAD_GLOBAL              utils
             1384  LOAD_ATTR                get_deeplink
             1386  LOAD_FAST                'study'
             1388  LOAD_DEREF               'piv_df'
             1390  CALL_FUNCTION_2       2  '2 positional arguments'
             1392  LOAD_FAST                'rep_df'
             1394  LOAD_STR                 'deeplink'
             1396  STORE_SUBSCR     

 L. 145      1398  LOAD_FAST                'rep_df'
             1400  LOAD_ATTR                rename
             1402  LOAD_GLOBAL              a
             1404  LOAD_STR                 'FIELD_NAME_DICT'
             1406  BINARY_SUBSCR    
             1408  LOAD_CONST               ('columns',)
             1410  CALL_FUNCTION_KW_1     1  '1 total positional and keyword args'
             1412  STORE_FAST               'rep_df'

 L. 146      1414  LOAD_FAST                'rep_df'
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

 L. 148      1436  LOAD_FAST                'report_dict'
             1438  LOAD_FAST                'subcate_report_dict'
             1440  LOAD_FAST                'sub_cat'
             1442  STORE_SUBSCR     

 L. 150      1444  LOAD_FAST                'ae_record2'
             1446  LOAD_STR                 'AESPID'
             1448  BINARY_SUBSCR    
             1450  LOAD_ATTR                values
             1452  LOAD_CONST               0
             1454  BINARY_SUBSCR    
             1456  STORE_FAST               'aespid'

 L. 151      1458  LOAD_FAST                'ae_record2'
             1460  LOAD_STR                 'AETERM'
             1462  BINARY_SUBSCR    
             1464  LOAD_ATTR                values
             1466  LOAD_CONST               0
             1468  BINARY_SUBSCR    
             1470  STORE_FAST               'aeterm'

 L. 152      1472  LOAD_FAST                'ae_record2'
             1474  LOAD_STR                 'AESTDAT'
             1476  BINARY_SUBSCR    
             1478  LOAD_ATTR                values
             1480  LOAD_CONST               0
             1482  BINARY_SUBSCR    
             1484  STORE_FAST               'aestdat'

 L. 153      1486  LOAD_FAST                'lb_record2'
             1488  LOAD_STR                 'LBDAT'
             1490  BINARY_SUBSCR    
             1492  LOAD_ATTR                values
             1494  LOAD_CONST               0
             1496  BINARY_SUBSCR    
             1498  STORE_FAST               'labdate'

 L. 155      1500  LOAD_FAST                'lb_record'
             1502  LOAD_ATTR                iloc
             1504  LOAD_CONST               0
             1506  BINARY_SUBSCR    
             1508  STORE_FAST               'lb_record1'

 L. 157      1510  LOAD_FAST                'gap'
             1512  LOAD_CONST               -5
             1514  COMPARE_OP               <
         1516_1518  POP_JUMP_IF_FALSE  1536  'to 1536'
             1520  LOAD_FAST                'gap'
             1522  LOAD_CONST               -30
             1524  COMPARE_OP               >
         1526_1528  POP_JUMP_IF_FALSE  1536  'to 1536'

 L. 159      1530  LOAD_STR                 'But the AE start date is more than 5 days prior to lab sample collection date'
             1532  STORE_FAST               'option_text'
             1534  JUMP_FORWARD       1580  'to 1580'
           1536_0  COME_FROM          1516  '1516'

 L. 160      1536  LOAD_FAST                'ae_record'
             1538  LOAD_STR                 'AESTDAT'
             1540  BINARY_SUBSCR    
             1542  LOAD_ATTR                values
             1544  LOAD_CONST               0
             1546  BINARY_SUBSCR    
             1548  LOAD_FAST                'lb_record'
             1550  LOAD_STR                 'LBDAT'
             1552  BINARY_SUBSCR    
             1554  LOAD_ATTR                values
             1556  LOAD_CONST               0
             1558  BINARY_SUBSCR    
             1560  COMPARE_OP               >
         1562_1564  POP_JUMP_IF_FALSE  1580  'to 1580'
             1566  LOAD_FAST                'gap'
             1568  LOAD_CONST               30
             1570  COMPARE_OP               <
         1572_1574  POP_JUMP_IF_FALSE  1580  'to 1580'

 L. 162      1576  LOAD_STR                 'But the AE start date is after lab date'
             1578  STORE_FAST               'option_text'
           1580_0  COME_FROM          1572  '1572'
           1580_1  COME_FROM          1562  '1562'
           1580_2  COME_FROM          1534  '1534'

 L. 164      1580  LOAD_FAST                'aespid'

 L. 165      1582  LOAD_FAST                'aeterm'

 L. 166      1584  LOAD_GLOBAL              str
             1586  LOAD_FAST                'aestdat'
             1588  CALL_FUNCTION_1       1  '1 positional argument'
             1590  LOAD_ATTR                split
             1592  LOAD_STR                 'T'
             1594  CALL_FUNCTION_1       1  '1 positional argument'
             1596  LOAD_CONST               0
             1598  BINARY_SUBSCR    

 L. 167      1600  LOAD_FAST                'test'
             1602  LOAD_ATTR                split
             1604  LOAD_STR                 '_'
             1606  CALL_FUNCTION_1       1  '1 positional argument'
             1608  LOAD_CONST               0
             1610  BINARY_SUBSCR    

 L. 168      1612  LOAD_GLOBAL              str
             1614  LOAD_FAST                'labdate'
             1616  CALL_FUNCTION_1       1  '1 positional argument'
             1618  LOAD_ATTR                split
             1620  LOAD_STR                 'T'
             1622  CALL_FUNCTION_1       1  '1 positional argument'
             1624  LOAD_CONST               0
             1626  BINARY_SUBSCR    

 L. 169      1628  LOAD_FAST                'grade'

 L. 170      1630  LOAD_FAST                'option_text'
             1632  LOAD_CONST               ('AESPID', 'AETERM', 'AESTDAT', 'LBTEST', 'LBDAT', 'grade', 'option')
             1634  BUILD_CONST_KEY_MAP_7     7 
             1636  STORE_FAST               'keys'

 L. 174      1638  LOAD_FAST                'sub_cat'

 L. 175      1640  LOAD_FAST                'self'
             1642  LOAD_ATTR                get_model_query_text_json
             1644  LOAD_FAST                'study'
             1646  LOAD_FAST                'sub_cat'
             1648  LOAD_FAST                'keys'
             1650  LOAD_CONST               ('params',)
             1652  CALL_FUNCTION_KW_3     3  '3 total positional and keyword args'

 L. 176      1654  LOAD_GLOBAL              str
             1656  LOAD_FAST                'lb_record1'
             1658  LOAD_STR                 'form_index'
             1660  BINARY_SUBSCR    
             1662  CALL_FUNCTION_1       1  '1 positional argument'

 L. 177      1664  LOAD_FAST                'self'
             1666  LOAD_ATTR                get_subcategory_json
             1668  LOAD_FAST                'study'
             1670  LOAD_FAST                'sub_cat'
             1672  CALL_FUNCTION_2       2  '2 positional arguments'

 L. 178      1674  LOAD_GLOBAL              str
             1676  LOAD_FAST                'lb_record1'
             1678  LOAD_STR                 'modif_dts'
             1680  BINARY_SUBSCR    
             1682  CALL_FUNCTION_1       1  '1 positional argument'

 L. 179      1684  LOAD_GLOBAL              int
             1686  LOAD_FAST                'lb_record1'
             1688  LOAD_STR                 'ck_event_id'
             1690  BINARY_SUBSCR    
             1692  CALL_FUNCTION_1       1  '1 positional argument'

 L. 180      1694  LOAD_GLOBAL              str
             1696  LOAD_FAST                'lb_record1'
             1698  LOAD_STR                 'formrefname'
             1700  BINARY_SUBSCR    
             1702  CALL_FUNCTION_1       1  '1 positional argument'

 L. 181      1704  LOAD_FAST                'subcate_report_dict'

 L. 182      1706  LOAD_GLOBAL              np
             1708  LOAD_ATTR                random
             1710  LOAD_ATTR                uniform
             1712  LOAD_CONST               0.7
             1714  LOAD_CONST               0.97
             1716  CALL_FUNCTION_2       2  '2 positional arguments'
             1718  LOAD_CONST               ('subcategory', 'query_text', 'form_index', 'question_present', 'modif_dts', 'stg_ck_event_id', 'formrefname', 'report', 'confid_score')
             1720  BUILD_CONST_KEY_MAP_9     9 
             1722  STORE_FAST               'payload'

 L. 185      1724  LOAD_FAST                'self'
             1726  LOAD_ATTR                insert_query
             1728  LOAD_FAST                'study'
             1730  LOAD_FAST                'subject'
             1732  LOAD_FAST                'payload'
             1734  CALL_FUNCTION_3       3  '3 positional arguments'
             1736  POP_TOP          
           1738_0  COME_FROM          1224  '1224'
           1738_1  COME_FROM          1184  '1184'
           1738_2  COME_FROM          1124  '1124'
           1738_3  COME_FROM          1114  '1114'
             1738  POP_BLOCK        
             1740  JUMP_FORWARD       1758  'to 1758'
           1742_0  COME_FROM_EXCEPT    778  '778'

 L. 187      1742  POP_TOP          
             1744  POP_TOP          
             1746  POP_TOP          

 L. 189  1748_1750  CONTINUE_LOOP       772  'to 772'
             1752  POP_EXCEPT       
             1754  JUMP_FORWARD       1758  'to 1758'
             1756  END_FINALLY      
           1758_0  COME_FROM          1754  '1754'
           1758_1  COME_FROM          1740  '1740'
         1758_1760  JUMP_BACK           772  'to 772'
             1762  POP_BLOCK        
           1764_0  COME_FROM_LOOP      754  '754'
             1764  POP_BLOCK        
             1766  JUMP_FORWARD       1784  'to 1784'
           1768_0  COME_FROM_EXCEPT    334  '334'

 L. 190      1768  POP_TOP          
             1770  POP_TOP          
             1772  POP_TOP          

 L. 192  1774_1776  CONTINUE_LOOP       328  'to 328'
             1778  POP_EXCEPT       
             1780  JUMP_FORWARD       1784  'to 1784'
             1782  END_FINALLY      
           1784_0  COME_FROM          1780  '1780'
           1784_1  COME_FROM          1766  '1766'
         1784_1786  JUMP_BACK           328  'to 328'
             1788  POP_BLOCK        
           1790_0  COME_FROM_LOOP      310  '310'
           1790_1  COME_FROM           290  '290'
             1790  POP_BLOCK        
             1792  JUMP_FORWARD       1810  'to 1810'
           1794_0  COME_FROM_EXCEPT    268  '268'

 L. 194      1794  POP_TOP          
             1796  POP_TOP          
             1798  POP_TOP          

 L. 196  1800_1802  CONTINUE_LOOP       262  'to 262'
             1804  POP_EXCEPT       
             1806  JUMP_FORWARD       1810  'to 1810'
             1808  END_FINALLY      
           1810_0  COME_FROM          1806  '1806'
           1810_1  COME_FROM          1792  '1792'
         1810_1812  JUMP_BACK           262  'to 262'
             1814  POP_BLOCK        
           1816_0  COME_FROM_LOOP      254  '254'
             1816  JUMP_BACK           248  'to 248'
             1818  POP_BLOCK        
           1820_0  COME_FROM_LOOP      240  '240'
             1820  POP_BLOCK        
             1822  JUMP_BACK            42  'to 42'
           1824_0  COME_FROM_EXCEPT     48  '48'

 L. 198      1824  POP_TOP          
             1826  POP_TOP          
             1828  POP_TOP          

 L. 200      1830  CONTINUE_LOOP        42  'to 42'
             1832  POP_EXCEPT       
             1834  JUMP_BACK            42  'to 42'
             1836  END_FINALLY      
             1838  JUMP_BACK            42  'to 42'
             1840  POP_BLOCK        
           1842_0  COME_FROM_LOOP       28  '28'

Parse error at or near `POP_JUMP_IF_TRUE' instruction at offset 1144_1146


if __name__ == '__main__':
    study_id = sys.argv[1]
    rule_id = sys.argv[2]
    version = sys.argv[3]
    rule = LBAE1(study_id, rule_id, version)
    rule.run()