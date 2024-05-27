try:
    from scripts.SubcatModels.model_libs.ctc_utils import *
except:
    from ctc_utils import *

p_out_ctc_name = None
p_out_ctc_grade = None
p_out_ctc_grade = "TBD"
p_seperator = None
p_seperator = "~"

"""p_lvalstd - LBORRES
p_stdunit - LNMTUNIT
p_lln - LNMTLOW
p_uln - LNMTHIGH
p_lvalue - (character test result)
p_nd_ind - LBSTAT ("not done" indicators)
(p_lvalstd, p_lln, p_uln, p_stdunit)"""

def f_get_v4_hgb(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) == "G/DL":
			if p_lvalstd < 8:
				p_out_ctc_name = "ANEMIA"
				p_out_ctc_grade = "3"
			elif 8 <= p_lvalstd and p_lvalstd < 10:
				p_out_ctc_name = "ANEMIA"
				p_out_ctc_grade = "2"
			elif 10 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "ANEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "ANEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "ANEMIA"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "ANEMIA"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "ANEMIA"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_plts(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) in ("10**3/MM**3", "10*3/MM*3") :
			if p_lvalstd < 25:
				p_out_ctc_name = "THROMBOCYTOPENIA"
				p_out_ctc_grade = "4"
			elif 25 <= p_lvalstd and p_lvalstd < 50:
				p_out_ctc_name = "THROMBOCYTOPENIA"
				p_out_ctc_grade = "3"
			elif 50 <= p_lvalstd and p_lvalstd < 75:
				p_out_ctc_name = "THROMBOCYTOPENIA"
				p_out_ctc_grade = "2"
			elif 75 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "THROMBOCYTOPENIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "THROMBOCYTOPENIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "THROMBOCYTOPENIA"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "THROMBOCYTOPENIA"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "THROMBOCYTOPENIA"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_wbc(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) in ("10**3/MM**3", "10*3/MM*3") :
			if p_lvalstd < 1:
				p_out_ctc_name = "LEUKOPENIA"
				p_out_ctc_grade = "4"
			elif 1 <= p_lvalstd and p_lvalstd < 2:
				p_out_ctc_name = "LEUKOPENIA"
				p_out_ctc_grade = "3"
			elif 2 <= p_lvalstd and p_lvalstd < 3:
				p_out_ctc_name = "LEUKOPENIA"
				p_out_ctc_grade = "2"
			elif 3 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "LEUKOPENIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "LEUKOPENIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "LEUKOPENIA"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "LEUKOPENIA"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "LEUKOPENIA"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_pt_quick(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) == "SEC":
			if defined_round(2.5 * p_uln, 4) < p_lvalstd and p_uln is  not  None :
				p_out_ctc_name = "INCREASED PT"
				p_out_ctc_grade = "3"
			elif defined_round(1.5 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(2.5 * p_uln, 4):
				p_out_ctc_name = "INCREASED PT"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= defined_round(1.5 * p_uln, 4):
				p_out_ctc_name = "INCREASED PT"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "INCREASED PT"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "INCREASED PT"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "INCREASED PT"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "INCREASED PT"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_bili_tot(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) == "MG/DL":
			if defined_round(10.0 * p_uln, 4) < p_lvalstd and p_uln is  not  None :
				p_out_ctc_name = "HYPERBILIRUBINEMIA"
				p_out_ctc_grade = "4"
			elif defined_round(3.0 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(10.0 * p_uln, 4):
				p_out_ctc_name = "HYPERBILIRUBINEMIA"
				p_out_ctc_grade = "3"
			elif defined_round(1.5 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(3.0 * p_uln, 4):
				p_out_ctc_name = "HYPERBILIRUBINEMIA"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= defined_round(1.5 * p_uln, 4):
				p_out_ctc_name = "HYPERBILIRUBINEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERBILIRUBINEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERBILIRUBINEMIA"
				p_out_ctc_grade = "0"


		if upper(p_stdunit) == "UMOL/L":
			if defined_round(10.0 * p_uln, 4) < p_lvalstd and p_uln is  not  None :
				p_out_ctc_name = "HYPERBILIRUBINEMIA"
				p_out_ctc_grade = "4"
			elif defined_round(3.0 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(10.0 * p_uln, 4):
				p_out_ctc_name = "HYPERBILIRUBINEMIA"
				p_out_ctc_grade = "3"
			elif defined_round(1.5 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(3.0 * p_uln, 4):
				p_out_ctc_name = "HYPERBILIRUBINEMIA"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= defined_round(1.5 * p_uln, 4):
				p_out_ctc_name = "HYPERBILIRUBINEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERBILIRUBINEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERBILIRUBINEMIA"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "HYPERBILIRUBINEMIA"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "HYPERBILIRUBINEMIA"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_alb(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) == "G/DL":
			if p_lvalstd < 2:
				p_out_ctc_name = "HYPOALBUMINEMIA"
				p_out_ctc_grade = "3"
			elif 2 <= p_lvalstd and p_lvalstd < 3:
				p_out_ctc_name = "HYPOALBUMINEMIA"
				p_out_ctc_grade = "2"
			elif 3 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "HYPOALBUMINEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPOALBUMINEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPOALBUMINEMIA"
				p_out_ctc_grade = "0"


		if upper(p_stdunit) == "G/L":
			if p_lvalstd < 20:
				p_out_ctc_name = "HYPOALBUMINEMIA"
				p_out_ctc_grade = "3"
			elif 20 <= p_lvalstd and p_lvalstd < 30:
				p_out_ctc_name = "HYPOALBUMINEMIA"
				p_out_ctc_grade = "2"
			elif 30 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "HYPOALBUMINEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPOALBUMINEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPOALBUMINEMIA"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "HYPOALBUMINEMIA"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "HYPOALBUMINEMIA"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_ast(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) in ("U/L", "IU/L") :
			if defined_round(20.0 * p_uln, 4) < p_lvalstd and p_uln is  not  None :
				p_out_ctc_name = "INCREASED AST"
				p_out_ctc_grade = "4"
			elif defined_round(5.0 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(20.0 * p_uln, 4):
				p_out_ctc_name = "INCREASED AST"
				p_out_ctc_grade = "3"
			elif defined_round(3.0 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(5.0 * p_uln, 4):
				p_out_ctc_name = "INCREASED AST"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= defined_round(3.0 * p_uln, 4):
				p_out_ctc_name = "INCREASED AST"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "INCREASED AST"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "INCREASED AST"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "INCREASED AST"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "INCREASED AST"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_ck(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) in ("U/L", "IU/L") :
			if defined_round(10.0 * p_uln, 4) < p_lvalstd and p_uln is  not  None :
				p_out_ctc_name = "INCREASED CK"
				p_out_ctc_grade = "4"
			elif defined_round(5.0 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(10.0 * p_uln, 4):
				p_out_ctc_name = "INCREASED CK"
				p_out_ctc_grade = "3"
			elif defined_round(2.5 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(5.0 * p_uln, 4):
				p_out_ctc_name = "INCREASED CK"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= defined_round(2.5 * p_uln, 4):
				p_out_ctc_name = "INCREASED CK"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "INCREASED CK"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "INCREASED CK"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "INCREASED CK"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "INCREASED CK"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_alt(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) in ("U/L", "IU/L") :
			if defined_round(20.0 * p_uln, 4) < p_lvalstd and p_uln is  not  None :
				p_out_ctc_name = "INCREASED ALT"
				p_out_ctc_grade = "4"
			elif defined_round(5.0 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(20.0 * p_uln, 4):
				p_out_ctc_name = "INCREASED ALT"
				p_out_ctc_grade = "3"
			elif defined_round(3.0 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(5.0 * p_uln, 4):
				p_out_ctc_name = "INCREASED ALT"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= defined_round(3.0 * p_uln, 4):
				p_out_ctc_name = "INCREASED ALT"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "INCREASED ALT"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "INCREASED ALT"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "INCREASED ALT"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "INCREASED ALT"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_ggt(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) in ("U/L", "IU/L") :
			if defined_round(20.0 * p_uln, 4) < p_lvalstd and p_uln is  not  None :
				p_out_ctc_name = "INCREASED GGT"
				p_out_ctc_grade = "4"
			elif defined_round(5.0 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(20.0 * p_uln, 4):
				p_out_ctc_name = "INCREASED GGT"
				p_out_ctc_grade = "3"
			elif defined_round(2.5 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(5.0 * p_uln, 4):
				p_out_ctc_name = "INCREASED GGT"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= defined_round(2.5 * p_uln, 4):
				p_out_ctc_name = "INCREASED GGT"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "INCREASED GGT"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "INCREASED GGT"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "INCREASED GGT"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "INCREASED GGT"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_ap(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) in ("U/L", "IU/L") :
			if defined_round(20.0 * p_uln, 4) < p_lvalstd and p_uln is  not  None :
				p_out_ctc_name = "INCREASED AP"
				p_out_ctc_grade = "4"
			elif defined_round(5.0 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(20.0 * p_uln, 4):
				p_out_ctc_name = "INCREASED AP"
				p_out_ctc_grade = "3"
			elif defined_round(2.5 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(5.0 * p_uln, 4):
				p_out_ctc_name = "INCREASED AP"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= defined_round(2.5 * p_uln, 4):
				p_out_ctc_name = "INCREASED AP"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "INCREASED AP"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "INCREASED AP"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "INCREASED AP"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "INCREASED AP"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_creat(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) == "MG/DL":
			if defined_round(6.0 * p_uln, 4) < p_lvalstd and p_uln is  not  None :
				p_out_ctc_name = "HYPERCREATINEMIA"
				p_out_ctc_grade = "4"
			elif defined_round(3.0 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(6.0 * p_uln, 4):
				p_out_ctc_name = "HYPERCREATINEMIA"
				p_out_ctc_grade = "3"
			elif defined_round(1.5 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(3.0 * p_uln, 4):
				p_out_ctc_name = "HYPERCREATINEMIA"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= defined_round(1.5 * p_uln, 4):
				p_out_ctc_name = "HYPERCREATINEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERCREATINEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERCREATINEMIA"
				p_out_ctc_grade = "0"


		if upper(p_stdunit) == "UMOL/L":
			if defined_round(6.0 * p_uln, 4) < p_lvalstd and p_uln is  not  None :
				p_out_ctc_name = "HYPERCREATINEMIA"
				p_out_ctc_grade = "4"
			elif defined_round(3.0 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(6.0 * p_uln, 4):
				p_out_ctc_name = "HYPERCREATINEMIA"
				p_out_ctc_grade = "3"
			elif defined_round(1.5 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(3.0 * p_uln, 4):
				p_out_ctc_name = "HYPERCREATINEMIA"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= defined_round(1.5 * p_uln, 4):
				p_out_ctc_name = "HYPERCREATINEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERCREATINEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERCREATINEMIA"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "HYPERCREATINEMIA"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "HYPERCREATINEMIA"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_creat_cl(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) == "ML/MIN/1.73M2":
			if p_lvalstd < 15:
				p_out_ctc_name = "CHRONIC KIDNEY DISEASE"
				p_out_ctc_grade = "4"
			elif 15 <= p_lvalstd and p_lvalstd < 30:
				p_out_ctc_name = "CHRONIC KIDNEY DISEASE"
				p_out_ctc_grade = "3"
			elif 30 <= p_lvalstd and p_lvalstd < 60:
				p_out_ctc_name = "CHRONIC KIDNEY DISEASE"
				p_out_ctc_grade = "2"
			elif 60 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "CHRONIC KIDNEY DISEASE"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "CHRONIC KIDNEY DISEASE"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "CHRONIC KIDNEY DISEASE"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "CHRONIC KIDNEY DISEASE"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "CHRONIC KIDNEY DISEASE"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_na(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) == "MEQ/L":
			if 160 < p_lvalstd:
				p_out_ctc_name = "HYPERNATREMIA"
				p_out_ctc_grade = "4"
			elif 155 < p_lvalstd and p_lvalstd <= 160:
				p_out_ctc_name = "HYPERNATREMIA"
				p_out_ctc_grade = "3"
			elif 150 < p_lvalstd and p_lvalstd <= 155:
				p_out_ctc_name = "HYPERNATREMIA"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= 150 and p_uln is  not  None :
				p_out_ctc_name = "HYPERNATREMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERNATREMIA; HYPONATREMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd < 120:
				p_out_ctc_name = "HYPONATREMIA"
				p_out_ctc_grade = "4"
			elif 120 <= p_lvalstd and p_lvalstd < 130:
				p_out_ctc_name = "HYPONATREMIA"
				p_out_ctc_grade = "3"
			elif 130 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "HYPONATREMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERNATREMIA; HYPONATREMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERNATREMIA; HYPONATREMIA"
				p_out_ctc_grade = "0"


		if upper(p_stdunit) == "MMOL/L":
			if 160 < p_lvalstd:
				p_out_ctc_name = "HYPERNATREMIA"
				p_out_ctc_grade = "4"
			elif 155 < p_lvalstd and p_lvalstd <= 160:
				p_out_ctc_name = "HYPERNATREMIA"
				p_out_ctc_grade = "3"
			elif 150 < p_lvalstd and p_lvalstd <= 155:
				p_out_ctc_name = "HYPERNATREMIA"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= 150 and p_uln is  not  None :
				p_out_ctc_name = "HYPERNATREMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERNATREMIA; HYPONATREMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd < 120:
				p_out_ctc_name = "HYPONATREMIA"
				p_out_ctc_grade = "4"
			elif 120 <= p_lvalstd and p_lvalstd < 130:
				p_out_ctc_name = "HYPONATREMIA"
				p_out_ctc_grade = "3"
			elif 130 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "HYPONATREMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERNATREMIA; HYPONATREMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERNATREMIA; HYPONATREMIA"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "HYPERNATREMIA; HYPONATREMIA"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "HYPERNATREMIA; HYPONATREMIA"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_k(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) == "MEQ/L":
			if 7.0 < p_lvalstd:
				p_out_ctc_name = "HYPERKALEMIA"
				p_out_ctc_grade = "4"
			elif 6.0 < p_lvalstd and p_lvalstd <= 7.0:
				p_out_ctc_name = "HYPERKALEMIA"
				p_out_ctc_grade = "3"
			elif 5.5 < p_lvalstd and p_lvalstd <= 6.0:
				p_out_ctc_name = "HYPERKALEMIA"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= 5.5 and p_uln is  not  None :
				p_out_ctc_name = "HYPERKALEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERKALEMIA; HYPOKALEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd < 2.5:
				p_out_ctc_name = "HYPOKALEMIA"
				p_out_ctc_grade = "4"
			elif 2.5 <= p_lvalstd and p_lvalstd < 3.0:
				p_out_ctc_name = "HYPOKALEMIA"
				p_out_ctc_grade = "3"
			elif 3.0 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "HYPOKALEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERKALEMIA; HYPOKALEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERKALEMIA; HYPOKALEMIA"
				p_out_ctc_grade = "0"


		if upper(p_stdunit) == "MMOL/L":
			if 7.0 < p_lvalstd:
				p_out_ctc_name = "HYPERKALEMIA"
				p_out_ctc_grade = "4"
			elif 6.0 < p_lvalstd and p_lvalstd <= 7.0:
				p_out_ctc_name = "HYPERKALEMIA"
				p_out_ctc_grade = "3"
			elif 5.5 < p_lvalstd and p_lvalstd <= 6.0:
				p_out_ctc_name = "HYPERKALEMIA"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= 5.5 and p_uln is  not  None :
				p_out_ctc_name = "HYPERKALEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERKALEMIA; HYPOKALEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd < 2.5:
				p_out_ctc_name = "HYPOKALEMIA"
				p_out_ctc_grade = "4"
			elif 2.5 <= p_lvalstd and p_lvalstd < 3.0:
				p_out_ctc_name = "HYPOKALEMIA"
				p_out_ctc_grade = "3"
			elif 3.0 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "HYPOKALEMIA"
				p_out_ctc_grade = "2"
			elif 3.0 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "HYPOKALEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERKALEMIA; HYPOKALEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERKALEMIA; HYPOKALEMIA"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "HYPERKALEMIA; HYPOKALEMIA"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "HYPERKALEMIA; HYPOKALEMIA"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_ca(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) == "MG/DL":
			if 13.5 < p_lvalstd:
				p_out_ctc_name = "HYPERCALCEMIA"
				p_out_ctc_grade = "4"
			elif 12.5 < p_lvalstd and p_lvalstd <= 13.5:
				p_out_ctc_name = "HYPERCALCEMIA"
				p_out_ctc_grade = "3"
			elif 11.5 < p_lvalstd and p_lvalstd <= 12.5:
				p_out_ctc_name = "HYPERCALCEMIA"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= 11.5 and p_uln is  not  None :
				p_out_ctc_name = "HYPERCALCEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERCALCEMIA; HYPOCALCEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd < 6:
				p_out_ctc_name = "HYPOCALCEMIA"
				p_out_ctc_grade = "4"
			elif 6 <= p_lvalstd and p_lvalstd < 7:
				p_out_ctc_name = "HYPOCALCEMIA"
				p_out_ctc_grade = "3"
			elif 7 <= p_lvalstd and p_lvalstd < 8:
				p_out_ctc_name = "HYPOCALCEMIA"
				p_out_ctc_grade = "2"
			elif 8 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "HYPOCALCEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERCALCEMIA; HYPOCALCEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERCALCEMIA; HYPOCALCEMIA"
				p_out_ctc_grade = "0"


		if upper(p_stdunit) == "MMOL/L":
			if 3.4 < p_lvalstd:
				p_out_ctc_name = "HYPERCALCEMIA"
				p_out_ctc_grade = "4"
			elif 3.1 < p_lvalstd and p_lvalstd <= 3.4:
				p_out_ctc_name = "HYPERCALCEMIA"
				p_out_ctc_grade = "3"
			elif 2.9 < p_lvalstd and p_lvalstd <= 3.1:
				p_out_ctc_name = "HYPERCALCEMIA"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= 2.9 and p_uln is  not  None :
				p_out_ctc_name = "HYPERCALCEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERCALCEMIA; HYPOCALCEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd < 1.5:
				p_out_ctc_name = "HYPOCALCEMIA"
				p_out_ctc_grade = "4"
			elif 1.50 <= p_lvalstd and p_lvalstd < 1.75:
				p_out_ctc_name = "HYPOCALCEMIA"
				p_out_ctc_grade = "3"
			elif 1.75 <= p_lvalstd and p_lvalstd < 2:
				p_out_ctc_name = "HYPOCALCEMIA"
				p_out_ctc_grade = "2"
			elif 2 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "HYPOCALCEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERCALCEMIA; HYPOCALCEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERCALCEMIA; HYPOCALCEMIA"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "HYPERCALCEMIA; HYPOCALCEMIA"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "HYPERCALCEMIA; HYPOCALCEMIA"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_phosphate(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) == "MG/DL":
			if p_lvalstd < 1.0:
				p_out_ctc_name = "HYPOPHOSPHATEMIA"
				p_out_ctc_grade = "4"
			elif 1.0 <= p_lvalstd and p_lvalstd < 2.0:
				p_out_ctc_name = "HYPOPHOSPHATEMIA"
				p_out_ctc_grade = "3"
			elif 2.0 <= p_lvalstd and p_lvalstd < 2.5:
				p_out_ctc_name = "HYPOPHOSPHATEMIA"
				p_out_ctc_grade = "2"
			elif 2.5 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "HYPOPHOSPHATEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPOPHOSPHATEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPOPHOSPHATEMIA"
				p_out_ctc_grade = "0"


		if upper(p_stdunit) == "MMOL/L":
			if p_lvalstd < 0.3:
				p_out_ctc_name = "HYPOPHOSPHATEMIA"
				p_out_ctc_grade = "4"
			elif 0.3 <= p_lvalstd and p_lvalstd < 0.6:
				p_out_ctc_name = "HYPOPHOSPHATEMIA"
				p_out_ctc_grade = "3"
			elif 0.6 <= p_lvalstd and p_lvalstd < 0.8:
				p_out_ctc_name = "HYPOPHOSPHATEMIA"
				p_out_ctc_grade = "2"
			elif 0.8 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "HYPOPHOSPHATEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPOPHOSPHATEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPOPHOSPHATEMIA"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "HYPOPHOSPHATEMIA"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "HYPOPHOSPHATEMIA"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_chol_ran(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) == "MG/DL":
			if 500 < p_lvalstd:
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "4"
			elif 400 < p_lvalstd and p_lvalstd <= 500:
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "3"
			elif 300 < p_lvalstd and p_lvalstd <= 400:
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= 300 and p_uln is  not  None :
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "0"


		if upper(p_stdunit) == "MMOL/L":
			if 12.92 < p_lvalstd:
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "4"
			elif 10.34 < p_lvalstd and p_lvalstd <= 12.92:
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "3"
			elif 7.75 < p_lvalstd and p_lvalstd <= 10.34:
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= 7.75 and p_uln is  not  None :
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_tg_ran(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) == "MG/DL":
			if 1000 < p_lvalstd:
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "4"
			elif 500 < p_lvalstd and p_lvalstd <= 1000:
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "3"
			elif 300 < p_lvalstd and p_lvalstd <= 500:
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "2"
			elif 150 < p_lvalstd and p_lvalstd <= 300:
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "0"


		if upper(p_stdunit) == "MMOL/L":
			if 11.4 < p_lvalstd:
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "4"
			elif 5.7 < p_lvalstd and p_lvalstd <= 11.4:
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "3"
			elif 3.42 < p_lvalstd and p_lvalstd <= 5.7:
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "2"
			elif 1.71 < p_lvalstd and p_lvalstd <= 3.42:
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_gluc_fast(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) == "MG/DL":
			if 500 < p_lvalstd:
				p_out_ctc_name = "HYPERGLYCEMIA"
				p_out_ctc_grade = "4"
			elif 250 < p_lvalstd and p_lvalstd <= 500:
				p_out_ctc_name = "HYPERGLYCEMIA"
				p_out_ctc_grade = "3"
			elif 160 < p_lvalstd and p_lvalstd <= 250:
				p_out_ctc_name = "HYPERGLYCEMIA"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= 160 and p_uln is  not  None :
				p_out_ctc_name = "HYPERGLYCEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERGLYCEMIA; HYPOGLYCEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd < 30:
				p_out_ctc_name = "HYPOGLYCEMIA"
				p_out_ctc_grade = "4"
			elif 30 <= p_lvalstd and p_lvalstd < 40:
				p_out_ctc_name = "HYPOGLYCEMIA"
				p_out_ctc_grade = "3"
			elif 40 <= p_lvalstd and p_lvalstd < 55:
				p_out_ctc_name = "HYPOGLYCEMIA"
				p_out_ctc_grade = "2"
			elif 55 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "HYPOGLYCEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERGLYCEMIA; HYPOGLYCEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERGLYCEMIA; HYPOGLYCEMIA"
				p_out_ctc_grade = "0"


		if upper(p_stdunit) == "MMOL/L":
			if 27.8 < p_lvalstd:
				p_out_ctc_name = "HYPERGLYCEMIA"
				p_out_ctc_grade = "4"
			elif 13.9 < p_lvalstd and p_lvalstd <= 27.8:
				p_out_ctc_name = "HYPERGLYCEMIA"
				p_out_ctc_grade = "3"
			elif 8.9 < p_lvalstd and p_lvalstd <= 13.9:
				p_out_ctc_name = "HYPERGLYCEMIA"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= 8.9 and p_uln is  not  None :
				p_out_ctc_name = "HYPERGLYCEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERGLYCEMIA; HYPOGLYCEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd < 1.7:
				p_out_ctc_name = "HYPOGLYCEMIA"
				p_out_ctc_grade = "4"
			elif 1.7 <= p_lvalstd and p_lvalstd < 2.2:
				p_out_ctc_name = "HYPOGLYCEMIA"
				p_out_ctc_grade = "3"
			elif 2.2 <= p_lvalstd and p_lvalstd < 3.0:
				p_out_ctc_name = "HYPOGLYCEMIA"
				p_out_ctc_grade = "2"
			elif 3.0 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "HYPOGLYCEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERGLYCEMIA; HYPOGLYCEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERGLYCEMIA; HYPOGLYCEMIA"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "HYPERGLYCEMIA; HYPOGLYCEMIA"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "HYPERGLYCEMIA; HYPOGLYCEMIA"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_gluc_ran(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) == "MG/DL":
			if 500 < p_lvalstd:
				p_out_ctc_name = "HYPERGLYCEMIA"
				p_out_ctc_grade = "4"
			elif 250 < p_lvalstd and p_lvalstd <= 500:
				p_out_ctc_name = "HYPERGLYCEMIA"
				p_out_ctc_grade = "3"
			elif 160 < p_lvalstd and p_lvalstd <= 250:
				p_out_ctc_name = "HYPERGLYCEMIA"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= 160 and p_uln is  not  None :
				p_out_ctc_name = "HYPERGLYCEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERGLYCEMIA; HYPOGLYCEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd < 30:
				p_out_ctc_name = "HYPOGLYCEMIA"
				p_out_ctc_grade = "4"
			elif 30 <= p_lvalstd and p_lvalstd < 40:
				p_out_ctc_name = "HYPOGLYCEMIA"
				p_out_ctc_grade = "3"
			elif 40 <= p_lvalstd and p_lvalstd < 55:
				p_out_ctc_name = "HYPOGLYCEMIA"
				p_out_ctc_grade = "2"
			elif 55 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "HYPOGLYCEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERGLYCEMIA; HYPOGLYCEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERGLYCEMIA; HYPOGLYCEMIA"
				p_out_ctc_grade = "0"


		if upper(p_stdunit) == "MMOL/L":
			if 27.8 < p_lvalstd:
				p_out_ctc_name = "HYPERGLYCEMIA"
				p_out_ctc_grade = "4"
			elif 13.9 < p_lvalstd and p_lvalstd <= 27.8:
				p_out_ctc_name = "HYPERGLYCEMIA"
				p_out_ctc_grade = "3"
			elif 8.9 < p_lvalstd and p_lvalstd <= 13.9:
				p_out_ctc_name = "HYPERGLYCEMIA"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= 8.9 and p_uln is  not  None :
				p_out_ctc_name = "HYPERGLYCEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERGLYCEMIA; HYPOGLYCEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd < 1.7:
				p_out_ctc_name = "HYPOGLYCEMIA"
				p_out_ctc_grade = "4"
			elif 1.7 <= p_lvalstd and p_lvalstd < 2.2:
				p_out_ctc_name = "HYPOGLYCEMIA"
				p_out_ctc_grade = "3"
			elif 2.2 <= p_lvalstd and p_lvalstd < 3.0:
				p_out_ctc_name = "HYPOGLYCEMIA"
				p_out_ctc_grade = "2"
			elif 3.0 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "HYPOGLYCEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERGLYCEMIA; HYPOGLYCEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERGLYCEMIA; HYPOGLYCEMIA"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "HYPERGLYCEMIA; HYPOGLYCEMIA"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "HYPERGLYCEMIA; HYPOGLYCEMIA"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_ptt(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) == "SEC":
			if defined_round(2.5 * p_uln, 4) < p_lvalstd and p_uln is  not  None :
				p_out_ctc_name = "INCREASED PTT"
				p_out_ctc_grade = "3"
			elif defined_round(1.5 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(2.5 * p_uln, 4):
				p_out_ctc_name = "INCREASED PTT"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= defined_round(1.5 * p_uln, 4):
				p_out_ctc_name = "INCREASED PTT"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "INCREASED PTT"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "INCREASED PTT"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "INCREASED PTT"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "INCREASED PTT"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_fbgn(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) == "MG/DL":
			if p_lvalstd < 50:
				p_out_ctc_name = "INCREASED FIBRINOGEN"
				p_out_ctc_grade = "4"
			elif p_lvalstd < defined_round(0.25 * p_lln, 4):
				p_out_ctc_name = "INCREASED FIBRINOGEN"
				p_out_ctc_grade = "4"
			elif defined_round(0.25 * p_lln, 4) <= p_lvalstd and p_lvalstd < defined_round(0.5 * p_lln, 4):
				p_out_ctc_name = "INCREASED FIBRINOGEN"
				p_out_ctc_grade = "3"
			elif defined_round(0.5 * p_lln, 4) <= p_lvalstd and p_lvalstd < defined_round(0.75 * p_lln, 4):
				p_out_ctc_name = "INCREASED FIBRINOGEN"
				p_out_ctc_grade = "2"
			elif defined_round(0.75 * p_lln, 4) <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "INCREASED FIBRINOGEN"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "INCREASED FIBRINOGEN"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "INCREASED FIBRINOGEN"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "INCREASED FIBRINOGEN"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "INCREASED FIBRINOGEN"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_mg(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) == "MG/DL":
			if 8 < p_lvalstd:
				p_out_ctc_name = "HYPERMAGNESEMIA"
				p_out_ctc_grade = "4"
			elif 3 < p_lvalstd and p_lvalstd <= 8:
				p_out_ctc_name = "HYPERMAGNESEMIA"
				p_out_ctc_grade = "3"
			elif p_uln < p_lvalstd and p_lvalstd <= 3 and p_uln is  not  None :
				p_out_ctc_name = "HYPERMAGNESEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERMAGNESEMIA; HYPOMAGNESEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd < 0.7:
				p_out_ctc_name = "HYPOMAGNESEMIA"
				p_out_ctc_grade = "4"
			elif 0.7 <= p_lvalstd and p_lvalstd < 0.9:
				p_out_ctc_name = "HYPOMAGNESEMIA"
				p_out_ctc_grade = "3"
			elif 0.9 <= p_lvalstd and p_lvalstd < 1.2:
				p_out_ctc_name = "HYPOMAGNESEMIA"
				p_out_ctc_grade = "2"
			elif 1.2 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "HYPOMAGNESEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERMAGNESEMIA; HYPOMAGNESEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERMAGNESEMIA; HYPOMAGNESEMIA"
				p_out_ctc_grade = "0"


		if upper(p_stdunit) == "MMOL/L":
			if 3.30 < p_lvalstd:
				p_out_ctc_name = "HYPERMAGNESEMIA"
				p_out_ctc_grade = "4"
			elif 1.23 < p_lvalstd and p_lvalstd <= 3.30:
				p_out_ctc_name = "HYPERMAGNESEMIA"
				p_out_ctc_grade = "3"
			elif p_uln < p_lvalstd and p_lvalstd <= 1.23 and p_uln is  not  None :
				p_out_ctc_name = "HYPERMAGNESEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERMAGNESEMIA; HYPOMAGNESEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd < 0.3:
				p_out_ctc_name = "HYPOMAGNESEMIA"
				p_out_ctc_grade = "4"
			elif 0.3 <= p_lvalstd and p_lvalstd < 0.4:
				p_out_ctc_name = "HYPOMAGNESEMIA"
				p_out_ctc_grade = "3"
			elif 0.4 <= p_lvalstd and p_lvalstd < 0.5:
				p_out_ctc_name = "HYPOMAGNESEMIA"
				p_out_ctc_grade = "2"
			elif 0.5 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "HYPOMAGNESEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERMAGNESEMIA; HYPOMAGNESEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERMAGNESEMIA; HYPOMAGNESEMIA"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "HYPERMAGNESEMIA; HYPOMAGNESEMIA"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "HYPERMAGNESEMIA; HYPOMAGNESEMIA"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_ca_ion(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) == "MMOL/L":
			if 1.8 < p_lvalstd:
				p_out_ctc_name = "HYPERCALCEMIA"
				p_out_ctc_grade = "4"
			elif 1.6 < p_lvalstd and p_lvalstd <= 1.8:
				p_out_ctc_name = "HYPERCALCEMIA"
				p_out_ctc_grade = "3"
			elif 1.5 < p_lvalstd and p_lvalstd <= 1.6:
				p_out_ctc_name = "HYPERCALCEMIA"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= 1.5 and p_uln is  not  None :
				p_out_ctc_name = "HYPERCALCEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERCALCEMIA; HYPOCALCEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd < 0.8:
				p_out_ctc_name = "HYPOCALCEMIA"
				p_out_ctc_grade = "4"
			elif 0.8 <= p_lvalstd and p_lvalstd < 0.9:
				p_out_ctc_name = "HYPOCALCEMIA"
				p_out_ctc_grade = "3"
			elif 0.9 <= p_lvalstd and p_lvalstd < 1.0:
				p_out_ctc_name = "HYPOCALCEMIA"
				p_out_ctc_grade = "2"
			elif 1.0 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "HYPOCALCEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERCALCEMIA; HYPOCALCEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERCALCEMIA; HYPOCALCEMIA"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "HYPERCALCEMIA; HYPOCALCEMIA"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "HYPERCALCEMIA; HYPOCALCEMIA"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_amylase_s(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) in ("U/L", "IU/L") :
			if defined_round(5.0 * p_uln, 4) < p_lvalstd and p_uln is  not  None :
				p_out_ctc_name = "INCREASED AMYLASE"
				p_out_ctc_grade = "4"
			elif defined_round(2.0 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(5.0 * p_uln, 4):
				p_out_ctc_name = "INCREASED AMYLASE"
				p_out_ctc_grade = "3"
			elif defined_round(1.5 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(2.0 * p_uln, 4):
				p_out_ctc_name = "INCREASED AMYLASE"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= defined_round(1.5 * p_uln, 4):
				p_out_ctc_name = "INCREASED AMYLASE"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "INCREASED AMYLASE"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "INCREASED AMYLASE"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "INCREASED AMYLASE"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "INCREASED AMYLASE"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_glom_filt_rate(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) == "ML/MIN/1.73M2":
			if p_lvalstd < 15:
				p_out_ctc_name = "CHRONIC KIDNEY DISEASE"
				p_out_ctc_grade = "4"
			elif 15 <= p_lvalstd and p_lvalstd < 30:
				p_out_ctc_name = "CHRONIC KIDNEY DISEASE"
				p_out_ctc_grade = "3"
			elif 30 <= p_lvalstd and p_lvalstd < 60:
				p_out_ctc_name = "CHRONIC KIDNEY DISEASE"
				p_out_ctc_grade = "2"
			elif 60 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "CHRONIC KIDNEY DISEASE"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "CHRONIC KIDNEY DISEASE"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "CHRONIC KIDNEY DISEASE"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "CHRONIC KIDNEY DISEASE"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "CHRONIC KIDNEY DISEASE"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_neut_a(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) in ("10**3/MM**3", "10*3/MM*3") :
			if p_lvalstd < 0.5:
				p_out_ctc_name = "NEUTROPENIA"
				p_out_ctc_grade = "4"
			elif 0.5 <= p_lvalstd and p_lvalstd < 1.0:
				p_out_ctc_name = "NEUTROPENIA"
				p_out_ctc_grade = "3"
			elif 1.0 <= p_lvalstd and p_lvalstd < 1.5:
				p_out_ctc_name = "NEUTROPENIA"
				p_out_ctc_grade = "2"
			elif 1.5 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "NEUTROPENIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "NEUTROPENIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "NEUTROPENIA"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "NEUTROPENIA"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "NEUTROPENIA"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_lymph_a(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) in ("10**3/MM**3", "10*3/MM*3") :
			if 20 < p_lvalstd:
				p_out_ctc_name = "LYMPHOCYTE COUNT INCREASED"
				p_out_ctc_grade = "3"
			elif 4 < p_lvalstd and p_lvalstd <= 20:
				p_out_ctc_name = "LYMPHOCYTE COUNT INCREASED"
				p_out_ctc_grade = "2"
			elif p_lvalstd < 0.2:
				p_out_ctc_name = "LYMPHOPENIA"
				p_out_ctc_grade = "4"
			elif 0.2 <= p_lvalstd and p_lvalstd < 0.5:
				p_out_ctc_name = "LYMPHOPENIA"
				p_out_ctc_grade = "3"
			elif 0.5 <= p_lvalstd and p_lvalstd < 0.8:
				p_out_ctc_name = "LYMPHOPENIA"
				p_out_ctc_grade = "2"
			elif 0.8 <= p_lvalstd and p_lvalstd < p_lln:
				p_out_ctc_name = "LYMPHOPENIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "LYMPHOCYTE COUNT INCREASED; LYMPHOPENIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "LYMPHOCYTE COUNT INCREASED; LYMPHOPENIA"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "LYMPHOCYTE COUNT INCREASED; LYMPHOPENIA"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "LYMPHOCYTE COUNT INCREASED; LYMPHOPENIA"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_pt_inr(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) == "RATIO":
			if defined_round(2.5 * p_uln, 4) < p_lvalstd and p_uln is  not  None :
				p_out_ctc_name = "INCREASED INR"
				p_out_ctc_grade = "3"
			elif defined_round(1.5 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(2.5 * p_uln, 4):
				p_out_ctc_name = "INCREASED INR"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= defined_round(1.5 * p_uln, 4):
				p_out_ctc_name = "INCREASED INR"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "INCREASED INR"
				p_out_ctc_grade = "0"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "INCREASED INR"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "INCREASED INR"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "INCREASED INR"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "INCREASED INR"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_lipase_s(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) in ("U/L", "IU/L") :
			if defined_round(5.0 * p_uln, 4) < p_lvalstd and p_uln is  not  None :
				p_out_ctc_name = "INCREASED LIPASE"
				p_out_ctc_grade = "4"
			elif defined_round(2.0 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(5.0 * p_uln, 4):
				p_out_ctc_name = "INCREASED LIPASE"
				p_out_ctc_grade = "3"
			elif defined_round(1.5 * p_uln, 4) < p_lvalstd and p_lvalstd <= defined_round(2.0 * p_uln, 4):
				p_out_ctc_name = "INCREASED LIPASE"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= defined_round(1.5 * p_uln, 4):
				p_out_ctc_name = "INCREASED LIPASE"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "INCREASED LIPASE"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "INCREASED LIPASE"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "INCREASED LIPASE"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "INCREASED LIPASE"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_chol_fast(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) == "MG/DL":
			if 500 < p_lvalstd:
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "4"
			elif 400 < p_lvalstd and p_lvalstd <= 500:
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "3"
			elif 300 < p_lvalstd and p_lvalstd <= 400:
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= 300 and p_uln is  not  None :
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "0"


		if upper(p_stdunit) == "MMOL/L":
			if 12.92 < p_lvalstd:
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "4"
			elif 10.34 < p_lvalstd and p_lvalstd <= 12.92:
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "3"
			elif 7.75 < p_lvalstd and p_lvalstd <= 10.34:
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "2"
			elif p_uln < p_lvalstd and p_lvalstd <= 7.75 and p_uln is  not  None :
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "HYPERCHOLESTEROLEMIA"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


def f_get_v4_tg_fast(p_lvalstd, p_lln, p_uln, p_stdunit):
	p_out_ctc_name = None
	p_out_ctc_grade = None
	# BEGIN a BODY
	try:
		p_out_ctc_name = None 
		p_out_ctc_grade = "TBD"
		if upper(p_stdunit) == "MG/DL":
			if 1000 < p_lvalstd:
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "4"
			elif 500 < p_lvalstd and p_lvalstd <= 1000:
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "3"
			elif 300 < p_lvalstd and p_lvalstd <= 500:
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "2"
			elif 150 < p_lvalstd and p_lvalstd <= 300:
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "0"


		if upper(p_stdunit) == "MMOL/L":
			if 11.4 < p_lvalstd:
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "4"
			elif 5.7 < p_lvalstd and p_lvalstd <= 11.4:
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "3"
			elif 3.42 < p_lvalstd and p_lvalstd <= 5.7:
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "2"
			elif 1.71 < p_lvalstd and p_lvalstd <= 3.42:
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "1"
			elif p_lln <= p_lvalstd and p_lvalstd <= p_uln and p_lln is  not  None :
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "0"
			elif p_lvalstd is  not  None  and p_lln is  not  None  and p_uln is  not  None :
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "0"


		if p_out_ctc_grade == "TBD":
			if p_lvalstd is  None  and rtrim(p_lvalue) is  not  None  and instr("~" + str(p_nd_ind) + "~", "~" + str(ltrim(rtrim(p_lvalue))) + "~") > 0:
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "ND"
			elif p_lvalstd is  not  None  or p_lln is  not  None  or p_uln is  not  None  or p_stdunit is  not  None :
				p_out_ctc_name = "HYPERTRIGLYCERIDEMIA"
				p_out_ctc_grade = "TBD"


		return str(p_out_ctc_name) + "~" + str(p_out_ctc_grade)
	except Exception as e:
		print(e)


