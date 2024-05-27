keydict = {
'ND #' : 'PRAENO',
'ND Start Date' : 'PRSTDAT',
'PRONGO' : 'PRONGO',    
'PRONGO' : 'PRONGO',
'PRAER' : 'PRAER',
'AEONGO' : 'AEONGO',
'AEONGO1' : 'AEONGO_1',
'CMONGO' : 'CMONGO',
'CMONGO1' : 'CMONGO_1',
'ND Term' : 'PRTRT',
'ECADJ' : 'ECADJ',
'CM#' : 'CM_FORMIDX',
'CM #' : 'CM_FORMIDX',
'CM #1' : 'CM_FORMIDX_1' ,
'AE #' : 'AESPID' ,
'AE #1' : 'AESPID_1' ,
'AETOXGR' : 'AETOXGR',
'AE#' : 'AESPID',
'CMAER' : 'CMAER', 
'AEACN1' : 'AEACN_1',
'AENDGIV' : 'AENDGIV',    
'AE Term1' : 'AETERM_1',
'AE Start Date1'   : 'AESTDAT_1' ,
'AE End Date/Ongoing1' : ['AEENDAT_1', 'AEONGO_1'] ,    
'Adverse Event' : 'ECADJ',  
'CM Given?' : 'AECMGIV' ,
'AE Start Date'   : 'AESTDAT' ,
'AE End Date/Ongoing' : ['AEENDAT', 'AEONGO'] ,
'AE End Date' : 'AEENDAT',
'AE End Date1' : 'AEENDAT1',
'AE Term' : 'AETERM' , 
'CM Term' : 'CMTRT' ,
'CM Term1' : 'CMTRT_1' ,
'CM Start Date' : 'CMSTDAT' , 
'CM Start Date1' : 'CMSTDAT_1' , 
'CM End Date/Ongoing' : ['CMENDAT', 'CMONGO'] ,
'CM End Date/Ongoing1' : ['CMENDAT_1', 'CMONGO_1'] ,
'CM End Date' : 'CMENDAT',  
'CM End Date1' : 'CMENDAT_1',    
'CM Assoc. AE Identifier' : 'CMAENO' ,
'Reason for Changed/Missed Dose' : 'MISDOST' ,
'Reason Dose Adj from Planned' : 'ECADJ' ,
'Event Related?' : 'AEREL' ,
'Latest Action with Study Trt' : 'AEACN' ,
'Latest Action with Bkgd Trt' : 'AEBTDRG' ,
'Dose Start Date' : 'ECSTDAT' ,
'Dose End Date' : 'ECENDAT' ,
'Planned Dose' : 'ECPRGM, ECPDOSTX,ECPDOSE' ,
'Actual Dose' : 'ECDOSE' ,
'AE Identifier' : 'ECAENO' ,
'Latest Action with Study Trt' : 'AEACN' ,
'Did the adverse event cause the subject to be discontinued from the study?' : 'AEDIS' ,
'Was med given to treat AE' :  'CMAER' ,
'insert EC record' : 'ec_rec',
"AE #": 'AESPID',
"EC #": 'EC_ITEMSETIDX',
'EC #1': 'EC_ITEMSETIDX_1',
'AE Term': 'AETERM',
'AE action': 'AEACN',
'AE start date': 'AESTDAT',
'AE end date' : 'AEENDAT',
'EC start date' : 'ECSTDAT',
'EC end date' : 'ECENDAT',
'EC start date1' : 'ECSTDAT_1',
'EC end date1' : 'ECENDAT_1',
'If other specify text field': 'ECADJO',
'ECAENO': 'ECAENO',
}



text_dict = {
    
'AECM0': 'Concomitant Medication is given to treat AE [insert AE #], [insert AE Term]; however there are seemingly no corresponding medication(s) recorded in the CM log. Please review and update EDC as appropriate, else clarify.',

'AECM1' : 'CM [insert CM #] and [insert CM Term] is given to treat AE [insert AE #], [insert AE Term]; however CM started [insert CM Start Date] prior to AE [insert AE Start Date].  Please review and update EDC as appropriate, else clarify.',

'AECM2' : 'CM [insert CM #] and [insert CM Term] is given to treat AE [insert AE #], [insert AE Term]; however CM started [insert CM Start Date] after AE ended [insert AE End Date].  Please review and update EDC as appropriate, else clarify.',

'AECM3' : 'CM [insert CM #], [insert CM Term] is given to treat AE [insert AE #], [insert AE Term]; however CM duration [insert CM Start Date] - [insert CM End Date] and Ongoing [insert CMONGO] is inconsistent with this event [insert AE Start Date] - [insert AE End Date].  Please review and update EDC as appropriate, else clarify.',

'AECM4' : 'CM [insert CM #] and [insert CM Term] is given to treat AE [insert AE #], [insert AE Term]; however this medication is not a logical treatment for this event.  Please review and update EDC as appropriate, else clarify.',

'AECM5' : 'Was a Concomitant Medication given? = No; however there is a potentially corresponding medication reported [insert CM #], [insert CM Term], [insert CM Start Date] - [insert CM End Date] and Ongoing [insert CMONGO]. Please review and update EDC as appropriate, else clarify.',

'AECM6' : 'Was a Concomitant Medication given? = Yes; however no medications are recorded in the CM log. Please review and update EDC as appropriate, else clarify.',

'AECM7' : 'Was a Concomitant Medication given?=Yes; however corresponding CM [insert CM #], [insert CM Term] appears to be a Non-Drug Treatment. Please verify treatment is captured in the correct form and update AE Treatment response(s) as appropriate, else clarify.',

'AECM8': 'Was a Concomitant Medication given?=Yes; however potentially corresponding CM [insert CM #], [insert CM Term], Start Date [insert CM Start Date], End Date [insert CM End Date] does not indicate [insert AE #] in the AE Identifier field. Please review and update EDC as appropriate, else clarify.',
    
'AEOV1' : 'Duplicate AE [insert AE #], [insert AE Term] and AE [insert AE #1], [insert AE Term1] with same coding term [insert AE Term], toxicity grade [insert AETOXGR], Start Date [insert AE Start Date], End Date [insert AE End Date] please review',

'AEOV2' : 'AE [insert AE #], [insert AE Term], AE [insert AE #1], [insert AE Term1] with same coding term [insert AE Term] and Toxicity Grade [insert AETOXGR], have overlapping Start Date [insert AE Start Date] and End Date[insert AE End Date], please kindly consider to update toxicity grade or combine into 1 record for same toxicity grade AE.',

'AEOV3': "AE [insert AE #], [insert AE Term], AE [insert AE #1], [insert AE Term1] with same coding term [insert AEDECOD]  but different toxicity grade have overlapping Start Date[insert AE Start Date], End Date[insert AE End Date], AE to be sequential please review",

'CMAE0' : 'CM [insert CM #] and [insert CM Term] is given to treat AE, however there are seemingly no corresponding AE(s) recorded in the AE log. Please review and update EDC as appropriate, else clarify.', # Add [insert AE #] in CMAE0
    
'CMAE1' : 'CM [insert CM #] and [insert CM Term] is associated with AE [insert AE #], [insert AE Term]; however CM started [insert CM Start Date] prior to AE [insert AE Start Date].  Please review and update EDC as appropriate, else clarify.',

'CMAE2' : 'CM [insert CM #], [insert CM Term] is associated with AE [insert AE #], [insert AE Term]; however CM started [insert CM Start Date] after AE ended [insert AE End Date].  Please review and update EDC as appropriate, else clarify.',
    
'CMAE3' : 'CM [insert CM #], [insert CM Term] is associated with AE [insert AE #], [insert AE Term]; however this medication is not a logical treatment for this event.  Please review and update EDC as appropriate, else clarify.',
    
'CMAE4' : 'Medication [insert CM #], [insert CM Term], [insert CM Start Date] - [insert CM End Date] and Ongoing [insert CMONGO] potentially taken to treat AE [insert AE #], [insert AE Term]; however Was a Concomitant Medication given? = No on AE form. Please review and update EDC as appropriate, else clarify.',
    
'CMAE5': 'Medication is reported for treatment of [insert AE #] [insert AE Term]; however this is not a logical treatment for this type of event. Please review and update EDC as appropriate, else clarify.',
    
'CMAE6' : '"Was this Medication Given to Treat an Adverse Event?" = Yes;  however there is are no Adverse Events reported. Please review and update EDC as appropriate, else clarify.',

'CMAE8': 'Medication [insert CM #], [insert CM Term], [insert CM Start Date] - [insert CM End Date] and Ongoing [insert CMONGO] reported for treatment of this AE; however duration of AE Start Date [insert AE Start Date] and End Date [insert AE End Date] is inconsistent  with this medication . Please review and update EDC as appropriate, else clarify.',
    
'CMOV1' : 'There appears to be a duplicate CM [insert CM #], [insert CM Term] and CM [insert CM #1], [insert CM Term1] with same coding term [insert CM Term], Start Date [insert CM Start Date] and End Date [insert CM End Date]. Please review and update as needed or else clarify.',
    
'CMOV2' : 'CM [insert CM #], [insert CM Term] and CM [insert CM #1], [insert CM Term1] with same coding term [insert CM Term], have overlapping Start Date [insert CM Start Date] End Date [insert CM End Date], medications should be sequential please review',

'AEDR0': "AE [insert AE #], [insert AE Term] Action Taken is reported as [insert AE action]; however corresponding dosing record indicates Reason for Change is Adverse Event [insert AE #]. Please review and update EDC as appropriate, else clarify.",

'AEDR1': "AE [insert AE #], [insert AE Term] Action Taken is reported as Not Applicable; however dosing log indicates study drug is still being taken during this time. Please review and update EDC as appropriate, else clarify.",

'AEDR2' : "AE [insert AE #], [insert AE Term] Action Taken is reported other than Not Applicable; however dosing log indicates study drug is not taken during this time. Please review and update EDC as appropriate, else clarify.",

'AEDR3' : "AE [insert AE #],[insert AE Term] Action Taken is reported as [insert AE action]; however corresponding dosing record indicating Reason for Change Adverse Event [insert AE #] does not exist. Please review and update EDC as appropriate, else clarify.",

'AEDR10' : "AE [insert AE #], [insert AE Term] Relationship with study drug is reported as Related; however dosing log indicates study drug is not taken during this time. Please review and update EDC as appropriate, else clarify.",

'DRAE0' : "Dose record [insert EC #] indicates the reason for the adjusted dose is [insert If other specify text field] and corresponds to the AE Term [insert AE Term] from AE [insert AE #]. Review if it is appropriate to update the reason in dose record to ADVERSE EVENT.",

'DRAE1' : "Dose record [insert EC #] indicates a relationship to AE Identifier [insert ECAENO] but this AE Identifier does not exist in the Adverse Event log. Please review dosing data and Adverse Event data to update as appropriate.",

'DRAE2' : "Dose record [insert EC #] indicates a relationship to AE Identifier [insert AE #]. However, there is no Adverse Event reported in AE log. Please reconcile between dose records and AE log.",

'DRAE4': "Dose record [insert EC #] and AE Identifier [insert AE #] indicate that dosing increased due to Adverse Event. However, the dose record start date [insert EC start date] is before the AE start date [insert AE start date]. Please reconcile between dose records and AE log.",

'DRAE5' : "Dose record [insert EC #] and AE Identifier [insert AE #] indicate that dosing decreased due to Adverse Event. However, the dose record start date [insert EC start date] is before the AE start date [insert AE start date]. Please reconcile between dose records and AE log.",

'DRAE6' : "Dose record [insert EC #] and AE Identifier [insert AE #] indicate drug interruption due to Adverse Event. However, the dose record start date [insert EC start date] is before the AE start date [insert AE start date]. Please reconcile between dose records and AE log.",

'DRAE7' : "Dose record [insert EC #] indicates a dose adjustment INCREASE due to an AE when compared to dose record [insert EC #]. However, AE Identifier [insert AE #] does not indicate DOSE INCREASED. Please review dosing records and AE logs to update data appropriately.",

'DRAE8' : "Dose record [insert EC #] indicates a dose adjustment DECREASE due to an AE when compared to dose record [insert EC #]. However, AE Identifier [insert AE #] does not indicate DOSE REDUCED. Please review dosing records and AE logs to update data appropriately.",

'DRAE9': "Dose record [insert EC #] indicates a dose INTERRUPTION due to an AE when compared to dose record [insert EC #]. However, AE Identifier [insert AE #] does not indicate DRUG INTERRUPTED. Please review dosing records and AE logs to update data appropriately.",

'DROV1' : "Dose record [insert EC #] with a start date of [insert EC start date] and an end date of [insert EC end date] overlaps with dose record [insert EC #1] with a start date of [insert EC start date1] and an end date of [insert EC end date1]. Please review and reconcile dosing records.",

'DROV2' : "Dose record [insert EC #] looks to be a duplicate of dose record [insert EC #1] as the  start dates and end dates are the same for the same treatment. Please review and reconcile dosing records.",
    
}