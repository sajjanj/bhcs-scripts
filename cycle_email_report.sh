#!/usr/bin/ksh
# Script Name : cycle_email_report.sh
# Author      : Sajjan Janardhanan
# Created On  : 10/28/2012
# Description : Email the load statistics (aka Audit Report) for a particular process
#               A process is a container for multiple sub-processes, like a workflow & 
#               a subprocess can contain various tasks, synonomous to a session.
# Parameters  : #1-subject area, #2-process name, #3-number of delinquency days, #4-email_dl
#               It is recommended to make use of all 4 parameters to make use of the script effectively;
#               However the first 2 parameters are mandatory, which is the ProcessName & SubjectArea.
#               Pls refer to the 'Parameter value checks' section for more information
#
# Revision History
# ~~~~~~~~~~~~~~~~
# Rev-Dt        Rev-By                Rev-Desc
# 10/28/2012    Sajjan Janardhanan    created new
# 12/14/2012    Sajjan Janardhanan    Miscellaneous changes after testing
# 01/17/2013    Sajjan Janardhanan    Added logic to alert regarding delinquent files
# 01/20/2013    Sajjan Janardhanan    Added logic to alert during logical rejections

## Parameter value checks ##
total_parms=$#
if [ $total_parms -eq 4 ]; then
	subject_area_nme=$1
	process_nme=$2
	num_of_days=$3
	email_dl=$4
elif [ $total_parms -eq 3 ]; then
	subject_area_nme=$1
	process_nme=$2
	num_of_days=$3
	email_dl=EDWETLSupport@BaylorHealth.edu
elif [ $total_parms -eq 2 ]; then
	subject_area_nme=$1
	process_nme=$2
	num_of_days=0
	email_dl=EDWETLSupport@BaylorHealth.edu
else
	echo "\n[`date +%Y-%m-%d_%T`] ERROR: Insufficient parameters"; exit 1
fi

. /dw001/app/edwetl/scripts/common/etl_func_env.sh
function_identify_env
function_get_db_pswd etlsvc $EDWDB

# Local Variables
curr_script_nm=`basename $0|cut -d'.' -f1`
now=`date +%Y-%m-%d_%T`
today=`date +%Y-%m-%d` # <== exact format(yyyy-mm-dd)
dir_bin=${ETL_SCRIPTS}/common
dir_log=${dir_bin}/log
dir_tmp=${INFA_SHARED}/temp
email_subj="$ENV Load report for ${process_nme}"
file_log="${dir_log}/${curr_script_nm}_${process_nme}_${now}.log"
file_spool_delinq="${dir_tmp}/${curr_script_nm}_${process_nme}_delinquent.spl"
file_spool_loadrpt="${dir_tmp}/${curr_script_nm}_${process_nme}_LoadRpt.spl"
std_abort_msg="The process has encountered a fatal error and will now abort. Pls contact EDW support.\n"
indent="  "
noerrmsg="0 - No errors encountered."
clear

{ # <== logging mechanism begin ==

    echo "Server information       = `uname -a`"
	echo "Script Location & Name   = $dir_bin/$curr_script_nm"
	echo "Environment              = $ENV"
	echo "Run Timestamp            = $now"
	echo "Run by User              = [`whoami`]"
	echo "Input Parameter          = $process_nme"
	echo "Script Log File Name     = $file_log"
	echo "Data Spool File Name     = $file_spool_loadrpt"
	echo "Email Notification DL    = $email_dl"
	
	
	echo "\n[`date +%Y-%m-%d_%T`] ## CHECKING FOR DELINQUENT DATA (aka) SOURCE THAT HAD ZERO RECORDS FOR MORE THAN [$num_of_days] DAYS ## \n"
	flg_deliq_file=0
	if [ $num_of_days -eq 0 ] ; then
		echo "${indent} Data delinquency check skipped"
	else
		if [ $num_of_days -ne 0 ]; then
			`sqlplus -s  << SQLBLOCK0 > ${dir_log}/${curr_script_nm}_${process_nme}_sqlplus_delinq.log
				etlsvc/${ORA_PSWD}@${ORA_DB}
				set feed off head off pages 0 pagesize 0 tab off ver off serverout on size 1000000
				whenever sqlerror exit 9
				spool ${file_spool_delinq};
				select distinct '${indent}${indent}' || P.TABLE_NAME from 
					(select distinct PROCESS_NME, SUB_PROCESS_NME, TASK_NME, SUBJECT_AREA_NME, TABLE_NAME 
					 from T_EDW_PROCESS_LOAD_STATS where TABLE_NAME not like '%RJT') P 
				left outer join 
				(select * from 
					(select PROCESS_NME, SUB_PROCESS_NME, SUB_PROCESS_TYPE, TASK_NME, SUBJECT_AREA_NME, START_DTE, COMPLETE_DTE, 
					 STATUS_CDE, STATUS_DESC, TABLE_NAME, RECORDS_READ_CNT,
					 ROW_NUMBER() over (partition by PROCESS_NME, SUB_PROCESS_NME, TASK_NME, SUBJECT_AREA_NME 
										order by COMPLETE_DTE desc) as RN
					 from T_EDW_PROCESS_LOAD_STATS 
					 where STATUS_DESC = 'SUCCEEDED' 
					 and RECORDS_READ_CNT > 0  
					 and COMPLETE_DTE >= sysdate - ${num_of_days}) 
				 where RN=1 ) PH
				on PH.PROCESS_NME = P.PROCESS_NME
				and PH.SUBJECT_AREA_NME = P.SUBJECT_AREA_NME
				and PH.SUB_PROCESS_NME = P.SUB_PROCESS_NME
				and PH.TASK_NME = P.TASK_NME
				and PH.TABLE_NAME = P.TABLE_NAME
				where P.PROCESS_NME = '$process_nme'
				and COMPLETE_DTE is null ;  
				spool off ;
				exit;
				SQLBLOCK0`	

			rowcnt_delinq_file=`wc -l $file_spool_delinq`
			if [ $? -ne 0 ]; then
				echo "${indent} ERROR: Could not fetch row count"; exit 1
			fi
			
			rowcnt_delinq_file=`echo $rowcnt_delinq_file | cut -d"/" -f1 | sed -e "s/ //g" `
			if [ $? -ne 0 ]; then
				echo "${indent} ERROR: Could not trim row count value"; exit 1
			fi
			
			if [ $rowcnt_delinq_file -ne 0 ]; then
				
				cat $file_spool_delinq
				echo "\n${indent} !! ALERT !!  data for the following tables are delinquent as of this load cycle"
				flg_deliq_file=1
			else
				echo "${indent} None of the sub-processes or tasks were found to be delinquent in the last [$num_of_days] days"
			fi
		fi
	fi
	
	echo "\n[`date +%Y-%m-%d_%T`] ## CREATING AUDIT SPOOL FILE ## \n"
	`sqlplus -s <<SQLBLOCK  >> ${dir_log}/${curr_script_nm}_${process_nme}_sqlplus_LoadRpt.log
	etlsvc/${ORA_PSWD}@${ORA_DB} 
	WHENEVER SQLERROR EXIT -1
	set linesize 1300 trimspool on heading off echo off term off pagesize 0 feedback off timing off verify off
	set serveroutput on size 1000000
	spool ${file_spool_loadrpt};
	select 
		RECORD_NUM || '~' ||
		PROCESS_NME || '~' ||
		SUBJECT_AREA_NME || '~' ||
		SUB_PROCESS_NME || '~' ||
		TASK_NME || '~' ||
		TABLE_NAME || '~' ||
		to_char(START_DTE,'yyyy-mm-dd hh24:mi:ss') || '~' ||
		ELAPSED_TIME_SECS || '~' ||
		RECORDS_READ_CNT || '~' ||
		RECORDS_LOADED_CNT || '~' ||
		RECORDS_FAILED_CNT || '~' ||
		STATUS_DESC || '~' ||
		ERROR_MESSAGE_TXT
	from ETLSVC.V_EDW_PROCESS_LOAD_RPT  
	where SUBJECT_AREA_NME = '$subject_area_nme' and PROCESS_NME = '${process_nme}' 
	order by PROCESS_NME, SUB_PROCESS_NME, TASK_NME, TABLE_NAME, START_DTE ;
	spool off;
	exit;
	SQLBLOCK`
	if [ $? -eq 0 ]; then
		echo "${indent} The audit spool file [${file_spool_loadrpt}] was created successfully"
	else
		echo "${indent} ERROR: The audit spool file [${file_spool_loadrpt}] creation ended in failure"; exit 1
	fi
	
	
	echo "\n[`date +%Y-%m-%d_%T`] ## CHECKING FOR LOGICAL REJECTIONS ## \n"
	flg_rejections=0
	while read record; do
		table_name=`echo ${record}|cut -d "~" -f 6`
		start_dte=`echo ${record}|cut -d "~" -f 7`
		records_loaded_cnt=`echo ${record}|cut -d "~" -f 10|sed -e "s/ //"`
		table_type=`echo $table_name|awk '{ print substr($0,length($0)-3,4) }'   ` 
		if [  $table_type == '_RJT' ] && [ $records_loaded_cnt -ne 0 ]; then
			echo "$indent [$records_loaded_cnt] $table_name @ $start_dte "
			flg_rejections=`expr $flg_rejections + 1`
		fi
	done < $file_spool_loadrpt
	if [ $flg_rejections -eq 0 ]; then
		echo "${indent} There were no logical rejections in this load"
	else
		echo "\n${indent} !! ALERT !! There were logical rejections in [$flg_rejections] tables in this load cycle"
	fi
	
	
	echo "\n[`date +%Y-%m-%d_%T`] ## PREPARING THE AUDIT (aka) DAILY LOAD REPORT ## \n"
	flg_load_errors=0
	while read record; do
		table_name=`echo ${record}|cut -d "~" -f 6`
		start_dte=`echo ${record}|cut -d "~" -f 7`
		elapsed_time_secs=`echo ${record}|cut -d "~" -f 8|sed -e "s/ //"`
		records_loaded_cnt=`echo ${record}|cut -d "~" -f 10|sed -e "s/ //"`
		error_message_txt=`echo ${record}|cut -d "~" -f 13`
		table_type=`echo $table_name|awk '{ print substr($0,length($0)-3,4) }'   ` 
		if [ $table_type != '_RJT' ]; then
			if [ $records_loaded_cnt -gt 0 ]; then
				echo "$indent [$records_loaded_cnt] $table_name @ $start_dte in [$elapsed_time_secs] seconds"
			fi
			if [ "${error_message_txt}" != "${noerrmsg}" ]; then
				echo "$indent $indent => Errors in load. Please validate data."
				flg_load_errors=`expr $flg_load_errors + 1`
			fi
		fi
	done < $file_spool_loadrpt
	if [ $flg_load_errors -eq 0 ]; then
		echo "\n${indent} There were no errors in this load"
	else
		echo "\n${indent} !! ALERT !! There were errors with [$flg_load_errors] tasks in this load cycle"
	fi
	
	echo "\n[`date +%Y-%m-%d_%T`] ## END OF REPORT <sent from cycle_email_report.sh> ## \n"
	
} > $file_log 2>&1 # <== logging mechanism end ==

if [ $flg_deliq_file -eq 1 ] || [ $flg_rejections -gt 0 ]; then
	email_subj="!! ALERT !! "$email_subj
fi

cat $file_log | mailx -s "${email_subj}" ${email_dl}

exit 0
 