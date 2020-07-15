#!/usr/bin/ksh
# Script Name : cycle_audit_report.sh
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
# 10/28/2012    Sajjan Janardhanan    created new cycle_email_report.sh
# 12/14/2012    Sajjan Janardhanan    Miscellaneous changes after testing
# 01/17/2013    Sajjan Janardhanan    Added logic to alert regarding delinquent files
# 01/20/2013    Sajjan Janardhanan    Added logic to alert during logical rejections
# 01/28/2013    Sajjan Janardhanan    Renamed script to cycle_audit_report.sh
# 01/30/2013    Sajjan Janardhanan    Changes to the audit report construction
# 02/21/2013    Sajjan Janardhanan    Added logic to include execution time information & Removed aggregation in audit spool SQL
# 04/04/2013    Sajjan Janardhanan    Added logic to fetch the run-stats information from MX views

## Parameter value checks ##
total_parms_cnt=$#
if [ $total_parms_cnt -eq 4 ]; then
	subject_area_nme=$1
	process_nme=$2
	num_of_days_cnt=$3
	email_dl=$4
elif [ $total_parms_cnt -eq 3 ]; then
	subject_area_nme=$1
	process_nme=$2
	num_of_days_cnt=$3
	email_dl=EDWETLSupport@BaylorHealth.edu
elif [ $total_parms_cnt -eq 2 ]; then
	subject_area_nme=$1
	process_nme=$2
	num_of_days_cnt=0
	email_dl=EDWETLSupport@BaylorHealth.edu
else
	echo "\n[`date +%Y-%m-%d_%T`] ERROR: Insufficient parameters"; exit 1
fi

. /dw001/app/edwetl/scripts/common/etl_func_env.sh
function_identify_env
function_get_db_pswd etlsvc $EDWDB

# Local Variables
script_nme=`basename $0|cut -d'.' -f1`
now_dte=`date +%Y-%m-%d_%T`
dir_bin=${ETL_SCRIPTS}/common
dir_log=${dir_bin}/log
dir_tmp=${INFA_SHARED}/temp
email_subj_txt="$ENV Load report for ${process_nme}"
log_file_abs_path="${dir_log}/${script_nme}_${process_nme}_${now_dte}.log"
delinq_spool_file_abs_path="${dir_tmp}/${script_nme}_${process_nme}_delinquent.spl"
loadrpt_spool_file_abs_path="${dir_tmp}/${script_nme}_${process_nme}_LoadRpt.spl"
std_abort_msg_txt="Fatal error encountered. Pls contact EDW support.\n"
indent="  "
noerrmsg_txt="0 - No errors encountered."

{ # <== logging mechanism begin ==

	execution_time=`sqlplus -s  << SQLBLOCK0 
		etlsvc/${ORA_PSWD}@${ORA_DB}
		set feed off head off pages 0 pagesize 0 tab off ver off serverout on size 1000000
		whenever sqlerror exit 9
		select '[ ' ||
		  floor(((COMPLETE_DTE-START_DTE)*24*60*60)/3600) || ' hours ' ||
		  floor((((COMPLETE_DTE-START_DTE)*24*60*60) - 
			floor(((COMPLETE_DTE-START_DTE)*24*60*60)/3600)*3600)/60) || ' minutes ' ||
		  round((((COMPLETE_DTE-START_DTE)*24*60*60) - 
			floor(((COMPLETE_DTE-START_DTE)*24*60*60)/3600)*3600 - (
			  floor((((COMPLETE_DTE-START_DTE)*24*60*60) - 
				floor(((COMPLETE_DTE-START_DTE)*24*60*60)/3600)*3600)/60)*60))) || ' secs ]' as exec_time
		from ETLSVC.T_EDW_PROCESS_AUDIT
		where PROCESS_NME = '$process_nme'
		and CURRENT_IND = 1 ;
		exit;
		SQLBLOCK0`
		
    echo "Server information         = `uname -a`"
	echo "Script Location & Name     = $dir_bin/$script_nme"
	echo "Environment                = $ENV"
	echo "Run Timestamp              = $now_dte"
	echo "Run by User                = [`whoami`]"
	echo "Script Log File Name       = $log_file_abs_path"
	echo "Delinquent data spool file = $delinq_spool_file_abs_path"
	echo "Audit data spool file      = $loadrpt_spool_file_abs_path"
	echo "ETL execution time         = $execution_time"
	echo "\nParameters:"
	echo "${indent}Process Name               = $process_nme"
	echo "${indent}Subject Area               = $subject_area_nme"
	echo "${indent}Data Delinquency Threshold = $num_of_days_cnt"
	echo "${indent}Email Notification DL      = $email_dl"
	
    
    echo "\n[`date +%Y-%m-%d_%T`] ## LOADING MX VIEW DATA ## "
    `sqlplus -s / <<SQL
    etlsvc/${ORA_PSWD}@${ORA_DB}			
    WHENEVER SQLERROR EXIT 9
    insert into ETLSVC.T_EDW_PROCESS_LOAD_STATS (
        PROCESS_NME, SUB_PROCESS_NME, SUB_PROCESS_TYPE, TASK_NME, SUBJECT_AREA_NME, 
        START_DTE, COMPLETE_DTE, STATUS_CDE, STATUS_DESC, TABLE_NAME, TABLE_INSTANCE_NAME, 
        RECORDS_READ_CNT, RECORDS_LOADED_CNT, RECORDS_FAILED_CNT, ELAPSED_TIME_SECS, THROUGHPUT, 
        ERROR_MESSAGE_TXT, ETL_INSERT_DTE, ETL_UPDATE_DTE, CREATED_USER, MODIFIED_USER )
    select 
        PROCESS_NME, SUB_PROCESS_NME, SUB_PROCESS_TYPE, TASK_NME, SUBJECT_AREA_NME, 
        START_DTE, COMPLETE_DTE, STATUS_CDE, STATUS_DESC, TABLE_NAME, TABLE_INSTANCE_NAME, 
        RECORDS_READ_CNT, RECORDS_LOADED_CNT, RECORDS_FAILED_CNT, ELAPSED_TIME_SECS, THROUGHPUT, 
        ERROR_MESSAGE_TXT, ETL_INSERT_DTE, ETL_UPDATE_DTE, CREATED_USER, MODIFIED_USER
    from ETLSVC.V_INFAMXVIEW_LOAD_STATS 
    where PROCESS_NME = '${process_nme}'
    and subject_area_nme = '${subject_area_nme}' ;
    commit ;
    exit ;
    SQL`
    
    
	echo "\n[`date +%Y-%m-%d_%T`] ## CHECKING FOR DELINQUENT DATA ## \n"
	flg_deliq_file=0
	if [ $num_of_days_cnt -eq 0 ] ; then
		echo "${indent} Data delinquency check skipped."
	else
		echo "${indent} Data Delinquency Threshold = $num_of_days_cnt days \n"
		if [ $num_of_days_cnt -ne 0 ]; then
			`sqlplus -s  << SQL > ${dir_log}/${script_nme}_${process_nme}_sqlplus_delinq.log
				etlsvc/${ORA_PSWD}@${ORA_DB}
				set feed off head off pages 0 pagesize 0 tab off ver off serverout on size 1000000
				whenever sqlerror exit 9
				spool ${delinq_spool_file_abs_path};
				select distinct '${indent}${indent}' || P.TABLE_NAME from 
					(select distinct PROCESS_NME, SUB_PROCESS_NME, TASK_NME, SUBJECT_AREA_NME, TABLE_NAME from T_EDW_PROCESS_LOAD_STATS 
					 where (TABLE_NAME like '%STG' or TABLE_NAME like '%HIST') and ACTIVE_IND = 'Y') P 
				left outer join 
				(select * from 
					(select PROCESS_NME, SUB_PROCESS_NME, SUB_PROCESS_TYPE, TASK_NME, SUBJECT_AREA_NME, START_DTE, COMPLETE_DTE, 
					 STATUS_CDE, STATUS_DESC, TABLE_NAME, RECORDS_READ_CNT,
					 ROW_NUMBER() over (partition by PROCESS_NME, SUB_PROCESS_NME, TASK_NME, SUBJECT_AREA_NME 
										order by COMPLETE_DTE desc) as RN
					 from T_EDW_PROCESS_LOAD_STATS 
					 where STATUS_DESC = 'SUCCEEDED' 
					 and RECORDS_READ_CNT > 0  
					 and COMPLETE_DTE >= sysdate - ${num_of_days_cnt}) 
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
				SQL`	

			if [ $? -ne 0 ]; then
				echo "${indent} ERROR: Creation of the data delinquency spool file [${delinq_spool_file_abs_path}] ended in failure."; exit 1
			fi
			
			rowcnt_delinq_file=`wc -l $delinq_spool_file_abs_path`
			if [ $? -ne 0 ]; then
				echo "${indent} ERROR: Could not fetch row count."; exit 1
			fi
			
			rowcnt_delinq_file=`echo $rowcnt_delinq_file | cut -d"/" -f1 | sed -e "s/ //g" `
			if [ $? -ne 0 ]; then
				echo "${indent} ERROR: Could not trim row count value."; exit 1
			fi
			
			if [ $rowcnt_delinq_file -ne 0 ]; then
				
				cat $delinq_spool_file_abs_path
				echo "\n${indent} !! ALERT !!  data for the following tables are delinquent as of this load cycle.\n"
				flg_deliq_file=1
			else
				echo "${indent} None of the sub-processes or tasks were found to be delinquent"
			fi
		fi
	fi

    
	echo "\n[`date +%Y-%m-%d_%T`] ## CREATING AUDIT SPOOL FILE ## \n"
	`sqlplus -s <<SQLBLOCK  >> ${dir_log}/${script_nme}_${process_nme}_sqlplus_LoadRpt.log
	etlsvc/${ORA_PSWD}@${ORA_DB} 
	WHENEVER SQLERROR EXIT -1
	set linesize 1300 trimspool on heading off echo off term off pagesize 0 feedback off timing off verify off
	set serveroutput on size 1000000
	spool ${loadrpt_spool_file_abs_path};
	select TN || '~' || SD || '~' || EMT || '~' || RRC || '~' || RLC || '~' || TIN from (
        select 
            TABLE_NAME as TN,   
            TABLE_INSTANCE_NAME as TIN,
            to_char(START_DTE,'yyyy-mm-dd hh24:mi:ss') as SD,
            RECORDS_READ_CNT as RRC,
            RECORDS_LOADED_CNT as RLC,
            ERROR_MESSAGE_TXT as EMT
        from ETLSVC.V_EDW_PROCESS_LOAD_RPT  
        where SUBJECT_AREA_NME = '$subject_area_nme' 
        and PROCESS_NME = '${process_nme}' ) 
	order by EMT desc, TN, TIN, SD ;
	spool off;
	exit;
	SQLBLOCK`
	if [ $? -eq 0 ]; then
		echo "${indent} Creation of the audit spool file [${loadrpt_spool_file_abs_path}] completed successfully.\n"
	else
		echo "${indent} ERROR: Creation of the audit spool file [${loadrpt_spool_file_abs_path}] ended in failure."; exit 1
	fi
	
	
	echo "\n[`date +%Y-%m-%d_%T`] ## CHECKING FOR LOGICAL REJECTIONS ## \n"
    flg_rejections=0
	while read record; do
		table_name=`echo ${record}|cut -d "~" -f 1`
        records_loaded_cnt=`echo ${record}|cut -d "~" -f 5|sed -e "s/ //"`
        table_instance_name=`echo ${record}|cut -d "~" -f 6`
		table_type=`echo $table_name|awk '{ print substr($0,length($0)-3,4) }'` 
		if [  $table_type == '_RJT' ] && [ $records_loaded_cnt -ne 0 ]; then
			echo "$indent $table_name [${records_loaded_cnt} records]"
			flg_rejections=`expr $flg_rejections + 1`
		fi
	done < $loadrpt_spool_file_abs_path
	if [ $flg_rejections -eq 0 ]; then
		echo "${indent} There were no logical rejections in this load.\n"
	else
		echo "\n${indent} !! ALERT !! There were logical rejections in [$flg_rejections] tables in this load cycle.\n"
	fi
	
	
	echo "\n[`date +%Y-%m-%d_%T`] ## PREPARING THE AUDIT (aka) DAILY LOAD REPORT ## \n"
	flg_load_errors=0
	while read record; do
		table_name=`echo ${record}|cut -d "~" -f 1`
        table_instance_name=`echo ${record}|cut -d "~" -f 6`
		start_dte=`echo ${record}|cut -d "~" -f 2`
		records_read_cnt=`echo ${record}|cut -d "~" -f 4|sed -e "s/ //"`
		records_loaded_cnt=`echo ${record}|cut -d "~" -f 5|sed -e "s/ //"`
		error_message_txt=`echo ${record}|cut -d "~" -f 3`
		table_type=`echo $table_name|awk '{ print substr($0,length($0)-3,4) }'   ` 
		if [ $table_type != "_RJT" ] && [ $table_type != "_WRK" ] || [ "${error_message_txt}" != "${noerrmsg_txt}" ]; then
			if [ "${error_message_txt}" != "${noerrmsg_txt}" ]; then
				txt_errmsg="## Errors in load - Please validate ##"
				flg_load_errors=`expr $flg_load_errors + 1`
			else
				txt_errmsg="none"
			fi

			echo "$indent Table name          = $table_name "
            echo "$indent Table instance name = $table_instance_name "
			echo "$indent Records fetched     = $records_read_cnt "
			echo "$indent Records loaded      = $records_loaded_cnt " 
			echo "$indent Load time           = [$start_dte] "
			echo "$indent Load errors         = $txt_errmsg \n"
		fi
	done < $loadrpt_spool_file_abs_path
	if [ $flg_load_errors -eq 0 ]; then
		echo "\n${indent} There were no errors in this load.\n"
	else
		echo "\n${indent} !! ALERT !! There were errors with [$flg_load_errors] tasks in this load cycle.\n"
	fi
	
    
	echo "\n[`date +%Y-%m-%d_%T`] ## END OF REPORT <sent from cycle_audit_report.sh> ## \n"
} > $log_file_abs_path 2>&1 # <== logging mechanism end ==

if [ $flg_deliq_file -eq 1 ] || [ $flg_rejections -gt 0 ]; then
	email_subj_txt="!! ALERT !! $email_subj_txt"
fi

cat $log_file_abs_path | mailx -s "${email_subj_txt}" ${email_dl}

exit 0
 