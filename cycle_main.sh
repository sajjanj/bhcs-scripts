#!/usr/bin/ksh
# Script Name : cycle_main.sh
# Author      : Sajjan Janardhanan
# Created On  : 06/01/2012
# Description : Make entries in the new ETL control table T_ETL_PROCESS_AUDIT under the ETLSVC schema in the EDW database
#               Parameter sequence = 1.ProcessType (INFA,ORCL,UNIX,OTH)
#                                    2.ProcessName (Workflow Name, SP Name, Script Name)
#                                    3.SubjectArea (Folder, Schema, Project name etc.)
#                                    4.Action (BEGIN,SUCCESS,FAILURE)
#
# Revision History
# ~~~~~~~~~~~~~~~~
# Rev-Dt        Rev-By                Rev-Desc
# 06/01/2012    Sajjan Janardhanan    created new
# 08/08/2012    Sajjan Janardhanan    changes related to the new structure of the ETL control table
# 04/04/2013    Sajjan Janardhanan    Moved the logic to load the run-stats information from MX views to cycle_audit_report.sh

. /dw001/app/edwetl/scripts/common/etl_func_env.sh
function_identify_env
function_get_db_pswd etlsvc $EDWDB

# setting the audit and stats table variables
audit_schema=ETLSVC
audit_table=T_EDW_PROCESS_AUDIT

# local function to update the EDW audit table
function_upd_audit()
{
status_update_value=$1
if [ $status_update_value -eq $status_failure_cde ]; then
sqlplus -s / <<+SQLBLOCK_IUA+
etlsvc/${ORA_PSWD}@${ORA_DB}
WHENEVER SQLERROR EXIT 9
update ${audit_schema}.${audit_table} oq set 
status_cde=${status_update_value}
where 1=1
and subject_area_nme = '${subject_area_nme}' 
and process_nme = '${process_nme}' 
and process_type = '${process_type_cde}' 
and current_ind = 1;
commit;
exit;
+SQLBLOCK_IUA+
else
sqlplus -s / <<+SQLBLOCK_IUA+
etlsvc/${ORA_PSWD}@${ORA_DB}
WHENEVER SQLERROR EXIT 9
update ${audit_schema}.${audit_table} oq set 
status_cde=${status_update_value} , complete_dte=sysdate
where 1=1
and subject_area_nme = '${subject_area_nme}' 
and process_nme = '${process_nme}' 
and process_type = '${process_type_cde}' 
and current_ind = 1;
commit;
exit;
+SQLBLOCK_IUA+
fi
}

# local function to make a new entry in the EDW audit table
function_new_audit()
{
sqlplus -s / <<+SQLBLOCK_FNA+
etlsvc/${ORA_PSWD}@${ORA_DB}
WHENEVER SQLERROR EXIT 9
update ${audit_schema}.${audit_table} oq set current_ind = 0 
where 1=1
and subject_area_nme='${subject_area_nme}' 
and process_nme='${process_nme}' 
and process_type = '${process_type_cde}' 
and complete_dte = (
select max(iq.COMPLETE_DTE) from ${audit_schema}.${audit_table} iq 
where 1=1
and iq.process_nme = oq.process_nme 
and iq.subject_area_nme = oq.subject_area_nme 
and iq.process_type = oq.process_type) ;

insert into ${audit_schema}.${audit_table} 
(PROCESS_NME, SUBJECT_AREA_NME, PROCESS_TYPE, START_DTE, STATUS_CDE, CURRENT_IND)
values 
('${process_nme}', '${subject_area_nme}', '${process_type_cde}', sysdate, ${status_running_cde}, 1);

commit;
exit;
+SQLBLOCK_FNA+
}

clear
# Local Variables
action_name_lst="BEGIN,FAILURE,SUCCESS"
action_begin_cde="BEGIN"
action_failure_cde="FAILURE"
action_success_cde="SUCCESS"
dir_bin=${ETL_SCRIPTS}/common
dir_log=${dir_bin}/log
now_dte=`date +%Y-%m-%d_%T`
process_type_lst="INFA,ORCL,UNIX,OTH"
script_nme=`basename $0` 
status_running_cde=6
status_success_cde=1
status_failure_cde=3
std_abort_msg_txt="Fatal error encountered. Pls contact EDW support.\n"
log_file_abs_path="${dir_log}/${script_nme}_${1}-${2}_${now_dte}.log"

{ # <== logging mechanism begin ==

	echo "Environment              = $ENV"
	echo "Run Timestamp            = $now_dte"
	echo "Run by User              = [`whoami`]"
	echo "Server information       = `uname -a`"
	echo "Script Location & Name   = $dir_bin/$script_nme"
	echo "Script Log File Name     = $log_file_abs_path \n"
	
	# Checking parameters
	total_parms_cnt=$#
	if [ $total_parms_cnt -eq 4 ]; then
		process_type_cde=$1
		process_nme=$2
		subject_area_nme=$3
		action_nme=$4
		echo "[`date +%Y-%m-%d_%T`] Parameter check =>"
		echo "                      Process Type = [${process_type_cde}]"
		echo "                      Process Name = [${process_nme}]"
		echo "                      Subject Area = [${subject_area_nme}]"
		echo "                      Action Name  = [${action_nme}]"
		process_type_chk=`echo $process_type_lst|grep -w $process_type_cde`
		if [[ $process_type_lst != $process_type_chk ]]; then
			echo "[`date +%Y-%m-%d_%T`] ERROR: Process Type [${process_type_cde}] is not supported. $std_abort_msg_txt"; exit 1
		fi
		action_name_chk=`echo $action_name_lst|grep -w $action_nme`
		if [[ $action_name_lst != $action_name_chk ]]; then
			echo "[`date +%Y-%m-%d_%T`] ERROR: Action Type [${action_nme}] is not supported. $std_abort_msg_txt"; exit 2
		fi
	else
		echo "[`date +%Y-%m-%d_%T`] ERROR: Insufficient parameters. $std_abort_msg_txt"; exit 3
	fi

	# Checking if the previous cycle for the process is still open
	open_cycle_cnt=`sqlplus -s  << SQLBLOCK0 
	etlsvc/${ORA_PSWD}@${ORA_DB}
	set feed off
	set head off
	set pages 0
	set pagesize 0
	set serverout on
	set tab off
	set ver off
	WHENEVER SQLERROR EXIT 9
	select COUNT(*) from ${audit_schema}.${audit_table} where 1=1 
	and PROCESS_NME = '${process_nme}' 
	and SUBJECT_AREA_NME = '${subject_area_nme}' 
	and PROCESS_TYPE = '${process_type_cde}'
	and COMPLETE_DTE is null  
	and CURRENT_IND = 1 ;
	EXIT;
	SQLBLOCK0`	

	open_cycle_cnt=`echo $open_cycle_cnt | sed -e "s/^ //"`
	echo "[`date +%Y-%m-%d_%T`] Open cycle count = ${open_cycle_cnt}"

	if [ $action_nme == $action_begin_cde ]; then
	
		if [ $open_cycle_cnt -eq 0 ]; then
			echo "[`date +%Y-%m-%d_%T`] New Cycle: BEGIN"
			function_new_audit
			if [ $? -eq 0 ]; then
				echo "[`date +%Y-%m-%d_%T`] New Cycle: END"
			else
				echo "[`date +%Y-%m-%d_%T`] ERROR: [function_new_audit] ended in failure! $std_abort_msg_txt"; exit 4
			fi
		else
			echo "[`date +%Y-%m-%d_%T`] ERROR: Open cycle found! $std_abort_msg_txt"; exit 5
		fi
	
	elif [ $action_nme == $action_success_cde ]; then
		
		if [ $open_cycle_cnt -eq 0 ]; then
			echo "[`date +%Y-%m-%d_%T`] ERROR: No open cycle! $std_abort_msg_txt"; exit 6
			echo "[`date +%Y-%m-%d_%T`] $std_abort_msg_txt"			
			exit 6
		else
			echo "[`date +%Y-%m-%d_%T`] Ending Cycle: BEGIN"	
			function_upd_audit ${status_success_cde}
			if [ $? -ne 0 ]; then
				echo "[`date +%Y-%m-%d_%T`] ERROR: [function_upd_audit] ended in failure! $std_abort_msg_txt"; exit 7
			fi		
		fi
	
	elif [ $action_nme == $action_failure_cde ]; then

		if [ $open_cycle_cnt -eq 0 ]; then
			echo "[`date +%Y-%m-%d_%T`] ERROR: No open cycle! $std_abort_msg_txt"; exit 9
		else
			echo "[`date +%Y-%m-%d_%T`] Failing Cycle: BEGIN"	
			function_upd_audit ${status_failure_cde}
			if [ $? -ne 0 ]; then
				echo "[`date +%Y-%m-%d_%T`] ERROR: [function_upd_audit] ended in failure! $std_abort_msg_txt"; exit 10
			fi		
		fi
	fi

	echo "[`date +%Y-%m-%d_%T`] The script execution has completed successfully."

} > $log_file_abs_path 2>&1 # <== logging mechanism end ==

exit 0


