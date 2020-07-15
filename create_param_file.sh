#!/usr/bin/ksh
# Script Name : create_param_file.sh
# Author      : Sajjan Janardhanan
# Created On  : 08/10/2012
# Description : To create a parameter file for the Informatica workflow
#               Parameters to pass to the script:
#					#1 - INFA Folder Name (aka) Subject Area
#					#2 - INFA Workflow Name (aka) Sub-Process Name
#					#3 - Absolute path of the parameter file used by the INFA Workflow
#
# Revision History
# ~~~~~~~~~~~~~~~~
# Rev-Dt        Rev-By                Rev-Desc
# 08/10/2012    Sajjan Janardhanan    created new
# 08/12/2012    Sajjan Janardhanan    Added logging & comments
# 08/21/2012    Sajjan Janardhanan    Altered logic to capture errors within the SQL block
# 01/07/2013    Sajjan Janardhanan    Added additional error handling condition after the spooling block

. /dw001/app/edwetl/scripts/common/etl_func_env.sh
function_identify_env
function_get_db_pswd etlsvc $EDWDB

# Local Variables
bin_dir=${ETL_SCRIPTS}/common
log_file_dir=${bin_dir}/log
notify_list=sajjan.janardhanan@BaylorHealth.edu
now=`date +%Y-%m-%d_%T`
script_nm=`basename $0` 
std_abort_msg="The process has encountered a fatal error and will now abort. Pls contact EDW support.\n"
today=`date +%m-%d-%Y` # <== exact format(mm-dd-yyyy)
log_file_nm="${log_file_dir}/${script_nm}_${1}-${2}_${now}.log"

{ # <== logging mechanism begin ==

	# Environment identification
	server_name=`uname -a|awk '{ print $2 }'`
	case $server_name in
	  etldev ) env="[ETL DEV ]";;
	  etltst ) env="[ETL TEST]";;
	  etlprd ) env="[ETL PROD]";;
	  *      ) env="[Unknown ]";;
	esac

	echo "Environment              = $env"
	echo "Run Timestamp            = $now"
	echo "Run by User              = [`whoami`]"
	echo "Server information       = `uname -a`"
	echo "Script Location & Name   = $bin_dir/$script_nm"
	echo "Script Log File Name     = $log_file_nm "
	
	# Checking parameters
	total_parms=$#
	if [ $total_parms -eq 3 ]; then
		infa_folder_name=$1
		infa_wf_name=$2
		param_file_path=$3
		echo "[`date +%Y-%m-%d_%T`] Parameter check =>"
		echo "                      INFA Folder Name   = [${infa_folder_name}]"
		echo "                      INFA Workflow Name = [${infa_wf_name}]"
		echo "                      Parameter File     = [${param_file_path}]"
	else
		echo "[`date +%Y-%m-%d_%T`] ERROR: Insufficient parameters."
		echo "[`date +%Y-%m-%d_%T`] $std_abort_msg"		
		exit 2
	fi

	return_value=`sqlplus -s etlsvc/${ORA_PSWD}@${ORA_DB} <<SQL+
	WHENEVER SQLERROR EXIT -1
	set linesize 1300 trimspool on heading off echo off term off pagesize 0 feedback off timing off verify off
	set serveroutput on size 1000000
	spool ${param_file_path};
	SELECT PARAM_NM || PARAM_VAL AS PARAM_LINE FROM (
	SELECT PARAMETER_NME AS PARAM_NM, 
	case PARAMETER_VALUE when 'N/A' then null else '=' || parameter_value end as param_val
	FROM ETLSVC.T_EDW_PROCESS_PARAM
	WHERE SUB_PROCESS_TYPE='INFA'
	AND SUB_PROCESS_NME='${infa_wf_name}'
	AND SUBJECT_AREA_NME='${infa_folder_name}'
	order by parameter_seq);
	spool off;
	exit;
	SQL`
	
	spool_err=$?
	sp_err=`echo $return_value|grep -l "SP2-"|wc -c`
	ora_err=`echo $return_value|grep -l "ORA-"|wc -c`

	if [ $sp_err -gt 0 ] || [ $ora_err -gt 0 ] || [ $spool_err -ne 0 ]; then
		echo "\n[`date +%Y-%m-%d_%T`] ERROR: Spooling completed in failure. [${return_value}]. $std_abort_msg"; exit 1
	else	
		echo "\n[`date +%Y-%m-%d_%T`] The parameter file has been created successfully."
	fi

} > $log_file_nm 2>&1 # <== logging mechanism end ==

exit 0